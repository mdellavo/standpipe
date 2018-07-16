import time
import socket
import select
import logging
from threading import Thread
from Queue import Queue, Empty

from standpipe.model import Record
from standpipe.util import grouper

import boto3

BACKLOG = 10
TIMEOUT = 1
RECV_SIZE = 2**16
MAX_CHUNK_SIZE = 500
CHECK_INTERVAL = 5
NUM_WORKERS = 4
DROP_MESSAGES = True
NUM_RETRIES = 5

log = logging.getLogger(__name__)

TERMINATOR = object()
READABLE = select.POLLIN | select.POLLPRI | select.POLLHUP | select.POLLERR


def is_readable(flag):
    return flag & (select.POLLIN | select.POLLPRI)


def is_hangup(flag):
    return flag & select.POLLHUP


def is_error(flag):
    return flag & select.POLLERR


class Client(object):
    def __init__(self, socket, address):
        self.socket = socket
        self.address = address
        self.buffer = ""


class Server(object):
    def __init__(self, address, port, router_queue, terminator="\n", backlog=BACKLOG):
        self.address = (address, port)
        self.backlog = backlog

        self.router_queue = router_queue

        self.running = False
        self.server = None

        self.poll = select.poll()
        self.sockets = {}
        self.clients = {}

        self.terminator = terminator

    def stop(self):
        if self.running:
            self.running = False
            self.stop_server()

    def create_server(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setblocking(0)
        self.server.bind(self.address)
        self.server.listen(self.backlog)
        self.poll.register(self.server, READABLE)
        self.sockets[self.server.fileno()] = self.server
        log.info("server started on %s", self.address)

    def stop_server(self):
        self.server.close()
        log.info("server stopped on %s", self.address)

    def drop_client(self, client_socket):
        self.unregister_socket(client_socket)
        del self.clients[client_socket.fileno()]
        client_socket.shutdown(socket.SHUT_RDWR)
        client_socket.close()

    def register_socket(self, sock):
        self.poll.register(sock, READABLE)
        self.sockets[sock.fileno()] = sock

    def unregister_socket(self, sock):
        self.poll.unregister(sock)
        del self.sockets[sock.fileno()]

    def add_client(self, sock, address):
        client = Client(sock, address)
        self.clients[sock.fileno()] = client
        return client

    def get_client(self, sock):
        return self.clients.get(sock.fileno())

    def wait_for_events(self):
        events = self.poll.poll(TIMEOUT)
        return [(self.sockets[fd], flags) for fd, flags in events]

    def on_client_connect(self):
        client_sock, client_address = self.server.accept()
        client_sock.setblocking(0)
        self.register_socket(client_sock)
        client = self.add_client(client_sock, client_address)
        log.debug("client %s connected", client.address)
        return client

    def on_client_readable(self, client_socket):
        data = client_socket.recv(RECV_SIZE)
        if data:
            client = self.get_client(client_socket)
            self.on_client_data(client, data)

    def on_client_data(self, client, data):
        client.buffer += data

        while self.terminator in client.buffer:
            item, client.buffer = client.buffer.split(self.terminator, 1)
            stream_name, record_bytes = item.split(" ", 1)
            record = Record(stream_name, record=record_bytes)
            self.submit_record(record)

    def submit_record(self, record):
        self.router_queue.put(record)

    def on_client_hangup(self, client_socket):
        log.debug("client hungup %s", client_socket.getpeername())
        self.drop_client(client_socket)

    def on_error(self, client_socket):
        log.debug("socket error %s", client_socket.getpeername())
        self.drop_client(client_socket)

    def run(self):
        self.create_server()
        self.running = True
        while self.running:
            for sock, flag in self.wait_for_events():
                if is_readable(flag):
                    if sock is self.server:
                        self.on_client_connect()
                    else:
                        self.on_client_readable(sock)
                elif is_hangup(flag):
                    self.on_client_hangup(sock)
                elif is_error(flag):
                    self.on_error(sock)


class Stream(object):
    def __init__(self, name, deadline=5):
        self.name = name
        self.deadline = deadline
        self.last = time.time()
        self.items = []
        self.count = 0
        self.total_bytes = 0

    def add(self, item):
        self.items.append(item)
        self.count += 1
        self.total_bytes += len(item.record)

    def flush(self):
        rv = self.items
        self.items = []
        self.last = time.time()
        return rv

    @property
    def is_flushable(self):
        return (
            (self.items and (time.time() > (self.last + self.deadline))) or
            (len(self.items) >= MAX_CHUNK_SIZE)
        )


def router(input_queue, upload_queue):

    streams = {}
    last = time.time()

    while True:
        try:
            record = input_queue.get(timeout=1)
        except Empty:
            record = None

        if record is TERMINATOR:
            break

        if record:
            # slot items
            stream = streams.get(record.stream_name)
            if not stream:
                stream = Stream(record.stream_name)
                streams[stream.name] = stream

            stream.add(record)
        else:
            stream = None

        if time.time() > (last + CHECK_INTERVAL) or (stream and stream.is_flushable):
            last = time.time()

            for stream in streams.values():
                if stream.is_flushable:
                    items = stream.flush()
                    for chunk in grouper(MAX_CHUNK_SIZE, items):
                        upload_queue.put(chunk)

    log.info("router shutdown")


def try_batch(batch):
    log.debug("putting %d records for stream %s", len(batch), batch[0].stream_name)

    if DROP_MESSAGES:
        time.sleep(.25)
        return

    client = boto3.client("firehose")

    for i in range(NUM_RETRIES):
        try:
            rv = client.put_records_batch(
                DeliveryStreamName=batch[0].stream_name,
                Records=[{"Data": record.record} for record in batch]
            )
            fail_count = rv["FailedPutCount"]
            if fail_count == 0:
                return
            elif fail_count > 0:
                errors = rv["RequestResponses"]
                batch = [record for record, error in zip(batch, errors) if error["ErrorCode"]]
        except client.exceptions.ClientError:
            log.exception("error putting batch")
            continue
        time.sleep(2**i)


def schlepper(upload_queue):
    while True:
        batch = upload_queue.get()
        if batch is TERMINATOR:
            break
        # noinspection PyBroadException
        try:
            try_batch(batch)
        except Exception:
            log.exception("error putting bactch")


def main():
    logging.basicConfig(level=logging.DEBUG)

    address = "0.0.0.0"
    port = 15555

    router_queue = Queue()
    upload_queue = Queue()
    server = Server(address, port, router_queue)

    router_thread = Thread(target=router, args=(router_queue, upload_queue))
    server_thread = Thread(target=server.run)

    threads = [router_thread, server_thread]

    for _ in range(NUM_WORKERS):
        worker_thread = Thread(target=schlepper, args=(upload_queue,))
        threads.append(worker_thread)

    for thread in threads:
        thread.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("shutting down...")
        server.stop()
        router_queue.put(TERMINATOR)
        for _ in range(NUM_WORKERS):
            upload_queue.put(TERMINATOR)

    for thread in threads:
        thread.join()
