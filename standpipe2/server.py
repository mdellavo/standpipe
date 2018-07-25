import os
import abc
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
    def __init__(self, sock, address):
        self.socket = sock
        self.address = address
        self.buffer = ""


class Listener(object):
    __metaclass__ = abc.ABCMeta

    def __init__(self, family, address, backlog=BACKLOG):
        self.family = family
        self.address = address
        self.backlog = backlog
        self.socket = None

    def fileno(self):
        return self.socket.fileno() if self.socket else None

    def start(self):
        if self.socket is None:
            self.socket = socket.socket(self.family, socket.SOCK_STREAM)
            self.socket.setblocking(0)
            self.socket.bind(self.address)
            self.socket.listen(self.backlog)

    def stop(self):
        if self.socket is not None:
            self.socket.close()
            self.socket = None

    def accept(self):
        return self.socket.accept()


class TCPListener(Listener):
    def __init__(self, address, port, **kwargs):
        super(TCPListener, self).__init__(socket.AF_INET, (address, port), **kwargs)

    def start(self):
        super(TCPListener, self).start()
        log.info("started tcp listener on %s", self.address)


class UnixListener(Listener):
    def __init__(self, address, **kwargs):
        super(UnixListener, self).__init__(socket.AF_UNIX, address, **kwargs)

    def start(self):
        self.cleanup()
        super(UnixListener, self).start()
        log.info("started unix listener on %s", self.address)

    def stop(self):
        super(UnixListener, self).stop()
        self.cleanup()

    def cleanup(self):
        try:
            os.unlink(self.address)
        except OSError:
            if os.path.exists(self.address):
                raise


class Server(object):
    def __init__(self, router_queue, terminator="\n"):
        self.listeners = {}
        self.router_queue = router_queue
        self.running = False
        self.poll = select.poll()
        self.sockets = {}
        self.clients = {}
        self.terminator = terminator

    def stop(self):
        if self.running:
            self.running = False
        for listener in self.listeners.values():
            listener.stop()

    def attach_listener(self, listener):
        self.listeners[listener.socket] = listener
        self.register_socket(listener.socket)

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

    def on_client_connect(self, listener):
        client_sock, client_address = listener.accept()
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
        self.running = True
        while self.running:
            for sock, flag in self.wait_for_events():
                if is_readable(flag):
                    if sock in self.listeners:
                        listener = self.listeners[sock]
                        self.on_client_connect(listener)
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

    def add(self, item):
        self.items.append(item)

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

    total_records = total_bytes = 0

    while True:
        try:
            record = input_queue.get(timeout=1)
        except Empty:
            record = None

        if record is TERMINATOR:
            break

        flushable_streams = []

        if record:
            # slot items
            stream = streams.get(record.stream_name)
            if not stream:
                stream = Stream(record.stream_name)
                streams[stream.name] = stream

            stream.add(record)
            if stream.is_flushable:
                flushable_streams.append(stream)

        window_expired = time.time() > (last + CHECK_INTERVAL)

        if window_expired:
            flushable_streams.extend([stream for stream in streams.values() if stream.is_flushable])

        if flushable_streams:
            for stream in flushable_streams:
                items = stream.flush()

                num_records = len(items)
                batch_bytes = sum(len(record.record) for record in items)
                log.info("flushing %d records (%.2fKB) for stream %s", num_records, batch_bytes/1024., stream.name)

                total_bytes += batch_bytes
                total_records += num_records

                for chunk in grouper(MAX_CHUNK_SIZE, items):
                    upload_queue.put(chunk)

        if window_expired:
            now = time.time()
            delta = now - last

            if total_records:
                log.info("flushed %d records (%.2fKB) in %.2fs (%.2frps)",
                         total_records, total_bytes/1024., delta, total_records / delta)

            last = now
            total_records = total_bytes = 0

    log.info("router shutdown")


def try_batch(batch):
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

    ip_address, port = "0.0.0.0", 15555
    unix_address = "./socket"

    router_queue = Queue()
    upload_queue = Queue()
    server = Server(router_queue)

    tcp_listener = TCPListener(ip_address, port)
    unix_listener = UnixListener(unix_address)

    for listener in [tcp_listener, unix_listener]:
        listener.start()
        server.attach_listener(listener)

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
