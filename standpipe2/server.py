import socket
import select
import logging
from threading import Thread
from Queue import Queue

from standpipe.model import Record

BACKLOG = 10
TIMEOUT = 1
RECV_SIZE = 2**16

log = logging.getLogger(__name__)


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


def router(queue, streams):
    while True:
        record = queue.get()
        if record is None:
            break

        stream_queue = streams.get(record.stream_name)
        if not stream_queue:
            stream_queue = Queue()
            streams[record.stream_name] = stream_queue

        stream_queue.put(record)
        print(record)
    log.info("router shutdown")


def schlepper():
    pass


def server(address, port, router_queue):
    server = Server(address, port, router_queue)
    server.run()


def main():
    logging.basicConfig(level=logging.DEBUG)

    address = "0.0.0.0"
    port = 15555

    router_queue = Queue()
    streams = {}

    router_thread = Thread(target=router, args=(router_queue, streams))
    schlepper_thread = Thread(target=schlepper)
    server_thread = Thread(target=server, args=(address, port, router_queue))

    threads = [router_thread, schlepper_thread, server_thread]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()
