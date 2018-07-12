import socket
import select
import logging
from cStringIO import StringIO

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
        self.buffer = StringIO()


class Server(object):
    def __init__(self, address, port, terminator="\n", backlog=BACKLOG):
        self.address = (address, port)
        self.backlog = backlog

        self.running = False
        self.server = None

        self.poll = select.poll()
        self.sockets = {}

        self.terminator = terminator

    def create_server(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setblocking(0)
        self.server.bind(self.address)
        self.server.listen(self.backlog)
        self.register_socket(self.server)
        log.info("server started on %s", self.address)

    def stop_server(self):
        self.server.close()
        log.info("server stopped on %s", self.address)

    def drop_client(self, client_socket):
        self.unregister_socket(client_socket)
        client_socket.shutdown(socket.SHUT_RDWR)
        client_socket.close()

    def register_socket(self, sock):
        self.poll.register(sock, READABLE)
        self.sockets[sock.fileno()] = sock

    def unregister_socket(self, sock):
        self.poll.unregister(sock)
        del self.sockets[sock.fileno()]

    def wait_for_events(self):
        events = self.poll.poll(TIMEOUT)
        return [(self.sockets[fd], flags) for fd, flags in events]

    def on_client_connect(self, client):
        log.debug("client %s connected", client.address)
        client.socket.setblocking(0)
        self.register_socket(client.socket)

    def on_client_readable(self, client_socket):
        data = client_socket.recv(RECV_SIZE)
        if data:
            pass
        else:
            log.debug("client %s closed", client_socket.getpeername())
            self.drop_client(client_socket)

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
                        client_sock, client_address = self.server.accept()
                        client = Client(client_sock, client_address)
                        self.on_client_connect(client)
                    else:
                        self.on_client_readable(sock)
                elif is_hangup(flag):
                    self.on_client_hangup(sock)
                elif is_error(flag):
                    self.on_error(sock)


def main():
    logging.basicConfig(level=logging.DEBUG)
    address = "0.0.0.0"
    port = 15555
    server = Server(address, port)
    server.run()
