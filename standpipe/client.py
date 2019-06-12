import json
import socket
import logging

log = logging.getLogger(__name__)


try:
    import snappy
except ImportError:
    log.debug("could not import snappy")
    snappy = None


class StandpipeClient:
    def __init__(self, addr, port, compress=False):
        self.addr, self.port = addr, port
        if compress and not snappy:
            raise ValueError("snappy not available")
        self.compress = compress
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.setblocking(False)

    def encode(self, msg):
        rv = (json.dumps(msg, sort_keys=True) + "\n").encode()
        if self.compress:
            rv = snappy.compress(rv)
        return rv

    def send(self, msg: dict):
        raw = self.encode(msg)
        sent = self.socket.sendto(raw, (self.addr, self.port))
        return sent == len(raw)
