import socket
import threading
import logging
from queue import Queue

from standpipe import config

log = logging.getLogger(__name__)


def drain(queue, sock):
    while True:
        item = queue.get()
        if item is None:
            queue.task_done()
            return
        stream_name, record = item
        sock.sendall(bytes(stream_name + " " + record, "utf-8"))
        queue.task_done()


def worker(host, port, queue):
    while True:
        with socket.create_connection((host, port)) as sock:
            # noinspection PyBroadException
            try:
                return drain(queue, sock)
            except Exception:
                log.exception("worker died, restarting")


class StreamClient(object):
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.queue = Queue(maxsize=config.CLIENT_QUEUE_SIZE)
        self.thread = threading.Thread(target=worker, args=(self.host, self.port, self.queue))
        self.thread.setDaemon(True)
        self.thread.start()

    def write(self, stream_name, record):
        self.queue.put((stream_name, record))

    def close(self):
        self.queue.put(None)
        self.queue.join()
