import socket
import threading
import logging
from Queue import Queue
from contextlib import closing

log = logging.getLogger(__name__)

CLIENT_QUEUE_SIZE = 5000


def drain(queue, sock):
    while True:
        item = queue.get()
        if item is None:
            queue.task_done()
            return
        stream_name, record = item
        data = (stream_name + " ").encode("utf-8") + record
        sock.sendall(data)
        queue.task_done()


def worker(host, port, queue):
    while True:
        with closing(socket.create_connection((host, port))) as sock:
            # noinspection PyBroadException
            try:
                return drain(queue, sock)
            except Exception:
                log.exception("worker died, restarting")


class StreamClient(object):
    def __init__(self, host, port, queue_size=CLIENT_QUEUE_SIZE):
        self.host = host
        self.port = port
        self.queue = Queue(maxsize=queue_size)
        self.thread = threading.Thread(target=worker, args=(self.host, self.port, self.queue))
        self.thread.setDaemon(True)
        self.thread.start()

    def write(self, stream_name, record):
        self.queue.put((stream_name, record))

    def close(self):
        self.queue.put(None)
        self.queue.join()
