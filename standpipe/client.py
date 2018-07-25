import abc
import json
import time
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
        for attempt in range(5):
            log.info("connecting...")
            # noinspection PyBroadException
            try:
                with closing(socket.create_connection((host, port))) as sock:
                    return drain(queue, sock)
            except Exception:
                log.exception("worker died, restarting")
            log.info("sleeping before reconnect...")
            time.sleep(2**attempt)


class Encoder(object):
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def encode(self, event):
        pass


class JsonEncoder(Encoder):
    def encode(self, event):
        return json.dumps(event, sort_keys=True)


class StreamClient(object):
    def __init__(self, host, port, encoder=None, terminator=None, queue_size=CLIENT_QUEUE_SIZE):
        self.host = host
        self.port = port
        self.queue = Queue(maxsize=queue_size)
        self.encoder = encoder or JsonEncoder()
        self.terminator = terminator or u"\n"
        self.thread = threading.Thread(target=worker, args=(self.host, self.port, self.queue))
        self.thread.setDaemon(True)
        self.thread.start()

    def write(self, stream_name, event):
        record = self.encoder.encode(event) + self.terminator
        self.queue.put((stream_name, record))

    def close(self):
        self.queue.put(None)
        self.queue.join()
