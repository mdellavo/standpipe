import asyncio
from asyncio.queues import Queue

from standpipe.config import MAX_QUEUE_SIZE, HOST, PORT


async def writer(queue):
    while True:
        message = queue.get()


class StreamClient(object):
    def __init__(self, host, port, max_queue_size=MAX_QUEUE_SIZE):
        self.host = host
        self.port = port
        self.queue = Queue(maxsize=max_queue_size)

        asyncio.ensure_future(writer(self.queue))

    def write(self, record):
        if not self.queue.full():
            self.queue.put_nowait(record)

