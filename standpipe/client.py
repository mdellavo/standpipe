import asyncio

from standpipe.config import MAX_QUEUE_SIZE
from standpipe.server import Stream


async def worker(host, port, loop, stream):
    while True:
        _, writer = await asyncio.open_connection(host=host, port=port, loop=loop)
        while True:
            item = await stream.get()
            if item is None:
                stream.task_done()
                return
            writer.write(item)
            stream.task_done()


class StreamClient(object):
    def __init__(self, host, port, max_queue_size=MAX_QUEUE_SIZE, loop=None):
        self.host = host
        self.port = port
        self.stream = Stream(maxsize=max_queue_size, loop=loop)
        self.loop = loop or asyncio.get_event_loop()
        asyncio.ensure_future(worker(host, port, loop, self.stream))

    def write(self, stream_name, record):
        if not self.stream.full():
            self.stream.put_nowait((stream_name + " " + record).encode("utf-8"))

    def close(self):
        self.loop.run_until_complete(self.stream.put(None))
        self.loop.run_until_complete(self.stream.join())
