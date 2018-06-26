import zlib
import struct
import asyncio
import logging
from enum import Enum
from asyncio.queues import PriorityQueue, Queue, QueueFull

import bson
import aiofile

from standpipe.config import HOST, PORT, MAX_QUEUE_SIZE, FLUSH_INTERVAL


log = logging.getLogger(__name__)


class MessageTypes(Enum):
    CHECKPOINT = 0
    RECORD = 1


class Record(object):
    def __init__(self, stream_name, record):
        self.id = bson.ObjectId()
        self.stream_name = stream_name
        self.record = record


class Stream(Queue):
    def drain(self):
        rv = []
        while not self.empty():
            rv.append(self.get_nowait())
        return rv


class StreamRegistry(object):
    def __init__(self, loop):
        self.loop = loop
        self.streams = {}

    def get_stream(self, name):
        if name not in self.streams:
            self.streams[name] = Stream(maxsize=MAX_QUEUE_SIZE, loop=self.loop)
        return self.streams[name]

    def __iter__(self):
        return iter(self.streams)


class Server(object):
    def __init__(self, host, port, loop, queue):
        self.host = host
        self.port = port
        self.loop = loop
        self.queue = queue

    def start(self):
        coro = asyncio.start_server(self.handle_client, self.host, self.port, loop=self.loop)
        return coro

    async def handle_client(self, reader, _):
        while True:
            line = await reader.readline()
            if not line:
                break
            stream_name, record = line.lsplit(" ", 1)

            try:
                self.queue.put((MessageTypes.RECORD, Record(stream_name, record)))
            except QueueFull:
                log.warning("dropping message")


def wal_record_header(record):
    crc = zlib.crc32(record.record)
    return struct.pack("%c%12s%i%i", MessageTypes.RECORD, record.id.binary, crc, len(record.record))


def wal_checkpoint_header(record):
    return struct.pack("%c%12s", MessageTypes.CHECKPOINT, record.id.binary)


async def wal_writer(path, loop, queue):

    async with aiofile.AIOFile(path, 'a+', loop=loop) as f:
        writer = aiofile.Writer(f)
        count = 0
        while True:
            message_type, record = await queue.get()

            if message_type == MessageTypes.RECORD:
                await writer(wal_record_header(record))
                await writer(record.record)
            elif message_type == MessageTypes.CHECKPOINT:
                await writer(wal_checkpoint_header(record))

            queue.task_done()

            count += 1

            if count % FLUSH_INTERVAL:
                await f.fsync()


def main():
    loop = asyncio.get_event_loop()

    wal_log = PriorityQueue(loop=loop)
    asyncio.ensure_future(wal_writer("wal.log", loop, wal_log))

    # registry = StreamRegistry(loop)

    s = Server(HOST, PORT, loop, wal_log)
    server = loop.run_until_complete(s.start())

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())

    wal_log.join()

    loop.close()
