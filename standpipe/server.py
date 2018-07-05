import zlib
import struct
import asyncio
import logging
import itertools
from enum import Enum
from asyncio.queues import PriorityQueue, Queue, QueueFull

import bson
import aiofile
import aiobotocore

from standpipe.config import HOST, PORT, MAX_QUEUE_SIZE, FLUSH_INTERVAL, MONITOR_TIMEOUT

MAX_BATCH_SIZE = 500

log = logging.getLogger(__name__)


# NB in priority order
class MessageTypes(Enum):
    CHECKPOINT = 0
    RECORD = 1
    SHUTDOWN = 2


class Record(object):
    def __init__(self, stream_name, record):
        self.id = bson.ObjectId()
        self.stream_name = stream_name
        self.record = record

    def __eq__(self, other):
        return self.id == other.id

    def __ne__(self, other):
        return self.id != other.id

    def __lt__(self, other):
        return self.id < other.id

    def __le__(self, other):
        return self < other or self.id == other.id

    def __gt__(self, other):
        return self.id > other.id

    def __ge__(self, other):
        return self > other or self.id == other.id

    def __cmp__(self, other):
        return (other > self) - (other < self)


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
        self.server = None

    @property
    def running(self):
        return self.server is not None

    async def start(self):
        self.server = await asyncio.start_server(self.handle_client, self.host, self.port, loop=self.loop,
                                                 reuse_address=True, reuse_port=True)

    async def close(self):
        self.server.close()
        await self.server.wait_closed()
        self.server = None

    async def handle_client(self, reader, _):
        log.info("new client")

        while True:
            line = (await reader.readline()).decode("utf-8")
            if not line:
                break

            stream_name, record = line.split(" ", 1)

            try:
                await self.queue.put((MessageTypes.RECORD, Record(stream_name, record)))
            except QueueFull:
                log.warning("dropping message")


def wal_record_header(record):
    crc = zlib.crc32(bytes(record.record, "utf-8"))
    return struct.pack("i12sLi", MessageTypes.RECORD.value, bytes(record.id.binary), crc, len(record.record))


def wal_checkpoint_header(record):
    return struct.pack("i12s", MessageTypes.CHECKPOINT.value, bytes(record.id.binary))


async def wal_writer(path, loop, wal_log, stream_registry):

    async with aiofile.AIOFile(path, 'r+b', loop=loop) as f:
        writer = aiofile.Writer(f)
        count = 0
        while True:
            item = await wal_log.get()
            message_type, record = item
            if message_type == MessageTypes.SHUTDOWN:
                wal_log.task_done()
                break

            if message_type == MessageTypes.RECORD:
                await writer(wal_record_header(record))
                await writer(bytes(record.record, "utf8"))

                # now queue the message for upload
                stream_registry.get_stream(record.stream_name).put(record)

            elif message_type == MessageTypes.CHECKPOINT:
                await writer(wal_checkpoint_header(record))

            wal_log.task_done()

            count += 1

            if count % FLUSH_INTERVAL:
                await f.fsync()

    log.info("wal writer shutdown")


def grouper(n, events):
    it = iter(events)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


def try_chunk():



async def event_schlepper(loop, stream_registry):
    session = aiobotocore.get_session(loop=loop)

    async with session.create_client('firehose') as client:
        while True:
            asyncio.sleep(MONITOR_TIMEOUT)

            for stream_name in stream_registry:
                stream = stream_registry.get_stream(stream_name)
                if not stream.empty():
                    events = stream.drain()
                    for chunk in grouper(MAX_QUEUE_SIZE, events):
                        try_chunk()
                        client.put_records_batch()


def main():
    loop = asyncio.get_event_loop()

    wal_log = PriorityQueue(loop=loop)
    stream_registry = StreamRegistry(loop)

    server = Server(HOST, PORT, loop, wal_log)

    wal_coro = wal_writer("wal.log", loop, wal_log, stream_registry)
    asyncio.ensure_future(wal_coro)

    loop.run_until_complete(server.start())
    log.info("serving on %s:%s", HOST, PORT)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        log.info("shutting down...")
        loop.run_until_complete(server.close())
        loop.run_until_complete(wal_log.put((MessageTypes.SHUTDOWN, None)))

    loop.run_until_complete(wal_log.join())
    asyncio.wait(wal_coro, loop=loop)
