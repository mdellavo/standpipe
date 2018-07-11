import os
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
from botocore.exceptions import ClientError

from standpipe import config

MAX_BATCH_SIZE = 500

log = logging.getLogger(__name__)


TERMINATOR = object()


class TaskPool(object):
    def __init__(self, loop, num_workers):
        self.loop = loop
        self.tasks = Queue(loop=self.loop)
        self.workers = []
        for _ in range(num_workers):
            worker = asyncio.ensure_future(self.worker(), loop=self.loop)
            self.workers.append(worker)

    async def worker(self):
        while True:
            future, task = await self.tasks.get()
            if task is TERMINATOR:
                break
            result = await asyncio.wait_for(task, None, loop=self.loop)
            future.set_result(result)

    def submit(self, task):
        future = asyncio.Future(loop=self.loop)
        self.tasks.put_nowait((future, task))
        return future

    async def join(self):
        for _ in self.workers:
            self.tasks.put_nowait((None, TERMINATOR))
        await asyncio.gather(*self.workers, loop=self.loop)


# NB in priority order
class MessageTypes(Enum):
    CHECKPOINT = 0
    RECORD = 1
    SHUTDOWN = 2


class Record(object):
    def __init__(self, stream_name, record=None, record_id=None):
        self.id = bson.ObjectId(record_id) or bson.ObjectId()
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
            self.streams[name] = Stream(maxsize=config.MAX_QUEUE_SIZE, loop=self.loop)
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
                await self.queue.put((MessageTypes.RECORD.value, Record(stream_name, record)))
            except QueueFull:
                log.warning("dropping message")


WAL_ENTRY_PREFIX = "i"
WAL_ENTRY_PREFIX_SIZE = struct.calcsize(WAL_ENTRY_PREFIX)

WAL_RECORD_IDENTIFIER = "64s12s"

WAL_RECORD_HEADER = WAL_RECORD_IDENTIFIER + "Li"
WAL_RECORD_HEADER_SIZE = struct.calcsize(WAL_RECORD_HEADER)
WAL_ENTRY_RECORD_HEADER = WAL_ENTRY_PREFIX + WAL_RECORD_HEADER

WAL_CHECKPOINT = WAL_RECORD_IDENTIFIER
WAL_CHECKPOINT_SIZE = struct.calcsize(WAL_CHECKPOINT)
WAL_ENTRY_CHECKPOINT = WAL_ENTRY_PREFIX + WAL_CHECKPOINT


def calc_crc(s):
    return zlib.crc32(bytes(s, "utf-8"))


def write_wal_record_header(record):
    crc = calc_crc(record.record)
    return struct.pack(WAL_ENTRY_RECORD_HEADER,
                       MessageTypes.RECORD.value,
                       bytes(record.stream_name, "utf-8"),
                       bytes(record.id.binary),
                       crc,
                       len(record.record))


def write_wal_checkpoint(record):
    return struct.pack(WAL_ENTRY_CHECKPOINT,
                       MessageTypes.CHECKPOINT.value,
                       bytes(record.stream_name, "utf-8"),
                       bytes(record.id.binary))


def read_wal_record(f):
    buf = f.read(WAL_RECORD_HEADER_SIZE)
    stream_name, _id, crc, length = struct.unpack(WAL_RECORD_HEADER, buf)
    record = f.read(length)
    record_crc = calc_crc(record)

    if record_crc != crc:
        print("crc mismatch {} != {}".format(record_crc, crc))

    return Record(stream_name, record=record, record_id=_id)


def read_wal_checkpoint(f):
    buf = f.read(WAL_CHECKPOINT_SIZE)
    if not buf:
        return
    stream_name, _id = struct.unpack(WAL_CHECKPOINT, buf)
    return Record(stream_name, record_id=_id)


def read_wal_entry(f):
    buf = f.read(WAL_ENTRY_PREFIX_SIZE)
    if not buf:
        return None, None
    record_type, = struct.unpack(WAL_ENTRY_PREFIX, buf)
    if record_type == MessageTypes.RECORD.value:
        record = read_wal_record(f)
        return MessageTypes.RECORD, record
    elif record_type == MessageTypes.CHECKPOINT.value:
        record = read_wal_checkpoint(f)
        return MessageTypes.CHECKPOINT, record
    else:
        raise ValueError("unsupported record type [{}]".format(buf))


def scan_wal(f):
    while True:
        record_type, record = read_wal_entry(f)
        if not record:
            break

        yield record_type, record


def replay_log(path):
    with open(path) as f:
        for record_type, record in scan_wal(f):
            print(record_type, record)


async def wal_writer(path, loop, wal_log, stream_registry):

    async with aiofile.AIOFile(path, 'w+b', loop=loop) as f:
        writer = aiofile.Writer(f)
        count = 0
        while True:
            item = await wal_log.get()
            message_type, record = item
            if message_type == MessageTypes.SHUTDOWN.value:
                wal_log.task_done()
                break

            if message_type == MessageTypes.RECORD.value:
                await writer(write_wal_record_header(record))
                await writer(bytes(record.record, "utf8"))

                # now queue the message for upload
                await stream_registry.get_stream(record.stream_name).put(record)

            elif message_type == MessageTypes.CHECKPOINT.value:
                log.info("checkpoint %s:%s", record.stream_name, record.id)
                await writer(write_wal_checkpoint(record))

            wal_log.task_done()

            count += 1

            if False and count % config.FLUSH_INTERVAL:
                await f.fsync()

    log.info("wal writer shutdown")


def grouper(n, events):
    it = iter(events)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


async def try_batch(loop, stream_name, records):
    log.info("schlepping %d events for stream %s", len(records), stream_name)

    if config.DROP_MESSAGES:
        return

    session = aiobotocore.get_session(loop=loop)

    async with session.create_client('firehose') as client:
        for i in range(config.NUM_RETRIES):

            try:
                rv = client.put_records_batch(
                    DeliveryStreamName=stream_name,
                    Records=[{"Data": record.record} for record in records]
                )
                fail_count = rv["FailedPutCount"]
                if fail_count == 0:
                    return
                elif fail_count > 0:
                    errors = rv["RequestResponses"]
                    records = [record for record, error in zip(records, errors) if error["ErrorCode"]]
            except ClientError:
                log.exception("error putting batch")
                continue
            asyncio.sleep(2**i)


async def event_schlepper(loop, stream_registry, wal_log):
    while True:
        await asyncio.sleep(config.MONITOR_TIMEOUT)

        pool = TaskPool(loop, config.NUM_SCHLEPPERS)
        futures = []

        checkpoints = []
        for stream_name in stream_registry:
            stream = stream_registry.get_stream(stream_name)
            if not stream.empty():
                events = sorted(stream.drain())

                for chunk in grouper(MAX_BATCH_SIZE, events):
                    future = pool.submit(try_batch(loop, stream_name, chunk))
                    futures.append(future)

                checkpoints.append(events[-1])

        await pool.join()
        for event in checkpoints:
            await wal_log.put((MessageTypes.CHECKPOINT.value, event))


def main():
    loop = asyncio.get_event_loop()

    wal_log = PriorityQueue(loop=loop)
    stream_registry = StreamRegistry(loop)

    server = Server(config.HOST, config.PORT, loop, wal_log)

    wal_path = "wal.log"
    if os.path.exists(wal_path):
        replay_log(wal_path)

    wal_coro = wal_writer(wal_path, loop, wal_log, stream_registry)
    asyncio.ensure_future(wal_coro)

    event_schlepper_coro = event_schlepper(loop, stream_registry, wal_log)
    asyncio.ensure_future(event_schlepper_coro)

    loop.run_until_complete(server.start())
    log.info("serving on %s:%s", config.HOST, config.PORT)
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        log.info("shutting down...")
        loop.run_until_complete(server.close())
        loop.run_until_complete(wal_log.put((MessageTypes.SHUTDOWN.value, None)))

    loop.run_until_complete(wal_log.join())
    asyncio.wait(wal_coro, loop=loop)
