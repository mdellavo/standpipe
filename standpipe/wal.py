import zlib
import time
import struct
from enum import Enum
import logging

import aiofile

WAL_ENTRY_PREFIX = "if"
WAL_ENTRY_PREFIX_SIZE = struct.calcsize(WAL_ENTRY_PREFIX)

WAL_RECORD_HEADER = "Li"
WAL_RECORD_HEADER_SIZE = struct.calcsize(WAL_RECORD_HEADER)
WAL_ENTRY_RECORD_HEADER = WAL_ENTRY_PREFIX + WAL_RECORD_HEADER


FLUSH_INTERVAL = 100

log = logging.getLogger(__name__)


# NB in priority order
class MessageTypes(Enum):
    CHECKPOINT = 0
    RECORD = 1
    SHUTDOWN = 2


def calc_crc(s):
    return zlib.crc32(bytes(s, "utf-8"))


def write_wal_record_header(record):
    crc = calc_crc(record)
    return struct.pack(WAL_ENTRY_RECORD_HEADER, MessageTypes.RECORD.value, time.time(), crc, len(record))


def write_wal_checkpoint():
    return struct.pack(WAL_ENTRY_PREFIX, MessageTypes.CHECKPOINT.value, time.time())


def read_wal_record(f):
    buf = f.read(WAL_RECORD_HEADER_SIZE)
    stream_name, _id, crc, length = struct.unpack(WAL_RECORD_HEADER, buf)
    record = f.read(length)
    record_crc = calc_crc(record)

    if record_crc != crc:
        print("crc mismatch {} != {}".format(record_crc, crc))

    return record


def read_wal_entry(f):
    buf = f.read(WAL_ENTRY_PREFIX_SIZE)
    if not buf:
        return None, None
    record_type, ts = struct.unpack(WAL_ENTRY_PREFIX, buf)
    if record_type == MessageTypes.RECORD.value:
        record = read_wal_record(f)
        return MessageTypes.RECORD, ts, record
    elif record_type == MessageTypes.CHECKPOINT.value:

        return MessageTypes.CHECKPOINT, ts
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


# FIXME thread pool executor
async def wal_writer(path, wal_log):

    async with aiofile.AIOFile(path, 'w+b') as f:
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

                # FIXME

            elif message_type == MessageTypes.CHECKPOINT.value:
                log.info("checkpoint %s:%s", record.stream_name, record.id)
                await writer(write_wal_checkpoint())

            wal_log.task_done()

            count += 1

            if count % FLUSH_INTERVAL:
                await f.fsync()

    log.info("wal writer shutdown")

