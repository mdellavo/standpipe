import asyncio
import logging
import itertools
from asyncio.queues import Queue

import aiobotocore
from botocore.exceptions import ClientError


ADDR, PORT = "0.0.0.0", 4444
MAX_BATCH_SIZE = 500
NUM_RETRIES = 5
MONITOR_TIMEOUT = 10
NUM_WORKERS = 10


log = logging.getLogger(__name__)


TERMINATOR = object()


class Stream(Queue):
    def drain(self):
        rv = []
        while not self.empty():
            rv.append(self.get_nowait())
        return rv


def grouper(n, events):
    it = iter(events)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


async def try_batch(stream_name, records):
    log.info("schlepping %d events for stream %s", len(records), stream_name)

    session = aiobotocore.get_session(loop=asyncio.get_running_loop())

    async with session.create_client('firehose') as client:
        for i in range(NUM_RETRIES):

            try:
                rv = client.put_records_batch(
                    DeliveryStreamName=stream_name,
                    Records=[{"Data": record} for record in records]
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
            await asyncio.sleep(2**i)


async def event_schlepper(stream_name, stream):
    while True:
        await asyncio.sleep(MONITOR_TIMEOUT)
        if not stream.empty():
            batches = grouper(MAX_BATCH_SIZE, sorted(stream.drain()))
            await asyncio.wait([try_batch(stream_name, chunk) for chunk in batches])


class Protocol(asyncio.Protocol):
    def __init__(self, incoming):
        self.incoming = incoming

    def datagram_received(self, data, _):
        self.incoming.put_nowait(data)


async def main():
    incoming = asyncio.Queue()

    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: Protocol(incoming),
        local_addr=(ADDR, PORT),
        reuse_address=True,
        reuse_port=True,
    )
    log.info("serving on %s:%s", ADDR, PORT)

    running = True
    while running:
        try:
            await asyncio.sleep(1)
        except KeyboardInterrupt:
            running = False
            log.info("shutting down...")
    transport.close()

    event_schlepper_coro = event_schlepper("fixme", incoming)
    asyncio.ensure_future(event_schlepper_coro)
