import threading
import json

from standpipe import client, config

RECORD_SEPARATOR = "\n"

NUM_WORKERS = 5
NUM_RECORDS = 1000000


def serialize(record):
    return json.dumps(record) + RECORD_SEPARATOR


def producer(i, n):
    c = client.StreamClient(config.HOST, config.PORT)

    count = 0
    while count < n:
        c.write("test-{}".format(i), serialize({"n": n}))
        count += 1
    c.close()


threads = [threading.Thread(target=producer, args=(i, NUM_RECORDS)) for i in range(5)]

for thread in threads:
    thread.start()

for thread in threads:
    thread.join()
