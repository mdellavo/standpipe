import logging
import threading

from standpipe import client

ADDRESS = "unix://./socket"
NUM_WORKERS = 5
NUM_RECORDS = 100000

logging.basicConfig(level=logging.DEBUG)


def producer(i, n):
    c = client.StreamClient(ADDRESS)

    count = 0
    while count < n:
        c.write("test-{}".format(i), {"i": i, "n": n, "count": count})
        count += 1
    c.close()


threads = [threading.Thread(target=producer, args=(i, NUM_RECORDS)) for i in range(25)]

for thread in threads:
    thread.setDaemon(True)
    thread.start()

for thread in threads:
    thread.join()
