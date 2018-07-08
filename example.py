import json

from standpipe import client, config

RECORD_SEPARATOR = "\n"


def serialize(record):
    return json.dumps(record) + RECORD_SEPARATOR


def producer(n):
    c = client.StreamClient(config.HOST, config.PORT)
    for i in range(n):
        c.write("test", serialize({"n": n}))
    print("wrote {} records".format(n))
    c.close()


producer(1000)
