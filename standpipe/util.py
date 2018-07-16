import itertools

def grouper(n, events):
    it = iter(events)
    while True:
        chunk = tuple(itertools.islice(it, n))
        if not chunk:
            return
        yield chunk


