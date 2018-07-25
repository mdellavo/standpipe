# Standpipe

An aggregator and relay for AWS Kinesis Firehose streams.

Something you hook firehoses up to

## Why do I need this?

- Aggregate events across a host or cluster to minimize upstream API calls
- simple async fire-and-forget API for clients, no need to deal with handling retries

## Design

Standpipe is a simple client/server accessed over TCP or Unix domain socket.  The standpipe client
will accept events written to it, encode and forward them to the server.
The standpipe server will aggregate events per stream and periodically forward to Kinesis Firehose.

The standpipe server is a simple threaded/evented server consiting of a network thread, a router thread and a number of upload worker threads.
The network thread is responsible for servicing the client sockets using poll(), decoding the messages and placing them on a queue for the router thread.
The router thread reads messages off the queue, slots them according to their stream and peridocially flushes the streams in batches to the worker threads.
The worker threads simply take batches of events from the queue and upload them to Kinesis Firehose.

The client server protocol is simply:

STREAM_NAME + " " + PAYLOAD + TERMINATOR

Events are not currently durable!
The client and server will buffer events in memory but will drop events if overloaded, does not currently provide back pressure.

Clients will attempt to reconnect with backoff if the server becomes unavailable while buffering messages in memory.

## ToDo
- [ ] Add gzip to client/server
- [ ] Add preconfigured encoders for other formats
- [ ] python3 asycio server
- [ ] http support for ingestion
- [ ] expose some stats
- [ ] throttling / back pressure
- [ ] message durability (WAL)

## Author

Marc DellaVolpe  (marc.dellavolpe@gmail.com)

## License
    The MIT License (MIT)

    Copyright (c) 2018 Marc DellaVolpe

    Permission is hereby granted, free of charge, to any person obtaining a copy of this
    software and associated documentation files (the "Software"), to deal in the Software
    without restriction, including without limitation the rights to use, copy, modify, merge,
    publish, distribute, sublicense, and/or sell copies of the Software, and to permit
    persons to whom the Software is furnished to do so, subject to the following conditions:

    The above copyright notice and this permission notice shall be included in all copies
    or substantial portions of the Software.

    THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
    INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR
    PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE
    FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
    OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
    DEALINGS IN THE SOFTWARE.
