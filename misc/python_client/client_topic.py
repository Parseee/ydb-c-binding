import time
import typing
import concurrent

import ydb  # needs to be installed from pypi version 3.x

from ctypes import *

connectionString = "grpc://localhost:2135/?database=local"
topicName = "topic"
consumerName = "consumer"

class PyModConn:
    _driver: ydb.Driver
    _reader: ydb.TopicReader
    _closed: bool

    def __init__(self):
        self._closed = False

    def connect(self):
            self._driver = ydb.Driver(connection_string=connectionString)
            self._driver.wait(timeout=5)

    def close(self):
        if self._closed:
            return

        self._closed = True
        self._reader.close()

    def prepare(self):
        self.connect()
        self._reader = self._driver.topic_client.reader(
            topicName,
            consumerName,
            buffer_size_bytes=1*1024*1024,
            decoder_executor=concurrent.futures.ThreadPoolExecutor(
                max_workers=100,
                thread_name_prefix="topic_asyncio_executor",
            )
        )
		# WARMUP, for test purposes only
        _ = self._reader.receive_message(timeout=1)
        time.sleep(1)

    def test(self):
        print("start")
        count = 0
        start = time.monotonic()
        iterations = 0
        while True:
            try:
                res = self._reader.receive_batch(timeout=1)
                count += len(res.messages)
                print(f"Messages len: {len(res.messages)} ({count})")
                # print("message:", res["messages"][0])
                iterations += 1
            except TimeoutError:
                break

        duration = time.monotonic() - start - 1
        return {
             "time": duration,
             "count": count +1, # +1 - warmup
             "iterations": iterations,
        }


class CModConn:
    l: typing.Any
    connection: typing.Any
    mod: typing.Any
    reader: typing.Any

    _closed: bool

    def __init__(self):
        self._closed = False

        import sys
        from os.path import dirname, abspath
        libdir = dirname(dirname(abspath(__file__))) + "/go_for_python/_obj"
        print(libdir)
        sys.path.append(libdir)

        import go_for_python
        self.mod = go_for_python

    def connect(self):
            self.connection = self.mod.connect(connectionString)
            print(self.connection)

    def close(self):
        if self._closed:
            return

        self._closed = True

    def prepare(self):
        self.connect()
        self.reader = self.mod.start_reader(self.connection, topicName, consumerName)
        time.sleep(1)
        # res = self.mod.read_batch(self.reader) # warnup
        # if res is None:
        #     print("None on warnup")
        #     return

    def test(self):
        print("start")
        count = 0
        start = time.monotonic()
        iterations = 0
        while True:
            res = self.mod.read_batch(self.reader)
            if res is None:
                break
            # print("Messages len:", len(res["messages"]))
            # print("message:", res["messages"][0])
            count += len(res["messages"])
            iterations += 1

        duration = time.monotonic() - start - 1
        return {
             "time": duration,
             "count": count + 1, # +1 for warmup
             "iterations": iterations,
        }


pyMod = PyModConn()
pyMod.prepare()
print(pyMod.test())
pyMod.close()

cMod = CModConn()
cMod.prepare()
print(cMod.test())
