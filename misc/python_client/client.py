import time
import typing

from ctypes import *

connectionString = "grpc://localhost:2136/?database=local"

from os.path import dirname, abspath, join
project_dir = dirname(dirname(abspath(__file__)))


class CTypesConn:
    lib: typing.Any
    handler: typing.Any

    _closed: bool

    def __init__(self):
        self._closed = False

        self.lib = CDLL(join(project_dir, "go", "_obj", "libydb.so"))
        # self.lib = CDLL("../rust_ydb_client/target/release/librust_ydb_client.dylib")
        lib = self.lib
        lib.ydb_connect.argtypes = [c_char_p]
        lib.ydb_connect.restype = c_void_p
        lib.ydb_connect_wait.argtypes = [c_void_p]
        lib.ydb_connect_wait.restype = c_int
        lib.ydb_connect_free.argtypes = [c_void_p]
        lib.ydb_connect_free.restype = None
        lib.ydb_query.argtypes = [c_void_p, c_char_p]
        lib.ydb_query.restype = c_void_p
        lib.ydb_result_free.argtypes = [c_void_p]
        lib.ydb_result_free.restype = None
        lib.ydb_result_wait.argtypes = [c_void_p]
        lib.ydb_result_wait.restype = None
        lib.ydb_result_next_readset.argtypes = [c_void_p]
        lib.ydb_result_next_readset.restype = c_int
        lib.ydb_result_next_row.argtypes = [c_void_p]
        lib.ydb_result_next_row.restype = c_int
        lib.ydb_result_read_first_field_text.argtypes = [c_void_p, c_void_p, c_int]

    def connect(self):
            conn_string_c = c_char_p(bytes(connectionString.encode()))
            self.handler = self.lib.ydb_connect(conn_string_c)
            print(self.handler)
            print(self.lib.ydb_connect_wait(self.handler))

    def close(self):
        if self._closed:
            return

        self._closed = True
        self.lib.ydb_connect_free(self.handler)

    def __del__(self):
        self.close()

    def query(self, query: str):
        lib = self.lib
        res = lib.ydb_query(self.handler, c_char_p(query.encode()))
        lib.ydb_result_wait(res)

        rs = []
        while lib.ydb_result_next_readset(res) == 0:
            result_set = []

            while lib.ydb_result_next_row(res) == 0:
                row = {}

                bufLen = 100
                c_res = create_string_buffer(b'\000', bufLen)
                lib.ydb_result_read_first_field_text(res, c_res, bufLen)
                field = c_res.value.decode()
                row["first"] = field
                result_set.append(row)
            rs.append(result_set)

        lib.ydb_result_free(res)
        return rs


class CModConn:
    l: typing.Any
    handler: typing.Any
    mod: typing.Any

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
            self.handler = self.mod.connect(connectionString)
            print(self.handler)

    def close(self):
        if self._closed:
            return

        self._closed = True

    def __del__(self):
        self.close()

    def query(self, query: str):
        lib = self.lib
        res = lib.ydb_query(self.handler, c_char_p(query.encode()))
        lib.ydb_result_wait(res)

        rs = []
        while lib.ydb_result_next_readset(res) == 0:
            result_set = []

            while lib.ydb_result_next_row(res) == 0:
                row = {}

                bufLen = 100
                c_res = create_string_buffer(b'\000', bufLen)
                lib.ydb_result_read_first_field_text(res, c_res, bufLen)
                field = c_res.value.decode()
                row["first"] = field
                result_set.append(row)
            rs.append(result_set)

        lib.ydb_result_free(res)
        return rs


class CPythonConn:
    _connection: typing.Any
    _mod: typing.Any

    def __init__(self) -> None:
        import sys
        from ctypes import cdll

        libdir = project_dir + "/cython_ydb_extension/"

        cdll.LoadLibrary(join(libdir, "libydb.so"))

        print(libdir)
        sys.path.append(libdir)

        import cython_ydb_extension
        self._mod = cython_ydb_extension

    def connect(self):
        self._connection = self._mod.open(connectionString)

    def query(self, query):
        res = self._connection.query(query)
        # print("received py result", res)
        return res.to_results()


def benchmark():
    import ydb  # needs to be installed from pypi version 3.x

    # conn = CTypesConn()
    conn = CPythonConn()
    conn.connect()

    driver = ydb.Driver(connection_string=connectionString)
    driver.wait()

    query = "SELECT 'asd' as col"
    print(conn.query(query))

    session = driver.table_client.session().create()

    with session.transaction() as tx:
        res = tx.execute(query, commit_tx=True)
        res = res[0]
        print(res.rows[0])


    iterations = 100

    start = time.monotonic()
    for i in range(iterations):
        conn.query(query)
    finish = time.monotonic()
    print()
    print("c interface")
    print(finish-start)

    start = time.monotonic()
    for i in range(iterations):
        with session.transaction() as tx:
            res = tx.execute(query, commit_tx=True)
    finish = time.monotonic()
    print()
    print("python driver interface")
    print(finish-start)


# cMod = CPythonConn()
# cMod.connect()
# print("query result:", cMod.query("SELECT 'asd' as col"))

benchmark()
