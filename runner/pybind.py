"""
Python test for the ydb-c-binding shared library.

Uses ctypes to call libYDB-C.so directly — the same functions
that wrapper.c calls, but from Python.

Usage:
    python3 runner/test_insert.py

The script expects libYDB-C.so to be in the build directory.
Set LD_LIBRARY_PATH if needed:
    LD_LIBRARY_PATH=build python3 runner/test_insert.py
"""

import ctypes
import sys
import os

# ------------------------------------------------------------------ #
# 1. Load the shared library                                          #
# ------------------------------------------------------------------ #

LIB_PATH = os.environ.get(
    "YDB_C_LIB",
    os.path.join(os.path.dirname(__file__), "..", "build", "libYDB-C.so"),
)

try:
    lib = ctypes.CDLL(LIB_PATH)
except OSError as e:
    sys.exit(f"[ERROR] Cannot load {LIB_PATH}: {e}\n"
             "  Set YDB_C_LIB=/path/to/libYDB-C.so or add the build dir to LD_LIBRARY_PATH")

# ------------------------------------------------------------------ #
# 2. Declare opaque handle types                                      #
# ------------------------------------------------------------------ #

class YdbDriverConfig(ctypes.Structure): pass
class YdbDriver(ctypes.Structure):       pass
class YdbTableClient(ctypes.Structure):  pass
class YdbQueryParams(ctypes.Structure):  pass
class YdbResultSets(ctypes.Structure):   pass

YdbDriverConfigPtr  = ctypes.POINTER(YdbDriverConfig)
YdbDriverPtr        = ctypes.POINTER(YdbDriver)
YdbTableClientPtr   = ctypes.POINTER(YdbTableClient)
YdbQueryParamsPtr   = ctypes.POINTER(YdbQueryParams)
YdbResultSetsPtr    = ctypes.POINTER(YdbResultSets)
YdbResultSetsPtrPtr = ctypes.POINTER(YdbResultSetsPtr)

ydb_status_t = ctypes.c_int32

# ------------------------------------------------------------------ #
# 3. Declare function signatures                                      #
# ------------------------------------------------------------------ #

def _fn(name, restype, *argtypes):
    f = getattr(lib, name)
    f.restype  = restype
    f.argtypes = list(argtypes)
    return f

# Error
ydb_last_error_message      = _fn("ydb_last_error_message", ctypes.c_char_p)

# Driver config
ydb_driver_config_create    = _fn("ydb_driver_config_create",    YdbDriverConfigPtr)
ydb_driver_config_free      = _fn("ydb_driver_config_free",      None, YdbDriverConfigPtr)
ydb_driver_config_set_endpoint = _fn("ydb_driver_config_set_endpoint",
                                      ydb_status_t, YdbDriverConfigPtr, ctypes.c_char_p)
ydb_driver_config_set_database = _fn("ydb_driver_config_set_database",
                                      ydb_status_t, YdbDriverConfigPtr, ctypes.c_char_p)
ydb_driver_config_set_auth_token = _fn("ydb_driver_config_set_auth_token",
                                        ydb_status_t, YdbDriverConfigPtr, ctypes.c_char_p)

# Driver
ydb_driver_create           = _fn("ydb_driver_create", YdbDriverPtr, YdbDriverConfigPtr)
ydb_driver_free             = _fn("ydb_driver_free",   None,         YdbDriverPtr)

# Table client
ydb_table_client_create     = _fn("ydb_table_client_create", YdbTableClientPtr, YdbDriverPtr)
ydb_table_client_free       = _fn("ydb_table_client_free",   None, YdbTableClientPtr)

# Query params
ydb_query_params_create     = _fn("ydb_query_params_create", YdbQueryParamsPtr)
ydb_query_params_free       = _fn("ydb_query_params_free",   None, YdbQueryParamsPtr)
ydb_query_params_set_utf8   = _fn("ydb_query_params_set_utf8",
                                   ydb_status_t, YdbQueryParamsPtr,
                                   ctypes.c_char_p, ctypes.c_char_p)
ydb_query_params_set_uint64 = _fn("ydb_query_params_set_uint64",
                                   ydb_status_t, YdbQueryParamsPtr,
                                   ctypes.c_char_p, ctypes.c_uint64)

# DDL + DML
ydb_table_execute_scheme    = _fn("ydb_table_execute_scheme",
                                   ydb_status_t, YdbTableClientPtr, ctypes.c_char_p)
ydb_table_execute_query     = _fn("ydb_table_execute_query",
                                   ydb_status_t,
                                   YdbTableClientPtr,
                                   ctypes.c_char_p,
                                   YdbQueryParamsPtr,   # nullable
                                   YdbResultSetsPtrPtr) # nullable

# ------------------------------------------------------------------ #
# 4. Helpers                                                          #
# ------------------------------------------------------------------ #

YDB_OK = 0

def check(status: int, op: str) -> None:
    if status != YDB_OK:
        msg = ydb_last_error_message()
        decoded = msg.decode() if msg else "(no message)"
        sys.exit(f"[ERROR] {op} failed (code={status}): {decoded}")

def b(s: str) -> bytes:
    """Encode a Python str to bytes for ctypes c_char_p."""
    return s.encode()

# ------------------------------------------------------------------ #
# 5. Main test                                                        #
# ------------------------------------------------------------------ #

def main():
    endpoint = os.environ.get("YDB_ENDPOINT", "ydb-local:2136")
    database = os.environ.get("YDB_DATABASE", "/local")

    print(f"Connecting to {endpoint}  db={database}")

    # --- Driver config ---
    cfg = ydb_driver_config_create()
    assert cfg, "ydb_driver_config_create returned NULL"

    check(ydb_driver_config_set_endpoint(cfg, b(endpoint)), "set_endpoint")
    check(ydb_driver_config_set_database(cfg, b(database)), "set_database")

    # --- Driver ---
    drv = ydb_driver_create(cfg)
    ydb_driver_config_free(cfg)   # safe to free immediately after create
    if not drv:
        msg = ydb_last_error_message()
        sys.exit(f"[ERROR] ydb_driver_create failed: {msg.decode() if msg else '?'}")

    # --- Table client ---
    tc = ydb_table_client_create(drv)
    if not tc:
        ydb_driver_free(drv)
        msg = ydb_last_error_message()
        sys.exit(f"[ERROR] ydb_table_client_create failed: {msg.decode() if msg else '?'}")

    # --- DDL ---
    print("Creating table 'users' …")
    ddl = b("""
        CREATE TABLE IF NOT EXISTS users (
            id   Uint64,
            name Utf8,
            PRIMARY KEY (id)
        );
    """)
    check(ydb_table_execute_scheme(tc, ddl), "CREATE TABLE")
    print("  OK")

    # --- DML: UPSERT via params ---
    print("Inserting record id=42, name='Alice' …")
    params = ydb_query_params_create()
    assert params, "ydb_query_params_create returned NULL"

    check(ydb_query_params_set_uint64(params, b"$id",   42),      "params $id")
    check(ydb_query_params_set_utf8  (params, b"$name", b"Alice"), "params $name")

    check(
        ydb_table_execute_query(
            tc,
            b"UPSERT INTO users (id, name) VALUES ($id, $name)",
            params,
            None,   # no result sets needed for UPSERT
        ),
        "UPSERT",
    )
    ydb_query_params_free(params)
    print("  OK")

    # --- DML: a second record without params (literal values) ---
    print("Inserting record id=99, name='Bob' (literal YQL) …")
    check(
        ydb_table_execute_query(
            tc,
            b"UPSERT INTO users (id, name) VALUES (99, 'Bob')",
            None,   # no params
            None,
        ),
        "UPSERT literal",
    )
    print("  OK")

    # --- Cleanup ---
    ydb_table_client_free(tc)
    ydb_driver_free(drv)          # internally calls driver->Stop(true)

    print("\nAll done — records inserted successfully.")


if __name__ == "__main__":
    main()