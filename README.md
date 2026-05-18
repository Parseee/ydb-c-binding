# YDB C Binding

This repository contains an experimental C API wrapper over the YDB C++ SDK.
It provides:

- Driver configuration and lifecycle APIs
- Query execution APIs (transactional and non-transactional)
- Parameter builder APIs
- Result set read APIs for common scalar types

## Prerequisites
###### packaged with the devcontainer

- CMake 3.10+
- Ninja or Make
- Clang++18

## Build

```bash
cmake -S . -B build -G Ninja
cmake --build build -j
```

## Install

```bash
cmake --install build --prefix /usr/local
```

## Run tests

```bash
ctest --test-dir build --output-on-failure
```

Integration tests require a running YDB instance and use:

- `YDB_ENDPOINT` (default: `ydb-local:2136`)
- `YDB_DATABASE` (default: `/local`)

## Project layout

- `include/` public C headers
- `src/` C/C++ implementation
- `runner/` used for internal tests, not user oriented
- `tests/` unit and integration tests
- `.devcontainer/` devcontainer environment files
