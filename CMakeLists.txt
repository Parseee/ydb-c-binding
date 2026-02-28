cmake_minimum_required(VERSION 3.10)
project(YDB-C)

list(APPEND CMAKE_PREFIX_PATH "/home/vscode/ydb_deps/base64/lib/cmake/base64/" "/home/vscode/ydb_deps/jwt-cpp/cmake/")

include_directories(${PROJECT_SOURCE_DIR} ydb-cpp-sdk/include)

add_subdirectory(ydb-cpp-sdk)

add_executable(${PROJECT_NAME}
    src/driver.cpp
)