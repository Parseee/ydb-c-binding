cmake_minimum_required(VERSION 3.10)
project(runner)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

list(APPEND CMAKE_PREFIX_PATH "/root/ydb_deps/lib/cmake/ydb-cpp-sdk/" "/root/ydb_deps/")

include_directories(${PROJECT_SOURCE_DIR}
                    "/root/ydb_deps/include"
                    "/workspaces/ydb-c/ydb-cpp-sdk/"                    
)

add_executable(${PROJECT_NAME}
    main.cpp
)

file(GLOB YDB_STATIC_LIBS "/root/ydb_deps/lib/*.a")
file(GLOB YDB_DYNAMIC_LIBS "/root/ydb_deps/lib/*.so")
target_link_libraries(runner 
    PRIVATE YDB-C
    PRIVATE ${YDB_STATIC_LIBS}
    PRIVATE ${YDB_DYNAMIC_LIBS}
)