cmake_minimum_required(VERSION 3.10)
project(YDB-C)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_subdirectory(runner)

find_package(ydb-cpp-sdk REQUIRED)

include_directories(${PROJECT_SOURCE_DIR}
                    "/root/ydb_deps/include"
                    "/workspaces/ydb-c/ydb-cpp-sdk/"                    
)

add_library(${PROJECT_NAME} SHARED
    "src/driver.cpp"
    "include/ydb.h"
)

target_compile_options(${PROJECT_NAME}
    PUBLIC "-fPIC"
)

target_link_libraries(${PROJECT_NAME} 
    PUBLIC
    YDB-CPP-SDK::Query
    YDB-CPP-SDK::Params
    YDB-CPP-SDK::Driver
)

# rebuild the project with -fPIC. probably not only ydb-sdk but every dependency