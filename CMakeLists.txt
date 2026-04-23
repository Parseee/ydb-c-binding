cmake_minimum_required(VERSION 3.10)
project(YDB-C)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

find_package(ydb-cpp-sdk REQUIRED)

add_library(${PROJECT_NAME} SHARED
    "src/driver.cpp"
    "src/query_service.cpp"
    "src/error.cpp"
)

target_compile_options(${PROJECT_NAME}
    PUBLIC "-fPIC"
)

target_link_libraries(${PROJECT_NAME} 
    PRIVATE
    YDB-CPP-SDK::Driver
    YDB-CPP-SDK::Table
    YDB-CPP-SDK::Query
    YDB-CPP-SDK::Params
    YDB-CPP-SDK::Scheme
    dl
    rt
    yutil
    pthread
)

target_include_directories(${PROJECT_NAME}
    PUBLIC  ${PROJECT_SOURCE_DIR}/include
    PRIVATE ${PROJECT_SOURCE_DIR}
    PUBLIC /usr/local/include/__ydb_sdk_special_headers/
)

add_subdirectory(runner)

include(CTest)
if(BUILD_TESTING)
  add_subdirectory(tests)
endif()

