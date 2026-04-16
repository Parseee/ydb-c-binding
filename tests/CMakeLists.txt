include(FetchContent)

FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/refs/tags/v1.14.0.zip
)

set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(ydb_c_unit_tests
  retry_settings_test.cpp
  status_mapping_test.cpp
)

target_include_directories(ydb_c_unit_tests
  PRIVATE
  ${PROJECT_SOURCE_DIR}/include
  ${PROJECT_SOURCE_DIR}/src
  PRIVATE "/workspaces/ydb-c/ydb-cpp-sdk/"
)

target_link_libraries(ydb_c_unit_tests
  PRIVATE
  YDB-C
  GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(ydb_c_unit_tests)
