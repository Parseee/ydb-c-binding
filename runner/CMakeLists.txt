add_executable(basic_example)

find_package(ydb-cpp-sdk)

target_include_directories(basic_example
  PUBLIC ${PROJECT_SOURCE_DIR}
)

target_sources(basic_example PRIVATE
  # main.cpp
  wrapper.c
)

target_link_libraries(basic_example PRIVATE
  YDB-C
  dl
  rt
  pthread
)
