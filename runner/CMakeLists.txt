add_executable(basic_example)

target_include_directories(basic_example
  PUBLIC ${PROJECT_SOURCE_DIR}
)

target_sources(basic_example PRIVATE
  # main.cpp
  wrapper.c
  # param_builder.c
)

target_link_libraries(basic_example PRIVATE
  YDB-C
)
