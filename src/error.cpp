#include <cstdint>
#include <iostream>
#include <ydb-cpp-sdk/client/types/status_codes.h>

#include "ydb.h"

struct YdbErrorLogger {
  std::string error_msg;
  NYdb::EStatus status;
    // NT issue;
};

typedef enum ydb_log_level_t : uint32_t {
  YDB_LOG_ERROR = 1,
  YDB_LOG_WARN = 2,
  YDB_LOG_INFO = 3,
  YDB_LOG_DEBUG = 4,
} ydb_log_level_t;
