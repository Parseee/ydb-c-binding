#include "internal.hpp"
#include "ydb_error.h"

#include <cstdlib>
#include <cstring>
#include <string>

#include <ydb-cpp-sdk/client/types/status_codes.h>

namespace {} // namespace

extern "C" {

void ydb_result_details_init(YdbResultDetails *d) {
  d = new YdbResultDetails;
  d->code = 0;
  d->message = std::string();
  d->context = std::string();
}

void ydb_result_details_reset(YdbResultDetails *d) {
  if (!d) {
    return;
  }
  d->message.clear();
  d->context.clear();
}

void ydb_result_details_free(YdbResultDetails *d) {
  if (!d) {
    return;
  }
  delete d;
}

const char *get_message(const YdbResultDetails *d) {
  if (!d) {
    return nullptr;
  }
  return d->message.c_str();
}

int ydb_is_status_retriable(ydb_status_t sdk_status_code) {
  using NYdb::EStatus;
  EStatus s = static_cast<EStatus>(sdk_status_code);
  switch (s) {
  case EStatus::ABORTED:
  case EStatus::UNAVAILABLE:
  case EStatus::OVERLOADED:
  case EStatus::CLIENT_RESOURCE_EXHAUSTED:
  case EStatus::CLIENT_DISCOVERY_FAILED:
  case EStatus::SESSION_BUSY:
  case EStatus::SESSION_EXPIRED:
  case EStatus::TRANSPORT_UNAVAILABLE:
    return 1;
  default:
    return 0;
  }
}

ydb_status_t ydb_result_details_fail(YdbResultDetails *d, ydb_status_t code,
                                     const char *msg) {
  if (d) {
    d->code = code;
    ydb_result_details_set_message(d, msg ? msg : "");
  }

  if (msg) {
    ydb_result_details_print(msg);
  }

  return code;
}

void ydb_result_details_print(const char *err_msg) {
  std::cout << "[ydb-c]: " << err_msg << std::endl;
}

} // extern "C"

bool isFatal(YdbResultDetails *rd) {
  return rd->code != YDB_OK;
  switch (rd->code) {
  case (YDB_OK):
  case (YDB_ERR_NO_MORE_RESULTS):
    return false;
  default:
    return true;
  }
}

ydb_status_t ydb_fill_from_status(YdbResultDetails *details,
                                  const NYdb::TStatus &st) {
  const ydb_status_t code = status_to_ydb_code(st.GetStatus());
  const std::string msg = st.GetIssues().ToString();

  if (details) {
    ydb_result_details_set_status(details, code);
    ydb_result_details_set_message(details, msg);
    return code;
  }

  if (!msg.empty()) {
    ydb_result_details_print(msg.c_str());
  }

  return code;
}

void ydb_result_details_set_status(YdbResultDetails *d, ydb_status_t code) {
  if (!d) {
    return;
  }
  d->code = code;
}

ydb_status_t ydb_rd_fail(YdbResultDetails *rd, ydb_status_t code,
                         const char *details) {
  return ydb_result_details_fail(rd, code, details);
}

void ydb_append_fatal_context(YdbResultDetails *rd, const char *func) {
  std::string error_msg = std::string("from ") + func;
  ydb_result_details_append_message(rd, error_msg.c_str());
}

std::optional<ydb_status_t> ydb_check_rd_status(YdbResultDetails *rd,
                                                const char *func) {
  if (!rd) {
    return YDB_OK;
  }
  if (isFatal(rd)) {
    ydb_append_fatal_context(rd, func);
    return rd->code;
  }
  return std::nullopt;
}

bool ydb_check_rd_fatal(YdbResultDetails *rd, const char *func) {
  if (rd && isFatal(rd)) {
    ydb_append_fatal_context(rd, func);
    return true;
  }
  return false;
}

void ydb_result_details_set_message(YdbResultDetails *d,
                                    const std::string &msg) {
  if (!d) {
    return;
  }
  d->message = msg;
}

void ydb_result_details_append_message(YdbResultDetails *d,
                                       const std::string &msg) {
  if (!d) {
    return;
  }
  d->message += msg;
}

void ydb_result_details_set_context(YdbResultDetails *d,
                                    const std::string &ctx) {
  if (!d) {
    return;
  }
  d->context = ctx;
}
