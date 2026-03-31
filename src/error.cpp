#include "internal.hpp"
#include "ydb_error.h"

#include <cstdlib>
#include <cstring>
#include <string>

#include <ydb-cpp-sdk/client/types/status_codes.h>

namespace {

static int ensure_cap(char **buf, size_t *cap, size_t need) {
  if (!buf || !cap)
    return 0;
  if (*cap >= need)
    return 1;

  size_t new_cap = (*cap == 0) ? 128 : *cap;
  while (new_cap < need) {
    if (new_cap > (SIZE_MAX / 2)) {
      new_cap = need;
      break;
    }
    new_cap *= 2;
  }

  void *p = std::realloc(*buf, new_cap);
  if (!p)
    return 0;

  *buf = static_cast<char *>(p);
  *cap = new_cap;
  return 1;
}

static void set_str(char **buf, size_t *len, size_t *cap, const char *s) {
  if (!buf || !len || !cap)
    return;
  const char *src = s ? s : "";
  size_t n = std::strlen(src);

  if (!ensure_cap(buf, cap, n + 1))
    return;
  std::memcpy(*buf, src, n + 1);
  *len = n;
}

static void append_str(char **buf, size_t *len, size_t *cap, const char *s) {
  if (!buf || !len || !cap || !s)
    return;
  size_t n = std::strlen(s);
  if (n == 0)
    return;

  size_t need = *len + n + 1;
  if (!ensure_cap(buf, cap, need))
    return;

  std::memcpy((*buf) + *len, s, n + 1);
  *len += n;
}

} // namespace

extern "C" {

void ydb_result_details_init(ydb_result_details_t *d) {
  if (!d) {
    return;
  }
  d->code = 0;
  d->message = nullptr;
  d->message_len = 0;
  d->message_cap = 0;
  d->context = nullptr;
  d->context_len = 0;
  d->context_cap = 0;
}

void ydb_result_details_reset(ydb_result_details_t *d) {
  if (!d) {
    return;
  }
  d->code = 0;
  if (d->message && d->message_cap) {
    d->message[0] = '\0';
  }
  d->message_len = 0;
  if (d->context && d->context_cap) {
    d->context[0] = '\0';
  }
  d->context_len = 0;
}

void ydb_result_details_free(ydb_result_details_t *d) {
  if (!d) {
    return;
  }
  std::free(d->message);
  std::free(d->context);
  d->message = nullptr;
  d->context = nullptr;
  d->message_len = d->context_len = 0;
  d->message_cap = d->context_cap = 0;
}

void ydb_result_details_set_status(ydb_result_details_t *d, ydb_status_t code) {
  if (!d) {
    return;
  }
  d->code = code;
}

void ydb_result_details_set_message(ydb_result_details_t *d, const char *msg) {
  if (!d) {
    return;
  }
  set_str(&d->message, &d->message_len, &d->message_cap, msg);
}

void ydb_result_details_append_message(ydb_result_details_t *d,
                                       const char *msg) {
  if (!d) {
    return;
  }
  append_str(&d->message, &d->message_len, &d->message_cap, msg);
}

void ydb_result_details_set_context(ydb_result_details_t *d, const char *ctx) {
  if (!d) {
    return;
  }
  set_str(&d->context, &d->context_len, &d->context_cap, ctx);
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

ydb_status_t ydb_result_details_fail(ydb_result_details_t *d, ydb_status_t code,
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

} // extern "C"

ydb_status_t ydb_fill_from_status(ydb_result_details_t *details,
                                  const NYdb::TStatus &st) {
  const ydb_status_t code = status_to_ydb_code(st.GetStatus());
  const std::string msg = st.GetIssues().ToString();

  if (details) {
    ydb_result_details_set_status(details, code);
    ydb_result_details_set_message(details, msg.c_str());
    return code;
  }

  if (!msg.empty()) {
    ydb_result_details_print(msg.c_str());
  }

  return code;
}
