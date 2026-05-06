#include <gtest/gtest.h>

#include "internal.hpp"
#include "ydb.h"
#include "ydb_error.h"

namespace {
YdbResultDetails MakeDetails(ydb_status_t code = YDB_OK) {
  YdbResultDetails rd;
  rd.code = code;
  rd.message.clear();
  rd.context.clear();
  return rd;
}
} // namespace

TEST(QueryRetrySettings, CreateInitializesFields) {
  auto rd = MakeDetails();

  YdbQueryRetrySettings *rs = ydb_query_retry_settings_create(3, 0, &rd);
  ASSERT_NE(rs, nullptr);
  EXPECT_EQ(rs->max_retries, 3u);
  EXPECT_EQ(rs->current_retries, 0u);
  EXPECT_EQ(rs->timeout_ms, 0u);

  ydb_query_retry_settings_free(rs, &rd);
}

TEST(QueryRetrySettings, PerformRetryIncrementsCounter) {
  auto rd = MakeDetails(YDB_ERR_CONNECTION);
  YdbQueryRetrySettings *rs = ydb_query_retry_settings_create(2, 0, &rd);
  ASSERT_NE(rs, nullptr);

  EXPECT_EQ(ydb_query_perform_retry(rs, &rd), YDB_OK)
      << rd.message << std::endl;
  EXPECT_EQ(rs->current_retries, 1u);
  EXPECT_EQ(ydb_query_perform_retry(rs, &rd), YDB_OK);
  EXPECT_EQ(rs->current_retries, 2u);

  ydb_query_retry_settings_free(rs, &rd);
}

TEST(QueryRetrySettings, PerformRetryFailsWhenLimitExceeded) {
  auto rd = MakeDetails(YDB_ERR_CONNECTION);
  YdbQueryRetrySettings *rs = ydb_query_retry_settings_create(1, 0, &rd);
  ASSERT_NE(rs, nullptr);

  EXPECT_EQ(ydb_query_perform_retry(rs, &rd), YDB_OK);
  EXPECT_EQ(ydb_query_perform_retry(rs, &rd), YDB_ERR_RETRY_FAILED);

  ydb_query_retry_settings_free(rs, &rd);
}

TEST(QueryRetrySettings, PerformRetryFailsForNonRetriableStatus) {
  auto rd = MakeDetails(YDB_ERR_BAD_REQUEST);
  YdbQueryRetrySettings *rs = ydb_query_retry_settings_create(2, 0, &rd);
  ASSERT_NE(rs, nullptr);

  EXPECT_EQ(ydb_query_perform_retry(rs, &rd), YDB_ERR_RETRY_FAILED);
  EXPECT_EQ(rs->current_retries, 0u);

  ydb_query_retry_settings_free(rs, &rd);
}

TEST(QueryRetrySettings, TliFailureFlowRetriesAndEventuallySucceeds) {
  auto rd = MakeDetails();
  YdbQueryRetrySettings *rs = ydb_query_retry_settings_create(4, 0, &rd);
  ASSERT_NE(rs, nullptr);

  int attempts = 0;
  auto do_attempt = [&]() -> ydb_status_t {
    ++attempts;
    if (attempts <= 2) {
      rd.code = YDB_ERR_CONNECTION; // simulate transient link issue (TLI)
      return YDB_ERR_CONNECTION;
    }
    rd.code = YDB_OK;
    return YDB_OK;
  };

  for (;;) {
    const ydb_status_t st = do_attempt();
    if (st == YDB_OK) {
      break;
    }

    ASSERT_EQ(ydb_is_status_retriable(st), 1);
    ASSERT_EQ(ydb_query_perform_retry(rs, &rd), YDB_OK);
  }

  EXPECT_EQ(attempts, 3);
  EXPECT_EQ(rs->current_retries, 2u);

  ydb_query_retry_settings_free(rs, &rd);
}
