#include <gtest/gtest.h>

#include "internal.hpp"
#include "ydb.h"
#include "ydb_error.h"

#include <ydb-cpp-sdk/client/types/status_codes.h>

TEST(StatusMapping, MapsKnownDriverStatuses) {
  EXPECT_EQ(status_to_ydb_code(NYdb::EStatus::SUCCESS), YDB_OK);
  EXPECT_EQ(status_to_ydb_code(NYdb::EStatus::BAD_REQUEST), YDB_ERR_BAD_REQUEST);
  EXPECT_EQ(status_to_ydb_code(NYdb::EStatus::NOT_FOUND), YDB_ERR_NOT_FOUND);
  EXPECT_EQ(status_to_ydb_code(NYdb::EStatus::INTERNAL_ERROR), YDB_ERR_INTERNAL);
  EXPECT_EQ(status_to_ydb_code(NYdb::EStatus::TIMEOUT), YDB_ERR_TIMEOUT);
}

TEST(StatusMapping, MapsTransientStatusesToConnection) {
  EXPECT_EQ(status_to_ydb_code(NYdb::EStatus::ABORTED), YDB_ERR_CONNECTION);
  EXPECT_EQ(status_to_ydb_code(NYdb::EStatus::UNAVAILABLE), YDB_ERR_CONNECTION);
  EXPECT_EQ(status_to_ydb_code(NYdb::EStatus::SESSION_EXPIRED), YDB_ERR_CONNECTION);
  EXPECT_EQ(status_to_ydb_code(NYdb::EStatus::TRANSPORT_UNAVAILABLE),
            YDB_ERR_CONNECTION);
}

TEST(StatusMapping, RetriableClassifierMatchesTransientStatuses) {
  EXPECT_EQ(ydb_is_status_retriable(static_cast<ydb_status_t>(NYdb::EStatus::ABORTED)), 1);
  EXPECT_EQ(ydb_is_status_retriable(static_cast<ydb_status_t>(NYdb::EStatus::UNAVAILABLE)), 1);
  EXPECT_EQ(ydb_is_status_retriable(static_cast<ydb_status_t>(NYdb::EStatus::SESSION_BUSY)), 1);
  EXPECT_EQ(ydb_is_status_retriable(static_cast<ydb_status_t>(NYdb::EStatus::BAD_REQUEST)), 0);
}
