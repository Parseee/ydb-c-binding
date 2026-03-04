#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/table/table.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <iostream>

int main() {
    // 1. Configure and create driver
    auto config = NYdb::TDriverConfig()
        .SetEndpoint("grpc://localhost:2136")
        .SetDatabase("/local");
    NYdb::TDriver driver(config);

    // 2. Table client
    NYdb::NTable::TTableClient tableClient(driver);

    // 3. Create table
    auto status = tableClient.RetryOperationSync(
        [](NYdb::NTable::TSession session) {
            return session.ExecuteSchemeQuery(R"(
                CREATE TABLE users (
                  id Uint64,
                  name Utf8,
                  PRIMARY KEY (id)
                );
            )").GetValueSync();
        }
    );
    if (!status.IsSuccess()) {
        std::cerr << "DDL failed: " << status.GetIssues().ToString() << std::endl;
        return 1;
    }

    // 4. Insert a row
    status = tableClient.RetryOperationSync(
        [](NYdb::NTable::TSession session) {
            NYdb::TParamsBuilder params;
            params.AddParam("$id").Uint64(42).Build();
            params.AddParam("$name").Utf8("Alice").Build();

            return session.ExecuteDataQuery(
                "UPSERT INTO users (id, name) VALUES ($id, $name)",
                NYdb::NTable::TTxControl::BeginTx().CommitTx(),
                params.Build()
            ).GetValueSync();
        }
    );
    if (!status.IsSuccess()) {
        std::cerr << "UPSERT failed: " << status.GetIssues().ToString() << std::endl;
        return 2;
    }

    // 5. Query rows
    auto queryResult = tableClient.RetryOperationSync(
        [](NYdb::NTable::TSession session) {
            return session.ExecuteDataQuery(
                "SELECT id, name FROM users",
                NYdb::NTable::TTxControl::BeginTx().CommitTx()
            ).GetValueSync();
        }
    );
    if (!queryResult.IsSuccess()) {
        std::cerr << "Query failed: " << queryResult.GetIssues().ToString() << std::endl;
        return 3;
    }

    // 6. Iterate results
    // auto rsParser = queryResult.GetResultSetParser(0);
    auto tmp = NYdb::NQuery::TExecuteQueryResult(std::move(queryResult));
    auto rsParser = tmp.GetResultSetParser(0);
    while (rsParser.TryNextRow()) {
        uint64_t id = rsParser.ColumnParser(0).GetUint64();
        std::string name = rsParser.ColumnParser(1).GetUtf8();
        std::cout << "User: id=" << id << " name=" << name << std::endl;
    }

    return 0;
}