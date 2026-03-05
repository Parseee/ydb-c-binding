#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/table/table.h>
#include <ydb-cpp-sdk/client/query/client.h>
#include <iostream>

int main() {
    auto config = NYdb::TDriverConfig()
        .SetEndpoint("ydb-local:2136")
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

    driver.Stop(true);
    return 0;
}
