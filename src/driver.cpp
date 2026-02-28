#include "include/ydb.h"
#include <ydb-cpp-sdk/client/driver/driver.h>
#include <ydb-cpp-sdk/client/table/table.h>
#include <string>
#include <memory>

// ---- Internal struct definitions (hidden from C consumers) ----

struct YdbDriverConfig {
    std::string endpoint;
    std::string database;
    std::string auth_token;
};

struct YdbDriver {
    std::unique_ptr<NYdb::TDriverConfig> config;
    std::unique_ptr<NYdb::TDriver>       driver;
};

struct YdbTableClient {
    std::unique_ptr<NYdb::NTable::TTableClient> client;
    YdbDriver* parent_driver; // prevent use-after-free
};

// Thread-local error message storage
static thread_local std::string g_last_error;

static void set_last_error(const std::string& msg) {
    g_last_error = msg;
}

extern "C" {

const char* ydb_last_error_message(void) {
    return g_last_error.c_str();
}

// ---- Driver Config ----

YdbDriverConfig* ydb_driver_config_create(void) {
    return new (std::nothrow) YdbDriverConfig();
}

void ydb_driver_config_free(YdbDriverConfig* cfg) {
    delete cfg;
}

ydb_status_t ydb_driver_config_set_endpoint(YdbDriverConfig* cfg, const char* endpoint) {
    if (!cfg || !endpoint) return YDB_ERR_BAD_REQUEST;
    cfg->endpoint = endpoint;
    return YDB_OK;
}

ydb_status_t ydb_driver_config_set_database(YdbDriverConfig* cfg, const char* database) {
    if (!cfg || !database) return YDB_ERR_BAD_REQUEST;
    cfg->database = database;
    return YDB_OK;
}

ydb_status_t ydb_driver_config_set_auth_token(YdbDriverConfig* cfg, const char* token) {
    if (!cfg || !token) return YDB_ERR_BAD_REQUEST;
    cfg->auth_token = token;
    return YDB_OK;
}

// ---- Driver ----

YdbDriver* ydb_driver_create(const YdbDriverConfig* cfg) {
    if (!cfg) {
        set_last_error("config is null");
        return nullptr;
    }

    auto drv = new (std::nothrow) YdbDriver();
    if (!drv) return nullptr;

    try {
        auto driverConfig = NYdb::TDriverConfig()
            .SetEndpoint(cfg->endpoint)
            .SetDatabase(cfg->database)
            .SetAuthToken(cfg->auth_token);

        drv->config = std::make_unique<NYdb::TDriverConfig>(std::move(driverConfig));
        drv->driver = std::make_unique<NYdb::TDriver>(*drv->config);
    } catch (const std::exception& e) {
        set_last_error(std::string("driver creation failed: ") + e.what());
        delete drv;
        return nullptr;
    }

    return drv;
}

void ydb_driver_free(YdbDriver* drv) {
    if (drv) {
        if (drv->driver) {
            drv->driver->Stop(true /* wait */);
        }
        delete drv;
    }
}

// ---- Table Client ----

YdbTableClient* ydb_table_client_create(YdbDriver* drv) {
    if (!drv || !drv->driver) {
        set_last_error("driver is null or not initialized");
        return nullptr;
    }

    auto tc = new (std::nothrow) YdbTableClient();
    if (!tc) return nullptr;

    try {
        tc->client = std::make_unique<NYdb::NTable::TTableClient>(*drv->driver);
        tc->parent_driver = drv;
    } catch (const std::exception& e) {
        set_last_error(std::string("table client creation failed: ") + e.what());
        delete tc;
        return nullptr;
    }

    return tc;
}

void ydb_table_client_free(YdbTableClient* tc) {
    delete tc;
}

// ---- Query Execution (simplified, synchronous with auto-retry) ----

ydb_status_t ydb_table_execute_query(
    YdbTableClient* tc,
    const char* yql,
    const YdbQueryParams* params,  /* see params implementation separately */
    YdbResultSets** out_results)
{
    if (!tc || !yql || !out_results) return YDB_ERR_BAD_REQUEST;

    try {
        auto status = tc->client->RetryOperationSync(
            [&](NYdb::NTable::TSession session) -> NYdb::TStatus {
                auto txControl = NYdb::NTable::TTxControl::BeginTx(
                    NYdb::NTable::TTxSettings::SerializableRW()
                ).CommitTx();

                auto result = session.ExecuteDataQuery(yql, txControl).GetValueSync();
                if (result.IsSuccess()) {
                    // Store result sets in *out_results (see result.cpp)
                    *out_results = wrap_result_sets(result);
                }
                return result;
            }
        );

        if (!status.IsSuccess()) {
            set_last_error(status.GetIssues().ToString());
            return YDB_ERR_GENERIC;
        }
    } catch (const std::exception& e) {
        set_last_error(e.what());
        return YDB_ERR_INTERNAL;
    }

    return YDB_OK;
}

} // extern "C"