// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <fmt/core.h>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "gtest/gtest.h"
#include "http/builtin_http_calls.h"
#include "http/http_call_registry.h"
#include "http/http_server.h"
#include "replica/replica_http_service.h"
#include "replica/test/mock_utils.h"
#include "replica/test/replica_test_base.h"
#include "utils/flags.h"
#include "utils/test_macros.h"

using std::map;
using std::string;

namespace dsn {
namespace replication {
DSN_DECLARE_bool(duplication_enabled);
DSN_DECLARE_bool(fd_disabled);
DSN_DECLARE_uint32(config_sync_interval_ms);

class replica_http_service_test : public replica_test_base
{
public:
    replica_http_service_test()
    {
        // Disable unnecessary works before starting stub.
        FLAGS_fd_disabled = true;
        FLAGS_duplication_enabled = false;
        stub->initialize_start();

        http_call_registry::instance().clear_paths();
        _http_svc = std::make_unique<replica_http_service>(stub.get());
    }

    void SetUp() override
    {
        // Reset config_sync_interval_ms to 30000.
        NO_FATALS(test_update_config({{"config_sync_interval_ms", "30000"}},
                                     R"({"update_status":"ERR_OK"})"
                                     "\n"));
        NO_FATALS(test_check_config("config_sync_interval_ms", "30000"));
    }

    void test_update_config(const map<string, string> &configs, const string &expect_resp)
    {
        http_request req;
        for (const auto &config : configs) {
            req.query_args[config.first] = config.second;
        }

        http_response resp;
        _http_svc->update_config_handler(req, resp);
        ASSERT_EQ(resp.status_code, http_status_code::ok);
        ASSERT_EQ(expect_resp, resp.body);
    }

    void test_check_config(const string &config, const string &expect_value)
    {
        http_request req;
        http_response resp;
        req.query_args["name"] = config;
        get_config(req, resp);
        ASSERT_EQ(resp.status_code, http_status_code::ok);
        const string unfilled_resp =
            R"({{"name":"config_sync_interval_ms","section":"replication","type":"FV_UINT32","tags":"flag_tag::FT_MUTABLE","description":"The interval milliseconds of replica server to syncs replica configuration with meta server","value":"{}"}})"
            "\n";
        ASSERT_EQ(fmt::format(unfilled_resp, expect_value), resp.body);
    }

private:
    std::unique_ptr<replica_http_service> _http_svc;
};

INSTANTIATE_TEST_CASE_P(, replica_http_service_test, ::testing::Values(false, true));

TEST_P(replica_http_service_test, update_config_handler)
{
    // Test the default value.
    NO_FATALS(test_check_config("config_sync_interval_ms", "30000"));
    ASSERT_EQ(30000, FLAGS_config_sync_interval_ms);

    // Update config failed and value not changed.
    NO_FATALS(test_update_config(
        {},
        R"({"update_status":"ERR_INVALID_PARAMETERS: there should be exactly one config to be updated once"})"
        "\n"));
    NO_FATALS(test_check_config("config_sync_interval_ms", "30000"));
    ASSERT_EQ(30000, FLAGS_config_sync_interval_ms);

    // Update config failed and value not changed.
    NO_FATALS(test_update_config(
        {{"config_sync_interval_ms", "10"}, {"hdfs_write_limit_rate_mb_per_sec", "50"}},
        R"({"update_status":"ERR_INVALID_PARAMETERS: there should be exactly one config to be updated once"})"
        "\n"));
    NO_FATALS(test_check_config("config_sync_interval_ms", "30000"));
    ASSERT_EQ(30000, FLAGS_config_sync_interval_ms);

    // Update config failed and value not changed.
    NO_FATALS(test_update_config({{"config_sync_interval_ms", "-1"}},
                                 R"({"update_status":"ERR_INVALID_PARAMETERS: -1 is invalid"})"
                                 "\n"));
    NO_FATALS(test_check_config("config_sync_interval_ms", "30000"));
    ASSERT_EQ(30000, FLAGS_config_sync_interval_ms);

    // Update config success and value changed.
    NO_FATALS(test_update_config({{"config_sync_interval_ms", "10"}},
                                 R"({"update_status":"ERR_OK"})"
                                 "\n"));
    NO_FATALS(test_check_config("config_sync_interval_ms", "10"));
    ASSERT_EQ(10, FLAGS_config_sync_interval_ms);
}

} // namespace replication
} // namespace dsn
