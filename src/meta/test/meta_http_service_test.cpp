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

#include <chrono>
#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "backup_types.h"
#include "bulk_load_types.h"
#include "common/gpid.h"
#include "common/replication_other_types.h"
#include "gtest/gtest.h"
#include "http/http_server.h"
#include "meta/meta_backup_service.h"
#include "meta/meta_bulk_load_service.h"
#include "meta/meta_data.h"
#include "meta/meta_http_service.h"
#include "meta/meta_service.h"
#include "meta/meta_state_service.h"
#include "meta_service_test_app.h"
#include "meta_test_base.h"
#include "runtime/rpc/rpc_holder.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/task/task.h"
#include "runtime/task/task_code.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/chrono_literals.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"

namespace dsn {
namespace replication {

class meta_http_service_test : public meta_test_base
{
public:
    void SetUp() override
    {
        meta_test_base::SetUp();
        FLAGS_enable_http_server = false; // disable http server
        _mhs = std::make_unique<meta_http_service>(_ms.get());
        create_app(test_app);
    }

    /// === Tests ===

    void test_get_app_from_primary()
    {
        http_request fake_req;
        http_response fake_resp;
        fake_req.query_args.emplace("name", test_app);
        _mhs->get_app_handler(fake_req, fake_resp);

        ASSERT_EQ(fake_resp.status_code, http_status_code::ok)
            << http_status_code_to_string(fake_resp.status_code);
        std::string fake_json = R"({"general":{"app_name":")" + test_app + R"(","app_id":"2)" +
                                R"(","partition_count":"8","max_replica_count":"3"}})" + "\n";
        ASSERT_EQ(fake_resp.body, fake_json);
    }

    void test_get_app_envs()
    {
        // set app env
        std::string env_key = "replica.slow_query_threshold";
        std::string env_value = "100";
        update_app_envs(test_app, {env_key}, {env_value});

        // http get app envs
        http_request fake_req;
        http_response fake_resp;
        fake_req.query_args.emplace("name", test_app);
        _mhs->get_app_envs_handler(fake_req, fake_resp);

        // env (value_version, 1) was set by create_app
        std::string fake_json = R"({")" + env_key + R"(":)" + R"(")" + env_value + R"(",)" +
                                R"("value_version":"1"})" + "\n";
        ASSERT_EQ(fake_resp.status_code, http_status_code::ok)
            << http_status_code_to_string(fake_resp.status_code);
        ASSERT_EQ(fake_resp.body, fake_json);
    }

    std::unique_ptr<meta_http_service> _mhs;
    std::string test_app = "test_meta_http";
};

class meta_backup_test_base : public meta_test_base
{
public:
    void SetUp() override
    {
        meta_test_base::SetUp();

        _ms->_backup_handler = std::make_shared<backup_service>(
            _ms.get(),
            _ms->_cluster_root + "/backup_meta",
            _ms->_cluster_root + "/backup",
            [](backup_service *bs) { return std::make_shared<policy_context>(bs); });
        _ms->_backup_handler->start();
        _ms->_backup_handler->backup_option().app_dropped_retry_delay_ms = 500_ms;
        _ms->_backup_handler->backup_option().request_backup_period_ms = 20_ms;
        _ms->_backup_handler->backup_option().issue_backup_interval_ms = 1000_ms;
        const std::string policy_root = "/test";
        dsn::error_code ec;
        _ms->_storage
            ->create_node(
                _policy_root, dsn::TASK_CODE_EXEC_INLINED, [&ec](dsn::error_code err) { ec = err; })
            ->wait();
        _mhs = std::make_unique<meta_http_service>(_ms.get());
        create_app(test_app);
    }

    void add_backup_policy(const std::string &policy_name)
    {
        static const std::string test_policy_name = policy_name;
        const std::string policy_root = "/test";

        configuration_add_backup_policy_request request;
        configuration_add_backup_policy_response response;

        request.policy_name = policy_name;
        request.backup_provider_type = "local_service";
        request.backup_interval_seconds = 24 * 60 * 60;
        request.backup_history_count_to_keep = 1;
        request.start_time = "12:00";
        request.app_ids.clear();
        request.app_ids.push_back(2);

        auto result = fake_create_policy(_ms->_backup_handler.get(), request);

        fake_wait_rpc(result, response);
        // need to fix
        ASSERT_EQ(response.err, ERR_OK);
    }

    void test_get_backup_policy(const std::string &name,
                                const std::string &expected_json,
                                const http_status_code &http_status)
    {
        http_request req;
        http_response resp;
        if (!name.empty())
            req.query_args.emplace("name", name);
        _mhs->query_backup_policy_handler(req, resp);
        ASSERT_EQ(resp.status_code, http_status) << http_status_code_to_string(resp.status_code);
        ASSERT_EQ(resp.body, expected_json);
    }

protected:
    const std::string _policy_root = "/test";

    std::unique_ptr<meta_http_service> _mhs;
    std::string test_app = "test_meta_http";
};

TEST_F(meta_http_service_test, get_app_from_primary) { test_get_app_from_primary(); }

TEST_F(meta_http_service_test, get_app_envs) { test_get_app_envs(); }

TEST_F(meta_backup_test_base, get_backup_policy)
{
    struct http_backup_policy_test
    {
        std::string name;
        std::string expected_json;
        http_status_code http_status;
    } tests[5] = {
        {"", "{}\n", http_status_code::ok},
        {"TEST1",
         "{\"TEST1\":{\"name\":\"TEST1\",\"backup_provider_type\":\"local_service\","
         "\"backup_interval\":\"86400\",\"app_ids\":\"[2]\",\"start_time\":\"12:00\","
         "\"status\":\"enabled\",\"backup_history_count\":\"1\"}}\n",
         http_status_code::ok},
        {"TEST2",
         "{\"TEST2\":{\"name\":\"TEST2\",\"backup_provider_type\":\"local_service\","
         "\"backup_interval\":\"86400\",\"app_ids\":\"[2]\",\"start_time\":\"12:00\","
         "\"status\":\"enabled\",\"backup_history_count\":\"1\"}}\n",
         http_status_code::ok},
        {"",
         "{\"TEST1\":{\"name\":\"TEST1\",\"backup_provider_type\":\"local_service\",\"backup_"
         "interval\":\"86400\",\"app_ids\":\"[2]\",\"start_time\":\"12:00\",\"status\":\"enabled\","
         "\"backup_history_count\":\"1\"},\"TEST2\":{\"name\":\"TEST2\",\"backup_provider_"
         "type\":\"local_service\",\"backup_interval\":\"86400\",\"app_ids\":\"[2]\",\"start_"
         "time\":\"12:00\",\"status\":\"enabled\",\"backup_history_count\":\"1\"}}\n",
         http_status_code::ok},
        {"TEST3", "{}\n", http_status_code::ok},
    };
    test_get_backup_policy(tests[0].name, tests[0].expected_json, tests[0].http_status);
    add_backup_policy("TEST1");
    test_get_backup_policy(tests[1].name, tests[1].expected_json, tests[1].http_status);
    add_backup_policy("TEST2");
    test_get_backup_policy(tests[2].name, tests[2].expected_json, tests[2].http_status);
    test_get_backup_policy(tests[3].name, tests[3].expected_json, tests[3].http_status);
    test_get_backup_policy(tests[4].name, tests[4].expected_json, tests[4].http_status);
}

class meta_bulk_load_http_test : public meta_test_base
{
public:
    void SetUp() override
    {
        meta_test_base::SetUp();
        FLAGS_enable_http_server = false;
        _mhs = std::make_unique<meta_http_service>(_ms.get());
        create_app(APP_NAME);
    }

    void TearDown() override
    {
        drop_app(APP_NAME);
        _mhs = nullptr;
        meta_test_base::TearDown();
    }

    http_response test_start_bulk_load(std::string req_body_json)
    {
        http_request req;
        http_response resp;
        req.body = blob::create_from_bytes(std::move(req_body_json));
        _mhs->start_bulk_load_handler(req, resp);
        return resp;
    }

    std::string test_query_bulk_load(const std::string &app_name)
    {
        http_request req;
        http_response resp;
        req.query_args.emplace("name", app_name);
        _mhs->query_bulk_load_handler(req, resp);
        return resp.body;
    }

    http_response test_start_compaction(std::string req_body_json)
    {
        http_request req;
        http_response resp;
        req.body = blob::create_from_bytes(std::move(req_body_json));
        _mhs->start_compaction_handler(req, resp);
        return resp;
    }

    http_response test_update_scenario(std::string req_body_json)
    {
        http_request req;
        http_response resp;
        req.body = blob::create_from_bytes(std::move(req_body_json));
        _mhs->update_scenario_handler(req, resp);
        return resp;
    }

    void mock_bulk_load_context(const bulk_load_status::type &status)
    {
        auto app = find_app(APP_NAME);
        app->is_bulk_loading = true;
        const auto app_id = app->app_id;
        bulk_svc()._bulk_load_app_id.insert(app_id);
        bulk_svc()._apps_in_progress_count[app_id] = app->partition_count;
        bulk_svc()._app_bulk_load_info[app_id].status = status;
        for (int i = 0; i < app->partition_count; ++i) {
            gpid pid = gpid(app_id, i);
            bulk_svc()._partition_bulk_load_info[pid].status = status;
        }
    }

    void reset_local_bulk_load_states()
    {
        auto app = find_app(APP_NAME);
        bulk_svc().reset_local_bulk_load_states(app->app_id, APP_NAME, true);
        app->is_bulk_loading = false;
    }

protected:
    std::unique_ptr<meta_http_service> _mhs;
    std::string APP_NAME = "test_bulk_load";
};

TEST_F(meta_bulk_load_http_test, start_bulk_load_request)
{
    fail::setup();
    fail::cfg("meta_on_start_bulk_load", "return()");
    struct start_bulk_load_test
    {
        std::string request_json;
        http_status_code expected_code;
        std::string expected_response_json;
    } tests[] = {
        {R"({"app":"test_bulk_load","cluster_name":"onebox","file_provider_type":"local_service","remote_root_path":"bulk_load_root"})",
         http_status_code::bad_request,
         "invalid request structure"},
        {R"({"app_name":"test_bulk_load","cluster_name":"onebox","file_provider_type":"","remote_root_path":"bulk_load_root"})",
         http_status_code::bad_request,
         "file_provider_type should not be empty"},
        {R"({"app_name":"test_bulk_load","cluster_name":"onebox","file_provider_type":"local_service","remote_root_path":"bulk_load_root"})",
         http_status_code::ok,
         R"({"error":"ERR_OK","hint_msg":""})"},
    };
    for (const auto &test : tests) {
        http_response resp = test_start_bulk_load(test.request_json);
        ASSERT_EQ(resp.status_code, test.expected_code);
        std::string expected_json = test.expected_response_json;
        if (test.expected_code == http_status_code::ok) {
            expected_json += "\n";
        }
        ASSERT_EQ(resp.body, expected_json);
    }
    fail::teardown();
}

TEST_F(meta_bulk_load_http_test, query_bulk_load_request)
{
    const std::string NOT_BULK_LOAD = "not_bulk_load_app";
    const std::string NOT_FOUND = "app_not_exist";

    create_app(NOT_BULK_LOAD);
    mock_bulk_load_context(bulk_load_status::BLS_DOWNLOADING);

    struct query_bulk_load_test
    {
        std::string app_name;
        std::string expected_json;
    } tests[] = {
        {APP_NAME,
         R"({"error":"ERR_OK","app_status":"replication::bulk_load_status::BLS_DOWNLOADING"})"},
        {NOT_BULK_LOAD,
         R"({"error":"ERR_OK","app_status":"replication::bulk_load_status::BLS_INVALID"})"},
        {NOT_FOUND,
         R"({"error":"ERR_APP_NOT_EXIST","app_status":"replication::bulk_load_status::BLS_INVALID"})"}};
    for (const auto &test : tests) {
        ASSERT_EQ(test_query_bulk_load(test.app_name), test.expected_json + "\n");
    }

    drop_app(NOT_BULK_LOAD);
}

TEST_F(meta_bulk_load_http_test, start_compaction_test)
{
    struct start_compaction_test
    {
        std::string request_json;
        http_status_code expected_code;
        std::string expected_response_json;
    } tests[] = {
        {R"({"app_name":"test_bulk_load","type":"once","target_level":-1,"bottommost_level_compaction":"skip","max_concurrent_running_count":"0"})",
         http_status_code::bad_request,
         "invalid request structure"},
        {R"({"app_name":"test_bulk_load","type":"wrong","target_level":-1,"bottommost_level_compaction":"skip","max_concurrent_running_count":0,"trigger_time":""})",
         http_status_code::bad_request,
         "type should ony be 'once' or 'periodic'"},
        {R"({"app_name":"test_bulk_load","type":"once","target_level":-3,"bottommost_level_compaction":"skip","max_concurrent_running_count":0,"trigger_time":""})",
         http_status_code::bad_request,
         "target_level should be >= -1"},
        {R"({"app_name":"test_bulk_load","type":"once","target_level":-1,"bottommost_level_compaction":"wrong","max_concurrent_running_count":0,"trigger_time":""})",
         http_status_code::bad_request,
         "bottommost_level_compaction should ony be 'skip' or 'force'"},
        {R"({"app_name":"test_bulk_load","type":"once","target_level":-1,"bottommost_level_compaction":"skip","max_concurrent_running_count":-2,"trigger_time":""})",
         http_status_code::bad_request,
         "max_running_count should be >= 0"},
        {R"({"app_name":"test_bulk_load","type":"once","target_level":-1,"bottommost_level_compaction":"skip","max_concurrent_running_count":0,"trigger_time":""})",
         http_status_code::ok,
         R"({"error":"ERR_OK","hint_message":""})"}};

    for (const auto &test : tests) {
        http_response resp = test_start_compaction(test.request_json);
        ASSERT_EQ(resp.status_code, test.expected_code);
        std::string expected_json = test.expected_response_json;
        if (test.expected_code == http_status_code::ok) {
            expected_json += "\n";
        }
        ASSERT_EQ(resp.body, expected_json);
    }
}

TEST_F(meta_bulk_load_http_test, update_scenario_test)
{
    struct update_scenario_test
    {
        std::string request_json;
        http_status_code expected_code;
        std::string expected_response_json;
    } tests[] = {{R"({"app":"test_bulk_load","scenario":"normal"})",
                  http_status_code::bad_request,
                  "invalid request structure"},
                 {R"({"app_name":"test_bulk_load","scenario":"wrong"})",
                  http_status_code::bad_request,
                  "scenario should ony be 'normal' or 'bulk_load'"},
                 {R"({"app_name":"test_bulk_load","scenario":"bulk_load"})",
                  http_status_code::ok,
                  R"({"error":"ERR_OK","hint_message":""})"}};

    for (const auto &test : tests) {
        http_response resp = test_update_scenario(test.request_json);
        ASSERT_EQ(resp.status_code, test.expected_code);
        std::string expected_json = test.expected_response_json;
        if (test.expected_code == http_status_code::ok) {
            expected_json += "\n";
        }
        ASSERT_EQ(resp.body, expected_json);
    }
}

} // namespace replication
} // namespace dsn
