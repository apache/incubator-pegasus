// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/tool-api/http_server.h>

#include "dist/replication/meta_server/meta_http_service.h"
#include "meta_test_base.h"

namespace dsn {
namespace replication {

class meta_http_service_test : public meta_test_base
{
public:
    void SetUp() override
    {
        meta_test_base::SetUp();
        _mhs = dsn::make_unique<meta_http_service>(_ms.get());
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
        std::string fake_json = R"({")" + env_key + R"(":)" +
                                R"(")" + env_value + R"(",)" +
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
        _mhs = dsn::make_unique<meta_http_service>(_ms.get());
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

} // namespace replication
} // namespace dsn
