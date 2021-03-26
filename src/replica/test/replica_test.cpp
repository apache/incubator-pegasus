// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>

#include <dsn/utility/fail_point.h>
#include "replica_test_base.h"
#include <dsn/utility/defer.h>
#include <dsn/dist/replication/replica_envs.h>
#include "replica/replica_http_service.h"

namespace dsn {
namespace replication {

class replica_test : public replica_test_base
{
public:
    dsn::app_info _app_info;
    dsn::gpid pid = gpid(2, 1);
    mock_replica_ptr _mock_replica;

public:
    void SetUp() override
    {
        FLAGS_enable_http_server = false;
        stub->install_perf_counters();
        mock_app_info();
        _mock_replica = stub->generate_replica(_app_info, pid, partition_status::PS_PRIMARY, 1);
    }

    int get_write_size_exceed_threshold_count()
    {
        return stub->_counter_recent_write_size_exceed_threshold_count->get_value();
    }

    int get_table_level_backup_request_qps()
    {
        return _mock_replica->_counter_backup_request_qps->get_integer_value();
    }

    bool get_validate_partition_hash() const { return _mock_replica->_validate_partition_hash; }

    void reset_validate_partition_hash() { _mock_replica->_validate_partition_hash = false; }

    void update_validate_partition_hash(bool old_value, bool set_in_map, std::string new_value)
    {
        _mock_replica->_validate_partition_hash = old_value;
        std::map<std::string, std::string> envs;
        if (set_in_map) {
            envs[replica_envs::SPLIT_VALIDATE_PARTITION_HASH] = new_value;
        }
        _mock_replica->update_bool_envs(envs,
                                        replica_envs::SPLIT_VALIDATE_PARTITION_HASH,
                                        _mock_replica->_validate_partition_hash);
    }

    void mock_app_info()
    {
        _app_info.app_id = 2;
        _app_info.app_name = "replica_test";
        _app_info.app_type = "replica";
        _app_info.is_stateful = true;
        _app_info.max_replica_count = 3;
        _app_info.partition_count = 8;
    }
};

TEST_F(replica_test, write_size_limited)
{
    int count = 100;
    task_code default_code;
    struct dsn::message_header header;
    header.body_length = 10000000;

    auto write_request = dsn::message_ex::create_request(default_code);
    auto cleanup = dsn::defer([=]() { delete write_request; });
    write_request->header = &header;

    for (int i = 0; i < count; i++) {
        stub->on_client_write(pid, write_request);
    }

    ASSERT_EQ(get_write_size_exceed_threshold_count(), count);
}

TEST_F(replica_test, backup_request_qps)
{
    // create backup request
    struct dsn::message_header header;
    header.context.u.is_backup_request = true;
    message_ptr backup_request = dsn::message_ex::create_request(task_code());
    backup_request->header = &header;

    _mock_replica->on_client_read(backup_request);

    // We have to sleep >= 0.1s, or the value this perf-counter will be 0, according to the
    // implementation of perf-counter which type is COUNTER_TYPE_RATE.
    usleep(1e5);
    ASSERT_GT(get_table_level_backup_request_qps(), 0);
}

TEST_F(replica_test, query_data_version_test)
{
    replica_http_service http_svc(stub.get());
    struct query_data_version_test
    {
        std::string app_id;
        http_status_code expected_code;
        std::string expected_response_json;
    } tests[] = {{"", http_status_code::bad_request, "app_id should not be empty"},
                 {"wrong", http_status_code::bad_request, "invalid app_id=wrong"},
                 {"2",
                  http_status_code::ok,
                  R"({"1":{"data_version":"1"}})"},
                 {"4", http_status_code::not_found, "app_id=4 not found"}};
    for (const auto &test : tests) {
        http_request req;
        http_response resp;
        if (!test.app_id.empty()) {
            req.query_args["app_id"] = test.app_id;
        }
        http_svc.query_app_data_version_handler(req, resp);
        ASSERT_EQ(resp.status_code, test.expected_code);
        std::string expected_json = test.expected_response_json;
        ASSERT_EQ(resp.body, expected_json);
    }
}

TEST_F(replica_test, query_compaction_test)
{
    replica_http_service http_svc(stub.get());
    struct query_compaction_test
    {
        std::string app_id;
        http_status_code expected_code;
        std::string expected_response_json;
    } tests[] = {
        {"", http_status_code::bad_request, "app_id should not be empty"},
        {"xxx", http_status_code::bad_request, "invalid app_id=xxx"},
        {"2",
         http_status_code::ok,
         R"({"status":{"CompactionRunning":"0","CompactionQueue":"0","CompactionFinish":"1"}})"},
        {"4",
         http_status_code::ok,
         R"({"status":{"CompactionRunning":"0","CompactionQueue":"0","CompactionFinish":"0"}})"}};
    for (const auto &test : tests) {
        http_request req;
        http_response resp;
        if (!test.app_id.empty()) {
            req.query_args["app_id"] = test.app_id;
        }
        http_svc.query_compaction_handler(req, resp);
        ASSERT_EQ(resp.status_code, test.expected_code);
        std::string expected_json = test.expected_response_json;
        if (test.expected_code == http_status_code::ok) {
            expected_json += "\n";
        }
        ASSERT_EQ(resp.body, expected_json);
    }
}

TEST_F(replica_test, update_validate_partition_hash_test)
{
    struct update_validate_partition_hash_test
    {
        bool set_in_map;
        bool old_value;
        std::string new_value;
        bool expected_value;
    } tests[]{{true, false, "false", false},
              {true, false, "true", true},
              {true, true, "true", true},
              {true, true, "false", false},
              {false, false, "", false},
              {false, true, "", false},
              {true, true, "flase", true},
              {true, false, "ture", false}};
    for (const auto &test : tests) {
        update_validate_partition_hash(test.old_value, test.set_in_map, test.new_value);
        ASSERT_EQ(get_validate_partition_hash(), test.expected_value);
        reset_validate_partition_hash();
    }
}

} // namespace replication
} // namespace dsn
