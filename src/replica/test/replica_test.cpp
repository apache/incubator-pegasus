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

#include "common/replica_envs.h"
#include "utils/defer.h"
#include <gtest/gtest.h>
#include "utils/filesystem.h"
#include "runtime/rpc/network.sim.h"

#include "common/backup_common.h"
#include "replica_test_base.h"
#include "replica/replica.h"
#include "replica/replica_http_service.h"

namespace dsn {
namespace replication {

class replica_test : public replica_test_base
{
public:
    replica_test()
        : pid(gpid(2, 1)),
          _backup_id(dsn_now_ms()),
          _provider_name("local_service"),
          _policy_name("mock_policy")
    {
    }

    void SetUp() override
    {
        FLAGS_enable_http_server = false;
        stub->install_perf_counters();
        mock_app_info();
        _mock_replica = stub->generate_replica_ptr(_app_info, pid, partition_status::PS_PRIMARY, 1);

        // set cold_backup_root manually.
        // `cold_backup_root` is set by configuration "replication.cold_backup_root",
        // which is usually the cluster_name of production clusters.
        _mock_replica->_options->cold_backup_root = "test_cluster";
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

    bool get_allow_ingest_behind() const { return _mock_replica->_allow_ingest_behind; }

    void reset_allow_ingest_behind() { _mock_replica->_allow_ingest_behind = false; }

    void update_allow_ingest_behind(bool old_value, bool set_in_map, std::string new_value)
    {
        _mock_replica->_allow_ingest_behind = old_value;
        std::map<std::string, std::string> envs;
        if (set_in_map) {
            envs[replica_envs::ROCKSDB_ALLOW_INGEST_BEHIND] = new_value;
        }
        _mock_replica->update_bool_envs(
            envs, replica_envs::ROCKSDB_ALLOW_INGEST_BEHIND, _mock_replica->_allow_ingest_behind);
    }

    const deny_client &update_deny_client(const std::string &env_name, std::string env_value)
    {
        std::map<std::string, std::string> envs;
        envs[env_name] = std::move(env_value);
        _mock_replica->update_deny_client(envs);
        return _mock_replica->_deny_client;
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

    void test_on_cold_backup(const std::string user_specified_path = "")
    {
        backup_request req;
        req.pid = pid;
        policy_info backup_policy_info;
        backup_policy_info.__set_backup_provider_type(_provider_name);
        backup_policy_info.__set_policy_name(_policy_name);
        req.policy = backup_policy_info;
        req.app_name = _app_info.app_name;
        req.backup_id = _backup_id;
        if (!user_specified_path.empty()) {
            req.__set_backup_path(user_specified_path);
        }

        // test cold backup could complete.
        backup_response resp;
        do {
            _mock_replica->on_cold_backup(req, resp);
        } while (resp.err == ERR_BUSY);
        ASSERT_EQ(ERR_OK, resp.err);

        // test checkpoint files have been uploaded successfully.
        std::string backup_root = dsn::utils::filesystem::path_combine(
            user_specified_path, _mock_replica->_options->cold_backup_root);
        std::string current_chkpt_file =
            cold_backup::get_current_chkpt_file(backup_root, req.app_name, req.pid, req.backup_id);
        ASSERT_TRUE(dsn::utils::filesystem::file_exists(current_chkpt_file));
        int64_t size = 0;
        dsn::utils::filesystem::file_size(current_chkpt_file, size);
        ASSERT_LT(0, size);
    }

    error_code test_find_valid_checkpoint(const std::string user_specified_path = "")
    {
        configuration_restore_request req;
        req.app_id = _app_info.app_id;
        req.app_name = _app_info.app_name;
        req.backup_provider_name = _provider_name;
        req.cluster_name = _mock_replica->_options->cold_backup_root;
        req.time_stamp = _backup_id;
        if (!user_specified_path.empty()) {
            req.__set_restore_path(user_specified_path);
        }

        std::string remote_chkpt_dir;
        return _mock_replica->find_valid_checkpoint(req, remote_chkpt_dir);
    }

    void force_update_checkpointing(bool running)
    {
        _mock_replica->_is_manual_emergency_checkpointing = running;
    }

    bool is_checkpointing() { return _mock_replica->_is_manual_emergency_checkpointing; }

    replica *call_clear_on_failure(replica_stub *stub,
                                   replica *rep,
                                   const std::string &path,
                                   const gpid &gpid)
    {
        return replica::clear_on_failure(stub, rep, path, gpid);
    }

    bool has_gpid(gpid &gpid) const
    {
        for (const auto &node : stub->_fs_manager._dir_nodes) {
            if (node->has(gpid)) {
                return true;
            }
        }
        return false;
    }

    void test_update_app_max_replica_count()
    {
        const auto reserved_max_replica_count = _app_info.max_replica_count;
        const int32_t target_max_replica_count = 5;
        CHECK_NE(target_max_replica_count, reserved_max_replica_count);

        // store new max_replica_count into file
        _mock_replica->update_app_max_replica_count(target_max_replica_count);
        _app_info.max_replica_count = target_max_replica_count;

        dsn::app_info info;
        replica_app_info replica_info(&info);

        auto path = dsn::utils::filesystem::path_combine(_mock_replica->_dir,
                                                         dsn::replication::replica::kAppInfo);
        std::cout << "the path of .app-info file is " << path << std::endl;

        // load new max_replica_count from file
        auto err = replica_info.load(path);
        ASSERT_EQ(err, ERR_OK);
        ASSERT_EQ(info, _mock_replica->_app_info);
        std::cout << "the loaded new app_info is " << info << std::endl;

        // recover original max_replica_count
        _mock_replica->update_app_max_replica_count(reserved_max_replica_count);
        _app_info.max_replica_count = reserved_max_replica_count;

        // load original max_replica_count from file
        err = replica_info.load(path);
        ASSERT_EQ(err, ERR_OK);
        ASSERT_EQ(info, _mock_replica->_app_info);
        std::cout << "the loaded original app_info is " << info << std::endl;
    }

public:
    dsn::app_info _app_info;
    dsn::gpid pid;
    mock_replica_ptr _mock_replica;

private:
    const int64_t _backup_id;
    const std::string _provider_name;
    const std::string _policy_name;
};

TEST_F(replica_test, write_size_limited)
{
    int count = 100;
    struct dsn::message_header header;
    header.body_length = 10000000;

    auto write_request = dsn::message_ex::create_request(RPC_TEST);
    auto cleanup = dsn::defer([=]() { delete write_request; });
    write_request->header = &header;
    std::unique_ptr<tools::sim_network_provider> sim_net(
        new tools::sim_network_provider(nullptr, nullptr));
    write_request->io_session = sim_net->create_client_session(rpc_address());

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
    std::unique_ptr<tools::sim_network_provider> sim_net(
        new tools::sim_network_provider(nullptr, nullptr));
    backup_request->io_session = sim_net->create_client_session(rpc_address());

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
    } tests[] = {{"", http_status_code::bad_request, "app_id should not be empty"},
                 {"xxx", http_status_code::bad_request, "invalid app_id=xxx"},
                 {"2",
                  http_status_code::ok,
                  R"({"status":{"finished":0,"idle":1,"queuing":0,"running":0}})"},
                 {"4",
                  http_status_code::ok,
                  R"({"status":{"finished":0,"idle":0,"queuing":0,"running":0}})"}};
    for (const auto &test : tests) {
        http_request req;
        http_response resp;
        if (!test.app_id.empty()) {
            req.query_args["app_id"] = test.app_id;
        }
        http_svc.query_manual_compaction_handler(req, resp);
        ASSERT_EQ(resp.status_code, test.expected_code);
        ASSERT_EQ(resp.body, test.expected_response_json);
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

TEST_F(replica_test, update_allow_ingest_behind_test)
{
    struct update_allow_ingest_behind_test
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
        update_allow_ingest_behind(test.old_value, test.set_in_map, test.new_value);
        ASSERT_EQ(get_allow_ingest_behind(), test.expected_value);
        reset_allow_ingest_behind();
    }
}

TEST_F(replica_test, test_replica_backup_and_restore)
{
    test_on_cold_backup();
    auto err = test_find_valid_checkpoint();
    ASSERT_EQ(ERR_OK, err);
}

TEST_F(replica_test, test_replica_backup_and_restore_with_specific_path)
{
    std::string user_specified_path = "test/backup";
    test_on_cold_backup(user_specified_path);
    auto err = test_find_valid_checkpoint(user_specified_path);
    ASSERT_EQ(ERR_OK, err);
}

TEST_F(replica_test, test_trigger_manual_emergency_checkpoint)
{
    ASSERT_EQ(_mock_replica->trigger_manual_emergency_checkpoint(100), ERR_OK);
    ASSERT_TRUE(is_checkpointing());
    _mock_replica->update_last_durable_decree(100);

    // test no need start checkpoint because `old_decree` < `last_durable`
    ASSERT_EQ(_mock_replica->trigger_manual_emergency_checkpoint(100), ERR_OK);
    ASSERT_FALSE(is_checkpointing());

    // test has existed running task
    force_update_checkpointing(true);
    ASSERT_EQ(_mock_replica->trigger_manual_emergency_checkpoint(101), ERR_BUSY);
    ASSERT_TRUE(is_checkpointing());
    // test running task completed
    _mock_replica->tracker()->wait_outstanding_tasks();
    ASSERT_FALSE(is_checkpointing());

    // test exceed max concurrent count
    ASSERT_EQ(_mock_replica->trigger_manual_emergency_checkpoint(101), ERR_OK);
    force_update_checkpointing(false);
    FLAGS_max_concurrent_manual_emergency_checkpointing_count = 1;
    ASSERT_EQ(_mock_replica->trigger_manual_emergency_checkpoint(101), ERR_TRY_AGAIN);
    ASSERT_FALSE(is_checkpointing());
    _mock_replica->tracker()->wait_outstanding_tasks();
}

TEST_F(replica_test, test_query_last_checkpoint_info)
{
    // test no exist gpid
    auto req = std::make_unique<learn_request>();
    req->pid = gpid(100, 100);
    query_last_checkpoint_info_rpc rpc =
        query_last_checkpoint_info_rpc(std::move(req), RPC_QUERY_LAST_CHECKPOINT_INFO);
    stub->on_query_last_checkpoint(rpc);
    ASSERT_EQ(rpc.response().err, ERR_OBJECT_NOT_FOUND);

    learn_response resp;
    // last_checkpoint hasn't exist
    _mock_replica->on_query_last_checkpoint(resp);
    ASSERT_EQ(resp.err, ERR_PATH_NOT_FOUND);

    // query ok
    _mock_replica->update_last_durable_decree(100);
    _mock_replica->set_last_committed_decree(200);
    _mock_replica->on_query_last_checkpoint(resp);
    ASSERT_EQ(resp.last_committed_decree, 200);
    ASSERT_EQ(resp.base_local_dir, "./data/checkpoint.100");
}

TEST_F(replica_test, test_clear_on_failer)
{
    replica *rep =
        stub->generate_replica(_app_info, pid, partition_status::PS_PRIMARY, 1, false, true);
    auto path = stub->get_replica_dir(_app_info.app_type.c_str(), pid);
    dsn::utils::filesystem::create_directory(path);
    ASSERT_TRUE(dsn::utils::filesystem::path_exists(path));
    ASSERT_TRUE(has_gpid(pid));

    ASSERT_FALSE(call_clear_on_failure(stub.get(), rep, path, pid));

    ASSERT_FALSE(dsn::utils::filesystem::path_exists(path));
    ASSERT_FALSE(has_gpid(pid));
}

TEST_F(replica_test, update_deny_client_test)
{
    struct update_deny_client_test
    {
        std::string env_name;
        std::string env_value;
        deny_client expected;
    } tests[]{{"invalid", "invalid", {false, false, false}},
              {"replica.deny_client_request", "reconfig*all", {true, true, true}},
              {"replica.deny_client_request", "reconfig*write", {false, true, true}},
              {"replica.deny_client_request", "reconfig*read", {true, false, true}},
              {"replica.deny_client_request", "timeout*all", {true, true, false}},
              {"replica.deny_client_request", "timeout*write", {false, true, false}},
              {"replica.deny_client_request", "timeout*read", {true, false, false}}};
    for (const auto &test : tests) {
        ASSERT_EQ(update_deny_client(test.env_name, test.env_value), test.expected);
    }
}

TEST_F(replica_test, test_update_app_max_replica_count) { test_update_app_max_replica_count(); }

} // namespace replication
} // namespace dsn
