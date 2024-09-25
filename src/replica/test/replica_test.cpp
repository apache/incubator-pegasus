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

#include <stddef.h>
#include <stdint.h>
#include <atomic>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "backup_types.h"
#include "common/backup_common.h"
#include "common/fs_manager.h"
#include "common/gpid.h"
#include "common/replica_envs.h"
#include "common/replication.codes.h"
#include "common/replication_common.h"
#include "common/replication_enums.h"
#include "common/replication_other_types.h"
#include "consensus_types.h"
#include "dsn.layer2_types.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "http/http_server.h"
#include "http/http_status_code.h"
#include "metadata_types.h"
#include "replica/disk_cleaner.h"
#include "replica/replica.h"
#include "replica/replica_http_service.h"
#include "replica/replica_stub.h"
#include "replica/replication_app_base.h"
#include "replica/test/mock_utils.h"
#include "replica_test_base.h"
#include "rpc/network.sim.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_message.h"
#include "runtime/api_layer1.h"
#include "task/task_code.h"
#include "task/task_tracker.h"
#include "test_util/test_util.h"
#include "utils/autoref_ptr.h"
#include "utils/defer.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/metrics.h"
#include "utils/string_conv.h"
#include "utils/synchronize.h"
#include "utils/test_macros.h"

DSN_DECLARE_bool(fd_disabled);
DSN_DECLARE_string(cold_backup_root);
DSN_DECLARE_uint32(mutation_2pc_min_replica_count);

using pegasus::AssertEventually;

namespace dsn {
namespace replication {

class replica_test : public replica_test_base
{
public:
    replica_test()
        : _pid(gpid(2, 1)),
          _backup_id(dsn_now_ms()),
          _provider_name("local_service"),
          _policy_name("mock_policy")
    {
    }

    void SetUp() override
    {
        FLAGS_enable_http_server = false;
        mock_app_info();
        _mock_replica =
            stub->generate_replica_ptr(_app_info, _pid, partition_status::PS_PRIMARY, 1);
        _mock_replica->init_private_log(_log_dir);

        // set FLAGS_cold_backup_root manually.
        // FLAGS_cold_backup_root is set by configuration "replication.cold_backup_root",
        // which is usually the cluster_name of production clusters.
        FLAGS_cold_backup_root = "test_cluster";
    }

    int64_t get_backup_request_count() const { return _mock_replica->get_backup_request_count(); }

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
        _app_info.app_type = replication_options::kReplicaAppType;
        _app_info.is_stateful = true;
        _app_info.max_replica_count = 3;
        _app_info.partition_count = 8;
    }

    void test_on_cold_backup(const std::string user_specified_path = "")
    {
        backup_request req;
        req.pid = _pid;
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
        std::string backup_root =
            dsn::utils::filesystem::path_combine(user_specified_path, FLAGS_cold_backup_root);
        std::string current_chkpt_file =
            cold_backup::get_current_chkpt_file(backup_root, req.app_name, req.pid, req.backup_id);
        ASSERT_TRUE(dsn::utils::filesystem::file_exists(current_chkpt_file));
        int64_t size = 0;
        dsn::utils::filesystem::file_size(
            current_chkpt_file, dsn::utils::FileDataType::kSensitive, size);
        ASSERT_LT(0, size);
    }

    error_code test_find_valid_checkpoint(const std::string user_specified_path = "")
    {
        configuration_restore_request req;
        req.app_id = _app_info.app_id;
        req.app_name = _app_info.app_name;
        req.backup_provider_name = _provider_name;
        req.cluster_name = FLAGS_cold_backup_root;
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

    void test_trigger_manual_emergency_checkpoint(const decree min_checkpoint_decree,
                                                  const error_code expected_err,
                                                  std::function<void()> callback = {})
    {
        dsn::utils::notify_event op_completed;
        _mock_replica->async_trigger_manual_emergency_checkpoint(
            min_checkpoint_decree, 0, [&](error_code actual_err) {
                ASSERT_EQ(expected_err, actual_err);

                if (callback) {
                    callback();
                }

                op_completed.notify();
            });

        op_completed.wait();
    }

    bool has_gpid(gpid &pid) const
    {
        for (const auto &node : stub->_fs_manager.get_dir_nodes()) {
            if (node->has(pid)) {
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

        auto path = dsn::utils::filesystem::path_combine(
            _mock_replica->_dir, dsn::replication::replica_app_info::kAppInfo);
        std::cout << "the path of .app-info file is " << path << std::endl;

        // load new max_replica_count from file
        auto err = replica_info.load(path);
        ASSERT_EQ(ERR_OK, err);
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

    void test_auto_trash(error_code ec);

public:
    dsn::app_info _app_info;
    dsn::gpid _pid;
    mock_replica_ptr _mock_replica;

private:
    const int64_t _backup_id;
    const std::string _provider_name;
    const std::string _policy_name;
};

INSTANTIATE_TEST_SUITE_P(, replica_test, ::testing::Values(false, true));

TEST_P(replica_test, write_size_limited)
{
    const int count = 100;
    struct dsn::message_header header;
    header.body_length = 10000000;

    auto write_request = dsn::message_ex::create_request(RPC_TEST);
    auto cleanup = dsn::defer([=]() { delete write_request; });
    header.context.u.is_forwarded = false;
    write_request->header = &header;
    std::unique_ptr<tools::sim_network_provider> sim_net(
        new tools::sim_network_provider(nullptr, nullptr));
    write_request->io_session = sim_net->create_client_session(rpc_address());

    const auto initial_write_size_exceed_threshold_requests =
        METRIC_VALUE(*_mock_replica, write_size_exceed_threshold_requests);

    for (int i = 0; i < count; i++) {
        stub->on_client_write(_pid, write_request);
    }

    ASSERT_EQ(initial_write_size_exceed_threshold_requests + count,
              METRIC_VALUE(*_mock_replica, write_size_exceed_threshold_requests));
}

TEST_P(replica_test, backup_request_count)
{
    // create backup request
    struct dsn::message_header header;
    header.context.u.is_backup_request = true;
    message_ptr backup_request = dsn::message_ex::create_request(task_code());
    backup_request->header = &header;
    std::unique_ptr<tools::sim_network_provider> sim_net(
        new tools::sim_network_provider(nullptr, nullptr));
    backup_request->io_session = sim_net->create_client_session(rpc_address());

    const auto initial_backup_request_count = get_backup_request_count();
    _mock_replica->on_client_read(backup_request);
    ASSERT_EQ(initial_backup_request_count + 1, get_backup_request_count());
}

TEST_P(replica_test, query_data_version_test)
{
    replica_http_service http_svc(stub.get());
    struct query_data_version_test
    {
        std::string app_id;
        http_status_code expected_code;
        std::string expected_response_json;
    } tests[] = {{"", http_status_code::kBadRequest, "app_id should not be empty"},
                 {"wrong", http_status_code::kBadRequest, "invalid app_id=wrong"},
                 {"2", http_status_code::kOk, R"({"1":{"data_version":"1"}})"},
                 {"4", http_status_code::kNotFound, "app_id=4 not found"}};
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

TEST_P(replica_test, query_compaction_test)
{
    replica_http_service http_svc(stub.get());
    struct query_compaction_test
    {
        std::string app_id;
        http_status_code expected_code;
        std::string expected_response_json;
    } tests[] = {{"", http_status_code::kBadRequest, "app_id should not be empty"},
                 {"xxx", http_status_code::kBadRequest, "invalid app_id=xxx"},
                 {"2",
                  http_status_code::kOk,
                  R"({"status":{"finished":0,"idle":1,"queuing":0,"running":0}})"},
                 {"4",
                  http_status_code::kOk,
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

TEST_P(replica_test, update_validate_partition_hash_test)
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

TEST_P(replica_test, update_allow_ingest_behind_test)
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

TEST_P(replica_test, test_replica_backup_and_restore)
{
    // TODO(yingchun): this test last too long time, optimize it!
    return;
    test_on_cold_backup();
    auto err = test_find_valid_checkpoint();
    ASSERT_EQ(ERR_OK, err);
}

TEST_P(replica_test, test_replica_backup_and_restore_with_specific_path)
{
    // TODO(yingchun): this test last too long time, optimize it!
    return;
    std::string user_specified_path = "test/backup";
    test_on_cold_backup(user_specified_path);
    auto err = test_find_valid_checkpoint(user_specified_path);
    ASSERT_EQ(ERR_OK, err);
}

TEST_P(replica_test, test_trigger_manual_emergency_checkpoint)
{
    // There is only one replica for the unit test.
    PRESERVE_FLAG(mutation_2pc_min_replica_count);
    FLAGS_mutation_2pc_min_replica_count = 1;

    // Initially the mutation log is empty.
    ASSERT_EQ(0, _mock_replica->last_applied_decree());
    ASSERT_EQ(0, _mock_replica->last_durable_decree());

    // Commit at least an empty write to make the replica become non-empty.
    _mock_replica->update_expect_last_durable_decree(1);
    test_trigger_manual_emergency_checkpoint(1, ERR_OK);
    _mock_replica->tracker()->wait_outstanding_tasks();

    // Committing multiple empty writes (retry multiple times) might make the last
    // applied decree greater than 1.
    ASSERT_LE(1, _mock_replica->last_applied_decree());
    ASSERT_EQ(1, _mock_replica->last_durable_decree());

    test_trigger_manual_emergency_checkpoint(
        100, ERR_OK, [this]() { ASSERT_TRUE(is_checkpointing()); });
    _mock_replica->update_last_durable_decree(100);

    // There's no need to trigger checkpoint since min_checkpoint_decree <= last_durable_decree.
    test_trigger_manual_emergency_checkpoint(
        100, ERR_OK, [this]() { ASSERT_FALSE(is_checkpointing()); });

    // There's already an existing running manual emergency checkpoint task.
    force_update_checkpointing(true);
    test_trigger_manual_emergency_checkpoint(
        101, ERR_BUSY, [this]() { ASSERT_TRUE(is_checkpointing()); });

    // Wait until the running task is completed.
    _mock_replica->tracker()->wait_outstanding_tasks();
    ASSERT_FALSE(is_checkpointing());

    // The number of concurrent tasks exceeds the limit.
    test_trigger_manual_emergency_checkpoint(101, ERR_OK);
    force_update_checkpointing(false);

    PRESERVE_FLAG(max_concurrent_manual_emergency_checkpointing_count);
    FLAGS_max_concurrent_manual_emergency_checkpointing_count = 1;

    test_trigger_manual_emergency_checkpoint(
        101, ERR_TRY_AGAIN, [this]() { ASSERT_FALSE(is_checkpointing()); });
    _mock_replica->tracker()->wait_outstanding_tasks();
}

TEST_P(replica_test, test_query_last_checkpoint_info)
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
    ASSERT_STR_CONTAINS(resp.base_local_dir, "/data/checkpoint.100");
}

TEST_P(replica_test, test_clear_on_failure)
{
    // Clear up the remaining state.
    auto *dn = stub->get_fs_manager()->find_replica_dir(_app_info.app_type, _pid);
    if (dn != nullptr) {
        dsn::utils::filesystem::remove_path(dn->replica_dir(_app_info.app_type, _pid));
    }
    dn->holding_replicas.clear();

    // Disable failure detector to avoid connecting with meta server which is not started.
    FLAGS_fd_disabled = true;

    replica *rep =
        stub->generate_replica(_app_info, _pid, partition_status::PS_PRIMARY, 1, false, true);
    auto path = rep->dir();
    ASSERT_TRUE(has_gpid(_pid));

    stub->clear_on_failure(rep);

    ASSERT_FALSE(dsn::utils::filesystem::path_exists(path));
    ASSERT_FALSE(has_gpid(_pid));
}

void replica_test::test_auto_trash(error_code ec)
{
    // The replica path will only be moved to error path when encounter ERR_RDB_CORRUPTION
    // error.
    bool moved_to_err_path = (ec == ERR_RDB_CORRUPTION);

    // Clear up the remaining state.
    auto *dn = stub->get_fs_manager()->find_replica_dir(_app_info.app_type, _pid);
    if (dn != nullptr) {
        dsn::utils::filesystem::remove_path(dn->replica_dir(_app_info.app_type, _pid));
        dn->holding_replicas.clear();
    }

    // Disable failure detector to avoid connecting with meta server which is not started.
    FLAGS_fd_disabled = true;

    replica *rep =
        stub->generate_replica(_app_info, _pid, partition_status::PS_PRIMARY, 1, false, true);
    auto original_replica_path = rep->dir();
    ASSERT_TRUE(has_gpid(_pid));

    rep->handle_local_failure(ec);
    stub->wait_closing_replicas_finished();

    ASSERT_EQ(!moved_to_err_path, dsn::utils::filesystem::path_exists(original_replica_path));
    dn = stub->get_fs_manager()->get_dir_node(original_replica_path);
    ASSERT_NE(dn, nullptr);
    std::vector<std::string> subs;
    ASSERT_TRUE(dsn::utils::filesystem::get_subdirectories(dn->full_dir, subs, false));
    bool found_err_path = false;
    std::string err_path;
    const int ts_length = 16;
    size_t err_pos = original_replica_path.size() + ts_length + 1; // Add 1 for dot in path.
    for (const auto &sub : subs) {
        if (sub.size() <= original_replica_path.size()) {
            continue;
        }
        uint64_t ts = 0;
        if (sub.find(original_replica_path) == 0 && sub.find(kFolderSuffixErr) == err_pos &&
            dsn::buf2uint64(sub.substr(original_replica_path.size() + 1, ts_length), ts)) {
            err_path = sub;
            ASSERT_GT(ts, 0);
            found_err_path = true;
            break;
        }
    }
    ASSERT_EQ(moved_to_err_path, found_err_path);
    ASSERT_FALSE(has_gpid(_pid));
    ASSERT_EQ(moved_to_err_path, dn->status == disk_status::NORMAL)
        << moved_to_err_path << ", " << enum_to_string(dn->status);
    ASSERT_EQ(!moved_to_err_path, dn->status == disk_status::IO_ERROR)
        << moved_to_err_path << ", " << enum_to_string(dn->status);

    // It's safe to cleanup the .err path after been found.
    if (!err_path.empty()) {
        dsn::utils::filesystem::remove_path(err_path);
    }
}

TEST_P(replica_test, test_auto_trash_of_corruption)
{
    NO_FATALS(test_auto_trash(ERR_RDB_CORRUPTION));
}

TEST_P(replica_test, test_auto_trash_of_io_error) { NO_FATALS(test_auto_trash(ERR_DISK_IO_ERROR)); }

TEST_P(replica_test, update_deny_client_test)
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

TEST_P(replica_test, test_update_app_max_replica_count) { test_update_app_max_replica_count(); }

} // namespace replication
} // namespace dsn
