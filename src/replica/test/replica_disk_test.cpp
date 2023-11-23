/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <fmt/core.h>
#include <unistd.h>
#include <atomic>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/fs_manager.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "metadata_types.h"
#include "replica/disk_cleaner.h"
#include "replica/replica.h"
#include "replica/replica_stub.h"
#include "replica/test/mock_utils.h"
#include "replica_admin_types.h"
#include "replica_disk_test_base.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_holder.h"
#include "test_util/test_util.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"

using pegasus::AssertEventually;

namespace dsn {
namespace replication {
DSN_DECLARE_bool(fd_disabled);

using query_disk_info_rpc = rpc_holder<query_disk_info_request, query_disk_info_response>;

class replica_disk_test : public replica_disk_test_base
{
public:
    query_disk_info_rpc fake_query_disk_rpc;

public:
    void SetUp() override {}

    void generate_fake_rpc()
    {
        // create RPC_QUERY_DISK_INFO fake request
        auto query_request = std::make_unique<query_disk_info_request>();
        fake_query_disk_rpc = query_disk_info_rpc(std::move(query_request), RPC_QUERY_DISK_INFO);
    }

    error_code send_add_new_disk_rpc(const std::string disk_str)
    {
        auto add_disk_request = std::make_unique<add_new_disk_request>();
        add_disk_request->disk_str = disk_str;
        auto rpc = add_new_disk_rpc(std::move(add_disk_request), RPC_QUERY_DISK_INFO);
        stub->on_add_new_disk(rpc);
        error_code err = rpc.response().err;
        if (err != ERR_OK) {
            LOG_INFO("error msg: {}", rpc.response().err_hint);
        }
        return err;
    }
};

INSTANTIATE_TEST_CASE_P(, replica_disk_test, ::testing::Values(false, true));

TEST_P(replica_disk_test, on_query_disk_info_all_app)
{
    generate_fake_rpc();
    stub->on_query_disk_info(fake_query_disk_rpc);

    query_disk_info_response &disk_info_response = fake_query_disk_rpc.response();
    // test response disk_info
    ASSERT_EQ(disk_info_response.total_capacity_mb, 2500);
    ASSERT_EQ(disk_info_response.total_available_mb, 750);

    auto &disk_infos = disk_info_response.disk_infos;
    ASSERT_EQ(disk_infos.size(), 6);

    int info_size = disk_infos.size();
    int app_id_1_partition_index = 0;
    int app_id_2_partition_index = 0;
    for (int i = 0; i < info_size; i++) {
        if (disk_infos[i].holding_primary_replicas.empty() &&
            disk_infos[i].holding_secondary_replicas.empty()) {
            continue;
        }
        ASSERT_EQ(disk_infos[i].tag, "tag_" + std::to_string(i + 1));
        ASSERT_EQ(disk_infos[i].full_dir, "./tag_" + std::to_string(i + 1));
        ASSERT_EQ(disk_infos[i].disk_capacity_mb, 500);
        ASSERT_EQ(disk_infos[i].disk_available_mb, (i + 1) * 50);
        ASSERT_EQ(disk_infos[i].holding_primary_replicas.size(), 2);
        ASSERT_EQ(disk_infos[i].holding_secondary_replicas.size(), 2);

        // test the gpid of app_id_1
        // test primary
        ASSERT_EQ(disk_infos[i].holding_primary_replicas[app_info_1.app_id].size(),
                  app_id_1_primary_count_for_disk);
        for (std::set<gpid>::iterator it =
                 disk_infos[i].holding_primary_replicas[app_info_1.app_id].begin();
             it != disk_infos[i].holding_primary_replicas[app_info_1.app_id].end();
             it++) {
            ASSERT_EQ(it->get_app_id(), app_info_1.app_id);
            ASSERT_EQ(it->get_partition_index(), app_id_1_partition_index++);
        }
        // test secondary
        ASSERT_EQ(disk_infos[i].holding_secondary_replicas[app_info_1.app_id].size(),
                  app_id_1_secondary_count_for_disk);
        for (std::set<gpid>::iterator it =
                 disk_infos[i].holding_secondary_replicas[app_info_1.app_id].begin();
             it != disk_infos[i].holding_secondary_replicas[app_info_1.app_id].end();
             it++) {
            ASSERT_EQ(it->get_app_id(), app_info_1.app_id);
            ASSERT_EQ(it->get_partition_index(), app_id_1_partition_index++);
        }

        // test the gpid of app_id_2
        // test primary
        ASSERT_EQ(disk_infos[i].holding_primary_replicas[app_info_2.app_id].size(),
                  app_id_2_primary_count_for_disk);
        for (std::set<gpid>::iterator it =
                 disk_infos[i].holding_primary_replicas[app_info_2.app_id].begin();
             it != disk_infos[i].holding_primary_replicas[app_info_2.app_id].end();
             it++) {
            ASSERT_EQ(it->get_app_id(), app_info_2.app_id);
            ASSERT_EQ(it->get_partition_index(), app_id_2_partition_index++);
        }
        // test secondary
        ASSERT_EQ(disk_infos[i].holding_secondary_replicas[app_info_2.app_id].size(),
                  app_id_2_secondary_count_for_disk);
        for (std::set<gpid>::iterator it =
                 disk_infos[i].holding_secondary_replicas[app_info_2.app_id].begin();
             it != disk_infos[i].holding_secondary_replicas[app_info_2.app_id].end();
             it++) {
            ASSERT_EQ(it->get_app_id(), app_info_2.app_id);
            ASSERT_EQ(it->get_partition_index(), app_id_2_partition_index++);
        }
    }
}

TEST_P(replica_disk_test, on_query_disk_info_app_not_existed)
{
    generate_fake_rpc();
    query_disk_info_request &request = *fake_query_disk_rpc.mutable_request();
    request.app_name = "not_existed_app";
    stub->on_query_disk_info(fake_query_disk_rpc);
    ASSERT_EQ(fake_query_disk_rpc.response().err, ERR_OBJECT_NOT_FOUND);
}

TEST_P(replica_disk_test, on_query_disk_info_one_app)
{
    generate_fake_rpc();
    query_disk_info_request &request = *fake_query_disk_rpc.mutable_request();

    request.app_name = app_info_1.app_name;
    stub->on_query_disk_info(fake_query_disk_rpc);

    for (auto disk_info : fake_query_disk_rpc.response().disk_infos) {
        if (disk_info.holding_primary_replicas.empty() &&
            disk_info.holding_secondary_replicas.empty()) {
            continue;
        }
        ASSERT_EQ(disk_info.holding_primary_replicas.size(), 1);
        ASSERT_EQ(disk_info.holding_secondary_replicas.size(), 1);
        ASSERT_EQ(disk_info.holding_primary_replicas[app_info_1.app_id].size(),
                  app_id_1_primary_count_for_disk);
        ASSERT_EQ(disk_info.holding_secondary_replicas[app_info_1.app_id].size(),
                  app_id_1_secondary_count_for_disk);
        ASSERT_TRUE(disk_info.holding_primary_replicas.find(app_info_2.app_id) ==
                    disk_info.holding_primary_replicas.end());
        ASSERT_TRUE(disk_info.holding_secondary_replicas.find(app_info_2.app_id) ==
                    disk_info.holding_secondary_replicas.end());
    }
}

TEST_P(replica_disk_test, check_data_dir_removable)
{
    struct test_case
    {
        std::string path;
        bool expected_removable;
        bool expected_invalid;
    } tests[] = {{"./replica.0.err", true, true},
                 {"./replica.1.gar", true, true},
                 {"./replica.2.tmp", true, true},
                 {"./replica.3.ori", true, true},
                 {"./replica.4.bak", false, true},
                 {"./replica.5.abcde", false, false},
                 {"./replica.6.x", false, false},
                 {"./replica.7.8", false, false}};

    for (const auto &test : tests) {
        EXPECT_EQ(test.expected_removable, is_data_dir_removable(test.path));
        EXPECT_EQ(test.expected_invalid, is_data_dir_invalid(test.path));
    }
}

TEST_P(replica_disk_test, gc_disk_useless_dir)
{
    PRESERVE_FLAG(gc_disk_error_replica_interval_seconds);
    PRESERVE_FLAG(gc_disk_garbage_replica_interval_seconds);
    PRESERVE_FLAG(gc_disk_migration_origin_replica_interval_seconds);
    PRESERVE_FLAG(gc_disk_migration_tmp_replica_interval_seconds);

    FLAGS_gc_disk_error_replica_interval_seconds = 1;
    FLAGS_gc_disk_garbage_replica_interval_seconds = 1;
    FLAGS_gc_disk_migration_origin_replica_interval_seconds = 1;
    FLAGS_gc_disk_migration_tmp_replica_interval_seconds = 1;

    struct test_case
    {
        std::string path;
        bool expected_exists;
    } tests[] = {{"./replica1.err", false},
                 {"./replica2.err", false},
                 {"./replica.gar", false},
                 {"./replica.tmp", false},
                 {"./replica.ori", false},
                 {"./replica.bak", true},
                 {"./replica.1.1", true},
                 {"./1.1.pegasus.1234567890.err", false},
                 {"./1.2.pegasus.0123456789.gar", false},
                 {"./2.1.pegasus.1234567890123456.err", false},
                 {"./2.2.pegasus.1234567890abcdef.gar", false},
                 {fmt::format("./1.1.pegasus.{}.err", dsn_now_us()), false},
                 {fmt::format("./2.1.pegasus.{}.gar", dsn_now_us()), false},
                 {fmt::format("./1.2.pegasus.{}.gar", dsn_now_us() + 1000 * 1000 * 1000), true},
                 {fmt::format("./2.2.pegasus.{}.err", dsn_now_us() + 1000 * 1000 * 1000), true}};

    for (const auto &test : tests) {
        // Ensure that every directory does not exist and should be created.
        CHECK_TRUE(utils::filesystem::create_directory(test.path));
        ASSERT_TRUE(utils::filesystem::directory_exists(test.path));
    }

    sleep(5);

    disk_cleaning_report report{};
    ASSERT_TRUE(dsn::replication::disk_remove_useless_dirs(
        {std::make_shared<dir_node>("test", "./")}, report));

    for (const auto &test : tests) {
        ASSERT_EQ(test.expected_exists, utils::filesystem::directory_exists(test.path));
        if (test.expected_exists) {
            // Delete existing directories, in case that they are mixed with later test cases
            // to affect test results.
            CHECK_TRUE(dsn::utils::filesystem::remove_path(test.path));
        }
    }

    ASSERT_EQ(report.remove_dir_count, 11);
    ASSERT_EQ(report.disk_migrate_origin_count, 1);
    ASSERT_EQ(report.disk_migrate_tmp_count, 1);
    ASSERT_EQ(report.garbage_replica_count, 5);
    ASSERT_EQ(report.error_replica_count, 6);
}

TEST_P(replica_disk_test, disk_status_test)
{
    struct disk_status_test
    {
        disk_status::type old_status;
        disk_status::type new_status;
    } tests[]{{disk_status::NORMAL, disk_status::NORMAL},
              {disk_status::NORMAL, disk_status::SPACE_INSUFFICIENT},
              {disk_status::SPACE_INSUFFICIENT, disk_status::SPACE_INSUFFICIENT},
              {disk_status::SPACE_INSUFFICIENT, disk_status::NORMAL}};
    auto dn = stub->get_fs_manager()->get_dir_nodes()[0];
    for (const auto &test : tests) {
        dn->status = test.new_status;
        for (const auto &pids_of_app : dn->holding_replicas) {
            for (const auto &pid : pids_of_app.second) {
                replica_ptr rep = stub->get_replica(pid);
                ASSERT_NE(nullptr, rep);
                ASSERT_EQ(test.new_status, rep->get_dir_node()->status);
            }
        }
    }
    dn->status = disk_status::NORMAL;
}

TEST_P(replica_disk_test, add_new_disk_test)
{
    // Test case:
    // - invalid params
    // - dir is available dir
    // - dir is not empty
    // - create dir failed
    // - dir can't read or write
    // - succeed
    struct add_disk_test
    {
        std::string disk_str;
        std::string create_dir;
        std::string rw_flag;
        error_code expected_err;
    } tests[]{{"", "true", "true", ERR_INVALID_PARAMETERS},
              {"wrong_format", "true", "true", ERR_INVALID_PARAMETERS},
              {"add_new_exist_tag:add_new_exist_disk0", "true", "true", ERR_NODE_ALREADY_EXIST},
              {"add_new_exist_tag0:add_new_exist_disk", "true", "true", ERR_NODE_ALREADY_EXIST},
              {"add_new_not_empty_tag:add_new_not_empty_disk", "true", "true", ERR_DIR_NOT_EMPTY},
              {"new_tag1:new_disk1", "false", "true", ERR_FILE_OPERATION_FAILED},
              {"new_tag1:new_disk1", "true", "false", ERR_FILE_OPERATION_FAILED},
              {"new_tag:new_disk", "true", "true", ERR_OK}};
    for (const auto &test : tests) {
        prepare_before_add_new_disk_test(test.create_dir, test.rw_flag);
        ASSERT_EQ(send_add_new_disk_rpc(test.disk_str), test.expected_err);
        reset_after_add_new_disk_test();
    }
}

TEST_P(replica_disk_test, disk_io_error_test)
{
    // Disable failure detector to avoid connecting with meta server which is not started.
    FLAGS_fd_disabled = true;

    gpid test_pid(app_info_1.app_id, 0);
    const auto rep = stub->get_replica(test_pid);
    auto *old_dn = rep->get_dir_node();

    rep->handle_local_failure(ERR_DISK_IO_ERROR);
    ASSERT_EVENTUALLY([&] { ASSERT_TRUE(!old_dn->has(test_pid)); });

    // The replica will not be located on the old dir_node.
    auto *new_dn = stub->get_fs_manager()->find_best_dir_for_new_replica(test_pid);
    ASSERT_NE(old_dn, new_dn);

    // The replicas will not be located on the old dir_node.
    const int kNewAppId = 3;
    // Make sure the app with id 'kNewAppId' is not existed.
    ASSERT_EQ(nullptr, stub->get_replica(gpid(kNewAppId, 0)));
    for (int i = 0; i < 16; i++) {
        new_dn = stub->get_fs_manager()->find_best_dir_for_new_replica(gpid(kNewAppId, i));
        ASSERT_NE(old_dn, new_dn);
    }
}

} // namespace replication
} // namespace dsn
