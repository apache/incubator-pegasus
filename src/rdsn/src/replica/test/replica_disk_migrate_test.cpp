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

#include <gtest/gtest.h>
#include "utils/fail_point.h"

#include "replica/test/replica_disk_test_base.h"
#include "replica/replica_disk_migrator.h"

namespace dsn {
namespace replication {
using disk_migrate_rpc = rpc_holder<replica_disk_migrate_request, replica_disk_migrate_response>;

// this test is based the node disk mock of replica_disk_test_base, please see the mock disk
// information in replica_disk_test_base
class replica_disk_migrate_test : public replica_disk_test_base
{
public:
    replica_disk_migrate_rpc fake_migrate_rpc;

public:
    void SetUp() override { generate_fake_rpc(); }

    replica_ptr get_replica(const dsn::gpid &pid) const
    {
        replica_ptr rep = stub->get_replica(pid);
        return rep;
    }

    void set_replica_status(const dsn::gpid &pid, partition_status::type status) const
    {
        get_replica(pid)->_config.status = status;
    }

    void set_migration_status(const dsn::gpid &pid, const disk_migration_status::type &status)
    {
        replica_ptr rep = get_replica(pid);
        ASSERT_TRUE(rep);
        rep->disk_migrator()->set_status(status);
    }

    void set_replica_dir(const dsn::gpid &pid, const std::string &dir)
    {
        replica_ptr rep = get_replica(pid);
        ASSERT_TRUE(rep);
        rep->_dir = dir;
    }

    void set_replica_target_dir(const dsn::gpid &pid, const std::string &dir)
    {
        replica_ptr rep = get_replica(pid);
        ASSERT_TRUE(rep);
        rep->disk_migrator()->_target_replica_dir = dir;
    }

    void check_migration_args(replica_disk_migrate_rpc &rpc)
    {
        replica_ptr rep = get_replica(rpc.request().pid);
        ASSERT_TRUE(rep);
        rep->disk_migrator()->check_migration_args(fake_migrate_rpc);
    }

    void init_migration_target_dir(replica_disk_migrate_rpc &rpc)
    {
        replica_ptr rep = get_replica(rpc.request().pid);
        ASSERT_TRUE(rep);
        rep->disk_migrator()->init_target_dir(rpc.request());
    }

    void migrate_replica_checkpoint(replica_disk_migrate_rpc &rpc)
    {
        replica_ptr rep = get_replica(rpc.request().pid);
        ASSERT_TRUE(rep);
        rep->disk_migrator()->migrate_replica_checkpoint(rpc.request());
    }

    void migrate_replica_app_info(replica_disk_migrate_rpc &rpc)
    {
        replica_ptr rep = get_replica(rpc.request().pid);
        ASSERT_TRUE(rep);
        rep->disk_migrator()->migrate_replica_app_info(rpc.request());
    }

    dsn::task_ptr close_current_replica(replica_disk_migrate_rpc &rpc)
    {
        replica_ptr rep = get_replica(rpc.request().pid);
        return rep->disk_migrator()->close_current_replica(rpc.request());
    }

    void update_replica_dir(replica_disk_migrate_rpc &rpc)
    {
        replica_ptr rep = get_replica(rpc.request().pid);
        rep->disk_migrator()->update_replica_dir();
    }

    void open_replica(const app_info &app, gpid id)
    {
        stub->open_replica(app, id, nullptr, nullptr);
    }

private:
    void generate_fake_rpc()
    {
        // create RPC_REPLICA_DISK_MIGRATE fake request
        auto migrate_request = dsn::make_unique<replica_disk_migrate_request>();
        fake_migrate_rpc = disk_migrate_rpc(std::move(migrate_request), RPC_REPLICA_DISK_MIGRATE);
    }
};

TEST_F(replica_disk_migrate_test, on_migrate_replica)
{
    auto &request = *fake_migrate_rpc.mutable_request();
    auto &response = fake_migrate_rpc.response();

    // replica not existed
    request.pid = dsn::gpid(app_info_1.app_id, 100);
    request.origin_disk = "tag_1";
    request.target_disk = "tag_2";
    stub->on_disk_migrate(fake_migrate_rpc);
    ASSERT_EQ(response.err, ERR_OBJECT_NOT_FOUND);

    request.pid = dsn::gpid(app_info_1.app_id, 2);
    request.origin_disk = "tag_1";
    request.target_disk = "tag_2";
    stub->on_disk_migrate(fake_migrate_rpc);
    get_replica(request.pid)->tracker()->wait_outstanding_tasks();
    ASSERT_EQ(response.err, ERR_OK);
}

TEST_F(replica_disk_migrate_test, migrate_disk_replica_check)
{
    auto &request = *fake_migrate_rpc.mutable_request();
    auto &response = fake_migrate_rpc.response();

    request.pid = dsn::gpid(app_info_1.app_id, 1);
    request.origin_disk = "tag_1";
    request.target_disk = "tag_2";

    // check existed task
    set_migration_status(request.pid, disk_migration_status::MOVING);
    check_migration_args(fake_migrate_rpc);
    ASSERT_EQ(response.err, ERR_BUSY);
    set_migration_status(fake_migrate_rpc.request().pid,
                         disk_migration_status::IDLE); // revert IDLE status

    // check invalid partition status
    check_migration_args(fake_migrate_rpc);
    ASSERT_EQ(response.err, ERR_INVALID_STATE);

    // check same disk
    request.pid = dsn::gpid(app_info_1.app_id, 2);
    request.origin_disk = "tag_1";
    request.target_disk = "tag_1";
    check_migration_args(fake_migrate_rpc);
    ASSERT_EQ(response.err, ERR_INVALID_PARAMETERS);

    // check invalid origin disk
    request.origin_disk = "tag_100";
    request.target_disk = "tag_0";
    check_migration_args(fake_migrate_rpc);
    ASSERT_EQ(response.err, ERR_OBJECT_NOT_FOUND);
    // check invalid target disk
    request.origin_disk = "tag_1";
    request.target_disk = "tag_200";
    check_migration_args(fake_migrate_rpc);
    ASSERT_EQ(response.err, ERR_OBJECT_NOT_FOUND);

    // check replica doesn't existed origin disk
    request.origin_disk = "tag_empty_1";
    request.target_disk = "tag_6";
    check_migration_args(fake_migrate_rpc);
    ASSERT_EQ(response.err, ERR_OBJECT_NOT_FOUND);
    // check replica has existed on target disk
    request.origin_disk = "tag_1";
    request.target_disk = "tag_new";
    generate_mock_dir_node(app_info_1, request.pid, request.target_disk);
    check_migration_args(fake_migrate_rpc);
    ASSERT_EQ(response.err, ERR_PATH_ALREADY_EXIST);
    remove_mock_dir_node(request.target_disk);

    // check passed
    request.origin_disk = "tag_1";
    request.target_disk = "tag_empty_1";
    ASSERT_EQ(get_replica(request.pid)->disk_migrator()->status(), disk_migration_status::IDLE);
    check_migration_args(fake_migrate_rpc);
    ASSERT_EQ(response.err, ERR_OK);
}

TEST_F(replica_disk_migrate_test, disk_migrate_replica_run)
{
    auto &request = *fake_migrate_rpc.mutable_request();

    request.pid = dsn::gpid(app_info_1.app_id, 2);
    request.origin_disk = "tag_1";
    request.target_disk = "tag_empty_1";
    set_replica_dir(request.pid,
                    fmt::format("./{}/{}.replica", request.origin_disk, request.pid.to_string()));
    set_migration_status(request.pid, disk_migration_status::MOVING);

    const std::string kTargetReplicaDir = fmt::format(
        "./{}/{}.replica.disk.migrate.tmp/", request.target_disk, request.pid.to_string());

    const std::string kTargetDataDir = fmt::format(
        "./{}/{}.replica.disk.migrate.tmp/data/rdb/", request.target_disk, request.pid.to_string());
    const std::string kTargetCheckPointFile =
        fmt::format("./{}/{}.replica.disk.migrate.tmp/data/rdb/checkpoint.file",
                    request.target_disk,
                    request.pid.to_string());
    const std::string kTargetInitInfoFile = fmt::format("./{}/{}.replica.disk.migrate.tmp/{}",
                                                        request.target_disk,
                                                        request.pid.to_string(),
                                                        replica_init_info::kInitInfo);
    const std::string kTargetAppInfoFile = fmt::format("./{}/{}.replica.disk.migrate.tmp/{}",
                                                       request.target_disk,
                                                       request.pid.to_string(),
                                                       replica::kAppInfo);

    init_migration_target_dir(fake_migrate_rpc);
    ASSERT_TRUE(utils::filesystem::directory_exists(kTargetDataDir));

    migrate_replica_checkpoint(fake_migrate_rpc);
    ASSERT_TRUE(utils::filesystem::file_exists(kTargetCheckPointFile));

    migrate_replica_app_info(fake_migrate_rpc);
    ASSERT_TRUE(utils::filesystem::file_exists(kTargetInitInfoFile));
    ASSERT_TRUE(utils::filesystem::file_exists(kTargetAppInfoFile));

    // remove test tmp path
    utils::filesystem::remove_path(kTargetReplicaDir);

    fail::cfg("init_target_dir", "return()");
    fail::cfg("migrate_replica_checkpoint", "return()");
    fail::cfg("migrate_replica_app_info", "return()");

    const auto replica_ptr = get_replica(request.pid);

    set_migration_status(request.pid, disk_migration_status::MOVING);
    init_migration_target_dir(fake_migrate_rpc);
    ASSERT_FALSE(utils::filesystem::directory_exists(kTargetDataDir));
    ASSERT_EQ(replica_ptr->disk_migrator()->status(), disk_migration_status::IDLE);

    set_migration_status(request.pid, disk_migration_status::MOVING);
    migrate_replica_checkpoint(fake_migrate_rpc);
    ASSERT_FALSE(utils::filesystem::file_exists(kTargetCheckPointFile));
    ASSERT_EQ(replica_ptr->disk_migrator()->status(), disk_migration_status::IDLE);

    set_migration_status(request.pid, disk_migration_status::MOVING);
    migrate_replica_app_info(fake_migrate_rpc);
    ASSERT_FALSE(utils::filesystem::file_exists(kTargetInitInfoFile));
    ASSERT_FALSE(utils::filesystem::file_exists(kTargetAppInfoFile));
    ASSERT_EQ(replica_ptr->disk_migrator()->status(), disk_migration_status::IDLE);
}

TEST_F(replica_disk_migrate_test, disk_migrate_replica_close)
{
    auto &request = *fake_migrate_rpc.mutable_request();
    request.pid = dsn::gpid(app_info_1.app_id, 2);

    // test invalid replica status
    set_replica_status(request.pid, partition_status::PS_PRIMARY);
    ASSERT_FALSE(close_current_replica(fake_migrate_rpc));

    // test valid replica status
    set_migration_status(request.pid, disk_migration_status::MOVED);
    set_replica_status(request.pid, partition_status::PS_SECONDARY);
    ASSERT_TRUE(close_current_replica(fake_migrate_rpc));
}

TEST_F(replica_disk_migrate_test, disk_migrate_replica_update)
{
    auto &request = *fake_migrate_rpc.mutable_request();
    request.pid = dsn::gpid(app_info_1.app_id, 3);
    request.origin_disk = "tag_1";
    request.target_disk = "tag_empty_1";

    const std::string kReplicaOriginDir =
        fmt::format("./{}/{}.replica", request.origin_disk, request.pid.to_string());
    const std::string kReplicaNewTempDir = fmt::format(
        "./{}/{}.replica.disk.migrate.tmp/", request.target_disk, request.pid.to_string());
    const std::string kReplicaOriginSuffixDir = fmt::format(
        "./{}/{}.replica.disk.migrate.ori/", request.origin_disk, request.pid.to_string());
    const std::string kReplicaNewDir =
        fmt::format("./{}/{}.replica/", request.target_disk, request.pid.to_string());

    utils::filesystem::create_directory(kReplicaOriginDir);
    utils::filesystem::create_directory(kReplicaNewTempDir);

    // replica dir is error, rename origin dir to "*.ori" failed
    set_replica_dir(request.pid, "error");
    update_replica_dir(fake_migrate_rpc);
    ASSERT_EQ(get_replica(request.pid)->disk_migrator()->status(), disk_migration_status::IDLE);

    // replica target dir is error, rename "*.tmp" dir failed
    set_replica_dir(request.pid, kReplicaOriginDir);
    set_replica_target_dir(request.pid, "error");
    update_replica_dir(fake_migrate_rpc);
    ASSERT_EQ(get_replica(request.pid)->disk_migrator()->status(), disk_migration_status::IDLE);
    ASSERT_TRUE(utils::filesystem::directory_exists(kReplicaOriginDir));
    ASSERT_FALSE(utils::filesystem::directory_exists(kReplicaOriginSuffixDir));

    // update success
    set_replica_target_dir(request.pid, kReplicaNewTempDir);
    update_replica_dir(fake_migrate_rpc);
    ASSERT_TRUE(utils::filesystem::directory_exists(kReplicaOriginSuffixDir));
    ASSERT_TRUE(utils::filesystem::directory_exists(kReplicaNewDir));
    utils::filesystem::remove_path(fmt::format("./{}/", request.origin_disk));
    utils::filesystem::remove_path(fmt::format("./{}/", request.target_disk));
    for (const auto &node_disk : get_dir_nodes()) {
        if (node_disk->tag == request.origin_disk) {
            auto gpids = node_disk->holding_replicas[app_info_1.app_id];
            ASSERT_TRUE(gpids.find(request.pid) == gpids.end());
            continue;
        }

        if (node_disk->tag == request.target_disk) {
            auto gpids = node_disk->holding_replicas[app_info_1.app_id];
            ASSERT_TRUE(gpids.find(request.pid) != gpids.end());
            continue;
        }
    }
}

TEST_F(replica_disk_migrate_test, disk_migrate_replica_open)
{
    auto &request = *fake_migrate_rpc.mutable_request();
    request.pid = dsn::gpid(app_info_1.app_id, 4);
    request.origin_disk = "tag_2";
    request.target_disk = "tag_empty_1";

    remove_mock_dir_node(request.origin_disk);
    const std::string kReplicaOriginSuffixDir = fmt::format(
        "./{}/{}.replica.disk.migrate.ori/", request.origin_disk, request.pid.to_string());
    const std::string kReplicaNewDir =
        fmt::format("./{}/{}.replica/", request.target_disk, request.pid.to_string());
    utils::filesystem::create_directory(kReplicaOriginSuffixDir);
    utils::filesystem::create_directory(kReplicaNewDir);

    fail::cfg("mock_replica_load", "return()");
    const std::string kReplicaOriginDir =
        fmt::format("./{}/{}.replica", request.origin_disk, request.pid.to_string());
    const std::string kReplicaGarDir =
        fmt::format("./{}/{}.replica.gar", request.target_disk, request.pid.to_string());
    open_replica(app_info_1, request.pid);

    ASSERT_TRUE(utils::filesystem::directory_exists(kReplicaOriginDir));
    ASSERT_TRUE(utils::filesystem::directory_exists(kReplicaGarDir));

    utils::filesystem::remove_path(kReplicaOriginDir);
    utils::filesystem::remove_path(kReplicaGarDir);
}

} // namespace replication
} // namespace dsn
