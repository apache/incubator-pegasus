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
#include <dsn/utility/fail_point.h>

#include "replica/test/replica_disk_test_base.h"
#include "replica/replica_disk_migrator.h"

namespace dsn {
namespace replication {
using disk_migrate_rpc = rpc_holder<replica_disk_migrate_request, replica_disk_migrate_response>;

class replica_disk_migrate_test : public replica_disk_test_base
{
public:
    replica_disk_migrate_rpc fake_migrate_rpc;

public:
    replica_disk_migrate_test() { fail::setup(); }

    ~replica_disk_migrate_test() { fail::teardown(); }

    void SetUp() override { generate_fake_rpc(); }

    replica_ptr get_replica(const dsn::gpid &pid) const
    {
        replica_ptr rep = stub->get_replica(pid);
        return rep;
    }

    void set_status(const dsn::gpid &pid, const disk_migration_status::type &status)
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

private:
    void generate_fake_rpc()
    {
        // create RPC_REPLICA_DISK_MIGRATE fake request
        auto migrate_request = dsn::make_unique<replica_disk_migrate_request>();
        fake_migrate_rpc = disk_migrate_rpc(std::move(migrate_request), RPC_REPLICA_DISK_MIGRATE);
    }
};

// TODO(jiashuo1): test whole process
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

    // TODO(jiashuo1): replica existed
}

TEST_F(replica_disk_migrate_test, migrate_disk_replica_check)
{
    auto &request = *fake_migrate_rpc.mutable_request();
    auto &response = fake_migrate_rpc.response();

    request.pid = dsn::gpid(app_info_1.app_id, 0);
    request.origin_disk = "tag_1";
    request.target_disk = "tag_2";

    // check existed task
    set_status(request.pid, disk_migration_status::MOVING);
    check_migration_args(fake_migrate_rpc);
    ASSERT_EQ(response.err, ERR_BUSY);
    set_status(fake_migrate_rpc.request().pid, disk_migration_status::IDLE); // revert IDLE status

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
    request.target_disk = "tag_2";
    check_migration_args(fake_migrate_rpc);
    ASSERT_EQ(response.err, ERR_PATH_ALREADY_EXIST);

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
    set_status(request.pid, disk_migration_status::MOVING);

    const std::string kTargetReplicaDir = fmt::format(
        "./{}/{}.replica.disk.balance.tmp/", request.target_disk, request.pid.to_string());

    const std::string kTargetDataDir = fmt::format(
        "./{}/{}.replica.disk.balance.tmp/data/rdb/", request.target_disk, request.pid.to_string());
    const std::string kTargetCheckPointFile =
        fmt::format("./{}/{}.replica.disk.balance.tmp/data/rdb/checkpoint.file",
                    request.target_disk,
                    request.pid.to_string());
    const std::string kTargetInitInfoFile =
        fmt::format("./{}/{}.replica.disk.balance.tmp/.init-info",
                    request.target_disk,
                    request.pid.to_string());
    const std::string kTargetAppInfoFile = fmt::format(
        "./{}/{}.replica.disk.balance.tmp/.app-info", request.target_disk, request.pid.to_string());

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

    const auto repica_ptr = get_replica(request.pid);

    set_status(request.pid, disk_migration_status::MOVING);
    init_migration_target_dir(fake_migrate_rpc);
    ASSERT_FALSE(utils::filesystem::directory_exists(kTargetDataDir));
    ASSERT_EQ(repica_ptr->disk_migrator()->status(), disk_migration_status::IDLE);

    set_status(request.pid, disk_migration_status::MOVING);
    migrate_replica_checkpoint(fake_migrate_rpc);
    ASSERT_FALSE(utils::filesystem::file_exists(kTargetCheckPointFile));
    ASSERT_EQ(repica_ptr->disk_migrator()->status(), disk_migration_status::IDLE);

    set_status(request.pid, disk_migration_status::MOVING);
    migrate_replica_app_info(fake_migrate_rpc);
    ASSERT_FALSE(utils::filesystem::file_exists(kTargetInitInfoFile));
    ASSERT_FALSE(utils::filesystem::file_exists(kTargetAppInfoFile));
    ASSERT_EQ(repica_ptr->disk_migrator()->status(), disk_migration_status::IDLE);
}

} // namespace replication
} // namespace dsn
