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

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/duplication_common.h"
#include "common/gpid.h"
#include "common/replication_common.h"
#include "consensus_types.h"
#include "dsn.layer2_types.h"
#include "duplication_test_base.h"
#include "gtest/gtest.h"
#include "metadata_types.h"
#include "nfs/nfs_node.h"
#include "replica/duplication/replica_follower.h"
#include "replica/test/mock_utils.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"
#include "task/task_tracker.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"
#include "utils/filesystem.h"

namespace dsn {
namespace replication {

class replica_follower_test : public duplication_test_base
{
public:
    static const std::string kTestMasterClusterName;
    static const std::string kTestMasterAppName;

    replica_follower_test()
    {
        _app_info.app_id = 2;
        _app_info.app_name = kTestMasterAppName;
        _app_info.app_type = replication_options::kReplicaAppType;
        _app_info.is_stateful = true;
        _app_info.max_replica_count = 3;
        _app_info.partition_count = 8;
    }

    void update_mock_replica(const dsn::app_info &app)
    {
        bool is_duplication_follower =
            (app.envs.find(duplication_constants::kEnvMasterClusterKey) != app.envs.end()) &&
            (app.envs.find(duplication_constants::kEnvMasterMetasKey) != app.envs.end());
        _mock_replica = stub->generate_replica_ptr(
            app, gpid(2, 1), partition_status::PS_PRIMARY, 1, false, is_duplication_follower);
    }

    void set_duplicating(bool duplicating, replica_follower *follower)
    {
        follower->_duplicating_checkpoint = duplicating;
    }

    bool get_duplicating(replica_follower *follower) { return follower->_duplicating_checkpoint; }

    void async_duplicate_checkpoint_from_master_replica(replica_follower *follower)
    {
        follower->async_duplicate_checkpoint_from_master_replica();
    }

    bool wait_follower_task_completed(replica_follower *follower)
    {
        follower->_tracker.wait_outstanding_tasks();
        return follower->_tracker.all_tasks_success();
    }

    void mark_tracker_tasks_success(replica_follower *follower)
    {
        follower->_tracker.set_tasks_success();
    }

    error_code update_master_replica_config(replica_follower *follower, query_cfg_response &resp)
    {
        return follower->update_master_replica_config(ERR_OK, std::move(resp));
    }

    const partition_configuration &master_replica_config(replica_follower *follower) const
    {
        return follower->_pc;
    }

    error_code nfs_copy_checkpoint(replica_follower *follower, error_code err, learn_response resp)
    {
        return follower->nfs_copy_checkpoint(err, std::move(resp));
    }

    void init_nfs()
    {
        stub->_nfs = nfs_node::create();
        stub->_nfs->start();
    }

    void test_init_master_info(const std::string &expected_master_app_name)
    {
        _app_info.envs.emplace(duplication_constants::kEnvMasterClusterKey, kTestMasterClusterName);
        _app_info.envs.emplace(duplication_constants::kEnvMasterMetasKey,
                               "127.0.0.1:34801,127.0.0.1:34802,127.0.0.1:34803");
        update_mock_replica(_app_info);

        auto follower = _mock_replica->get_replica_follower();
        ASSERT_EQ(expected_master_app_name, follower->get_master_app_name());
        ASSERT_EQ(follower->get_master_cluster_name(), kTestMasterClusterName);
        ASSERT_TRUE(follower->is_need_duplicate());
        ASSERT_TRUE(_mock_replica->is_duplication_follower());
        std::vector<std::string> test_ip{"127.0.0.1:34801", "127.0.0.1:34802", "127.0.0.1:34803"};
        for (int i = 0; i < follower->get_master_meta_list().size(); i++) {
            ASSERT_EQ(test_ip[i], std::string(follower->get_master_meta_list()[i].to_string()));
        }

        _app_info.envs.clear();
        update_mock_replica(_app_info);
        follower = _mock_replica->get_replica_follower();
        ASSERT_FALSE(follower->is_need_duplicate());
        ASSERT_FALSE(_mock_replica->is_duplication_follower());
    }

public:
    dsn::app_info _app_info;
    mock_replica_ptr _mock_replica;
};

const std::string replica_follower_test::kTestMasterClusterName = "master";
const std::string replica_follower_test::kTestMasterAppName = "follower";

INSTANTIATE_TEST_SUITE_P(, replica_follower_test, ::testing::Values(false, true));

TEST_P(replica_follower_test, test_init_master_info_without_master_app_env)
{
    test_init_master_info(kTestMasterAppName);
}

TEST_P(replica_follower_test, test_init_master_info_with_master_app_env)
{
    static const std::string kTestAnotherMasterAppName("another_follower");
    _app_info.envs.emplace(duplication_constants::kEnvMasterAppNameKey, kTestAnotherMasterAppName);
    test_init_master_info(kTestAnotherMasterAppName);
}

TEST_P(replica_follower_test, test_duplicate_checkpoint)
{
    _app_info.envs.emplace(duplication_constants::kEnvMasterClusterKey, kTestMasterClusterName);
    _app_info.envs.emplace(duplication_constants::kEnvMasterMetasKey,
                           "127.0.0.1:34801,127.0.0.1:34802,127.0.0.1:34803");
    update_mock_replica(_app_info);

    auto follower = _mock_replica->get_replica_follower();

    ASSERT_EQ(follower->duplicate_checkpoint(), ERR_TRY_AGAIN);
    ASSERT_FALSE(get_duplicating(follower));

    mark_tracker_tasks_success(follower);
    ASSERT_EQ(follower->duplicate_checkpoint(), ERR_OK);
    ASSERT_FALSE(get_duplicating(follower));

    set_duplicating(true, follower);
    ASSERT_EQ(follower->duplicate_checkpoint(), ERR_BUSY);
}

TEST_P(replica_follower_test, test_async_duplicate_checkpoint_from_master_replica)
{
    _app_info.envs.emplace(duplication_constants::kEnvMasterClusterKey, kTestMasterClusterName);
    _app_info.envs.emplace(duplication_constants::kEnvMasterMetasKey,
                           "127.0.0.1:34801,127.0.0.1:34802,127.0.0.1:34803");
    update_mock_replica(_app_info);

    auto follower = _mock_replica->get_replica_follower();

    fail::setup();
    fail::cfg("duplicate_checkpoint_failed", "void()");
    async_duplicate_checkpoint_from_master_replica(follower);
    ASSERT_FALSE(wait_follower_task_completed(follower));
    fail::teardown();

    fail::setup();
    fail::cfg("duplicate_checkpoint_ok", "void()");
    async_duplicate_checkpoint_from_master_replica(follower);
    ASSERT_TRUE(wait_follower_task_completed(follower));
    fail::teardown();
}

TEST_P(replica_follower_test, test_update_master_replica_config)
{
    _app_info.envs.emplace(duplication_constants::kEnvMasterClusterKey, kTestMasterClusterName);
    _app_info.envs.emplace(duplication_constants::kEnvMasterMetasKey,
                           "127.0.0.1:34801,127.0.0.1:34802,127.0.0.1:34803");
    update_mock_replica(_app_info);
    auto *follower = _mock_replica->get_replica_follower();

    query_cfg_response resp;
    ASSERT_EQ(update_master_replica_config(follower, resp), ERR_INCONSISTENT_STATE);
    ASSERT_FALSE(master_replica_config(follower).primary);
    ASSERT_FALSE(master_replica_config(follower).hp_primary);

    resp.partition_count = 100;
    ASSERT_EQ(update_master_replica_config(follower, resp), ERR_INCONSISTENT_STATE);
    ASSERT_FALSE(master_replica_config(follower).primary);
    ASSERT_FALSE(master_replica_config(follower).hp_primary);

    resp.partition_count = _app_info.partition_count;
    partition_configuration pc;
    resp.partitions.emplace_back(pc);
    resp.partitions.emplace_back(pc);
    ASSERT_EQ(update_master_replica_config(follower, resp), ERR_INVALID_DATA);
    ASSERT_FALSE(master_replica_config(follower).primary);
    ASSERT_FALSE(master_replica_config(follower).hp_primary);

    resp.partitions.clear();
    pc.pid = gpid(2, 100);
    resp.partitions.emplace_back(pc);
    ASSERT_EQ(update_master_replica_config(follower, resp), ERR_INCONSISTENT_STATE);
    ASSERT_FALSE(master_replica_config(follower).primary);
    ASSERT_FALSE(master_replica_config(follower).hp_primary);

    resp.partitions.clear();
    RESET_IP_AND_HOST_PORT(pc, primary);
    pc.pid = gpid(2, 1);
    resp.partitions.emplace_back(pc);
    ASSERT_EQ(update_master_replica_config(follower, resp), ERR_INVALID_STATE);
    ASSERT_FALSE(master_replica_config(follower).primary);
    ASSERT_FALSE(master_replica_config(follower).hp_primary);

    resp.partitions.clear();
    pc.pid = gpid(2, 1);

    const host_port primary("localhost", 34801);
    const host_port secondary1("localhost", 34802);
    const host_port secondary2("localhost", 34803);

    SET_IP_AND_HOST_PORT_BY_DNS(pc, primary, primary);
    SET_IPS_AND_HOST_PORTS_BY_DNS(pc, secondaries, secondary1, secondary2);
    resp.partitions.emplace_back(pc);
    ASSERT_EQ(update_master_replica_config(follower, resp), ERR_OK);
    ASSERT_EQ(master_replica_config(follower).primary, pc.primary);
    ASSERT_EQ(master_replica_config(follower).hp_primary, pc.hp_primary);
    ASSERT_EQ(master_replica_config(follower).pid, pc.pid);
}

TEST_P(replica_follower_test, test_nfs_copy_checkpoint)
{
    _app_info.envs.emplace(duplication_constants::kEnvMasterClusterKey, kTestMasterClusterName);
    _app_info.envs.emplace(duplication_constants::kEnvMasterMetasKey,
                           "127.0.0.1:34801,127.0.0.1:34802,127.0.0.1:34803");
    update_mock_replica(_app_info);
    init_nfs();
    auto follower = _mock_replica->get_replica_follower();

    ASSERT_EQ(nfs_copy_checkpoint(follower, ERR_CORRUPTION, learn_response()), ERR_CORRUPTION);

    auto resp = learn_response();
    const host_port learnee("localhost", 34801);
    SET_IP_AND_HOST_PORT_BY_DNS(resp, learnee, learnee);

    std::string dest = utils::filesystem::path_combine(
        _mock_replica->dir(), duplication_constants::kDuplicationCheckpointRootDir);
    dsn::utils::filesystem::create_directory(dest);
    ASSERT_TRUE(dsn::utils::filesystem::path_exists(dest));
    ASSERT_EQ(nfs_copy_checkpoint(follower, ERR_OK, resp), ERR_OK);
    ASSERT_FALSE(dsn::utils::filesystem::path_exists(dest));
    ASSERT_FALSE(wait_follower_task_completed(follower));

    fail::setup();
    fail::cfg("nfs_copy_ok", "void()");
    ASSERT_EQ(nfs_copy_checkpoint(follower, ERR_OK, resp), ERR_OK);
    ASSERT_TRUE(wait_follower_task_completed(follower));
    fail::teardown();
}

} // namespace replication
} // namespace dsn
