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

#include <dsn/utility/filesystem.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/fail_point.h>

#include "replica/duplication/replica_follower.h"
#include "duplication_test_base.h"

namespace dsn {
namespace apps {

} // namespace apps
} // namespace dsn

namespace dsn {
namespace replication {

class replica_follower_test : public duplication_test_base
{
public:
    replica_follower_test()
    {
        _app_info.app_id = 2;
        _app_info.app_name = "follower";
        _app_info.app_type = "replica";
        _app_info.is_stateful = true;
        _app_info.max_replica_count = 3;
        _app_info.partition_count = 8;
    }

    void update_mock_replica(const dsn::app_info &app)
    {
        bool is_duplication_follower =
            (app.envs.find(duplication_constants::kDuplicationEnvMasterClusterKey) !=
             app.envs.end()) &&
            (app.envs.find(duplication_constants::kDuplicationEnvMasterMetasKey) != app.envs.end());
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

public:
    dsn::app_info _app_info;
    mock_replica_ptr _mock_replica;
};

TEST_F(replica_follower_test, test_init_master_info)
{
    _app_info.envs.emplace(duplication_constants::kDuplicationEnvMasterClusterKey, "master");
    _app_info.envs.emplace(duplication_constants::kDuplicationEnvMasterMetasKey,
                           "127.0.0.1:34801,127.0.0.2:34801,127.0.0.3:34802");
    update_mock_replica(_app_info);

    auto follower = _mock_replica->get_replica_follower();
    ASSERT_EQ(follower->get_master_app_name(), "follower");
    ASSERT_EQ(follower->get_master_cluster_name(), "master");
    ASSERT_TRUE(follower->is_need_duplicate());
    ASSERT_TRUE(_mock_replica->is_duplication_follower());
    std::vector<std::string> test_ip{"127.0.0.1:34801", "127.0.0.2:34801", "127.0.0.3:34802"};
    for (int i = 0; i < follower->get_master_meta_list().size(); i++) {
        ASSERT_EQ(std::string(follower->get_master_meta_list()[i].to_string()), test_ip[i]);
    }

    _app_info.envs.clear();
    update_mock_replica(_app_info);
    follower = _mock_replica->get_replica_follower();
    ASSERT_FALSE(follower->is_need_duplicate());
    ASSERT_FALSE(_mock_replica->is_duplication_follower());
}

TEST_F(replica_follower_test, test_duplicate_checkpoint)
{
    _app_info.envs.emplace(duplication_constants::kDuplicationEnvMasterClusterKey, "master");
    _app_info.envs.emplace(duplication_constants::kDuplicationEnvMasterMetasKey,
                           "127.0.0.1:34801,127.0.0.2:34801,127.0.0.3:34802");
    update_mock_replica(_app_info);

    auto follower = _mock_replica->get_replica_follower();

    ASSERT_EQ(follower->duplicate_checkpoint(), ERR_OK);
    ASSERT_FALSE(get_duplicating(follower));

    set_duplicating(true, follower);
    ASSERT_EQ(follower->duplicate_checkpoint(), ERR_BUSY);
}

TEST_F(replica_follower_test, test_async_duplicate_checkpoint_from_master_replica)
{
    _app_info.envs.emplace(duplication_constants::kDuplicationEnvMasterClusterKey, "master");
    _app_info.envs.emplace(duplication_constants::kDuplicationEnvMasterMetasKey,
                           "127.0.0.1:34801,127.0.0.2:34801,127.0.0.3:34802");
    update_mock_replica(_app_info);

    auto follower = _mock_replica->get_replica_follower();

    fail::setup();
    fail::cfg("duplicate_checkpoint_failed", "void()");
    async_duplicate_checkpoint_from_master_replica(follower);
    ASSERT_FALSE(wait_follower_task_completed(follower));

    fail::setup();
    fail::cfg("duplicate_checkpoint_ok", "void()");
    async_duplicate_checkpoint_from_master_replica(follower);
    ASSERT_TRUE(wait_follower_task_completed(follower));
    fail::teardown();
    fail::teardown();
}
} // namespace replication
} // namespace dsn
