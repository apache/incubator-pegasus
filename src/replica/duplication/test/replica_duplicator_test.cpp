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

#include <cstdint>
#include <map>
#include <memory>
#include <string>

#include "common/duplication_common.h"
#include "common/gpid.h"
#include "common/replication_other_types.h"
#include "duplication_test_base.h"
#include "duplication_types.h"
#include "gtest/gtest.h"
#include "metadata_types.h"
#include "replica/duplication/duplication_pipeline.h"
#include "replica/duplication/mutation_duplicator.h"
#include "replica/duplication/replica_duplicator.h"
#include "replica/mutation_log.h"
#include "replica/test/mock_utils.h"
#include "runtime/pipeline.h"
#include "task/task_code.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/threadpool_code.h"

namespace dsn {
namespace apps {

// for loading PUT mutations from log file.
DEFINE_TASK_CODE_RPC(RPC_RRDB_RRDB_PUT, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT);

} // namespace apps
} // namespace dsn

namespace dsn {
namespace replication {

class replica_duplicator_test : public duplication_test_base
{
public:
    replica_duplicator_test()
    {
        _replica->set_partition_status(partition_status::PS_PRIMARY);
        _replica->init_private_log(_log_dir);
    }

    mock_replica *replica() { return _replica.get(); }

    decree last_durable_decree() const { return _replica->last_durable_decree(); }

    decree min_checkpoint_decree(const std::unique_ptr<replica_duplicator> &dup) const
    {
        return dup->_min_checkpoint_decree;
    }

    void test_new_duplicator(const std::string &remote_app_name,
                             bool specify_remote_app_name,
                             int64_t confirmed_decree)
    {
        const dupid_t dupid = 1;
        const std::string remote = "remote_address";
        const duplication_status::type status = duplication_status::DS_PAUSE;

        duplication_entry dup_ent;
        dup_ent.dupid = dupid;
        dup_ent.remote = remote;
        dup_ent.status = status;
        dup_ent.progress[_replica->get_gpid().get_partition_index()] = confirmed_decree;
        if (specify_remote_app_name) {
            dup_ent.__set_remote_app_name(remote_app_name);
        }

        auto duplicator = std::make_unique<replica_duplicator>(dup_ent, _replica.get());
        ASSERT_EQ(dupid, duplicator->id());
        ASSERT_EQ(remote, duplicator->remote_cluster_name());
        ASSERT_EQ(remote_app_name, duplicator->remote_app_name());
        ASSERT_EQ(status, duplicator->_status);
        ASSERT_EQ(1, duplicator->_min_checkpoint_decree);
        ASSERT_EQ(confirmed_decree, duplicator->progress().confirmed_decree);
        if (confirmed_decree == invalid_decree) {
            ASSERT_EQ(1, duplicator->progress().last_decree);
        } else {
            ASSERT_EQ(confirmed_decree, duplicator->progress().last_decree);
        }

        auto &expected_env = *duplicator;
        ASSERT_EQ(duplicator->tracker(), expected_env.__conf.tracker);
        ASSERT_EQ(duplicator->get_gpid().thread_hash(), expected_env.__conf.thread_hash);
    }

    void test_pause_start_duplication()
    {
        mutation_log_ptr mlog =
            new mutation_log_private(_replica->dir(), 4, _replica->get_gpid(), _replica.get());
        EXPECT_EQ(mlog->open(nullptr, nullptr), ERR_OK);

        {
            _replica->init_private_log(mlog);
            auto duplicator = create_test_duplicator();

            duplicator->update_status_if_needed(duplication_status::DS_LOG);
            ASSERT_EQ(duplicator->_status, duplication_status::DS_LOG);
            auto expected_env = duplicator->_ship->_mutation_duplicator->_env;
            ASSERT_EQ(duplicator->tracker(), expected_env.__conf.tracker);
            ASSERT_EQ(duplicator->get_gpid().thread_hash(), expected_env.__conf.thread_hash);

            // corner cases: next_status is INIT
            duplicator->update_status_if_needed(duplication_status::DS_INIT);
            ASSERT_EQ(duplicator->_status, duplication_status::DS_LOG);
            duplicator->update_status_if_needed(duplication_status::DS_LOG);
            ASSERT_EQ(duplicator->_status, duplication_status::DS_LOG);

            duplicator->update_status_if_needed(duplication_status::DS_PAUSE);
            ASSERT_TRUE(duplicator->paused());
            ASSERT_EQ(duplicator->_status, duplication_status::DS_PAUSE);
            ASSERT_EQ(duplicator->_load_private.get(), nullptr);
            ASSERT_EQ(duplicator->_load.get(), nullptr);
            ASSERT_EQ(duplicator->_ship.get(), nullptr);

            // corner cases: next_status is INIT
            duplicator->update_status_if_needed(duplication_status::DS_INIT);
            ASSERT_EQ(duplicator->_status, duplication_status::DS_PAUSE);

            // corner cases: next_status is INIT
            duplicator->update_status_if_needed(duplication_status::DS_INIT);
            ASSERT_EQ(duplicator->_status, duplication_status::DS_PAUSE);

            duplicator->wait_all();
        }
    }
};

INSTANTIATE_TEST_SUITE_P(, replica_duplicator_test, ::testing::Values(false, true));

TEST_P(replica_duplicator_test, new_duplicator_without_remote_app_name)
{
    test_new_duplicator("temp", false, 100);
}

TEST_P(replica_duplicator_test, new_duplicator_with_remote_app_name)
{
    test_new_duplicator("another_test_app", true, 100);
}

// Initial confirmed decree immediately after the duplication was created is `invalid_decree`
// which was synced from meta server.
TEST_P(replica_duplicator_test, new_duplicator_with_initial_confirmed_decree)
{
    test_new_duplicator("test_initial_confirmed_decree", true, invalid_decree);
}

// The duplication progressed and confirmed decree became valid.
TEST_P(replica_duplicator_test, new_duplicator_with_non_initial_confirmed_decree)
{
    test_new_duplicator("test_non_initial_confirmed_decree", true, 1);
}

TEST_P(replica_duplicator_test, pause_start_duplication) { test_pause_start_duplication(); }

TEST_P(replica_duplicator_test, duplication_progress)
{
    auto duplicator = create_test_duplicator();

    // Start duplication from empty replica.
    ASSERT_EQ(1, min_checkpoint_decree(duplicator));
    ASSERT_EQ(1, duplicator->progress().last_decree);
    ASSERT_EQ(invalid_decree, duplicator->progress().confirmed_decree);

    // Update the max decree that has been duplicated to the remote cluster.
    duplicator->update_progress(duplicator->progress().set_last_decree(10));
    ASSERT_EQ(10, duplicator->progress().last_decree);
    ASSERT_EQ(invalid_decree, duplicator->progress().confirmed_decree);

    // Update the max decree that has been persisted in the meta server.
    duplicator->update_progress(duplicator->progress().set_confirmed_decree(10));
    ASSERT_EQ(10, duplicator->progress().last_decree);
    ASSERT_EQ(10, duplicator->progress().confirmed_decree);

    ASSERT_EQ(error_s::make(ERR_INVALID_STATE, "never decrease confirmed_decree: new(1) old(10)"),
              duplicator->update_progress(duplicator->progress().set_confirmed_decree(1)));

    ASSERT_EQ(error_s::make(ERR_INVALID_STATE,
                            "last_decree(10) should always larger than confirmed_decree(12)"),
              duplicator->update_progress(duplicator->progress().set_confirmed_decree(12)));

    // Test that the checkpoint has not been created.
    replica()->update_last_applied_decree(100);
    auto duplicator_for_checkpoint = create_test_duplicator();
    ASSERT_FALSE(duplicator_for_checkpoint->progress().checkpoint_has_prepared);

    // Test that the checkpoint has been created.
    replica()->update_last_durable_decree(100);
    duplicator_for_checkpoint->update_progress(duplicator->progress());
    ASSERT_TRUE(duplicator_for_checkpoint->progress().checkpoint_has_prepared);
}

TEST_P(replica_duplicator_test, prepare_dup)
{
    replica()->update_last_applied_decree(100);
    replica()->update_expect_last_durable_decree(100);

    auto duplicator = create_test_duplicator();
    duplicator->prepare_dup();
    wait_all(duplicator);

    ASSERT_EQ(100, min_checkpoint_decree(duplicator));
    ASSERT_EQ(100, last_durable_decree());
}

} // namespace replication
} // namespace dsn
