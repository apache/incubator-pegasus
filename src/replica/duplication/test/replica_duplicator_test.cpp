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
#include "runtime/task/task_code.h"
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

    decree log_dup_start_decree(const std::unique_ptr<replica_duplicator> &dup) const
    {
        return dup->_start_point_decree;
    }

    void test_new_duplicator()
    {
        dupid_t dupid = 1;
        std::string remote = "remote_address";
        duplication_status::type status = duplication_status::DS_PAUSE;
        int64_t confirmed_decree = 100;

        duplication_entry dup_ent;
        dup_ent.dupid = dupid;
        dup_ent.remote = remote;
        dup_ent.status = status;
        dup_ent.progress[_replica->get_gpid().get_partition_index()] = confirmed_decree;

        auto duplicator = std::make_unique<replica_duplicator>(dup_ent, _replica.get());
        ASSERT_EQ(duplicator->id(), dupid);
        ASSERT_EQ(duplicator->remote_cluster_name(), remote);
        ASSERT_EQ(duplicator->_status, status);
        ASSERT_EQ(duplicator->progress().confirmed_decree, confirmed_decree);
        ASSERT_EQ(duplicator->progress().last_decree, confirmed_decree);

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

INSTANTIATE_TEST_CASE_P(, replica_duplicator_test, ::testing::Values(false, true));

TEST_P(replica_duplicator_test, new_duplicator) { test_new_duplicator(); }

TEST_P(replica_duplicator_test, pause_start_duplication) { test_pause_start_duplication(); }

TEST_P(replica_duplicator_test, duplication_progress)
{
    auto duplicator = create_test_duplicator();
    ASSERT_EQ(duplicator->progress().last_decree, 0); // start duplication from empty plog
    ASSERT_EQ(duplicator->progress().confirmed_decree, invalid_decree);

    duplicator->update_progress(duplicator->progress().set_last_decree(10));
    ASSERT_EQ(duplicator->progress().last_decree, 10);
    ASSERT_EQ(duplicator->progress().confirmed_decree, invalid_decree);

    duplicator->update_progress(duplicator->progress().set_confirmed_decree(10));
    ASSERT_EQ(duplicator->progress().confirmed_decree, 10);
    ASSERT_EQ(duplicator->progress().last_decree, 10);

    ASSERT_EQ(duplicator->update_progress(duplicator->progress().set_confirmed_decree(1)),
              error_s::make(ERR_INVALID_STATE, "never decrease confirmed_decree: new(1) old(10)"));

    ASSERT_EQ(duplicator->update_progress(duplicator->progress().set_confirmed_decree(12)),
              error_s::make(ERR_INVALID_STATE,
                            "last_decree(10) should always larger than confirmed_decree(12)"));

    auto duplicator_for_checkpoint = create_test_duplicator(invalid_decree, 100);
    ASSERT_FALSE(duplicator_for_checkpoint->progress().checkpoint_has_prepared);

    replica()->update_last_durable_decree(101);
    duplicator_for_checkpoint->update_progress(duplicator->progress());
    ASSERT_TRUE(duplicator_for_checkpoint->progress().checkpoint_has_prepared);
}

TEST_P(replica_duplicator_test, prapre_dup)
{
    auto duplicator = create_test_duplicator(invalid_decree, 100);
    replica()->update_expect_last_durable_decree(100);
    duplicator->prepare_dup();
    wait_all(duplicator);
    ASSERT_EQ(last_durable_decree(), log_dup_start_decree(duplicator));
}

} // namespace replication
} // namespace dsn
