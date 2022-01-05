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

#include "replica/mutation_log_utils.h"
#include "replica/duplication/load_from_private_log.h"
#include "replica/duplication/duplication_pipeline.h"
#include "duplication_test_base.h"

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
    replica_duplicator_test() { _replica->init_private_log(_log_dir); }

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

        auto duplicator = make_unique<replica_duplicator>(dup_ent, _replica.get());
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

            duplicator->update_status_if_needed(duplication_status::DS_START);
            ASSERT_EQ(duplicator->_status, duplication_status::DS_START);
            auto expected_env = duplicator->_ship->_mutation_duplicator->_env;
            ASSERT_EQ(duplicator->tracker(), expected_env.__conf.tracker);
            ASSERT_EQ(duplicator->get_gpid().thread_hash(), expected_env.__conf.thread_hash);

            // corner cases: next_status is INIT
            duplicator->update_status_if_needed(duplication_status::DS_INIT);
            ASSERT_EQ(duplicator->_status, duplication_status::DS_START);
            duplicator->update_status_if_needed(duplication_status::DS_START);
            ASSERT_EQ(duplicator->_status, duplication_status::DS_START);

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

TEST_F(replica_duplicator_test, new_duplicator) { test_new_duplicator(); }

TEST_F(replica_duplicator_test, pause_start_duplication) { test_pause_start_duplication(); }

TEST_F(replica_duplicator_test, duplication_progress)
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
}

} // namespace replication
} // namespace dsn
