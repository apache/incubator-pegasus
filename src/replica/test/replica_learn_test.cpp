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

#include <memory>
#include <string>
#include <utility>

#include "common/fs_manager.h"
#include "common/gpid.h"
#include "common/replication_common.h"
#include "common/replication_other_types.h"
#include "consensus_types.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "mock_utils.h"
#include "replica/duplication/test/duplication_test_base.h"
#include "replica/prepare_list.h"
#include "replica/replica_context.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace replication {

/*static*/ mock_mutation_duplicator::duplicate_function mock_mutation_duplicator::_func;

class replica_learn_test : public duplication_test_base
{
public:
    replica_learn_test() = default;

    std::unique_ptr<mock_replica> create_duplicating_replica()
    {
        gpid pid(1, 0);
        app_info ai;
        ai.app_type = replication_options::kReplicaAppType;
        ai.duplicating = true;

        dir_node *dn = stub->get_fs_manager()->find_best_dir_for_new_replica(pid);
        CHECK_NOTNULL(dn, "");
        auto r = std::make_unique<mock_replica>(stub.get(), pid, ai, dn);
        r->as_primary();
        return r;
    }

    void test_get_learn_start_decree()
    {
        { // no duplication
            learn_request req;
            req.last_committed_decree_in_app = 5;
            req.max_gced_decree = 3;

            // local_committed_decree = 5
            _replica->mock_max_gced_decree(0);
            _replica->_prepare_list->reset(5);

            ASSERT_EQ(_replica->get_learn_start_decree(req), 6);
        }
        struct test_data
        {
            decree learner_last_committed_decree;
            decree learner_max_gced_decree;
            decree learnee_local_committed_decree;
            decree learnee_max_gced_decree;
            decree min_confirmed_decree;

            decree wlearn_start_decree;
        } tests[] = {
            // min_confirmed_decree(3) >= 0
            // learn_start_decree_for_dup(4) < learn_start_decree_no_dup(6)
            // request.max_gced_decree == invalid_decree
            {5, invalid_decree, 5, 0, 3, 4},

            // min_confirmed_decree(3) >= 0
            // learn_start_decree_for_dup(4) < learn_start_decree_no_dup(6)
            // learn_start_decree_for_dup(4) <= request.max_gced_decree(4)
            {5, 4, 5, 0, 3, 4},

            // min_confirmed_decree(3) >= 0
            // learn_start_decree_for_dup(4) < learn_start_decree_no_dup(6)
            // learn_start_decree_for_dup(4) > request.max_gced_decree(0)
            {5, 0, 5, 0, 3, 6},

            // min_confirmed_decree(3) >= 0
            // learn_start_decree_for_dup(4) > learn_start_decree_no_dup(1)
            {0, 4, 5, 0, 3, 1},

            // min_confirmed_decree == invalid_decree
            // local_gced == invalid_decree
            // abnormal case
            {5, invalid_decree, 5, invalid_decree, invalid_decree, 6},

            // min_confirmed_decree == invalid_decree
            // local_gced(2) != invalid_decree
            // learn_start_decree_for_dup(3) < learn_start_decree_no_dup(6)
            // request.max_gced_decree == invalid_decree
            {5, invalid_decree, 5, 2, invalid_decree, 3},

            // min_confirmed_decree == invalid_decree
            // local_gced(2) != invalid_decree
            // learn_start_decree_for_dup(3) < learn_start_decree_no_dup(6)
            // learn_start_decree_for_dup(3) <= request.max_gced_decree(3)
            {5, 3, 5, 2, invalid_decree, 3},
            // local_gced(0) != invalid_decree
            // learn_start_decree_for_dup(1) < learn_start_decree_no_dup(6)
            // learn_start_decree_for_dup(1) <= request.max_gced_decree(3)
            {5, 3, 5, 0, invalid_decree, 1},

            // min_confirmed_decree == invalid_decree
            // local_gced(2) != invalid_decree
            // learn_start_decree_for_dup(3) < learn_start_decree_no_dup(6)
            // learn_start_decree_for_dup(3) > request.max_gced_decree(0)
            {5, 0, 5, 2, invalid_decree, 6},

            // min_confirmed_decree == invalid_decree
            // local_gced(2) != invalid_decree
            // learn_start_decree_for_dup(3) > learn_start_decree_no_dup(1)
            {0, invalid_decree, 5, 2, invalid_decree, 1},
            // learn_start_decree_for_dup(3) > learn_start_decree_no_dup(2)
            {1, invalid_decree, 5, 2, invalid_decree, 2},
        };

        int id = 1;
        for (auto tt : tests) {
            _replica = create_duplicating_replica();
            _replica->mock_max_gced_decree(tt.learnee_max_gced_decree);

            learn_request req;
            req.last_committed_decree_in_app = tt.learner_last_committed_decree;
            req.max_gced_decree = tt.learner_max_gced_decree;

            _replica->_prepare_list->reset(tt.learnee_local_committed_decree);

            _replica->init_private_log(_log_dir);
            auto dup = create_test_duplicator(tt.min_confirmed_decree);
            add_dup(_replica.get(), std::move(dup));

            ASSERT_EQ(_replica->get_learn_start_decree(req), tt.wlearn_start_decree)
                << "case #" << id;
            id++;
        }
    }

    void test_get_max_gced_decree_for_learn()
    {
        struct test_data
        {
            decree first_learn_start_decree;
            decree plog_max_gced_decree;

            decree want;
        } tests[] = {{invalid_decree, 10, 10},
                     {invalid_decree, invalid_decree, invalid_decree},
                     {10, 20, 9},
                     {10, invalid_decree, 9},
                     {10, 5, 5}};
        for (auto tt : tests) {
            _replica = create_duplicating_replica();
            _replica->mock_max_gced_decree(tt.plog_max_gced_decree);
            _replica->_potential_secondary_states.first_learn_start_decree =
                tt.first_learn_start_decree;
            ASSERT_EQ(_replica->get_max_gced_decree_for_learn(), tt.want);
        }
    }
};

INSTANTIATE_TEST_SUITE_P(, replica_learn_test, ::testing::Values(false, true));

TEST_P(replica_learn_test, get_learn_start_decree) { test_get_learn_start_decree(); }

TEST_P(replica_learn_test, get_max_gced_decree_for_learn) { test_get_max_gced_decree_for_learn(); }

} // namespace replication
} // namespace dsn
