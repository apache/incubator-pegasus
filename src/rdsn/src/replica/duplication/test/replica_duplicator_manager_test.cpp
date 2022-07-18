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

#include "duplication_test_base.h"

namespace dsn {
namespace replication {

class replica_duplicator_manager_test : public duplication_test_base
{
public:
    void SetUp() override { stub = make_unique<mock_replica_stub>(); }

    void TearDown() override { stub.reset(); }

    void test_remove_non_existed_duplications()
    {
        auto r = stub->add_primary_replica(2, 1);
        auto &d = r->get_replica_duplicator_manager();

        duplication_entry ent;
        ent.dupid = 1;
        ent.status = duplication_status::DS_PAUSE;
        ent.remote = "dsn://slave-cluster";
        ent.progress[r->get_gpid().get_partition_index()] = 0;
        d.sync_duplication(ent);
        ASSERT_EQ(d._duplications.size(), 1);

        // remove all dup
        d.remove_non_existed_duplications({});
        ASSERT_EQ(d._duplications.size(), 0);

        ent.dupid = 2;
        d.sync_duplication(ent);
        ASSERT_EQ(d._duplications.size(), 1);
    }

    void test_set_confirmed_decree_non_primary()
    {
        auto r = stub->add_primary_replica(2, 1);
        auto &d = r->get_replica_duplicator_manager();

        duplication_entry ent;
        ent.dupid = 1;
        ent.status = duplication_status::DS_PAUSE;
        ent.remote = "dsn://slave-cluster";
        ent.progress[r->get_gpid().get_partition_index()] = 100;
        d.sync_duplication(ent);
        ASSERT_EQ(d._duplications.size(), 1);
        ASSERT_EQ(d._primary_confirmed_decree, invalid_decree);

        // replica failover
        r->as_secondary();

        d.update_confirmed_decree_if_secondary(99);
        ASSERT_EQ(d._duplications.size(), 0);
        ASSERT_EQ(d._primary_confirmed_decree, 99);

        // receives group check
        d.update_confirmed_decree_if_secondary(101);
        ASSERT_EQ(d._duplications.size(), 0);
        ASSERT_EQ(d._primary_confirmed_decree, 101);

        // confirmed decree never decreases
        d.update_confirmed_decree_if_secondary(0);
        ASSERT_EQ(d._primary_confirmed_decree, 101);
        d.update_confirmed_decree_if_secondary(1);
        ASSERT_EQ(d._primary_confirmed_decree, 101);

        // duplication removed and the confimed_decree = -1
        d.update_confirmed_decree_if_secondary(-1);
        ASSERT_EQ(d._primary_confirmed_decree, -1);
    }

    void test_get_duplication_confirms()
    {
        auto r = stub->add_primary_replica(2, 1);

        int total_dup_num = 10;
        int update_dup_num = 4; // the number of dups that will be updated

        for (dupid_t id = 1; id <= update_dup_num; id++) {
            duplication_entry ent;
            ent.dupid = id;
            ent.status = duplication_status::DS_PAUSE;
            ent.progress[r->get_gpid().get_partition_index()] = 0;

            auto dup = make_unique<replica_duplicator>(ent, r);
            dup->update_progress(dup->progress().set_last_decree(2).set_confirmed_decree(1));
            add_dup(r, std::move(dup));
        }

        for (dupid_t id = update_dup_num + 1; id <= total_dup_num; id++) {
            duplication_entry ent;
            ent.dupid = id;
            ent.status = duplication_status::DS_PAUSE;
            ent.progress[r->get_gpid().get_partition_index()] = 0;

            auto dup = make_unique<replica_duplicator>(ent, r);
            dup->update_progress(dup->progress().set_last_decree(1).set_confirmed_decree(1));
            add_dup(r, std::move(dup));
        }

        auto result = r->get_replica_duplicator_manager().get_duplication_confirms_to_update();
        ASSERT_EQ(result.size(), update_dup_num);
    }

    void test_min_confirmed_decree()
    {
        struct test_case
        {
            std::vector<int64_t> confirmed_decree;
            int64_t min_confirmed_decree;
        };

        auto r = stub->add_non_primary_replica(2, 1);
        auto assert_test = [r, this](test_case tt) {
            for (int id = 1; id <= tt.confirmed_decree.size(); id++) {
                duplication_entry ent;
                ent.dupid = id;
                ent.status = duplication_status::DS_PAUSE;
                ent.progress[r->get_gpid().get_partition_index()] = 0;

                auto dup = make_unique<replica_duplicator>(ent, r);
                dup->update_progress(dup->progress()
                                         .set_last_decree(tt.confirmed_decree[id - 1])
                                         .set_confirmed_decree(tt.confirmed_decree[id - 1]));
                add_dup(r, std::move(dup));
            }

            ASSERT_EQ(r->get_replica_duplicator_manager().min_confirmed_decree(),
                      tt.min_confirmed_decree);
            r->get_replica_duplicator_manager()._duplications.clear();
        };

        {
            // non-primary
            test_case tt{{1, 2, 3}, invalid_decree};
            assert_test(tt);
        }

        { // primary
            r->as_primary();
            test_case tt{{1, 2, 3}, 1};
            assert_test(tt);

            tt = {{1000}, 1000};
            assert_test(tt);

            tt = {{}, invalid_decree};
            assert_test(tt);
        }
    }
};

TEST_F(replica_duplicator_manager_test, get_duplication_confirms)
{
    test_get_duplication_confirms();
}

TEST_F(replica_duplicator_manager_test, set_confirmed_decree_non_primary)
{
    test_set_confirmed_decree_non_primary();
}

TEST_F(replica_duplicator_manager_test, remove_non_existed_duplications)
{
    test_remove_non_existed_duplications();
}

TEST_F(replica_duplicator_manager_test, min_confirmed_decree) { test_min_confirmed_decree(); }

TEST_F(replica_duplicator_manager_test, update_checkpoint_prepared)
{
    auto r = stub->add_primary_replica(2, 1);
    duplication_entry ent;
    ent.dupid = 1;
    ent.status = duplication_status::DS_PAUSE;
    ent.progress[r->get_gpid().get_partition_index()] = 0;

    auto dup = make_unique<replica_duplicator>(ent, r);
    r->update_last_durable_decree(100);
    dup->update_progress(dup->progress().set_last_decree(2).set_confirmed_decree(1));
    add_dup(r, std::move(dup));
    auto updates = r->get_replica_duplicator_manager().get_duplication_confirms_to_update();
    for (const auto &update : updates) {
        ASSERT_TRUE(update.checkpoint_prepared);
    }
}

} // namespace replication
} // namespace dsn
