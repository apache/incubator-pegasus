// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "dist/replication/lib/duplication/mutation_batch.h"
#include "dist/replication/lib/duplication/duplication_pipeline.h"
#include "duplication_test_base.h"

namespace dsn {
namespace replication {

/*static*/ mock_mutation_duplicator::duplicate_function mock_mutation_duplicator::_func;

struct mock_stage : pipeline::when<>
{
    void run() override {}
};

struct ship_mutation_test : public duplication_test_base
{
    ship_mutation_test()
    {
        _replica->init_private_log(_log_dir);
        duplicator = create_test_duplicator();
    }

    // ensure ship_mutation retries after error.
    // ensure it clears up all pending mutations after stage ends.
    // ensure it update duplicator->last_decree after stage ends.
    void test_ship_mutation_tuple_set()
    {
        ship_mutation shipper(duplicator.get());
        mock_stage end;

        pipeline::base base;
        base.thread_pool(LPC_REPLICATION_LONG_LOW).task_tracker(_replica->tracker());
        base.from(shipper).link(end);

        mutation_batch batch(duplicator.get());
        batch.add(create_test_mutation(1, "hello"));
        batch.add(create_test_mutation(2, "hello"));
        mutation_tuple_set in = batch.move_all_mutations();
        _replica->set_last_committed_decree(2);

        std::vector<mutation_tuple> expected;
        for (auto mut : in) {
            expected.push_back(std::move(mut));
        }

        mock_mutation_duplicator::mock(
            [&expected](mutation_tuple_set muts, mutation_duplicator::callback cb) {
                int i = 0;
                for (auto mut : muts) {
                    ASSERT_EQ(std::get<0>(expected[i]), std::get<0>(mut));
                    ASSERT_EQ(std::get<1>(expected[i]), std::get<1>(mut));
                    ASSERT_EQ(std::get<2>(expected[i]).to_string(), std::get<2>(mut).to_string());
                    ASSERT_EQ(std::get<2>(expected[i]).to_string(), "hello");
                    i++;
                }
                cb(0);
            });

        shipper.run(2, std::move(in));

        base.wait_all();
        ASSERT_EQ(duplicator->progress().last_decree, 2);
    }

    std::unique_ptr<replica_duplicator> duplicator;
};

TEST_F(ship_mutation_test, ship_mutation_tuple_set) { test_ship_mutation_tuple_set(); }

} // namespace replication
} // namespace dsn
