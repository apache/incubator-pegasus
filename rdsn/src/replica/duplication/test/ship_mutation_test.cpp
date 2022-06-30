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

#include "replica/duplication/mutation_batch.h"
#include "replica/duplication/duplication_pipeline.h"
#include "duplication_test_base.h"

namespace dsn {
namespace replication {

/*static*/ mock_mutation_duplicator::duplicate_function mock_mutation_duplicator::_func;

struct mock_stage : pipeline::when<>
{
    void run() override {}
};

class ship_mutation_test : public duplication_test_base
{
public:
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

    ship_mutation *mock_ship_mutation()
    {
        duplicator->_ship = make_unique<ship_mutation>(duplicator.get());
        return duplicator->_ship.get();
    }

    std::unique_ptr<replica_duplicator> duplicator;
};

TEST_F(ship_mutation_test, ship_mutation_tuple_set) { test_ship_mutation_tuple_set(); }

void retry(pipeline::base *base)
{
    base->schedule([base]() { retry(base); }, 10_s);
}

TEST_F(ship_mutation_test, pause)
{
    auto shipper = mock_ship_mutation();

    mutation_batch batch(duplicator.get());
    batch.add(create_test_mutation(1, "hello"));
    batch.add(create_test_mutation(2, "hello"));
    mutation_tuple_set in = batch.move_all_mutations();
    ASSERT_EQ(in.size(), 1);
    _replica->set_last_committed_decree(2);

    mock_mutation_duplicator::mock([this](mutation_tuple_set, mutation_duplicator::callback) {
        // mock RPC retry infinitely.
        retry(duplicator.get());
    });
    shipper->run(2, std::move(in));

    // the ongoing RPC will be abandoned when pause_dup called.
    duplicator->pause_dup_log();
}

} // namespace replication
} // namespace dsn
