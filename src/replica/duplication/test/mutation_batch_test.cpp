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

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "common/replication_other_types.h"
#include "consensus_types.h"
#include "duplication_test_base.h"
#include "gtest/gtest.h"
#include "replica/duplication/mutation_batch.h"
#include "replica/duplication/mutation_duplicator.h"
#include "replica/mutation.h"
#include "replica/prepare_list.h"
#include "runtime/task/task_code.h"
#include "utils/autoref_ptr.h"

namespace dsn {
namespace replication {

class mutation_batch_test : public duplication_test_base
{
public:
    void
    reset_buffer(const mutation_batch &batcher, const decree last_commit, decree start, decree end)
    {
        batcher._mutation_buffer->reset(last_commit);
        batcher._mutation_buffer->_start_decree = start;
        batcher._mutation_buffer->_end_decree = end;
    }

    void commit_buffer(const mutation_batch &batcher, const decree current_decree)
    {
        batcher._mutation_buffer->commit(current_decree, COMMIT_TO_DECREE_HARD);
    }
};

INSTANTIATE_TEST_SUITE_P(, mutation_batch_test, ::testing::Values(false, true));

TEST_P(mutation_batch_test, prepare_mutation)
{
    auto duplicator = create_test_duplicator(0);
    mutation_batch batcher(duplicator.get());

    auto mu1 = create_test_mutation(1, 0, "first mutation");
    set_last_applied_decree(1);
    ASSERT_TRUE(batcher.add(mu1));
    ASSERT_EQ(1, batcher.last_decree());

    auto mu2 = create_test_mutation(2, 1, "abcde");
    set_last_applied_decree(2);
    ASSERT_TRUE(batcher.add(mu2));
    ASSERT_EQ(2, batcher.last_decree());

    auto mu3 = create_test_mutation(3, 2, "hello world");
    ASSERT_TRUE(batcher.add(mu3));

    // The last decree has not been updated.
    ASSERT_EQ(2, batcher.last_decree());

    auto mu4 = create_test_mutation(4, 2, "foo bar");
    ASSERT_TRUE(batcher.add(mu4));
    ASSERT_EQ(2, batcher.last_decree());

    // The committed mutation would be ignored.
    auto mu2_another = create_test_mutation(2, 1, "another second mutation");
    ASSERT_TRUE(batcher.add(mu2_another));
    ASSERT_EQ(2, batcher.last_decree());

    // The mutation with duplicate decree would be ignored.
    auto mu3_another = create_test_mutation(3, 2, "123 xyz");
    ASSERT_TRUE(batcher.add(mu3_another));
    ASSERT_EQ(2, batcher.last_decree());

    auto mu5 = create_test_mutation(5, 2, "5th mutation");
    set_last_applied_decree(5);
    ASSERT_TRUE(batcher.add(mu5));
    ASSERT_EQ(5, batcher.last_decree());

    // Check contents of the mutations.
    const auto all_mutations = batcher.move_all_mutations();
    std::set<std::string> actual_mutations;
    std::transform(all_mutations.begin(),
                   all_mutations.end(),
                   std::inserter(actual_mutations, actual_mutations.end()),
                   [](const mutation_tuple &tuple) { return std::get<2>(tuple).to_string(); });
    ASSERT_EQ(std::set<std::string>(
                  {"first mutation", "abcde", "hello world", "foo bar", "5th mutation"}),
              actual_mutations);
}

TEST_P(mutation_batch_test, add_mutation_if_valid)
{
    auto duplicator = create_test_duplicator(0);
    mutation_batch batcher(duplicator.get());

    mutation_tuple_set result;

    std::string s = "hello";
    mutation_ptr mu1 = create_test_mutation(1, s);
    batcher.add_mutation_if_valid(mu1, 0);
    result = batcher.move_all_mutations();
    mutation_tuple mt1 = *result.begin();

    s = "world";
    mutation_ptr mu2 = create_test_mutation(2, s);
    batcher.add_mutation_if_valid(mu2, 0);
    result = batcher.move_all_mutations();
    mutation_tuple mt2 = *result.begin();

    ASSERT_EQ(std::get<2>(mt1).to_string(), "hello");
    ASSERT_EQ(std::get<2>(mt2).to_string(), "world");

    // decree 1 should be ignored
    mutation_ptr mu3 = create_test_mutation(1, s);
    batcher.add_mutation_if_valid(mu2, 2);
    batcher.add_mutation_if_valid(mu3, 1);
    result = batcher.move_all_mutations();
    ASSERT_EQ(result.size(), 2);
}

TEST_P(mutation_batch_test, ignore_non_idempotent_write)
{
    auto duplicator = create_test_duplicator(0);
    mutation_batch batcher(duplicator.get());

    std::string s = "hello";
    mutation_ptr mu = create_test_mutation(1, s);
    mu->data.updates[0].code = RPC_DUPLICATION_NON_IDEMPOTENT_WRITE;
    batcher.add_mutation_if_valid(mu, 0);
    mutation_tuple_set result = batcher.move_all_mutations();
    ASSERT_EQ(result.size(), 0);
}

TEST_P(mutation_batch_test, mutation_buffer_commit)
{
    auto duplicator = create_test_duplicator(0);
    mutation_batch batcher(duplicator.get());
    // mock mutation_buffer[last=10, start=15, end=20], last + 1(next commit decree) is out of
    // [start~end]
    reset_buffer(batcher, 10, 15, 20);
    commit_buffer(batcher, 15);
    ASSERT_EQ(batcher.last_decree(), 14);
}

} // namespace replication
} // namespace dsn
