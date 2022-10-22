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
#include "replica/duplication/mutation_batch.h"

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

TEST_F(mutation_batch_test, add_mutation_if_valid)
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

TEST_F(mutation_batch_test, ignore_non_idempotent_write)
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

TEST_F(mutation_batch_test, mutation_buffer_commit)
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
