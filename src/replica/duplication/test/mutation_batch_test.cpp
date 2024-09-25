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

#include <algorithm>
#include <atomic>
#include <iterator>
#include <memory>
#include <set>
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
#include "replica/duplication/replica_duplicator.h"
#include "replica/mutation.h"
#include "replica/prepare_list.h"
#include "replica/test/mock_utils.h"
#include "task/task_code.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"

namespace dsn::replication {

class mutation_batch_test : public duplication_test_base
{
protected:
    mutation_batch_test()
    {
        _replica->init_private_log(_replica->dir());
        _duplicator = create_test_duplicator(0);
        _batcher = std::make_unique<mutation_batch>(_duplicator.get());
    }

    void reset_buffer(const decree last_commit, const decree start, const decree end) const
    {
        _batcher->_mutation_buffer->reset(last_commit);
        _batcher->_mutation_buffer->_start_decree = start;
        _batcher->_mutation_buffer->_end_decree = end;
    }

    void commit_buffer(const decree current_decree) const
    {
        _batcher->_mutation_buffer->commit(current_decree, COMMIT_TO_DECREE_HARD);
    }

    void check_mutation_contents(const std::vector<std::string> &expected_mutations) const
    {
        const auto all_mutations = _batcher->move_all_mutations();

        std::vector<std::string> actual_mutations;
        std::transform(all_mutations.begin(),
                       all_mutations.end(),
                       std::back_inserter(actual_mutations),
                       [](const mutation_tuple &tuple) { return std::get<2>(tuple).to_string(); });

        ASSERT_EQ(expected_mutations, actual_mutations);
    }

    std::unique_ptr<replica_duplicator> _duplicator;
    std::unique_ptr<mutation_batch> _batcher;
};

INSTANTIATE_TEST_SUITE_P(, mutation_batch_test, ::testing::Values(false, true));

TEST_P(mutation_batch_test, prepare_mutation)
{
    auto mu1 = create_test_mutation(1, 0, "first mutation");
    set_last_applied_decree(1);
    ASSERT_TRUE(_batcher->add(mu1));
    ASSERT_EQ(1, _batcher->last_decree());

    auto mu2 = create_test_mutation(2, 1, "abcde");
    set_last_applied_decree(2);
    ASSERT_TRUE(_batcher->add(mu2));
    ASSERT_EQ(2, _batcher->last_decree());

    auto mu3 = create_test_mutation(3, 2, "hello world");
    ASSERT_TRUE(_batcher->add(mu3));

    // The last decree has not been updated.
    ASSERT_EQ(2, _batcher->last_decree());

    auto mu4 = create_test_mutation(4, 2, "foo bar");
    ASSERT_TRUE(_batcher->add(mu4));
    ASSERT_EQ(2, _batcher->last_decree());

    // The committed mutation would be ignored.
    auto mu2_another = create_test_mutation(2, 1, "another second mutation");
    ASSERT_TRUE(_batcher->add(mu2_another));
    ASSERT_EQ(2, _batcher->last_decree());

    // The mutation with duplicate decree would be ignored.
    auto mu3_another = create_test_mutation(3, 2, "123 xyz");
    ASSERT_TRUE(_batcher->add(mu3_another));
    ASSERT_EQ(2, _batcher->last_decree());

    auto mu5 = create_test_mutation(5, 2, "5th mutation");
    set_last_applied_decree(5);
    ASSERT_TRUE(_batcher->add(mu5));
    ASSERT_EQ(5, _batcher->last_decree());

    check_mutation_contents({"first mutation", "abcde", "hello world", "foo bar", "5th mutation"});
}

TEST_P(mutation_batch_test, add_null_mutation)
{
    auto mu = create_test_mutation(1, nullptr);
    _batcher->add_mutation_if_valid(mu, 0);

    check_mutation_contents({""});
}

TEST_P(mutation_batch_test, add_empty_mutation)
{
    auto mu = create_test_mutation(1, "");
    _batcher->add_mutation_if_valid(mu, 0);

    check_mutation_contents({""});
}

// TODO(wangdan): once `string_view` function is removed from blob, drop this test.
TEST_P(mutation_batch_test, add_string_view_mutation)
{
    auto mu = create_test_mutation(1, nullptr);
    const std::string data("hello");
    mu->data.updates.back().data = blob(data.data(), 0, data.size());
    _batcher->add_mutation_if_valid(mu, 0);

    check_mutation_contents({"hello"});
}

TEST_P(mutation_batch_test, add_a_valid_mutation)
{
    auto mu = create_test_mutation(1, "hello");
    _batcher->add_mutation_if_valid(mu, 0);

    check_mutation_contents({"hello"});
}

TEST_P(mutation_batch_test, add_multiple_valid_mutations)
{
    // The mutation could not be reused, since in mutation_batch::add_mutation_if_valid
    // the elements of mutation::data::updates would be moved and nullified.
    auto mu1 = create_test_mutation(1, "hello");
    _batcher->add_mutation_if_valid(mu1, 0);

    auto mu2 = create_test_mutation(2, "world");
    _batcher->add_mutation_if_valid(mu2, 2);

    auto mu3 = create_test_mutation(3, "hi");
    _batcher->add_mutation_if_valid(mu3, 2);

    check_mutation_contents({"hello", "world", "hi"});
}

TEST_P(mutation_batch_test, add_invalid_mutation)
{
    auto mu2 = create_test_mutation(2, "world");
    _batcher->add_mutation_if_valid(mu2, 2);

    // mu1 would be ignored, since its decree is less than the start decree.
    auto mu1 = create_test_mutation(1, "hello");
    _batcher->add_mutation_if_valid(mu1, 2);

    auto mu3 = create_test_mutation(3, "hi");
    _batcher->add_mutation_if_valid(mu3, 2);

    auto mu4 = create_test_mutation(1, "ok");
    _batcher->add_mutation_if_valid(mu4, 1);

    // "ok" would be the first, since its timestamp (i.e. decree in create_test_mutation)
    // is the smallest.
    check_mutation_contents({"ok", "world", "hi"});
}

TEST_P(mutation_batch_test, ignore_non_idempotent_write)
{
    auto mu = create_test_mutation(1, "hello");
    mu->data.updates[0].code = RPC_DUPLICATION_NON_IDEMPOTENT_WRITE;
    _batcher->add_mutation_if_valid(mu, 0);
    check_mutation_contents({});
}

TEST_P(mutation_batch_test, mutation_buffer_commit)
{
    // Mock mutation_buffer[last=10, start=15, end=20], last + 1(next commit decree) is out of
    // [start~end], then last would become min_decree() - 1, see mutation_buffer::commit() for
    // details.
    reset_buffer(10, 15, 20);
    commit_buffer(15);
    ASSERT_EQ(14, _batcher->last_decree());
}

} // namespace dsn::replication
