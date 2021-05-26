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
};

TEST_F(mutation_batch_test, add_mutation_if_valid)
{
    mutation_tuple_set result;

    std::string s = "hello";
    mutation_ptr mu1 = create_test_mutation(1, s);
    add_mutation_if_valid(mu1, result, 0);
    mutation_tuple mt1 = *result.begin();

    result.clear();

    s = "world";
    mutation_ptr mu2 = create_test_mutation(2, s);
    add_mutation_if_valid(mu2, result, 0);
    mutation_tuple mt2 = *result.begin();

    ASSERT_EQ(std::get<2>(mt1).to_string(), "hello");
    ASSERT_EQ(std::get<2>(mt2).to_string(), "world");

    // decree 1 should be ignored
    mutation_ptr mu3 = create_test_mutation(1, s);
    add_mutation_if_valid(mu2, result, 2);
    ASSERT_EQ(result.size(), 2);
}

TEST_F(mutation_batch_test, ignore_non_idempotent_write)
{
    mutation_tuple_set result;

    std::string s = "hello";
    mutation_ptr mu = create_test_mutation(1, s);
    mu->data.updates[0].code = RPC_DUPLICATION_NON_IDEMPOTENT_WRITE;
    add_mutation_if_valid(mu, result, 0);
    ASSERT_EQ(result.size(), 0);
}

} // namespace replication
} // namespace dsn
