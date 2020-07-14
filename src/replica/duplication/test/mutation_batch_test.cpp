/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

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
