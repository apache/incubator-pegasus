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

#pragma once

#include <dsn/dist/replication/mutation_duplicator.h>

#include "replica/mutation.h"

namespace dsn {
namespace replication {

class replica_duplicator;
class prepare_list;

// A sorted array of committed mutations that are ready for duplication.
// Not thread-safe.
class mutation_batch : replica_base
{
public:
    static constexpr int64_t PREPARE_LIST_NUM_ENTRIES{200};

    explicit mutation_batch(replica_duplicator *r);

    error_s add(mutation_ptr mu);

    mutation_tuple_set move_all_mutations();

    decree last_decree() const;

    // mutations with decree < d will be ignored.
    void set_start_decree(decree d);

    size_t size() const { return _loaded_mutations.size(); }

private:
    friend class replica_duplicator_test;

    std::unique_ptr<prepare_list> _mutation_buffer;
    mutation_tuple_set _loaded_mutations;
    decree _start_decree{invalid_decree};
};

using mutation_batch_u_ptr = std::unique_ptr<mutation_batch>;

/// Extract mutations into mutation_tuple_set if they are not WRITE_EMPTY.
extern void add_mutation_if_valid(mutation_ptr &, mutation_tuple_set &, decree start_decree);

} // namespace replication
} // namespace dsn
