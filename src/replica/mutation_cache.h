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

#include "common/replication_common.h"
#include "mutation.h"
#include <vector>
#include <atomic>

namespace dsn {
namespace replication {

// mutation_cache is an in-memory array that stores a limited number
// (SEE FLAGS_max_mutation_count_in_prepare_list) of mutation log entries.
//
// Inherited by: prepare_list
class mutation_cache
{
public:
    mutation_cache(decree init_decree, int max_count);
    // only used when copy mutations whose client_request will not reply
    mutation_cache(const mutation_cache &cache);
    ~mutation_cache();

    error_code put(mutation_ptr &mu);
    mutation_ptr pop_min();
    mutation_ptr get_mutation_by_decree(decree decree);
    void reset(decree init_decree, bool clear_mutations);

    decree min_decree() const { return _start_decree; }
    decree max_decree() const { return _end_decree; }
    int count() const { return _interval; }
    int capacity() const { return _max_count; }

private:
    friend class mutation_batch_test;

    std::vector<mutation_ptr> _array;
    int _max_count;

    int _interval;

    int _start_idx;
    int _end_idx;
    decree _start_decree;
    std::atomic<decree> _end_decree;
};
}
} // namespace
