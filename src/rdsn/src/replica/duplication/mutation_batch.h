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

#pragma once

#include "perf_counter/perf_counter_wrapper.h"
#include "replica/duplication/mutation_duplicator.h"

#include "replica/mutation.h"
#include "replica/prepare_list.h"

namespace dsn {
namespace replication {

class replica_duplicator;

class mutation_buffer : public prepare_list
{
public:
    mutation_buffer(replica_base *r,
                    decree init_decree,
                    int max_count,
                    mutation_committer committer);

    void commit(decree d, commit_type ct);

private:
    perf_counter_wrapper _counter_dulication_mutation_loss_count;
};

// A sorted array of committed mutations that are ready for duplication.
// Not thread-safe.
class mutation_batch : replica_base
{
public:
    static constexpr int64_t PREPARE_LIST_NUM_ENTRIES{200};

    explicit mutation_batch(replica_duplicator *r);

    error_s add(mutation_ptr mu);

    void add_mutation_if_valid(mutation_ptr &, decree start_decree);

    mutation_tuple_set move_all_mutations();

    decree last_decree() const;

    // mutations with decree < d will be ignored.
    void set_start_decree(decree d);

    void reset_mutation_buffer(decree d);

    size_t size() const { return _loaded_mutations.size(); }

    uint64_t bytes() const { return _total_bytes; }

private:
    friend class replica_duplicator_test;
    friend class mutation_batch_test;

    std::unique_ptr<prepare_list> _mutation_buffer;
    mutation_tuple_set _loaded_mutations;
    decree _start_decree{invalid_decree};
    uint64_t _total_bytes{0};
};

using mutation_batch_u_ptr = std::unique_ptr<mutation_batch>;

/// Extract mutations into mutation_tuple_set if they are not WRITE_EMPTY.
} // namespace replication
} // namespace dsn
