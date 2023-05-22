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

#include <memory>

#include "common/replication_other_types.h"
#include "replica/duplication/mutation_duplicator.h"
#include "replica/replica_base.h"
#include "runtime/pipeline.h"
#include "utils/chrono_literals.h"
#include "utils/metrics.h"

namespace dsn {
namespace replication {
class load_from_private_log;
class replica;
class replica_duplicator;
class replica_stub;

using namespace literals::chrono_literals;

// load_mutation is a pipeline stage for loading mutations, aka mutation_tuple_set,
// to the next stage, `ship_mutation`.
// ThreadPool: THREAD_POOL_REPLICATION
class load_mutation final : public replica_base,
                            public pipeline::when<>,
                            public pipeline::result<decree, mutation_tuple_set>
{
public:
    void run() override;

    /// ==== Implementation ==== ///

    load_mutation(replica_duplicator *duplicator, replica *r, load_from_private_log *load_private);

    ~load_mutation();

private:
    load_from_private_log *_log_on_disk;
    decree _start_decree{0};

    replica *_replica{nullptr};
    replica_duplicator *_duplicator{nullptr};
};

// ship_mutation is a pipeline stage receiving a set of mutations,
// sending them to the remote cluster. After finished, the pipeline
// will restart from load_mutation.
// ThreadPool: THREAD_POOL_REPLICATION
class ship_mutation final : public replica_base,
                            public pipeline::when<decree, mutation_tuple_set>,
                            public pipeline::result<>
{
public:
    void run(decree &&last_decree, mutation_tuple_set &&in) override;

    /// ==== Implementation ==== ///

    explicit ship_mutation(replica_duplicator *duplicator);

    void ship(mutation_tuple_set &&in);

private:
    void update_progress();

    friend class ship_mutation_test;
    friend class replica_duplicator_test;

    std::unique_ptr<mutation_duplicator> _mutation_duplicator;

    replica_duplicator *_duplicator;
    replica *_replica;
    replica_stub *_stub;

    decree _last_decree{invalid_decree};

    METRIC_VAR_DECLARE_counter(dup_shipped_bytes);
};

} // namespace replication
} // namespace dsn
