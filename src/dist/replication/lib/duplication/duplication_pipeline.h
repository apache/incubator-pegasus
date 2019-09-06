// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/cpp/pipeline.h>
#include <dsn/dist/replication/replica_base.h>
#include <dsn/dist/replication/mutation_duplicator.h>

#include "dist/replication/lib/replica.h"
#include "replica_duplicator.h"

namespace dsn {
namespace replication {

using namespace literals::chrono_literals;

// load_mutation is a pipeline stage for loading mutations, aka mutation_tuple_set,
// to the next stage, `ship_mutation`.
// ThreadPool: THREAD_POOL_REPLICATION
class load_mutation : public replica_base,
                      public pipeline::when<>,
                      public pipeline::result<decree, mutation_tuple_set>
{
public:
    void run() override;

    /// ==== Implementation ==== ///

    load_mutation(replica_duplicator *duplicator, replica *r, load_from_private_log *load_private);

    ~load_mutation();
};

// ship_mutation is a pipeline stage receiving a set of mutations,
// sending them to the remote cluster. After finished, the pipeline
// will restart from load_mutation.
// ThreadPool: THREAD_POOL_REPLICATION
class ship_mutation : public replica_base,
                      public pipeline::when<decree, mutation_tuple_set>,
                      public pipeline::result<>
{
public:
    void run(decree &&last_decree, mutation_tuple_set &&in) override;

    /// ==== Implementation ==== ///

    explicit ship_mutation(replica_duplicator *duplicator);
};

} // namespace replication
} // namespace dsn
