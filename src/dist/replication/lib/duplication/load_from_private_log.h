// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/cpp/pipeline.h>
#include <dsn/utility/errors.h>
#include <dsn/dist/replication/mutation_duplicator.h>

#include "dist/replication/lib/mutation_log.h"

namespace dsn {
namespace replication {

class replica_duplicator;
class replica_stub;

/// Loads mutations from private log into memory.
/// It works in THREAD_POOL_REPLICATION_LONG (LPC_DUPLICATION_LOAD_MUTATIONS),
/// which permits tasks to be executed in a blocking way.
/// NOTE: The resulted `mutation_tuple_set` may be empty.
class load_from_private_log : public replica_base,
                              public pipeline::when<>,
                              public pipeline::result<decree, mutation_tuple_set>
{
public:
    load_from_private_log(replica *r, replica_duplicator *dup);

    // Start loading block from private log file.
    // The loaded mutations will be passed down to `ship_mutation`.
    void run() override;

private:
    friend class load_from_private_log_test;

    mutation_log_ptr _private_log;
    replica_duplicator *_duplicator;
};

} // namespace replication
} // namespace dsn
