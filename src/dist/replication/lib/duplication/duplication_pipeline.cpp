// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/dist/replication/replication_app_base.h>
#include <dsn/dist/fmt_logging.h>

#include "dist/replication/lib/replica_stub.h"
#include "duplication_pipeline.h"
#include "load_from_private_log.h"

namespace dsn {
namespace replication {

//               //
// load_mutation //
//               //

void load_mutation::run()
{
    // TBD
}

load_mutation::~load_mutation() = default;

load_mutation::load_mutation(replica_duplicator *duplicator,
                             replica *r,
                             load_from_private_log *load_private)
    : replica_base(r)
{
    // TBD
}

//               //
// ship_mutation //
//               //

void ship_mutation::run(decree &&last_decree, mutation_tuple_set &&in)
{
    // TBD
}

ship_mutation::ship_mutation(replica_duplicator *duplicator) : replica_base(duplicator)
{
    // TBD
}

} // namespace replication
} // namespace dsn
