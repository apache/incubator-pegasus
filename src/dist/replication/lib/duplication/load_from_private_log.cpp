// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "dist/replication/lib/replica.h"
#include "load_from_private_log.h"
#include "replica_duplicator.h"

namespace dsn {
namespace replication {

void load_from_private_log::run()
{
    // TBD
}

load_from_private_log::load_from_private_log(replica *r, replica_duplicator *dup)
    : replica_base(r), _private_log(r->private_log()), _duplicator(dup)
{
}

} // namespace replication
} // namespace dsn
