// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "dist/replication/test/replica_test/unit_test/replica_test_base.h"
#include "dist/replication/lib/duplication/replica_duplicator.h"
#include "dist/replication/lib/duplication/replica_duplicator_manager.h"

namespace dsn {
namespace replication {

class duplication_test_base : public replica_test_base
{
public:
    duplication_test_base() {}

    void add_dup(mock_replica *r, replica_duplicator_u_ptr dup)
    {
        r->get_replica_duplicator_manager()._duplications[dup->id()] = std::move(dup);
    }

    replica_duplicator *find_dup(mock_replica *r, dupid_t dupid)
    {
        auto &dup_entities = r->get_replica_duplicator_manager()._duplications;
        if (dup_entities.find(dupid) == dup_entities.end()) {
            return nullptr;
        }
        return dup_entities[dupid].get();
    }
};

} // namespace replication
} // namespace dsn
