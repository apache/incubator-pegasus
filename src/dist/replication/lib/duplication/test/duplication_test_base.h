// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "dist/replication/lib/mutation_log_utils.h"
#include "dist/replication/test/replica_test/unit_test/replica_test_base.h"
#include "dist/replication/lib/duplication/replica_duplicator.h"
#include "dist/replication/lib/duplication/replica_duplicator_manager.h"
#include "dist/replication/lib/duplication/duplication_sync_timer.h"

namespace dsn {
namespace replication {

DEFINE_STORAGE_WRITE_RPC_CODE(RPC_DUPLICATION_IDEMPOTENT_WRITE, NOT_ALLOW_BATCH, IS_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_DUPLICATION_NON_IDEMPOTENT_WRITE, NOT_ALLOW_BATCH, NOT_IDEMPOTENT)

class duplication_test_base : public replica_test_base
{
public:
    duplication_test_base()
    {
        mutation_duplicator::creator = [](replica_base *r, dsn::string_view, dsn::string_view) {
            return make_unique<mock_mutation_duplicator>(r);
        };
        stub->_duplication_sync_timer = make_unique<duplication_sync_timer>(stub.get());
    }

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

    std::unique_ptr<replica_duplicator> create_test_duplicator(decree confirmed = invalid_decree)
    {
        duplication_entry dup_ent;
        dup_ent.dupid = 1;
        dup_ent.remote = "remote_address";
        dup_ent.status = duplication_status::DS_PAUSE;
        dup_ent.progress[_replica->get_gpid().get_partition_index()] = confirmed;
        return make_unique<replica_duplicator>(dup_ent, _replica.get());
    }

    std::map<int, log_file_ptr> open_log_file_map(const std::string &log_dir)
    {
        std::map<int, log_file_ptr> log_file_map;
        error_s err = log_utils::open_log_file_map(log_dir, log_file_map);
        EXPECT_EQ(err, error_s::ok());
        return log_file_map;
    }

    mutation_ptr create_test_mutation(int64_t decree, string_view data) override
    {
        auto mut = replica_test_base::create_test_mutation(decree, data);
        mut->data.updates[0].code = RPC_DUPLICATION_IDEMPOTENT_WRITE; // must be idempotent write
        return mut;
    }
};

} // namespace replication
} // namespace dsn
