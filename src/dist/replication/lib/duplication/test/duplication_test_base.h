// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "dist/replication/lib/mutation_log_utils.h"
#include "dist/replication/test/replica_test/unit_test/replica_test_base.h"
#include "dist/replication/lib/duplication/replica_duplicator.h"
#include "dist/replication/lib/duplication/replica_duplicator_manager.h"

namespace dsn {
namespace replication {

class duplication_test_base : public replica_test_base
{
public:
    duplication_test_base()
    {
        mutation_duplicator::creator = [](replica_base *r, dsn::string_view, dsn::string_view) {
            return make_unique<mock_mutation_duplicator>(r);
        };
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
};

} // namespace replication
} // namespace dsn
