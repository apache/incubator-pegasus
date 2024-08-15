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

#include "replica/mutation_log_utils.h"
#include "replica/test/replica_test_base.h"
#include "replica/duplication/replica_duplicator.h"
#include "replica/duplication/replica_duplicator_manager.h"
#include "replica/duplication/duplication_sync_timer.h"

namespace dsn {
namespace replication {

DEFINE_STORAGE_WRITE_RPC_CODE(RPC_DUPLICATION_IDEMPOTENT_WRITE, NOT_ALLOW_BATCH, IS_IDEMPOTENT)
DEFINE_STORAGE_WRITE_RPC_CODE(RPC_DUPLICATION_NON_IDEMPOTENT_WRITE, NOT_ALLOW_BATCH, NOT_IDEMPOTENT)

class duplication_test_base : public replica_test_base
{
public:
    duplication_test_base()
    {
        mutation_duplicator::creator = [](replica_base *r, std::string_view, std::string_view) {
            return std::make_unique<mock_mutation_duplicator>(r);
        };
        stub->_duplication_sync_timer = std::make_unique<duplication_sync_timer>(stub.get());
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

    std::unique_ptr<replica_duplicator>
    create_test_duplicator(decree confirmed_decree = invalid_decree)
    {
        duplication_entry dup_ent;
        dup_ent.dupid = 1;
        dup_ent.remote = "remote_address";
        dup_ent.status = duplication_status::DS_PAUSE;
        dup_ent.progress[_replica->get_gpid().get_partition_index()] = confirmed_decree;

        auto duplicator = std::make_unique<replica_duplicator>(dup_ent, _replica.get());
        return duplicator;
    }

    mutation_log::log_file_map_by_index open_log_file_map(const std::string &log_dir)
    {
        mutation_log::log_file_map_by_index log_file_map;
        error_s err = log_utils::open_log_file_map(log_dir, log_file_map);
        EXPECT_EQ(err, error_s::ok());
        return log_file_map;
    }

    mutation_ptr
    create_test_mutation(int64_t decree, int64_t last_committed_decree, const char *data) override
    {
        auto mut = replica_test_base::create_test_mutation(decree, last_committed_decree, data);
        mut->data.updates[0].code = RPC_DUPLICATION_IDEMPOTENT_WRITE; // must be idempotent write
        return mut;
    }

    mutation_ptr create_test_mutation(int64_t decree, const char *data) override
    {
        return duplication_test_base::create_test_mutation(decree, decree - 1, data);
    }

    void wait_all(const std::unique_ptr<replica_duplicator> &dup)
    {
        dup->tracker()->wait_outstanding_tasks();
        dup->_replica->tracker()->wait_outstanding_tasks();
    }
};

} // namespace replication
} // namespace dsn
