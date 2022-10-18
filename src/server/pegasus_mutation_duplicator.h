/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include "replica/duplication/mutation_duplicator.h"
#include "replica/replica_base.h"
#include <rrdb/rrdb.code.definition.h>
#include "utils/flags.h"
#include "common//duplication_common.h"

#include "client_lib/pegasus_client_factory_impl.h"

namespace pegasus {
namespace server {

using namespace dsn::literals::chrono_literals;

// Duplicates the loaded mutations to the remote pegasus cluster using pegasus client.
class pegasus_mutation_duplicator : public dsn::replication::mutation_duplicator
{
    using mutation_tuple_set = dsn::replication::mutation_tuple_set;
    using mutation_tuple = dsn::replication::mutation_tuple;
    using duplicate_rpc = dsn::apps::duplicate_rpc;

public:
    pegasus_mutation_duplicator(dsn::replication::replica_base *r,
                                dsn::string_view remote_cluster,
                                dsn::string_view app);

    void duplicate(mutation_tuple_set muts, callback cb) override;

    ~pegasus_mutation_duplicator() override { _env.__conf.tracker->cancel_outstanding_tasks(); }

private:
    void send(uint64_t hash, callback cb);

    void on_duplicate_reply(uint64_t hash, callback, duplicate_rpc, dsn::error_code err);

private:
    friend class pegasus_mutation_duplicator_test;

    client::pegasus_client_impl *_client{nullptr};

    uint8_t _remote_cluster_id{0};
    std::string _remote_cluster;

    // The duplicate_rpc are isolated by their hash value from hash key.
    // Writes with the same hash are duplicated in mutation order to preserve data consistency,
    // otherwise they are duplicated concurrently to improve performance.
    std::map<uint64_t, std::deque<duplicate_rpc>> _inflights; // hash -> duplicate_rpc
    dsn::zlock _lock;

    size_t _total_shipped_size{0};

    dsn::perf_counter_wrapper _shipped_ops;
    dsn::perf_counter_wrapper _failed_shipping_ops;
};

// Decodes the binary `request_data` into write request in thrift struct, and
// calculates the hash value from the write's hash key.
extern uint64_t get_hash_from_request(dsn::task_code rpc_code, const dsn::blob &request_data);

} // namespace server
} // namespace pegasus
