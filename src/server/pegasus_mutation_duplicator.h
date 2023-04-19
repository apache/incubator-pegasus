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

#include <stddef.h>
#include <stdint.h>
#include <deque>
#include <map>
#include <string>

#include "perf_counter/perf_counter_wrapper.h"
#include "replica/duplication/mutation_duplicator.h"
#include "rrdb/rrdb.client.h"
#include "runtime/pipeline.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_tracker.h"
#include "utils/chrono_literals.h"
#include "utils/string_view.h"
#include "utils/zlocks.h"

namespace pegasus {
class blob;
class error_code;
namespace replication {
struct replica_base;
} // namespace replication
} // namespace pegasus

namespace pegasus {
namespace client {
class pegasus_client_impl;
} // namespace client

namespace server {

using namespace literals::chrono_literals;

// Duplicates the loaded mutations to the remote pegasus cluster using pegasus client.
class pegasus_mutation_duplicator : public replication::mutation_duplicator
{
    using mutation_tuple_set = replication::mutation_tuple_set;
    using mutation_tuple = replication::mutation_tuple;
    using duplicate_rpc = apps::duplicate_rpc;

public:
    pegasus_mutation_duplicator(replication::replica_base *r,
                                string_view remote_cluster,
                                string_view app);

    void duplicate(mutation_tuple_set muts, callback cb) override;

    ~pegasus_mutation_duplicator() override { _env.__conf.tracker->cancel_outstanding_tasks(); }

private:
    void send(uint64_t hash, callback cb);

    void on_duplicate_reply(uint64_t hash, callback, duplicate_rpc, error_code err);

private:
    friend class pegasus_mutation_duplicator_test;

    client::pegasus_client_impl *_client{nullptr};

    uint8_t _remote_cluster_id{0};
    std::string _remote_cluster;

    // The duplicate_rpc are isolated by their hash value from hash key.
    // Writes with the same hash are duplicated in mutation order to preserve data consistency,
    // otherwise they are duplicated concurrently to improve performance.
    std::map<uint64_t, std::deque<duplicate_rpc>> _inflights; // hash -> duplicate_rpc
    zlock _lock;

    size_t _total_shipped_size{0};

    perf_counter_wrapper _shipped_ops;
    perf_counter_wrapper _failed_shipping_ops;
};

// Decodes the binary `request_data` into write request in thrift struct, and
// calculates the hash value from the write's hash key.
extern uint64_t get_hash_from_request(task_code rpc_code, const blob &request_data);

} // namespace server
} // namespace pegasus
