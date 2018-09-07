// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/dist/replication/mutation_duplicator.h>
#include <dsn/dist/replication/replica_base.h>
#include <rrdb/rrdb.code.definition.h>

#include "client_lib/pegasus_client_factory_impl.h"

namespace pegasus {
namespace server {

// Duplicates the loaded mutations to the remote pegasus cluster.
class pegasus_mutation_duplicator : public dsn::replication::mutation_duplicator
{
    using mutation_tuple_set = dsn::replication::mutation_tuple_set;
    using mutation_tuple = dsn::replication::mutation_tuple;
    using duplicate_rpc = dsn::apps::duplicate_rpc;

public:
    pegasus_mutation_duplicator(const dsn::replication::replica_base &r,
                                dsn::string_view remote_cluster,
                                dsn::string_view app);

    void duplicate(mutation_tuple_set muts, callback cb) override;

private:
    void send(uint64_t hash, callback cb);

private:
    client::pegasus_client_impl *_client;

    uint8_t _remote_cluster_id{0};
    std::string _remote_cluster;

    // hash -> duplicate_rpc
    std::map<uint64_t, std::deque<duplicate_rpc>> _inflights;
    dsn::service::zlock _lock;

    dsn::perf_counter_wrapper _duplicate_qps;
    dsn::perf_counter_wrapper _duplicate_failed_qps;
    dsn::perf_counter_wrapper _duplicate_latency;
};

extern uint64_t get_hash_from_request(dsn::task_code rpc_code, const dsn::blob &request_data);

} // namespace server
} // namespace pegasus
