// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/dist/replication/duplication_mutation_duplicator.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replica_base.h>
#include <rrdb/rrdb.code.definition.h>

#include "client_lib/pegasus_client_factory_impl.h"

namespace pegasus {
namespace server {

// Duplicates the loaded mutations to the remote pegasus cluster.
class pegasus_mutation_duplicator : public dsn::replication::mutation_duplicator,
                                    public dsn::replication::replica_base
{
public:
    pegasus_mutation_duplicator(const dsn::replication::replica_base &r,
                                dsn::string_view remote_cluster,
                                dsn::string_view app);

    void duplicate(dsn::replication::mutation_tuple mutation, err_callback cb) override
    {
        send_request(std::get<0>(mutation), std::get<1>(mutation), std::get<2>(mutation), cb);
    }

private:
    void send_request(uint64_t timestamp, dsn_message_t req, dsn::blob data, err_callback cb);

private:
    client::pegasus_client_impl *_client;

    uint8_t _remote_cluster_id{0};

    dsn::perf_counter_wrapper _duplicate_qps;
    dsn::perf_counter_wrapper _duplicate_failed_qps;
    dsn::perf_counter_wrapper _duplicate_latency;
};

extern uint64_t get_hash_from_request(dsn::task_code rpc_code, const dsn::blob &request_data);

} // namespace server
} // namespace pegasus
