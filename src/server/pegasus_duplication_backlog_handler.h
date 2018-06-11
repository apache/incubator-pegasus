// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/dist/replication/duplication_backlog_handler.h>
#include <dsn/dist/fmt_logging.h>
#include <rrdb/rrdb.code.definition.h>

#include "client_lib/pegasus_client_factory_impl.h"

namespace pegasus {
namespace server {

// Duplicates the loaded mutations to the remote pegasus cluster.
class pegasus_duplication_backlog_handler : public dsn::replication::duplication_backlog_handler
{
public:
    pegasus_duplication_backlog_handler(dsn::gpid gpid,
                                        const std::string &remote_cluster,
                                        const std::string &app);

    void duplicate(dsn::replication::mutation_tuple mutation, err_callback cb) override
    {
        send_request(std::get<0>(mutation), std::get<1>(mutation), std::get<2>(mutation), cb);
    }

    void send_request(uint64_t timestamp, dsn_message_t req, dsn::blob data, err_callback cb);

private:
    client::pegasus_client_impl *_client;

    uint8_t _cluster_id{0}; // cluster id of local cluster.
    uint8_t _remote_cluster_id{0};
};

class pegasus_duplication_backlog_handler_factory
    : public dsn::replication::duplication_backlog_handler_factory
{
    using dup_handler = dsn::replication::duplication_backlog_handler;

public:
    pegasus_duplication_backlog_handler_factory() { pegasus_client_factory::initialize(nullptr); }

    std::unique_ptr<dup_handler>
    create(dsn::gpid gpid, const std::string &remote, const std::string &app) override
    {
        return dsn::make_unique<pegasus_duplication_backlog_handler>(gpid, remote, app);
    }
};

extern uint64_t get_hash_from_request(dsn::task_code rpc_code, const dsn::blob &request_data);

} // namespace server
} // namespace pegasus
