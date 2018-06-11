// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_duplication_backlog_handler.h"
#include "pegasus_server_impl.h"
#include "base/pegasus_rpc_types.h"

#include <dsn/cpp/message_utils.h>
#include <dsn/utility/chrono_literals.h>
#include <rrdb/rrdb.client.h>

namespace pegasus {
namespace server {

using namespace dsn::literals::chrono_literals;

bool _dummy = dsn::replication::register_backlog_handler_factory(
    []() { return new pegasus_duplication_backlog_handler_factory(); });

static inline dsn::task_code get_duplicate_type(dsn::task_code rpc_code)
{
    dsn::task_spec *task = dsn::task_spec::get(rpc_code);
    if (task == nullptr || !task->rpc_request_is_write_operation) {
        return dsn::task_code(-1);
    }
    if (task->rpc_request_is_write_allow_batch) {
        return dsn::apps::RPC_RRDB_RRDB_BATCHED_DUPLICATE;
    }
    return dsn::apps::RPC_RRDB_RRDB_DUPLICATE;
}

static inline bool is_duplicate_type(dsn::task_code tc)
{
    return tc == dsn::apps::RPC_RRDB_RRDB_DUPLICATE ||
           tc == dsn::apps::RPC_RRDB_RRDB_BATCHED_DUPLICATE;
}

static inline bool is_delete_operation(dsn_message_t req)
{
    dsn::task_code tc = dsn_msg_task_code(req);
    return tc == dsn::apps::RPC_RRDB_RRDB_REMOVE || tc == dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE;
}

/*extern*/ uint64_t get_hash_from_request(dsn::task_code tc, const dsn::blob &data)
{
    if (tc == dsn::apps::RPC_RRDB_RRDB_PUT || tc == dsn::apps::RPC_RRDB_RRDB_REMOVE) {
        dsn::apps::update_request thrift_request;
        dsn::from_blob_to_thrift(data, thrift_request);
        return pegasus_key_hash(thrift_request.key);
    }
    if (tc == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
        dsn::apps::multi_put_request thrift_request;
        dsn::from_blob_to_thrift(data, thrift_request);
        return pegasus_hash_key_hash(thrift_request.hash_key);
    }
    if (tc == dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE) {
        dsn::apps::multi_remove_request thrift_request;
        dsn::from_blob_to_thrift(data, thrift_request);
        return pegasus_hash_key_hash(thrift_request.hash_key);
    }
    dfatal("unexpected task code: %s", tc.to_string());
    __builtin_unreachable();
}

pegasus_duplication_backlog_handler::pegasus_duplication_backlog_handler(
    dsn::gpid gpid, const std::string &remote_cluster, const std::string &app)
    : dsn::replication::duplication_backlog_handler(gpid)
{
    ddebug_f(
        "initiates backlog handler for app({}) gpid({}) remote({})", app, gpid, remote_cluster);

    pegasus_client *client =
        pegasus_client_factory::get_client(remote_cluster.c_str(), app.c_str());
    _client = static_cast<client::pegasus_client_impl *>(client);

    _cluster_id = static_cast<uint8_t>(dsn_config_get_value_uint64(
        "pegasus.server", "pegasus_cluster_id", 1, "The ID of this pegasus cluster."));

    _remote_cluster_id = static_cast<uint8_t>(dsn_uri_to_cluster_id(remote_cluster.c_str()));
    if (_remote_cluster_id < 0) {
        dfatal("no ");
    }
}

void pegasus_duplication_backlog_handler::send_request(uint64_t timestamp,
                                                       dsn_message_t req,
                                                       dsn::blob data,
                                                       err_callback cb)
{
    dsn::task_code rpc_code(dsn_msg_task_code(req));

    dsn::task_code duplicate_type = get_duplicate_type(rpc_code);
    if (duplicate_type.code() != -1 && is_duplicate_type(rpc_code)) {
        dsn::apps::duplicate_request request;
        dsn::from_blob_to_thrift(data, request);

        uint8_t from_cluster_id =
            extract_cluster_id_from_timetag(static_cast<uint64_t>(request.timetag));
        if (from_cluster_id == _remote_cluster_id) {
            // ignore this mutation to prevent infinite replication loop.
            cb(dsn::error_s::ok());
            return;
        }

        rpc_code = request.task_code;
        data = std::move(request.raw_message);
        duplicate_type = get_duplicate_type(rpc_code);
    }
    dassert(duplicate_type != -1, "unexpected type(%s)", rpc_code.to_string());

    auto request = dsn::make_unique<dsn::apps::duplicate_request>();
    request->task_code = rpc_code;
    request->timetag = generate_timetag(timestamp, _cluster_id, is_delete_operation(req));
    request->raw_message = std::move(data);

    uint64_t partition_hash = get_hash_from_request(rpc_code, request->raw_message);
    duplicate_rpc rpc(std::move(request),
                      duplicate_type,
                      10_s, // TODO(wutao1): configurable timeout.
                      partition_hash);

    _client->async_duplicate(rpc, [ cb = std::move(cb), rpc ](dsn::error_code err) {
        if (err == dsn::ERR_OK) {
            err = dsn::error_code(rpc.response().error);
        }
        cb(dsn::error_s::make(err));
    });
}

} // namespace server
} // namespace pegasus
