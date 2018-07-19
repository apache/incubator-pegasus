// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_mutation_duplicator.h"
#include "pegasus_server_impl.h"
#include "base/pegasus_rpc_types.h"

#include <dsn/cpp/message_utils.h>
#include <dsn/utility/chrono_literals.h>
#include <dsn/dist/replication/duplication_common.h>
#include <rrdb/rrdb.client.h>

namespace dsn {
namespace replication {

/// static definition of mutation_duplicator::creator.
/*static*/ std::function<std::unique_ptr<mutation_duplicator>(
    const replica_base &, string_view, string_view)>
    mutation_duplicator::creator = [](const replica_base &r, string_view remote, string_view app) {
        return make_unique<pegasus::server::pegasus_mutation_duplicator>(r, remote, app);
    };

} // namespace replication
} // namespace dsn

namespace pegasus {
namespace server {

using namespace dsn::literals::chrono_literals;

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

pegasus_mutation_duplicator::pegasus_mutation_duplicator(const dsn::replication::replica_base &r,
                                                         dsn::string_view remote_cluster,
                                                         dsn::string_view app)
    : dsn::replication::replica_base(r)
{
    static bool _dummy = pegasus_client_factory::initialize(nullptr);

    pegasus_client *client = pegasus_client_factory::get_client(remote_cluster.data(), app.data());
    _client = static_cast<client::pegasus_client_impl *>(client);

    auto ret = dsn::replication::get_duplication_cluster_id(remote_cluster.data());
    dassert_replica(ret.is_ok(),
                    "invalid remote cluster: {}, err_ret: {}",
                    remote_cluster,
                    ret.get_error().description());
    _remote_cluster_id = static_cast<uint8_t>(ret.get_value());

    ddebug_replica("initiated backlog handler for local cluster [id:{}], "
                   "remote cluster [id:{}, addr:{}]",
                   get_current_cluster_id(),
                   _remote_cluster_id,
                   remote_cluster);
    dassert_replica(get_current_cluster_id() != _remote_cluster_id,
                    "invalid configuration of cluster_id");

    std::string str_gpid = fmt::format("{}", get_gpid());
    std::string name;

    name = fmt::format("duplicate_qps@{}", str_gpid);
    _duplicate_qps.init_app_counter(
        "app.pegasus", name.c_str(), COUNTER_TYPE_RATE, "statistic the qps of DUPLICATE request");

    name = fmt::format("duplicate_latency@{}", str_gpid);
    _duplicate_latency.init_app_counter("app.pegasus",
                                        name.c_str(),
                                        COUNTER_TYPE_NUMBER_PERCENTILES,
                                        "statistic the latency of DUPLICATE request");

    name = fmt::format("duplicate_failed_qps@{}", str_gpid);
    _duplicate_failed_qps.init_app_counter("app.pegasus",
                                           name.c_str(),
                                           COUNTER_TYPE_RATE,
                                           "statistic the qps of failed DUPLICATE request");
}

void pegasus_mutation_duplicator::send_request(uint64_t timestamp,
                                               dsn_message_t req,
                                               dsn::blob data,
                                               err_callback cb)
{
    dsn::task_code rpc_code(dsn_msg_task_code(req));

    // validate the rpc to be duplicated
    dsn::task_spec *task = dsn::task_spec::get(rpc_code);
    dassert_replica(task != nullptr && task->rpc_request_is_write_operation,
                    "invalid rpc type({})",
                    rpc_code.to_string());

    // extract the rpc wrapped inside if this is a DUPLICATE rpc
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_DUPLICATE) {
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

        // validate the rpc
        task = dsn::task_spec::get(rpc_code);
        dassert_replica(task != nullptr && task->rpc_request_is_write_operation,
                        "invalid rpc type({})",
                        rpc_code.to_string());
    }

    auto request = dsn::make_unique<dsn::apps::duplicate_request>();
    request->task_code = rpc_code;
    request->timetag =
        generate_timetag(timestamp, get_current_cluster_id(), is_delete_operation(req));
    request->raw_message = std::move(data);

    uint64_t partition_hash = get_hash_from_request(rpc_code, request->raw_message);
    duplicate_rpc rpc(std::move(request),
                      dsn::apps::RPC_RRDB_RRDB_DUPLICATE,
                      10_s, // TODO(wutao1): configurable timeout.
                      partition_hash);

    uint64_t start = dsn_now_ns();
    _client->async_duplicate(rpc,
                             [ cb = std::move(cb), rpc, start, this ](dsn::error_code err) mutable {
                                 _duplicate_qps->increment();
                                 if (err == dsn::ERR_OK) {
                                     err = dsn::error_code(rpc.response().error);
                                     _duplicate_latency->set(dsn_now_ns() - start);
                                 } else {
                                     _duplicate_failed_qps->increment();
                                 }
                                 cb(dsn::error_s::make(err));
                             });
}

} // namespace server
} // namespace pegasus
