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
    replica_base *, string_view, string_view)>
    mutation_duplicator::creator = [](replica_base *r, string_view remote, string_view app) {
        return make_unique<pegasus::server::pegasus_mutation_duplicator>(r, remote, app);
    };

} // namespace replication
} // namespace dsn

namespace pegasus {
namespace server {

using namespace dsn::literals::chrono_literals;

/*extern*/ uint64_t get_hash_from_request(dsn::task_code tc, const dsn::blob &data)
{
    if (tc == dsn::apps::RPC_RRDB_RRDB_PUT) {
        dsn::apps::update_request thrift_request;
        dsn::from_blob_to_thrift(data, thrift_request);
        return pegasus_key_hash(thrift_request.key);
    }
    if (tc == dsn::apps::RPC_RRDB_RRDB_REMOVE) {
        dsn::blob raw_key;
        dsn::from_blob_to_thrift(data, raw_key);
        return pegasus_key_hash(raw_key);
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

pegasus_mutation_duplicator::pegasus_mutation_duplicator(dsn::replication::replica_base *r,
                                                         dsn::string_view remote_cluster,
                                                         dsn::string_view app)
    : mutation_duplicator(r), _remote_cluster(remote_cluster)
{
    // initialize pegasus-client when this class is first time used.
    static __attribute__((unused)) bool _dummy = pegasus_client_factory::initialize(nullptr);

    pegasus_client *client = pegasus_client_factory::get_client(remote_cluster.data(), app.data());
    _client = static_cast<client::pegasus_client_impl *>(client);

    auto ret = dsn::replication::get_duplication_cluster_id(remote_cluster.data());
    dassert_replica(ret.is_ok(), // never possible, meta server disallows such remote_cluster.
                    "invalid remote cluster: {}, err_ret: {}",
                    remote_cluster,
                    ret.get_error());
    _remote_cluster_id = static_cast<uint8_t>(ret.get_value());

    ddebug_replica("initialize mutation duplicator for local cluster [id:{}], "
                   "remote cluster [id:{}, addr:{}]",
                   get_current_cluster_id(),
                   _remote_cluster_id,
                   remote_cluster);

    // never possible to duplicate data to itself
    dassert_replica(get_current_cluster_id() != _remote_cluster_id,
                    "invalid remote cluster: {} {}",
                    remote_cluster,
                    _remote_cluster_id);

    std::string str_gpid = fmt::format("{}", get_gpid());
    _shipped_ops.init_app_counter("app.pegasus",
                                  fmt::format("dup_shipped_ops@{}", str_gpid).c_str(),
                                  COUNTER_TYPE_RATE,
                                  "the total ops of DUPLICATE requests sent from this app");
    _failed_shipping_ops.init_app_counter(
        "app.pegasus",
        fmt::format("dup_failed_shipping_ops@{}", str_gpid).c_str(),
        COUNTER_TYPE_RATE,
        "the qps of failed DUPLICATE requests sent from this app");
}

static bool is_delete_operation(dsn::task_code code)
{
    return code == dsn::apps::RPC_RRDB_RRDB_REMOVE || code == dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE;
}

void pegasus_mutation_duplicator::send(uint64_t hash, callback cb)
{
    uint64_t start = dsn_now_ns();

    duplicate_rpc rpc;
    {
        dsn::zauto_lock _(_lock);
        rpc = _inflights[hash].front();
        _inflights[hash].pop_front();
    }

    _client->async_duplicate(rpc,
                             [cb, rpc, start, this](dsn::error_code err) mutable {
                                 on_duplicate_reply(std::move(cb), std::move(rpc), start, err);
                             },
                             _env.__conf.tracker);
}

void pegasus_mutation_duplicator::on_duplicate_reply(mutation_duplicator::callback cb,
                                                     duplicate_rpc rpc,
                                                     uint64_t start_ns,
                                                     dsn::error_code err)
{
    int perr = PERR_OK;
    if (err == dsn::ERR_OK) {
        perr = client::pegasus_client_impl::get_client_error(
            client::pegasus_client_impl::get_rocksdb_server_error(rpc.response().error));
    }

    if (perr != PERR_OK || err != dsn::ERR_OK) {
        _failed_shipping_ops->increment();

        // randomly log the 1% of the failed duplicate rpc.
        if (dsn::rand::next_double01() <= 0.01) {
            derror_replica("duplicate_rpc failed: {} [code:{}, cluster_id:{}, timestamp:{}]",
                           err == dsn::ERR_OK ? _client->get_error_string(perr) : err.to_string(),
                           rpc.request().task_code,
                           extract_cluster_id_from_timetag(rpc.request().timetag),
                           extract_timestamp_from_timetag(rpc.request().timetag));
        }
    } else {
        _shipped_ops->increment();
        _total_shipped_size +=
            rpc.dsn_request()->header->body_length + rpc.dsn_request()->header->hdr_length;
    }

    auto hash = static_cast<uint64_t>(rpc.request().hash);
    {
        dsn::zauto_lock _(_lock);
        if (perr != PERR_OK || err != dsn::ERR_OK) {
            // retry this rpc
            _inflights[hash].push_front(rpc);
            _env.schedule([hash, cb, this]() { send(hash, cb); }, 1_s);
            return;
        }
        if (_inflights[hash].empty()) {
            _inflights.erase(hash);
            if (_inflights.empty()) {
                // move forward to the next step.
                cb(_total_shipped_size);
            }
        } else {
            // start next rpc immediately
            _env.schedule([hash, cb, this]() { send(hash, cb); });
            return;
        }
    }
}

void pegasus_mutation_duplicator::duplicate(mutation_tuple_set muts, callback cb)
{
    _total_shipped_size = 0;

    for (auto mut : muts) {
        uint64_t timestamp = std::get<0>(mut);
        dsn::task_code rpc_code = std::get<1>(mut);
        dsn::blob data = std::get<2>(mut);
        uint64_t hash;

        // extract the rpc wrapped inside if this is a DUPLICATE rpc
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_DUPLICATE) {
            dsn::apps::duplicate_request dreq;
            dsn::from_blob_to_thrift(data, dreq);

            auto timetag = static_cast<uint64_t>(dreq.timetag);
            uint8_t from_cluster_id = extract_cluster_id_from_timetag(timetag);
            if (from_cluster_id == _remote_cluster_id) {
                // ignore this mutation to prevent infinite duplication loop.
                continue;
            }

            hash = static_cast<uint64_t>(dreq.hash);
            data = std::move(dreq.raw_message);
            rpc_code = dreq.task_code;
            timestamp = extract_timestamp_from_timetag(timetag);
        } else {
            hash = get_hash_from_request(rpc_code, data);
        }

        auto dreq = dsn::make_unique<dsn::apps::duplicate_request>();
        dreq->task_code = rpc_code;
        dreq->hash = hash;
        dreq->raw_message = std::move(data);
        dreq->timetag =
            generate_timetag(timestamp, get_current_cluster_id(), is_delete_operation(rpc_code));
        duplicate_rpc rpc(std::move(dreq),
                          dsn::apps::RPC_RRDB_RRDB_DUPLICATE,
                          10_s, // TODO(wutao1): configurable timeout.
                          hash);
        _inflights[hash].push_back(std::move(rpc));
    }

    if (_inflights.empty()) {
        cb(0);
        return;
    }
    auto inflights = _inflights;
    for (const auto &kv : inflights) {
        send(kv.first, cb);
    }
}

} // namespace server
} // namespace pegasus
