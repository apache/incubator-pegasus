// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/cpp/message_utils.h>

#include "base/pegasus_utils.h"
#include "base/pegasus_key_schema.h"
#include "pegasus_server_write.h"
#include "pegasus_server_impl.h"
#include "logging_utils.h"

namespace pegasus {
namespace server {

pegasus_server_write::pegasus_server_write(pegasus_server_impl *server)
    : replica_base(*server), _verbose_log(server->_verbose_log), _cluster_id(server->_cluster_id)
{
    _write_svc = dsn::make_unique<pegasus_write_service>(server);
}

int pegasus_server_write::on_batched_write_requests(dsn_message_t *requests,
                                                    int count,
                                                    int64_t decree,
                                                    uint64_t timestamp)
{
    _put_ctx = db_write_context::put(decree, timestamp, _cluster_id);
    _remove_ctx = db_write_context::remove(decree, timestamp, _cluster_id);

    // Write down empty record (RPC_REPLICATION_WRITE_EMPTY) to update
    // rocksdb's `last_flushed_decree` (see rocksdb::DB::GetLastFlushedDecree())
    // TODO(wutao1): remove it when shared log is removed.
    if (count == 0) {
        return _write_svc->empty_put(_put_ctx);
    }

    dsn::task_code rpc_code(dsn_msg_task_code(requests[0]));
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
        dassert(count == 1, "");
        auto rpc = multi_put_rpc::auto_reply(requests[0]);
        on_multi_put(rpc);
        return rpc.response().error;
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE) {
        dassert(count == 1, "");
        auto rpc = multi_remove_rpc::auto_reply(requests[0]);
        on_multi_remove(rpc);
        return rpc.response().error;
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_DUPLICATE) {
        dassert(count == 1, "");
        auto rpc = duplicate_rpc::auto_reply(requests[0]);
        on_duplicate(rpc);
        return rpc.response().error;
    }

    return on_batched_writes(requests, count, decree);
}

void pegasus_server_write::on_duplicate_impl(bool batched,
                                             const dsn::apps::duplicate_request &request,
                                             dsn::apps::duplicate_response &resp)
{
    dsn::task_code rpc_code = request.task_code;
    dsn_message_t write = dsn::from_blob_to_received_msg(rpc_code, dsn::blob(request.raw_message));

    auto remote_timetag = static_cast<uint64_t>(request.timetag);
    dassert(remote_timetag > 0, "timetag field is not set in duplicate_request");
    _remove_ctx.remote_timetag = remote_timetag;
    _put_ctx.remote_timetag = remote_timetag;

    if (batched) {
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_PUT) {
            put_rpc rpc(write);
            on_single_put_in_batch(rpc);
            resp.error = rpc.response().error;
            _put_rpc_batch.emplace_back(std::move(rpc));
            return;
        }
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_REMOVE) {
            remove_rpc rpc(write);
            on_single_remove_in_batch(rpc);
            resp.error = rpc.response().error;
            _remove_rpc_batch.emplace_back(std::move(rpc));
            return;
        }
    } else {
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
            multi_put_rpc rpc(write);
            on_multi_put(rpc);
            resp.error = rpc.response().error;
            return;
        }
        if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE) {
            multi_remove_rpc rpc(write);
            on_multi_remove(rpc);
            resp.error = rpc.response().error;
            return;
        }
    }

    dfatal_replica(
        "{}: this rpc({}) is not expected to be loaded in duplicate_request (is batched: {})",
        rpc_code.to_string(),
        batched);
    __builtin_unreachable();
}

int pegasus_server_write::on_batched_writes(dsn_message_t *requests, int count, int64_t decree)
{
    int err;
    {
        _write_svc->batch_prepare();

        for (int i = 0; i < count; ++i) {
            dassert(requests[i] != nullptr, "");

            dsn::task_code rpc_code(dsn_msg_task_code(requests[i]));
            if (rpc_code == dsn::apps::RPC_RRDB_RRDB_PUT) {
                auto rpc = put_rpc::auto_reply(requests[i]);
                on_single_put_in_batch(rpc);
                _put_rpc_batch.emplace_back(std::move(rpc));
            } else if (rpc_code == dsn::apps::RPC_RRDB_RRDB_REMOVE) {
                auto rpc = remove_rpc::auto_reply(requests[i]);
                on_single_remove_in_batch(rpc);
                _remove_rpc_batch.emplace_back(std::move(rpc));
            } else if (rpc_code == dsn::apps::RPC_RRDB_RRDB_BATCHED_DUPLICATE) {
                auto rpc = duplicate_rpc::auto_reply(requests[i]);
                on_single_duplicate_in_batch(rpc);
                _batched_duplicate_rpc_batch.emplace_back(std::move(rpc));
            } else {
                if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT ||
                    rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE) {
                    dfatal("rpc code not allow batch: %s", rpc_code.to_string());
                } else {
                    dfatal("rpc code not handled: %s", rpc_code.to_string());
                }
            }
        }

        err = _write_svc->batch_commit(decree);
    }

    _put_rpc_batch.clear();
    _remove_rpc_batch.clear();
    _batched_duplicate_rpc_batch.clear();
    return err;
}

void pegasus_server_write::request_key_check(int64_t decree, dsn_message_t m, const dsn::blob &key)
{
    auto msg = (dsn::message_ex *)m;
    if (msg->header->client.partition_hash != 0) {
        uint64_t partition_hash = pegasus_key_hash(key);
        dassert(msg->header->client.partition_hash == partition_hash,
                "inconsistent partition hash");
        int thread_hash = get_gpid().thread_hash();
        dassert(msg->header->client.thread_hash == thread_hash, "inconsistent thread hash");
    }

    if (_verbose_log) {
        ::dsn::blob hash_key, sort_key;
        pegasus_restore_key(key, hash_key, sort_key);

        ddebug_rocksdb("write",
                       "decree={}, code={}, hash_key={}, sort_key={}",
                       decree,
                       msg->local_rpc_code.to_string(),
                       utils::c_escape_string(hash_key),
                       utils::c_escape_string(sort_key));
    }
}

} // namespace server
} // namespace pegasus
