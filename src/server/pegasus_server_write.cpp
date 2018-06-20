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

pegasus_server_write::pegasus_server_write(pegasus_server_impl *server, bool verbose_log)
    : replica_base(*server), _verbose_log(verbose_log)
{
    _write_svc = dsn::make_unique<pegasus_write_service>(server);
}

int pegasus_server_write::on_batched_write_requests(dsn_message_t *requests,
                                                    int count,
                                                    int64_t decree,
                                                    uint64_t timestamp)
{
    _decree = decree;

    // Write down empty record (RPC_REPLICATION_WRITE_EMPTY) to update
    // rocksdb's `last_flushed_decree` (see rocksdb::DB::GetLastFlushedDecree())
    // TODO(wutao1): remove it when shared log is removed.
    if (count == 0) {
        return _write_svc->empty_put(decree);
    }

    dsn::task_code rpc_code(dsn_msg_task_code(requests[0]));
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
        dassert(count == 1, "count = %d", count);
        auto rpc = multi_put_rpc::auto_reply(requests[0]);
        on_multi_put(rpc);
        return rpc.response().error;
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE) {
        dassert(count == 1, "count = %d", count);
        auto rpc = multi_remove_rpc::auto_reply(requests[0]);
        on_multi_remove(rpc);
        return rpc.response().error;
    }

    return on_batched_writes(requests, count, decree);
}

int pegasus_server_write::on_batched_writes(dsn_message_t *requests, int count, int64_t decree)
{
    int err;
    {
        _write_svc->batch_prepare();

        for (int i = 0; i < count; ++i) {
            dassert(requests[i] != nullptr, "request[%d] is null", i);

            dsn::task_code rpc_code(dsn_msg_task_code(requests[i]));
            if (rpc_code == dsn::apps::RPC_RRDB_RRDB_PUT) {
                auto rpc = put_rpc::auto_reply(requests[i]);
                on_single_put_in_batch(rpc);
                _put_rpc_batch.emplace_back(std::move(rpc));
            } else if (rpc_code == dsn::apps::RPC_RRDB_RRDB_REMOVE) {
                auto rpc = remove_rpc::auto_reply(requests[i]);
                on_single_remove_in_batch(rpc);
                _remove_rpc_batch.emplace_back(std::move(rpc));
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

    // reply the batched RPCs
    _put_rpc_batch.clear();
    _remove_rpc_batch.clear();
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
