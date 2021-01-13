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

#include <dsn/cpp/message_utils.h>
#include <dsn/dist/replication/duplication_common.h>
#include <dsn/utility/defer.h>

#include "base/pegasus_key_schema.h"
#include "pegasus_server_write.h"
#include "pegasus_server_impl.h"
#include "logging_utils.h"
#include "pegasus_mutation_duplicator.h"

namespace pegasus {
namespace server {

pegasus_server_write::pegasus_server_write(pegasus_server_impl *server, bool verbose_log)
    : replica_base(server),
      _write_svc(new pegasus_write_service(server, verbose_log)),
      _verbose_log(verbose_log)
{
}

int pegasus_server_write::on_batched_write_requests(dsn::message_ex **requests,
                                                    int count,
                                                    int64_t decree,
                                                    uint64_t timestamp)
{
    _write_ctx = db_write_context::create(decree, timestamp);
    _decree = decree;

    // Write down empty record (RPC_REPLICATION_WRITE_EMPTY) to update
    // rocksdb's `last_flushed_decree` (see rocksdb::DB::GetLastFlushedDecree())
    // TODO(wutao1): remove it when shared log is removed.
    if (count == 0) {
        return _write_svc->empty_put(_decree);
    }

    dsn::task_code rpc_code(requests[0]->rpc_code());
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
        dassert(count == 1, "count = %d", count);
        auto rpc = multi_put_rpc::auto_reply(requests[0]);
        return _write_svc->multi_put(_write_ctx, rpc.request(), rpc.response());
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE) {
        dassert(count == 1, "count = %d", count);
        auto rpc = multi_remove_rpc::auto_reply(requests[0]);
        return _write_svc->multi_remove(_decree, rpc.request(), rpc.response());
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_INCR) {
        dassert(count == 1, "count = %d", count);
        auto rpc = incr_rpc::auto_reply(requests[0]);
        return _write_svc->incr(_decree, rpc.request(), rpc.response());
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_DUPLICATE) {
        dassert(count == 1, "count = %d", count);
        auto rpc = duplicate_rpc::auto_reply(requests[0]);
        return _write_svc->duplicate(_decree, rpc.request(), rpc.response());
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_SET) {
        dassert(count == 1, "count = %d", count);
        auto rpc = check_and_set_rpc::auto_reply(requests[0]);
        return _write_svc->check_and_set(_decree, rpc.request(), rpc.response());
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_MUTATE) {
        dassert(count == 1, "count = %d", count);
        auto rpc = check_and_mutate_rpc::auto_reply(requests[0]);
        return _write_svc->check_and_mutate(_decree, rpc.request(), rpc.response());
    }
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_BULK_LOAD) {
        dassert(count == 1, "count = %d", count);
        auto rpc = ingestion_rpc::auto_reply(requests[0]);
        return _write_svc->ingestion_files(_decree, rpc.request(), rpc.response());
    }

    return on_batched_writes(requests, count);
}

void pegasus_server_write::set_default_ttl(uint32_t ttl) { _write_svc->set_default_ttl(ttl); }

int pegasus_server_write::on_batched_writes(dsn::message_ex **requests, int count)
{
    int err = 0;
    {
        _write_svc->batch_prepare(_decree);

        for (int i = 0; i < count; ++i) {
            dassert(requests[i] != nullptr, "request[%d] is null", i);

            // Make sure all writes are batched even if they are failed,
            // since we need to record the total qps and rpc latencies,
            // and respond for all RPCs regardless of their result.

            int local_err = 0;
            dsn::task_code rpc_code(requests[i]->rpc_code());
            if (rpc_code == dsn::apps::RPC_RRDB_RRDB_PUT) {
                auto rpc = put_rpc::auto_reply(requests[i]);
                local_err = _write_svc->on_single_put_in_batch(_write_ctx, rpc);
                _put_rpc_batch.emplace_back(std::move(rpc));
            } else if (rpc_code == dsn::apps::RPC_RRDB_RRDB_REMOVE) {
                auto rpc = remove_rpc::auto_reply(requests[i]);
                local_err = on_single_remove_in_batch(rpc);
                _remove_rpc_batch.emplace_back(std::move(rpc));
            } else {
                if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT ||
                    rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE ||
                    rpc_code == dsn::apps::RPC_RRDB_RRDB_INCR ||
                    rpc_code == dsn::apps::RPC_RRDB_RRDB_DUPLICATE) {
                    dfatal("rpc code not allow batch: %s", rpc_code.to_string());
                } else {
                    dfatal("rpc code not handled: %s", rpc_code.to_string());
                }
            }

            if (!err && local_err) {
                err = local_err;
            }
        }

        if (err == 0) {
            err = _write_svc->batch_commit(_decree);
        } else {
            _write_svc->batch_abort(_decree, err);
        }
    }

    // reply the batched RPCs
    _put_rpc_batch.clear();
    _remove_rpc_batch.clear();
    return err;
}

void pegasus_server_write::request_key_check(int64_t decree,
                                             dsn::message_ex *msg,
                                             const dsn::blob &key)
{
    // TODO(wutao1): server should not assert when client's hash is incorrect.
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

        ddebug_rocksdb("Write",
                       "decree: {}, code: {}, hash_key: {}, sort_key: {}",
                       decree,
                       msg->local_rpc_code.to_string(),
                       utils::c_escape_string(hash_key),
                       utils::c_escape_string(sort_key));
    }
}

} // namespace server
} // namespace pegasus
