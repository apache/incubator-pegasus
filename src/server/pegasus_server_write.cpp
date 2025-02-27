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

#include <fmt/core.h>
#include <rocksdb/status.h>
#include <thrift/transport/TTransportException.h>
#include <algorithm>
#include <string_view>
#include <type_traits>
#include <utility>

#include "base/pegasus_key_schema.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "logging_utils.h"
#include "pegasus_rpc_types.h"
#include "pegasus_server_impl.h"
#include "pegasus_server_write.h"
#include "pegasus_utils.h"
#include "rpc/rpc_holder.h"
#include "rpc/rpc_message.h"
#include "rrdb/rrdb.code.definition.h"
#include "runtime/message_utils.h"
#include "server/pegasus_write_service.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"

METRIC_DEFINE_counter(replica,
                      corrupt_writes,
                      dsn::metric_unit::kRequests,
                      "The number of corrupt writes for each replica");

DSN_DECLARE_bool(rocksdb_verbose_log);

namespace pegasus {
namespace server {

pegasus_server_write::pegasus_server_write(pegasus_server_impl *server)
    : replica_base(server),
      _write_svc(new pegasus_write_service(server)),
      METRIC_VAR_INIT_replica(corrupt_writes)
{
    init_make_idempotent_handlers();
    init_non_batch_write_handlers();
    init_on_idempotent_handlers();
}

int pegasus_server_write::make_idempotent(dsn::message_ex *request, dsn::message_ex **new_request)
{
    auto iter = std::as_const(_make_idempotent_handlers).find(request->rpc_code());
    if (iter != _make_idempotent_handlers.end()) {
        return iter->second(request, new_request);
    }

    // Those requests not in the handlers are considered as idempotent. Always be successful
    // for them.
    return rocksdb::Status::kOk;
}

int pegasus_server_write::on_batched_write_requests(dsn::message_ex **requests,
                                                    int count,
                                                    int64_t decree,
                                                    uint64_t timestamp,
                                                    dsn::message_ex *original_request)
{
    _write_ctx = db_write_context::create(decree, timestamp);
    _decree = decree;

    // Write down empty record (RPC_REPLICATION_WRITE_EMPTY) to update
    // rocksdb's `last_flushed_decree` (see rocksdb::DB::GetLastFlushedDecree())
    // TODO(wutao1): remove it when shared log is removed.
    if (count == 0) {
        return _write_svc->empty_put(_decree);
    }

    try {
        auto iter = std::as_const(_non_batch_write_handlers).find(requests[0]->rpc_code());
        if (iter != _non_batch_write_handlers.end()) {
            CHECK_EQ(count, 1);
            return iter->second(requests[0]);
        }
    } catch (TTransportException &ex) {
        METRIC_VAR_INCREMENT(corrupt_writes);
        LOG_ERROR_PREFIX("pegasus non-batch write handler failed, from = {}, exception = {}",
                         requests[0]->header->from_address,
                         ex.what());

        // The corrupt write is likely to be an attack or a scan for security. Since it has
        // been in plog, just return rocksdb::Status::kOk to ignore it.
        // See https://github.com/apache/incubator-pegasus/pull/798.
        return rocksdb::Status::kOk;
    }

    if (original_request != nullptr) {
        // The request is regarded as idempotent once its original request is attached.
        CHECK_EQ(count, 1);
        return on_idempotent(requests[0], original_request);
    }

    return on_batched_writes(requests, count);
}

int pegasus_server_write::on_idempotent(dsn::message_ex *request, dsn::message_ex *original_request)
{
    try {
        auto iter = std::as_const(_on_idempotent_handlers).find(request->rpc_code());
        if (iter != _on_idempotent_handlers.end()) {
            return iter->second(request, original_request);
        }
    } catch (TTransportException &ex) {
        METRIC_VAR_INCREMENT(corrupt_writes);
        LOG_ERROR_PREFIX("pegasus on idempotent handler failed, from = {}, exception = {}",
                         request->header->from_address,
                         ex.what());
        // The corrupt write is likely to be an attack or a scan for security. Since it has
        // been in plog, just return rocksdb::Status::kOk to ignore it.
        // See https://github.com/apache/incubator-pegasus/pull/798.
        return rocksdb::Status::kOk;
    }

    CHECK(false, "unsupported idempotent write request: rpc_code={}", request->rpc_code());
    return rocksdb::Status::kNotSupported;
}

void pegasus_server_write::set_default_ttl(uint32_t ttl) { _write_svc->set_default_ttl(ttl); }

int pegasus_server_write::on_batched_writes(dsn::message_ex **requests, int count)
{
    _write_svc->batch_prepare(_decree);

    int err = rocksdb::Status::kOk;
    for (int i = 0; i < count; ++i) {
        CHECK_NOTNULL(requests[i], "request[{}] is null", i);

        // Make sure all writes are batched even if some of them failed, since we need to record
        // the total QPS and RPC latencies, and respond for all RPCs regardless of their result.
        int local_err = rocksdb::Status::kOk;
        try {
            dsn::task_code rpc_code(requests[i]->rpc_code());
            if (rpc_code == dsn::apps::RPC_RRDB_RRDB_PUT) {
                // Once this single-put request is found originating from an atomic request,
                // there's no need to reply to the client since now we must be in one of the
                // following situations:
                // - now we are replaying plog into RocksDB at startup of this replica.
                // - now we are in a secondary replica: just received a prepare request and
                // appended it to plog, now we are applying it into RocksDB.
                auto rpc = put_rpc(requests[i]);
                const auto &update = rpc.request();
                if (!update.__isset.type || update.type == dsn::apps::update_type::UT_PUT) {
                    // We must reply to the client for the plain single-put request.
                    rpc.enable_auto_reply();
                } else if (update.type == dsn::apps::update_type::UT_INCR) {
                    // This put request must originate from an incr request and never be
                    // batched in plog.
                    CHECK_EQ(count, 1);
                }

                local_err = on_single_put_in_batch(rpc);
                _put_rpc_batch.emplace_back(std::move(rpc));
            } else if (rpc_code == dsn::apps::RPC_RRDB_RRDB_REMOVE) {
                auto rpc = remove_rpc::auto_reply(requests[i]);
                local_err = on_single_remove_in_batch(rpc);
                _remove_rpc_batch.emplace_back(std::move(rpc));
            } else {
                if (_non_batch_write_handlers.find(rpc_code) != _non_batch_write_handlers.end()) {
                    LOG_FATAL("rpc code not allow batch: {}", rpc_code);
                } else {
                    LOG_FATAL("rpc code not handled: {}", rpc_code);
                }
            }
        } catch (TTransportException &ex) {
            METRIC_VAR_INCREMENT(corrupt_writes);
            LOG_ERROR_PREFIX("pegasus batch writes handler failed, from = {}, exception = {}",
                             requests[i]->header->from_address,
                             ex.what());
            // The corrupt write is likely to be an attack or a scan for security. Since it has
            // been in plog, just ignore it.
            // See https://github.com/apache/incubator-pegasus/pull/798.
        }

        if (err == rocksdb::Status::kOk && local_err != rocksdb::Status::kOk) {
            err = local_err;
        }
    }

    if (dsn_unlikely(err != rocksdb::Status::kOk ||
                     (_put_rpc_batch.empty() && _remove_rpc_batch.empty()))) {
        _write_svc->batch_abort(_decree, err == rocksdb::Status::kOk ? -1 : err);
    } else {
        err = _write_svc->batch_commit(_decree);
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
        CHECK_EQ_MSG(
            msg->header->client.partition_hash, partition_hash, "inconsistent partition hash");
        int thread_hash = get_gpid().thread_hash();
        CHECK_EQ_MSG(msg->header->client.thread_hash, thread_hash, "inconsistent thread hash");
    }

    if (FLAGS_rocksdb_verbose_log) {
        ::dsn::blob hash_key, sort_key;
        pegasus_restore_key(key, hash_key, sort_key);

        LOG_INFO_ROCKSDB("Write",
                         "decree: {}, code: {}, hash_key: {}, sort_key: {}",
                         decree,
                         msg->local_rpc_code,
                         utils::c_escape_sensitive_string(hash_key),
                         utils::c_escape_sensitive_string(sort_key));
    }
}

void pegasus_server_write::init_make_idempotent_handlers()
{
    _make_idempotent_handlers = {
        {dsn::apps::RPC_RRDB_RRDB_INCR,
         [this](dsn::message_ex *request, dsn::message_ex **new_request) -> int {
             auto rpc = incr_rpc(request);
             dsn::apps::update_request update;
             // Translate an incr request into a single-put request.
             const auto err = _write_svc->make_idempotent(rpc.request(), rpc.response(), update);
             if (dsn_likely(err == rocksdb::Status::kOk)) {
                 // Build the message based on the resulting put request.
                 *new_request = dsn::from_thrift_request_to_received_message(
                     update, dsn::apps::RPC_RRDB_RRDB_PUT);
             } else {
                 // Once it failed, just reply to the client with error immediately.
                 rpc.enable_auto_reply();
             }

             return err;
         }},
    };
}

void pegasus_server_write::init_non_batch_write_handlers()
{
    _non_batch_write_handlers = {
        {dsn::apps::RPC_RRDB_RRDB_MULTI_PUT,
         [this](dsn::message_ex *request) -> int {
             auto rpc = multi_put_rpc::auto_reply(request);
             return _write_svc->multi_put(_write_ctx, rpc.request(), rpc.response());
         }},
        {dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE,
         [this](dsn::message_ex *request) -> int {
             auto rpc = multi_remove_rpc::auto_reply(request);
             return _write_svc->multi_remove(_decree, rpc.request(), rpc.response());
         }},
        {dsn::apps::RPC_RRDB_RRDB_INCR,
         [this](dsn::message_ex *request) -> int {
             auto rpc = incr_rpc::auto_reply(request);
             return _write_svc->incr(_decree, rpc.request(), rpc.response());
         }},
        {dsn::apps::RPC_RRDB_RRDB_DUPLICATE,
         [this](dsn::message_ex *request) -> int {
             auto rpc = duplicate_rpc::auto_reply(request);
             return _write_svc->duplicate(_decree, rpc.request(), rpc.response());
         }},
        {dsn::apps::RPC_RRDB_RRDB_CHECK_AND_SET,
         [this](dsn::message_ex *request) -> int {
             auto rpc = check_and_set_rpc::auto_reply(request);
             return _write_svc->check_and_set(_decree, rpc.request(), rpc.response());
         }},
        {dsn::apps::RPC_RRDB_RRDB_CHECK_AND_MUTATE,
         [this](dsn::message_ex *request) -> int {
             auto rpc = check_and_mutate_rpc::auto_reply(request);
             return _write_svc->check_and_mutate(_decree, rpc.request(), rpc.response());
         }},
        {dsn::apps::RPC_RRDB_RRDB_BULK_LOAD,
         [this](dsn::message_ex *request) -> int {
             auto rpc = ingestion_rpc::auto_reply(request);
             return _write_svc->ingest_files(_decree, rpc.request(), rpc.response());
         }},
    };
}

void pegasus_server_write::init_on_idempotent_handlers()
{
    _on_idempotent_handlers = {
        {dsn::apps::RPC_RRDB_RRDB_PUT,
         [this](dsn::message_ex *request, dsn::message_ex *original_request) -> int {
             auto put = put_rpc(request);

             const auto &update = put.request();
             CHECK(update.__isset.type, "update_request::type is not set for idempotent write");

             if (update.type == dsn::apps::update_type::UT_INCR) {
                 auto incr = incr_rpc::auto_reply(original_request);
                 return _write_svc->put(_write_ctx, update, incr.response());
             }

             CHECK(false,
                   "unsupported update_request::type for idempotent write {}",
                   static_cast<int>(update.type));
             return rocksdb::Status::kNotSupported;
         }},
    };
}

} // namespace server
} // namespace pegasus
