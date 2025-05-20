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

#pragma once

#include <rocksdb/status.h>
#include <algorithm>
#include <array>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <vector>

#include "base/pegasus_rpc_types.h"
#include "common/replication_other_types.h"
#include "pegasus_write_service.h"
#include "replica/replica_base.h"
#include "rpc/rpc_message.h"
#include "rrdb/rrdb.code.definition.h"
#include "rrdb/rrdb_types.h"
#include "runtime/message_utils.h"
#include "task/task_code.h"
#include "task/task_spec.h"
#include "utils/metrics.h"

namespace dsn {
class blob;
} // namespace dsn

namespace pegasus::server {

class pegasus_server_impl;

/// This class implements the interface of `pegasus_sever_impl::on_batched_write_requests`.
class pegasus_server_write : public dsn::replication::replica_base
{
public:
    explicit pegasus_server_write(pegasus_server_impl *server);

    // See replication_app_base::make_idempotent() for details. Only called by primary replicas.
    int make_idempotent(dsn::message_ex *request, std::vector<dsn::message_ex *> &new_requests);

    // See replication_app_base::on_batched_write_requests() for details.
    //
    // **NOTE**
    // An error code other than rocksdb::Status::kOk would be regarded as the failure of the
    // replica, leading to cluster membership changes. Make sure no errors occur due to
    // invalid parameters.
    //
    // As long as the returned error is rocksdb::Status::kOk, the write requests are guaranteed
    // to be applied into RocksDB successfully, which means empty_put() will be called even if
    // there's no write.
    int on_batched_write_requests(dsn::message_ex **requests,
                                  uint32_t count,
                                  int64_t decree,
                                  uint64_t timestamp,
                                  dsn::message_ex *original_request);

    void set_default_ttl(uint32_t ttl);

private:
    // Used to call make_idempotent() for each type (specified by TRpcHolder) of atomic write.
    // Only called by primary replicas.
    template <typename TRpcHolder>
    int make_idempotent(dsn::message_ex *request, std::vector<dsn::message_ex *> &new_requests)
    {
        auto rpc = TRpcHolder(request);

        // Translate an atomic request into one or multiple idempotent single-update requests.
        std::vector<dsn::apps::update_request> updates;
        const int err = _write_svc->make_idempotent(rpc.request(), rpc.response(), updates);

        // When the condition checks of `check_and_set` and `check_and_mutate` fail,
        // make_idempotent() would return rocksdb::Status::kTryAgain. Therefore, there is
        // still a certain probability that a status code other than rocksdb::Status::kOk
        // is returned.
        if (err != rocksdb::Status::kOk) {
            // Once it failed, just reply to the client with error immediately.
            rpc.enable_auto_reply();
            return err;
        }

        // Build new messages based on the generated idempotent updates.
        new_requests.clear();
        for (const auto &update : updates) {
            new_requests.push_back(dsn::from_thrift_request_to_received_message(
                update,
                dsn::apps::RPC_RRDB_RRDB_PUT,
                request->header->client.thread_hash,
                request->header->client.partition_hash,
                static_cast<dsn::dsn_msg_serialize_format>(
                    request->header->context.u.serialize_format)));
        }

        return rocksdb::Status::kOk;
    }

    // Apply the batched (one or multiple) single-update requests into the storage engine.
    // Only called by primary replicas.
    template <typename TRpcHolder>
    inline int put(const std::vector<dsn::apps::update_request> &updates,
                   dsn::message_ex *original_request)
    {
        // Enable auto reply, since in primary replicas we need to reply to the client with
        // the response to the original atomic write request after the idempotent updates
        // were applied into the storage engine.
        auto rpc = TRpcHolder::auto_reply(original_request);
        return _write_svc->put(_write_ctx, updates, rpc.request(), rpc.response());
    }

    // Apply the idempotent updates `requests` into storage engine and respond to the original
    // atomic write request `original_request`. Both of `requests` and `original_request` should
    // not be nullptr, while `count` should always be > 0.
    int
    apply_idempotent(dsn::message_ex **requests, uint32_t count, dsn::message_ex *original_request);

    // Delay replying for the batched requests until all of them complete.
    int on_batched_writes(dsn::message_ex **requests, uint32_t count);

    int on_single_put_in_batch(put_rpc &rpc)
    {
        int err = _write_svc->batch_put(_write_ctx, rpc.request(), rpc.response());
        request_key_check(_decree, rpc.dsn_request(), rpc.request().key);
        return err;
    }

    int on_single_remove_in_batch(remove_rpc &rpc)
    {
        int err = _write_svc->batch_remove(_decree, rpc.request(), rpc.response());
        request_key_check(_decree, rpc.dsn_request(), rpc.request());
        return err;
    }

    // Ensure that the write request is directed to the right partition.
    // In verbose mode it will log for every request.
    void request_key_check(int64_t decree, dsn::message_ex *m, const dsn::blob &key);

    void init_make_idempotent_handlers();
    void init_non_batch_write_handlers();
    void init_idempotent_writers();

    friend class pegasus_server_write_test;
    friend class pegasus_write_service_test;
    friend class PegasusWriteServiceImplTest;
    friend class rocksdb_wrapper_test;

    std::unique_ptr<pegasus_write_service> _write_svc;
    std::vector<put_rpc> _put_rpc_batch;
    std::vector<remove_rpc> _remove_rpc_batch;

    db_write_context _write_ctx;
    int64_t _decree{invalid_decree};

    // Handlers that translate an atomic write request into one or multiple idempotent updates.
    using make_idempotent_map =
        std::map<dsn::task_code,
                 std::function<int(dsn::message_ex *, std::vector<dsn::message_ex *> &)>>;
    make_idempotent_map _make_idempotent_handlers;

    // Handlers that process a write request which could not be batched, e.g. multi put/remove.
    using non_batch_write_map = std::map<dsn::task_code, std::function<int(dsn::message_ex *)>>;
    non_batch_write_map _non_batch_write_handlers;

    // Writers that apply idempotent updates and respond to the original atomic write request.
    using idempotent_writer_map = std::array<
        std::function<int(const std::vector<dsn::apps::update_request> &, dsn::message_ex *)>,
        5>;
    idempotent_writer_map _idempotent_writers{};

    METRIC_VAR_DECLARE_counter(corrupt_writes);
};

} // namespace pegasus::server
