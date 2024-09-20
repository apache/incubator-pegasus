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

#include <iostream>

#include "client/partition_resolver.h"
#include "duplication_internal_types.h"
#include "rrdb.code.definition.h"
#include "rrdb_types.h"
#include "rpc/rpc_holder.h"
#include "task/task_tracker.h"
#include "rrdb/rrdb_types.h"
#include "rpc/rpc_holder.h"
#include "task/task_tracker.h"
#include "utils/optional.h"

namespace dsn {
namespace apps {

typedef rpc_holder<duplicate_request, duplicate_response> duplicate_rpc;

class rrdb_client
{
public:
    rrdb_client() {}
    explicit rrdb_client(const char *cluster_name,
                         const std::vector<dsn::host_port> &meta_list,
                         const char *app_name)
    {
        _resolver =
            dsn::replication::partition_resolver::get_resolver(cluster_name, meta_list, app_name);
    }
    ~rrdb_client() { _tracker.cancel_outstanding_tasks(); }

    // ---------- call RPC_RRDB_RRDB_PUT ------------
    // - synchronous
    std::pair<::dsn::error_code, update_response>
    put_sync(const update_request &args, std::chrono::milliseconds timeout, uint64_t partition_hash)
    {
        return ::dsn::rpc::wait_and_unwrap<update_response>(_resolver->call_op(
            RPC_RRDB_RRDB_PUT, args, &_tracker, empty_rpc_handler, timeout, partition_hash));
    }

    // - asynchronous with on-stack update_request and update_response
    template <typename TCallback>
    ::dsn::task_ptr put(const update_request &args,
                        TCallback &&callback,
                        std::chrono::milliseconds timeout,
                        uint64_t request_partition_hash,
                        int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_PUT,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_MULTI_PUT ------------
    // - synchronous
    std::pair<::dsn::error_code, update_response> multi_put_sync(const multi_put_request &args,
                                                                 std::chrono::milliseconds timeout,
                                                                 uint64_t partition_hash)
    {
        return ::dsn::rpc::wait_and_unwrap<update_response>(_resolver->call_op(
            RPC_RRDB_RRDB_MULTI_PUT, args, &_tracker, empty_rpc_handler, timeout, partition_hash));
    }

    // - asynchronous with on-stack multi_put_request and update_response
    template <typename TCallback>
    ::dsn::task_ptr multi_put(const multi_put_request &args,
                              TCallback &&callback,
                              std::chrono::milliseconds timeout,
                              uint64_t request_partition_hash,
                              int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_MULTI_PUT,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_REMOVE ------------
    // - synchronous
    std::pair<::dsn::error_code, update_response>
    remove_sync(const ::dsn::blob &args, std::chrono::milliseconds timeout, uint64_t partition_hash)
    {
        return ::dsn::rpc::wait_and_unwrap<update_response>(_resolver->call_op(
            RPC_RRDB_RRDB_REMOVE, args, &_tracker, empty_rpc_handler, timeout, partition_hash));
    }

    // - asynchronous with on-stack ::dsn::blob and update_response
    template <typename TCallback>
    ::dsn::task_ptr remove(const ::dsn::blob &args,
                           TCallback &&callback,
                           std::chrono::milliseconds timeout,
                           uint64_t request_partition_hash,
                           int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_REMOVE,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_MULTI_REMOVE ------------
    // - synchronous
    std::pair<::dsn::error_code, multi_remove_response>
    multi_remove_sync(const multi_remove_request &args,
                      std::chrono::milliseconds timeout,
                      uint64_t partition_hash)
    {
        return ::dsn::rpc::wait_and_unwrap<multi_remove_response>(
            _resolver->call_op(RPC_RRDB_RRDB_MULTI_REMOVE,
                               args,
                               &_tracker,
                               empty_rpc_handler,
                               timeout,
                               partition_hash));
    }

    // - asynchronous with on-stack multi_remove_request and multi_remove_response
    template <typename TCallback>
    ::dsn::task_ptr multi_remove(const multi_remove_request &args,
                                 TCallback &&callback,
                                 std::chrono::milliseconds timeout,
                                 uint64_t request_partition_hash,
                                 int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_MULTI_REMOVE,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_INCR ------------
    // - synchronous
    std::pair<::dsn::error_code, incr_response>
    incr_sync(const incr_request &args, std::chrono::milliseconds timeout, uint64_t partition_hash)
    {
        return ::dsn::rpc::wait_and_unwrap<incr_response>(_resolver->call_op(
            RPC_RRDB_RRDB_INCR, args, &_tracker, empty_rpc_handler, timeout, partition_hash));
    }

    // - asynchronous with on-stack incr_request and incr_response
    template <typename TCallback>
    ::dsn::task_ptr incr(const incr_request &args,
                         TCallback &&callback,
                         std::chrono::milliseconds timeout,
                         uint64_t request_partition_hash,
                         int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_INCR,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_CHECK_AND_SET ------------
    // - synchronous
    std::pair<::dsn::error_code, check_and_set_response>
    check_and_set_sync(const check_and_set_request &args,
                       std::chrono::milliseconds timeout,
                       uint64_t partition_hash)
    {
        return ::dsn::rpc::wait_and_unwrap<check_and_set_response>(
            _resolver->call_op(RPC_RRDB_RRDB_CHECK_AND_SET,
                               args,
                               &_tracker,
                               empty_rpc_handler,
                               timeout,
                               partition_hash));
    }

    // - asynchronous with on-stack check_and_set_request and check_and_set_response
    template <typename TCallback>
    ::dsn::task_ptr check_and_set(const check_and_set_request &args,
                                  TCallback &&callback,
                                  std::chrono::milliseconds timeout,
                                  uint64_t request_partition_hash,
                                  int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_CHECK_AND_SET,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_CHECK_AND_MUTATE ------------
    // - synchronous
    std::pair<::dsn::error_code, check_and_mutate_response>
    check_and_mutate_sync(const check_and_mutate_request &args,
                          std::chrono::milliseconds timeout,
                          uint64_t partition_hash)
    {
        return ::dsn::rpc::wait_and_unwrap<check_and_mutate_response>(
            _resolver->call_op(RPC_RRDB_RRDB_CHECK_AND_MUTATE,
                               args,
                               &_tracker,
                               empty_rpc_handler,
                               timeout,
                               partition_hash));
    }

    // - asynchronous with on-stack check_and_mutate_request and check_and_mutate_response
    template <typename TCallback>
    ::dsn::task_ptr check_and_mutate(const check_and_mutate_request &args,
                                     TCallback &&callback,
                                     std::chrono::milliseconds timeout,
                                     uint64_t request_partition_hash,
                                     int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_CHECK_AND_MUTATE,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_GET ------------
    // - synchronous
    std::pair<::dsn::error_code, read_response>
    get_sync(const ::dsn::blob &args, std::chrono::milliseconds timeout, uint64_t partition_hash)
    {
        return ::dsn::rpc::wait_and_unwrap<read_response>(_resolver->call_op(
            RPC_RRDB_RRDB_GET, args, &_tracker, empty_rpc_handler, timeout, partition_hash));
    }

    // - asynchronous with on-stack ::dsn::blob and read_response
    template <typename TCallback>
    ::dsn::task_ptr get(const ::dsn::blob &args,
                        TCallback &&callback,
                        std::chrono::milliseconds timeout,
                        uint64_t request_partition_hash,
                        int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_GET,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_MULTI_GET ------------
    // - synchronous
    std::pair<::dsn::error_code, multi_get_response> multi_get_sync(
        const multi_get_request &args, std::chrono::milliseconds timeout, uint64_t partition_hash)
    {
        return ::dsn::rpc::wait_and_unwrap<multi_get_response>(_resolver->call_op(
            RPC_RRDB_RRDB_MULTI_GET, args, &_tracker, empty_rpc_handler, timeout, partition_hash));
    }

    // - asynchronous with on-stack multi_get_request and multi_get_response
    template <typename TCallback>
    ::dsn::task_ptr multi_get(const multi_get_request &args,
                              TCallback &&callback,
                              std::chrono::milliseconds timeout,
                              uint64_t request_partition_hash,
                              int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_MULTI_GET,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_BATCH_GET ------------
    // - synchronous
    std::pair<::dsn::error_code, batch_get_response> batch_get_sync(
        const batch_get_request &args, std::chrono::milliseconds timeout, uint64_t partition_hash)
    {
        return ::dsn::rpc::wait_and_unwrap<batch_get_response>(_resolver->call_op(
            RPC_RRDB_RRDB_BATCH_GET, args, &_tracker, empty_rpc_handler, timeout, partition_hash));
    }

    // - asynchronous with on-stack BatchGetRequest and BatchGetResponse
    template <typename TCallback>
    ::dsn::task_ptr batch_get(const batch_get_request &args,
                              TCallback &&callback,
                              std::chrono::milliseconds timeout,
                              uint64_t request_partition_hash,
                              int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_BATCH_GET,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_SORTKEY_COUNT ------------
    // - synchronous
    std::pair<::dsn::error_code, count_response> sortkey_count_sync(
        const ::dsn::blob &args, std::chrono::milliseconds timeout, uint64_t partition_hash)
    {
        return ::dsn::rpc::wait_and_unwrap<count_response>(
            _resolver->call_op(RPC_RRDB_RRDB_SORTKEY_COUNT,
                               args,
                               &_tracker,
                               empty_rpc_handler,
                               timeout,
                               partition_hash));
    }

    // - asynchronous with on-stack ::dsn::blob and count_response
    template <typename TCallback>
    ::dsn::task_ptr sortkey_count(const ::dsn::blob &args,
                                  TCallback &&callback,
                                  std::chrono::milliseconds timeout,
                                  uint64_t request_partition_hash,
                                  int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_SORTKEY_COUNT,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_TTL ------------
    // - synchronous
    std::pair<::dsn::error_code, ttl_response>
    ttl_sync(const ::dsn::blob &args, std::chrono::milliseconds timeout, uint64_t partition_hash)
    {
        return ::dsn::rpc::wait_and_unwrap<ttl_response>(_resolver->call_op(
            RPC_RRDB_RRDB_TTL, args, &_tracker, empty_rpc_handler, timeout, partition_hash));
    }

    // - asynchronous with on-stack ::dsn::blob and ttl_response
    template <typename TCallback>
    ::dsn::task_ptr ttl(const ::dsn::blob &args,
                        TCallback &&callback,
                        std::chrono::milliseconds timeout,
                        uint64_t request_partition_hash,
                        int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_TTL,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_GET_SCANNER ------------
    // - synchronous
    std::pair<::dsn::error_code, scan_response>
    get_scanner_sync(const get_scanner_request &args,
                     std::chrono::milliseconds timeout,
                     uint64_t partition_hash,
                     dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<scan_response>(
            _resolver->call_op(RPC_RRDB_RRDB_GET_SCANNER,
                               args,
                               &_tracker,
                               empty_rpc_handler,
                               timeout,
                               partition_hash));
    }

    // - asynchronous with on-stack get_scanner_request and scan_response
    template <typename TCallback>
    ::dsn::task_ptr get_scanner(const get_scanner_request &args,
                                TCallback &&callback,
                                std::chrono::milliseconds timeout,
                                uint64_t request_partition_hash,
                                int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_GET_SCANNER,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_SCAN ------------
    // - synchronous
    std::pair<::dsn::error_code, scan_response>
    scan_sync(const scan_request &args, std::chrono::milliseconds timeout, uint64_t partition_hash)
    {
        return ::dsn::rpc::wait_and_unwrap<scan_response>(_resolver->call_op(
            RPC_RRDB_RRDB_SCAN, args, &_tracker, empty_rpc_handler, timeout, partition_hash));
    }

    // - asynchronous with on-stack scan_request and scan_response
    template <typename TCallback>
    ::dsn::task_ptr scan(const scan_request &args,
                         TCallback &&callback,
                         std::chrono::milliseconds timeout,
                         uint64_t request_partition_hash,
                         int reply_thread_hash = 0)
    {
        return _resolver->call_op(RPC_RRDB_RRDB_SCAN,
                                  args,
                                  &_tracker,
                                  std::forward<TCallback>(callback),
                                  timeout,
                                  request_partition_hash,
                                  reply_thread_hash);
    }

    // ---------- call RPC_RRDB_RRDB_CLEAR_SCANNER ------------
    void clear_scanner(const int64_t &args, uint64_t partition_hash)
    {
        _resolver->call_op(RPC_RRDB_RRDB_CLEAR_SCANNER,
                           args,
                           nullptr,
                           empty_rpc_handler,
                           std::chrono::milliseconds(0),
                           partition_hash);
    }

    // ---------- call RPC_RRDB_RRDB_DUPLICATE ------------

    // - asynchronous with on-stack duplicate_request and duplicate_response
    template <typename TCallback>
    task_ptr duplicate(duplicate_rpc &rpc, TCallback &&callback, dsn::task_tracker *tracker)
    {
        return rpc.call(_resolver, tracker, std::forward<TCallback &&>(callback));
    }

private:
    dsn::replication::partition_resolver_ptr _resolver;
    dsn::task_tracker _tracker;
};
} // namespace apps
} // namespace dsn
