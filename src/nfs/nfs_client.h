/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once
#include <iostream>
#include <dsn/utility/optional.h>
#include <dsn/tool-api/async_calls.h>

#include "nfs_types.h"
#include "nfs_code_definition.h"

namespace dsn {
namespace service {
class nfs_client
{
public:
    nfs_client(::dsn::rpc_address server) { _server = server; }
    nfs_client() {}
    virtual ~nfs_client() {}

    // ---------- call RPC_NFS_NFS_COPY ------------
    // - synchronous
    std::pair<::dsn::error_code, copy_response>
    copy_sync(const copy_request &request,
              std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
              int thread_hash = 0,
              uint64_t partition_hash = 0,
              dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<copy_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_NFS_COPY,
                             request,
                             nullptr,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack copy_request and copy_response
    template <typename TCallback>
    ::dsn::task_ptr copy(const copy_request &request,
                         TCallback &&callback,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                         int thread_hash = 0,
                         uint64_t partition_hash = 0,
                         int reply_thread_hash = 0,
                         dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_NFS_COPY,
                                request,
                                nullptr,
                                std::forward<TCallback>(callback),
                                timeout,
                                thread_hash,
                                partition_hash,
                                reply_thread_hash);
    }

    // ---------- call RPC_NFS_NFS_GET_FILE_SIZE ------------
    // - synchronous
    std::pair<::dsn::error_code, get_file_size_response>
    get_file_size_sync(const get_file_size_request &request,
                       std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                       int thread_hash = 0,
                       uint64_t partition_hash = 0,
                       dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<get_file_size_response>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_NFS_GET_FILE_SIZE,
                             request,
                             nullptr,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack get_file_size_request and get_file_size_response
    template <typename TCallback>
    ::dsn::task_ptr get_file_size(const get_file_size_request &request,
                                  TCallback &&callback,
                                  std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                                  int thread_hash = 0,
                                  uint64_t partition_hash = 0,
                                  int reply_thread_hash = 0,
                                  dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_NFS_GET_FILE_SIZE,
                                request,
                                nullptr,
                                std::forward<TCallback>(callback),
                                timeout,
                                thread_hash,
                                partition_hash,
                                reply_thread_hash);
    }

private:
    ::dsn::rpc_address _server;
};
}
}
