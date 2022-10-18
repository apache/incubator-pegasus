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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once
#include "fd.code.definition.h"
#include <iostream>
#include "utils/optional.h"
#include "runtime/task/async_calls.h"

namespace dsn {
namespace fd {
class failure_detector_client
{
public:
    failure_detector_client(::dsn::rpc_address server) { _server = server; }
    failure_detector_client() {}
    virtual ~failure_detector_client() {}

    // ---------- call RPC_FD_FAILURE_DETECTOR_PING ------------
    // - synchronous
    std::pair<::dsn::error_code, beacon_ack>
    ping_sync(const beacon_msg &beacon,
              std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
              int thread_hash = 0,
              uint64_t partition_hash = 0,
              dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return dsn::rpc::wait_and_unwrap<beacon_ack>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_FD_FAILURE_DETECTOR_PING,
                             beacon,
                             nullptr,
                             empty_rpc_handler,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack beacon_msg and beacon_ack
    template <typename TCallback>
    ::dsn::task_ptr ping(const beacon_msg &beacon,
                         TCallback &&callback,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                         int thread_hash = 0,
                         uint64_t partition_hash = 0,
                         int reply_thread_hash = 0,
                         dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_FD_FAILURE_DETECTOR_PING,
                                beacon,
                                this,
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
