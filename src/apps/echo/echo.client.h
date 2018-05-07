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
#include "echo.code.definition.h"
#include <iostream>

namespace dsn {
namespace example {
class echo_client : public virtual ::dsn::clientlet
{
public:
    echo_client(::dsn::rpc_address server) { _server = server; }
    echo_client() {}
    virtual ~echo_client() {}

    // ---------- call RPC_ECHO_ECHO_PING ------------
    // - synchronous
    std::pair<::dsn::error_code, std::string>
    ping_sync(const std::string &val,
              std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
              int thread_hash = 0,
              uint64_t partition_hash = 0,
              dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<std::string>(
            ::dsn::rpc::call(server_addr.unwrap_or(_server),
                             RPC_ECHO_ECHO_PING,
                             val,
                             nullptr,
                             empty_callback,
                             timeout,
                             thread_hash,
                             partition_hash));
    }

    // - asynchronous with on-stack std::string and std::string
    template <typename TCallback>
    ::dsn::task_ptr ping(const std::string &val,
                         TCallback &&callback,
                         std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
                         int thread_hash = 0,
                         uint64_t partition_hash = 0,
                         int reply_thread_hash = 0,
                         dsn::optional<::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::call(server_addr.unwrap_or(_server),
                                RPC_ECHO_ECHO_PING,
                                val,
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
