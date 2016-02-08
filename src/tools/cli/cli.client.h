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
# pragma once
# include "cli.types.h"
# include <iostream>


namespace dsn { 
DEFINE_TASK_CODE_RPC(RPC_DSN_CLI_CALL, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT);
class cli_client 
    : public virtual ::dsn::clientlet
{
public:
    cli_client(::dsn::rpc_address server) { _server = server; }
    cli_client() { }
    virtual ~cli_client() {}


    // ---------- call RPC_CLI_CLI_CALL ------------
    // - synchronous 
    std::pair< ::dsn::error_code, std::string> call_sync(
        const command& c, 
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0), 
        int hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none)
    {
        return ::dsn::rpc::wait_and_unwrap<std::string>(
            ::dsn::rpc::call(
                server_addr.unwrap_or(_server),
                RPC_DSN_CLI_CALL,
                c,
                nullptr,
                empty_callback,
                hash,
                timeout
                )
            );
    }
    
    // - asynchronous with on-stack command and std::string 
    template<typename TCallback>
    ::dsn::task_ptr call(
        const command& c, 
        TCallback&& callback,
        std::chrono::milliseconds timeout = std::chrono::milliseconds(0),
        int reply_hash = 0,
        int request_hash = 0,
        dsn::optional< ::dsn::rpc_address> server_addr = dsn::none
        )
    {
        return ::dsn::rpc::call(
                    server_addr.unwrap_or(_server), 
                    RPC_DSN_CLI_CALL, 
                    c, 
                    this,
                    std::forward<TCallback>(callback),
                    request_hash, 
                    timeout, 
                    reply_hash
                    );
    }

private:
    ::dsn::rpc_address _server;
};

} 