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
# include <dsn/service_api_cpp.h>
# include "cli.types.h"
# include <iostream>


namespace dsn { 

    DEFINE_TASK_CODE_RPC(RPC_DSN_CLI_CALL, TASK_PRIORITY_HIGH, THREAD_POOL_DEFAULT);

class cli_client 
    : public virtual ::dsn::clientlet
{
public:
    cli_client(::dsn::rpc_address server) { _server = server; }
    cli_client() {  }
    virtual ~cli_client() {}


    // ---------- call RPC_DSN_CLI_CALL ------------
    // - synchronous 
    ::dsn::error_code call(
        const command& c, 
        /*out*/ std::string& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        ::dsn::rpc_read_stream response;

        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            RPC_DSN_CLI_CALL, c, hash, std::chrono::milliseconds(timeout_milliseconds));
        if (err == ::dsn::ERR_OK)
        {
            unmarshall(response, resp);
        }
        
        return err;
    }
    
    // - asynchronous with on-stack command and std::string 
    ::dsn::task_ptr begin_call(
        const command& c, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
            p_server_addr ? *p_server_addr : _server,
            RPC_DSN_CLI_CALL,
            c,
            this,
            [=](error_code ec, const std::string& resp)
            {
                end_call(ec, resp, context);
            },
            request_hash,
            std::chrono::milliseconds(timeout_milliseconds),
            reply_hash
            );
    }

    virtual void end_call(
        ::dsn::error_code err, 
        const std::string& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK) std::cout << "reply RPC_DSN_CLI_CALL err : " << err.to_string() << std::endl;
        else
        {
            std::cout << "reply RPC_DSN_CLI_CALL ok" << std::endl;
        }
    }

private:
    ::dsn::rpc_address _server;
};

} 