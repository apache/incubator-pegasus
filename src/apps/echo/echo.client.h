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
# include "echo.code.definition.h"
# include <iostream>


namespace dsn { namespace example { 
class echo_client 
    : public virtual ::dsn::clientlet
{
public:
    echo_client(::dsn::rpc_address server) { _server = server; }    
    echo_client() { }
    virtual ~echo_client() {}


    // ---------- call RPC_ECHO_ECHO_PING ------------
    // - synchronous 
    ::dsn::error_code ping(
        const std::string& val, 
        /*out*/ std::string& resp, 
        int timeout_milliseconds = 0, 
        int hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        ::dsn::rpc_read_stream response;
        auto err = ::dsn::rpc::call_typed_wait(&response, p_server_addr ? *p_server_addr : _server,
            RPC_ECHO_ECHO_PING, val, hash, timeout_milliseconds);
        if (err == ::dsn::ERR_OK)
        {
            unmarshall(response, resp);
        }
        return err;
    }
    
    // - asynchronous with on-stack std::string and std::string 
    ::dsn::task_ptr begin_ping(
        const std::string& val, 
        void* context = nullptr,
        int timeout_milliseconds = 0, 
        int reply_hash = 0,
        int request_hash = 0,
        const ::dsn::rpc_address *p_server_addr = nullptr)
    {
        return ::dsn::rpc::call_typed(
                    p_server_addr ? *p_server_addr : _server, 
                    RPC_ECHO_ECHO_PING, 
                    val, 
                    this,
                    [=](error_code err, std::string&& resp)
                    {
                        end_ping(err, std::move(resp), context);
                    },
                    request_hash, 
                    timeout_milliseconds, 
                    reply_hash
                    );
    }

    virtual void end_ping(
        ::dsn::error_code err, 
        const std::string& resp,
        void* context)
    {
        if (err != ::dsn::ERR_OK)
        {
            //std::cout << "reply RPC_ECHO_ECHO_PING err : " << err.to_string() << std::endl;
        }
        else
        {
            //std::cout << "reply RPC_ECHO_ECHO_PING ok" << std::endl;
        }
    }
    

private:
    ::dsn::rpc_address _server;
};

} } 