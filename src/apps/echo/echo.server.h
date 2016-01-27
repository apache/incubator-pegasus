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
 *     Auto generated, do not modify
 *
 * Revision history:
 */
# pragma once
# include "echo.code.definition.h"
# include <iostream>

namespace dsn { namespace example { 
class echo_service 
    : public ::dsn::serverlet<echo_service>
{
public:
    echo_service() : ::dsn::serverlet<echo_service>("echo") {}
    virtual ~echo_service() {}

protected:
    // all service handlers to be implemented further
    // RPC_ECHO_ECHO_PING 
    virtual void on_ping(const std::string& val, ::dsn::rpc_replier<std::string>& reply)
    {
        reply(val);
    }
    
public:
    void open_service()
    {
        this->register_async_rpc_handler(RPC_ECHO_ECHO_PING, "ping", &echo_service::on_ping);
    }

    void close_service()
    {
        this->unregister_rpc_handler(RPC_ECHO_ECHO_PING);
    }
};

} } 