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
# include "fd.code.definition.h"
# include <iostream>

namespace dsn { namespace fd { 
class failure_detector_service 
    : public ::dsn::serverlet<failure_detector_service>
{
public:
    failure_detector_service() : ::dsn::serverlet<failure_detector_service>("failure_detector") {}
    virtual ~failure_detector_service() {}

protected:
    // all service handlers to be implemented further
    // RPC_FD_FAILURE_DETECTOR_PING 
    virtual void on_ping(const beacon_msg& beacon, ::dsn::rpc_replier<beacon_ack>& reply)
    {
        std::cout << "... exec RPC_FD_FAILURE_DETECTOR_PING ... (not implemented) " << std::endl;
        beacon_ack resp;
        reply(resp);
    }
    
public:
    void open_service()
    {
        this->register_async_rpc_handler(RPC_FD_FAILURE_DETECTOR_PING, "ping", &failure_detector_service::on_ping);
    }

    void close_service()
    {
        this->unregister_rpc_handler(RPC_FD_FAILURE_DETECTOR_PING);
    }
};

} } 