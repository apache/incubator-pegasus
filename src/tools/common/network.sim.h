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

#include <dsn/tool_api.h>

namespace dsn { namespace tools {

    class sim_network_provider;
    class sim_client_session : public rpc_client_session
    {
    public:
        sim_client_session(sim_network_provider& net, const end_point& remote_addr, rpc_client_matcher_ptr& matcher);

        virtual void connect();
        virtual void send(message_ptr& msg);
    };

    class sim_server_session : public rpc_server_session
    {
    public:
        sim_server_session(sim_network_provider& net, const end_point& remote_addr, rpc_client_session_ptr& client);

        virtual void send(message_ptr& reply_msg);

    private:
        rpc_client_session_ptr _client;
    };

    class sim_network_provider : public connection_oriented_network
    {
    public:
        sim_network_provider(rpc_engine* rpc, network* inner_provider);
        ~sim_network_provider(void) {}

        virtual error_code start(rpc_channel channel, int port, bool client_only);
    
        virtual const end_point& address() { return _address; }

        virtual rpc_client_session_ptr create_client_session(const end_point& server_addr)
        {
            auto matcher = new_client_matcher();
            return rpc_client_session_ptr(new sim_client_session(*this, server_addr, matcher));
        }

        uint32_t net_delay_milliseconds() const;

    private:
        end_point    _address;
        uint32_t     _min_message_delay_microseconds;
        uint32_t     _max_message_delay_microseconds;
    };

    //------------- inline implementations -------------


}} // end namespace

