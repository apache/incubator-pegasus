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

# include "net_provider.h"
# include "net_io.h"

namespace dsn {
    namespace tools {

        class asio_network_provider;
        class net_server_session
            : public rpc_server_session, public net_io
        {
        public:
            net_server_session(
                asio_network_provider& net, 
                const dsn_address_t& remote_addr,
                boost::asio::ip::tcp::socket& socket,
                std::shared_ptr<message_parser>& parser,
                boost::asio::io_service& ios);
            ~net_server_session();

            virtual void send(message_ex* reply_msg) override { return write(reply_msg); }
            virtual void on_failure() override { close(); }
            virtual void on_closed() override { return on_disconnected(); }
            virtual void on_message_read(message_ex* msg) override
            {
                return on_recv_request(msg, 0);
            }
            virtual void add_reference() override { add_ref(); }
            virtual void release_reference() override { release_ref(); }
            virtual void on_write_completed(message_ex* msg) override
            {
                on_send_completed(msg);
            }
            
        private:
            asio_network_provider           &_net;
        };
    }
}

