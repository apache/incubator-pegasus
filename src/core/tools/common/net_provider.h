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

# include <dsn/tool_api.h>
# include <boost/asio.hpp>

namespace dsn {
    namespace tools {
        
        class asio_network_provider : public connection_oriented_network
        {
        public:
            asio_network_provider(rpc_engine* srv, network* inner_provider);

            virtual error_code start(rpc_channel channel, int port, bool client_only);
            virtual const dsn_address_t& address() { return _address;  }
            virtual rpc_client_session_ptr create_client_session(const dsn_address_t& server_addr);

        private:
            void do_accept();

        private:
            friend class net_server_session;
            friend class net_client_session;

            std::shared_ptr<boost::asio::ip::tcp::acceptor> _acceptor;
            std::shared_ptr<boost::asio::ip::tcp::socket>   _socket;
            boost::asio::io_service                         _io_service;
            std::vector<std::shared_ptr<std::thread>>       _workers;
            dsn_address_t                                   _address;
        };

    }
}
