/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

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
# include <dsn/internal/message_parser.h>
# include "net_io.h"

namespace dsn {
    namespace tools {

        class asio_network_provider;
        class net_server_session
            : public rpc_server_session
        {
        public:
            net_server_session(
                asio_network_provider& net, 
                const end_point& remote_addr,
                boost::asio::ip::tcp::socket& socket,
                std::shared_ptr<message_parser>& parser);
            ~net_server_session();

            virtual void send(message_ptr& reply_msg) { return _io->write(reply_msg); }
            
        private:            
            void on_failure();
            void on_failed();
            void on_message_read(message_ptr& msg);

        private:
            class net_io_server : public net_io
            {
            public:
                net_io_server(const end_point& remote_addr,
                    boost::asio::ip::tcp::socket& socket,
                    std::shared_ptr<dsn::message_parser>& parser,
                    net_server_session* host)
                    : net_io(remote_addr, socket, parser)
                {
                }

                virtual void on_failure() { return _host->on_failure(); }
                virtual void on_failed() { return _host->on_failed(); }
                virtual void on_message_read(message_ptr& msg) 
                {
                    return _host->on_message_read(msg);
                }

            private:
                net_server_session* _host;
            };

        private:
            net_io_ptr                      _io;
            std::shared_ptr<message_parser> _parser;
            asio_network_provider           &_net;
        };
    }
}

