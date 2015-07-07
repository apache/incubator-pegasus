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

# include <dsn/internal/rpc_message.h>
# include <dsn/internal/priority_queue.h>
# include <dsn/internal/message_parser.h>
# include <boost/asio.hpp>

namespace dsn {
    namespace tools {

        class net_io
        {
        public:
            net_io(const end_point& remote_addr,
                boost::asio::ip::tcp::socket& socket,
                std::shared_ptr<dsn::message_parser>& parser);
            virtual ~net_io();

            virtual void write(message_ptr& msg);
            void close();
            void start_read(size_t sz = 256) { do_read(sz); }

        protected:
            void do_read(size_t sz = 256);
            void do_write();
            void set_options();
            
            virtual void on_failure() = 0;     
            virtual void on_closed() = 0;
            virtual void on_message_read(message_ptr& msg) = 0;
            virtual void add_reference() = 0;
            virtual void release_reference() = 0;

        protected:

            boost::asio::io_service      &_io_service;
            boost::asio::ip::tcp::socket _socket;
            end_point                    _remote_addr;
            std::shared_ptr<dsn::message_parser> _parser;
            
            // TODO: expose the queue to be customizable
            typedef utils::priority_queue<message_ptr, TASK_PRIORITY_COUNT> send_queue;
            send_queue                   _sq;
        };

        class client_net_io : public net_io
        {
        public:
            client_net_io(const end_point& remote_addr,
                boost::asio::ip::tcp::socket& socket,
                std::shared_ptr<dsn::message_parser>& parser);

            void connect();
            virtual void write(message_ptr& msg);

        private:
            virtual void on_failure();

        private:
            enum session_state
            {
                SS_CONNECTING,
                SS_CONNECTED,
                SS_CLOSED
            };

            std::atomic<session_state>   _state;
            int                          _reconnect_count;
        };
    }
}
