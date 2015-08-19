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
# include "io_looper.h"

namespace dsn {
    namespace tools {
        
        class hpc_network_provider : public connection_oriented_network
        {
        public:
            hpc_network_provider(rpc_engine* srv, network* inner_provider);

            virtual error_code start(rpc_channel channel, int port, bool client_only);
            virtual const dsn_address_t& address() { return _address;  }
            virtual rpc_client_session_ptr create_client_session(const dsn_address_t& server_addr);

        private:
            void do_accept();
            void on_accepted(int err, uint32_t size);

        private:
            SOCKET        _listen_fd;
            dsn_address_t _address;
            io_looper *_looper;

        private:
            void set_looper(io_looper* looper) { _looper = looper; }

# ifdef _WIN32
        public:
            struct completion_event
            {
                OVERLAPPED olp;
                std::function<void(int, uint32_t)> callback;
            };

        private:
            class hpc_network_io_loop_callback : public io_loop_callback
            {
            public:
                hpc_network_io_loop_callback(hpc_network_provider* provider)
                {
                    _provider = provider;
                }

                virtual void handle_event(int native_error, uint32_t io_size, uintptr_t lolp_or_events) override
                {
                    completion_event* ctx = CONTAINING_RECORD(lolp_or_events, completion_event, olp);
                    ctx->callback(native_error, io_size);
                }

            private:
                hpc_network_provider *_provider;
            };

        private:
            struct accept_completion_event : completion_event
            {
                SOCKET s;
                char   buffer[1024];
            };
            
            hpc_network_io_loop_callback _callback;
            accept_completion_event _accept_event;
# else

# endif
        };

        class hpc_rpc_session
        {
        public:
            hpc_rpc_session(
                SOCKET sock,
                std::shared_ptr<dsn::message_parser>& parser
                );

            void do_read(int sz = 256);
            void do_write(message_ex* msg);
            void close();

            virtual void on_read_completed(message_ex* msg) = 0;
            virtual void on_write_completed(message_ex* msg) = 0;
            virtual void on_failure() = 0;
            virtual void on_closed() = 0;
            virtual void add_reference() = 0;
            virtual void release_reference() = 0;

        private:
            SOCKET _rw_fd;
            std::shared_ptr<dsn::message_parser>   _parser;
# ifdef _WIN32
            hpc_network_provider::completion_event _read_event;
            hpc_network_provider::completion_event _write_event;
# else
# endif
        };

        class hpc_rpc_client_session : public rpc_client_session, public hpc_rpc_session
        {
        public:
            hpc_rpc_client_session(
                SOCKET sock, 
                std::shared_ptr<dsn::message_parser>& parser, 
                connection_oriented_network& net, 
                const dsn_address_t& remote_addr, 
                rpc_client_matcher_ptr& matcher
                );
            
            virtual void connect() override;

            // always call on_send_completed later
            virtual void send(message_ex* msg) override
            {
                if (SS_CONNECTED == _state)
                    do_write(msg);
            }
            
            virtual void on_closed() override { return on_disconnected(); }
            virtual void on_read_completed(message_ex* msg) override
            {
                on_recv_reply(msg->header->id, msg, 0);
            }
            virtual void add_reference() override { add_ref(); }
            virtual void release_reference() override { release_ref(); }
            virtual void on_write_completed(message_ex* msg) override
            {
                on_send_completed(msg);
            }

        private:
            SOCKET _socket;

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
        
        class hpc_rpc_server_session : public rpc_server_session, public hpc_rpc_session
        {
        public:
            hpc_rpc_server_session(
                SOCKET sock,
                std::shared_ptr<dsn::message_parser>& parser,
                connection_oriented_network& net,
                const dsn_address_t& remote_addr
                );
            
            // always call on_send_completed later
            virtual void send(message_ex* reply_msg) override
            {
                do_write(reply_msg);
            }

            virtual void on_failure() override { close(); }
            virtual void on_closed() override { return on_disconnected(); }
            virtual void on_read_completed(message_ex* msg) override
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
        };
    }
}
