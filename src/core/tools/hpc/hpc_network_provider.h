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

# ifdef _WIN32
    typedef SOCKET socket_t;
# else
    # include <sys/types.h>
    # include <sys/socket.h>
    # include <netdb.h>
    # include <arpa/inet.h>
    typedef int socket_t;
    # if defined(__FreeBSD__)
        # include <netinet/in.h>
    # endif
# endif

namespace dsn {
    namespace tools {
        

        class hpc_network_provider : public connection_oriented_network
        {
        public:
            hpc_network_provider(rpc_engine* srv, network* inner_provider);

            virtual error_code start(rpc_channel channel, int port, bool client_only, io_modifer& ctx);
            virtual const dsn_address_t& address() { return _address;  }
            virtual rpc_client_session_ptr create_client_session(const dsn_address_t& server_addr);
            
        private:
            socket_t      _listen_fd;
            dsn_address_t _address;
            io_looper     *_looper;
            
        private:
            void do_accept();

        public:
            struct ready_event
            {
# ifdef _WIN32
                OVERLAPPED olp;
# endif
                io_loop_callback callback;
            };

        private:            
            ready_event      _accept_event;
# ifdef _WIN32
            socket_t         _accept_sock;
            char             _accept_buffer[1024];
# endif
        };

        class hpc_rpc_session
        {
        public:
            hpc_rpc_session(
                socket_t sock,
                std::shared_ptr<dsn::message_parser>& parser
                );

            void bind_looper(io_looper* looper);
            void do_read(int sz = 256);
            void do_write(message_ex* msg);
            void close();

            virtual void on_read_completed(message_ex* msg) = 0;
            virtual void on_write_completed(message_ex* msg) = 0;
            virtual void on_failure() = 0;
            virtual void add_reference() = 0;
            virtual void release_reference() = 0;

        protected:
            socket_t                               _socket;
            std::shared_ptr<dsn::message_parser>   _parser;
            message_ex*                            _sending_msg;
            int                                    _sending_next_offset;

# ifdef _WIN32
            static io_loop_callback                _ready_event; // it is stateless, so static
            hpc_network_provider::ready_event      _read_event;
            hpc_network_provider::ready_event      _write_event;
# else
            io_loop_callback                       _ready_event;            
            struct sockaddr_in                     _peer_addr;
            io_looper*                             _looper;
# endif
        };

        class hpc_rpc_client_session : public rpc_client_session, public hpc_rpc_session
        {
        public:
            hpc_rpc_client_session(
                socket_t sock, 
                std::shared_ptr<dsn::message_parser>& parser, 
                connection_oriented_network& net, 
                const dsn_address_t& remote_addr, 
                rpc_client_matcher_ptr& matcher
                );
            
            virtual void connect() override;

            // always call on_send_completed later
            virtual void send(message_ex* msg) override
            {
                do_write(msg);
            }
            
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
# ifdef _WIN32
            // use _ready_event on linux, so this is only used on windows
            hpc_network_provider::ready_event      _connect_event;
# else
            void on_events(uint32_t events);
# endif

        private:
            virtual void on_failure();

        private:
            
        };
        
        class hpc_rpc_server_session : public rpc_server_session, public hpc_rpc_session
        {
        public:
            hpc_rpc_server_session(
                socket_t sock,
                std::shared_ptr<dsn::message_parser>& parser,
                connection_oriented_network& net,
                const dsn_address_t& remote_addr
                );
            
            // always call on_send_completed later
            virtual void send(message_ex* reply_msg) override
            {
                do_write(reply_msg);
            }

            virtual void on_failure() override 
            {
                close(); 
                on_disconnected(); 
            }
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
