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

# ifdef __linux__

# include "hpc_network_provider.h"
# include "mix_all_io_looper.h"
# include <netinet/tcp.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "network.provider.hpc"


namespace dsn
{
    namespace tools
    {
        static socket_t create_tcp_socket(sockaddr_in* addr)
        {
            socket_t s = -1;
            if ((s = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP)) == -1)
            {
                dwarn("socket failed, err = %s", strerror(errno));
                return -1;
            }

            int nodelay = 1;
            if (setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (char*)&nodelay, sizeof(int)) != 0)
            {
                dwarn("setsockopt TCP_NODELAY failed, err = %s", strerror(errno));
            }
            
            int buflen = 8 * 1024 * 1024;
            if (setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char*)&buflen, sizeof(buflen)) != 0)
            {
                dwarn("setsockopt SO_SNDBUF failed, err = %s", strerror(errno));
            }

            buflen = 8 * 1024 * 1024;
            if (setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char*)&buflen, sizeof(buflen)) != 0)
            {
                dwarn("setsockopt SO_RCVBUF failed, err = %s", strerror(errno));
            }

            int keepalive = 0;
            if (setsockopt(s, SOL_SOCKET, SO_KEEPALIVE, (char*)&keepalive, sizeof(keepalive)) != 0)
            {
                dwarn("setsockopt SO_KEEPALIVE failed, err = %s", strerror(errno));
            }

            if (addr != 0)
            {
                if (bind(s, (struct sockaddr*)addr, sizeof(*addr)) != 0)
                {
                    derror("bind failed, err = %s", strerror(errno));
                    ::close(s);
                    return -1;
                }
            }

            return s;
        }

        hpc_network_provider::hpc_network_provider(rpc_engine* srv, network* inner_provider)
            : connection_oriented_network(srv, inner_provider)
        {
            _listen_fd = -1;
            _looper = nullptr;
        }

        error_code hpc_network_provider::start(rpc_channel channel, int port, bool client_only, io_modifer& ctx)
        {
            if (_listen_fd != -1)
                return ERR_SERVICE_ALREADY_RUNNING;

            _looper = get_io_looper(node(), ctx.queue);

            dassert(channel == RPC_CHANNEL_TCP || channel == RPC_CHANNEL_UDP,
                "invalid given channel %s", channel.to_string());

            gethostname(_address.name, sizeof(_address.name));
            dsn_address_build(&_address, _address.name, port);

            if (!client_only)
            {
                struct sockaddr_in addr;
                addr.sin_family = AF_INET;
                addr.sin_addr.s_addr = INADDR_ANY;
                addr.sin_port = htons(port);

                _listen_fd = create_tcp_socket(&addr);
                if (_listen_fd == -1)
                {
                    dassert(false, "cannot create listen socket");
                }

                int forcereuse = 1;
                if (setsockopt(_listen_fd, SOL_SOCKET, SO_REUSEADDR,
                    (char*)&forcereuse, sizeof(forcereuse)) != 0)
                {
                    dwarn("setsockopt SO_REUSEDADDR failed, err = %s", strerror(errno));
                }

                if (listen(_listen_fd, SOMAXCONN) != 0)
                {
                    dwarn("listen failed, err = %s", strerror(errno));
                    return ERR_NETWORK_START_FAILED;
                }

                _accept_event.callback = [this](int err, uint32_t size, uintptr_t lpolp)
                {
                    this->do_accept();
                };

                _looper->bind_io_handle((dsn_handle_t)_listen_fd, &_accept_event.callback,
                    EPOLLIN | EPOLLET);
            }

            return ERR_OK;
        }

        rpc_client_session_ptr hpc_network_provider::create_client_session(const dsn_address_t& server_addr)
        {
            auto matcher = new_client_matcher();
            auto parser = new_message_parser();

            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = 0;

            auto sock = create_tcp_socket(&addr);
            auto client = new hpc_rpc_client_session(sock, parser, *this, server_addr, matcher);
            client->bind_looper(_looper);
            return client;
        }

        void hpc_network_provider::do_accept()
        {
            struct sockaddr_in addr;
            socklen_t addr_len = (socklen_t)sizeof(addr);
            socket_t s = ::accept(_listen_fd, (struct sockaddr*)&addr, &addr_len);
            if (s != -1)
            {                
                dsn_address_t client_addr;
                dsn_address_build_ipv4(&client_addr, ntohl(addr.sin_addr.s_addr), ntohs(addr.sin_port));

                auto parser = new_message_parser();
                auto rs = new hpc_rpc_server_session(s, parser, *this, client_addr);
                rs->bind_looper(_looper);

                rpc_server_session_ptr s1(rs);
                this->on_server_session_accepted(s1);
            }
            else
            {
                derror("accept failed, err = %s", strerror(errno));
            }
        }

        hpc_rpc_session::hpc_rpc_session(
            socket_t sock,
            std::shared_ptr<dsn::message_parser>& parser
            )
            : _socket(sock), _parser(parser)
        {
            _sending_msg = nullptr;
            _sending_next_offset = 0;
            _looper = nullptr;

            memset((void*)&_peer_addr, 0, sizeof(_peer_addr));
            _peer_addr.sin_family = AF_INET;
            _peer_addr.sin_addr.s_addr = INADDR_ANY;
            _peer_addr.sin_port = 0;
        }

        void hpc_rpc_session::bind_looper(io_looper* looper)
        {
            _looper = looper;
            looper->bind_io_handle((dsn_handle_t)_socket, &_ready_event, 
                EPOLLIN | EPOLLOUT | EPOLLHUP | EPOLLRDHUP | EPOLLET);
        }

        void hpc_rpc_session::do_read(int read_next)
        {
            while (true)
            {
                char* ptr = (char*)_parser->read_buffer_ptr((int)read_next);
                int remaining = _parser->read_buffer_capacity();

                int sz = recv(_socket, ptr, remaining, 0);
                if (sz > 0)
                {
                    message_ex* msg = _parser->get_message_on_receive(sz, read_next);

                    while (msg != nullptr)
                    {
                        this->on_read_completed(msg);
                        msg = _parser->get_message_on_receive(0, read_next);
                    }
                }
                else
                {
                    int err = errno;
                    if (err != EAGAIN && err != EWOULDBLOCK)
                    {
                        derror("recv failed, err = %s", strerror(err));
                        on_failure();
                    }
                    break;
                }
            }
        }

        void hpc_rpc_session::do_write(message_ex* msg)
        {
            static_assert (sizeof(dsn_message_parser::send_buf) == sizeof(struct iovec), 
                "make sure they are compatible");
                        
            // new msg
            if (_sending_msg != msg)
            {
                dassert(_sending_msg == nullptr, "only one sending msg is possible");
                _sending_msg = msg;
                _sending_next_offset = 0;
            }

            // continue old msg
            else
            {
                // nothing to do
            }

            // prepare send buffer, make sure header is already in the buffer
            int total_length = 0;
            int buffer_count = _parser->get_send_buffers_count_and_total_length(msg, &total_length);
            auto buffers = (dsn_message_parser::send_buf*)alloca(buffer_count * sizeof(dsn_message_parser::send_buf));
            int count = _parser->prepare_buffers_on_send(msg, _sending_next_offset, buffers);
            
            struct msghdr hdr;
            memset((void*)&hdr, 0, sizeof(hdr));
            hdr.msg_name = (void*)&_peer_addr;
            hdr.msg_namelen = (socklen_t)sizeof(_peer_addr);
            hdr.msg_iov = (struct iovec*)buffers;
            hdr.msg_iovlen = (size_t)count;

            int sz = sendmsg(_socket, &hdr, MSG_NOSIGNAL);
            if (sz > 0)
            {
                _sending_next_offset += sz;

                // message completed, continue next message
                if (_sending_next_offset == total_length)
                {
                    msg = _sending_msg;
                    _sending_msg = nullptr;
                    on_write_completed(msg);
                    return;
                }
            }
            else
            { 
                int err = errno;
                if (err == EAGAIN || err == EWOULDBLOCK)
                {
                    // wait for next ready
                }
                else
                {
                    derror("sendmsg failed, err = %s", strerror(err));
                    on_failure();
                }
            }
        }

        void hpc_rpc_session::close()
        {
            if (-1 != _socket)
            {
                _looper->unbind_io_handle((dsn_handle_t)(intptr_t)_socket);
                ::close(_socket);
            }
            on_closed();
        }

        hpc_rpc_client_session::hpc_rpc_client_session(
            socket_t sock,
            std::shared_ptr<dsn::message_parser>& parser,
            connection_oriented_network& net,
            const dsn_address_t& remote_addr,
            rpc_client_matcher_ptr& matcher
            )
            : rpc_client_session(net, remote_addr, matcher), hpc_rpc_session(sock, parser)
        {
            _state = SS_CLOSED;
            
            _ready_event = [this](int err, uint32_t length, uintptr_t lolp_or_events)
            {
                uint32_t events = (uint32_t)lolp_or_events;

                if ((events & EPOLLHUP) || (events & EPOLLRDHUP) || (events & EPOLLERR))
                {
                    on_failure();
                    return;
                }

                // connect established is a EPOLLOUT event, so detect OUT first
                if (events & EPOLLOUT)
                {
                    // connect
                    if (_state != SS_CONNECTED)
                    {
                        socklen_t addr_len = (socklen_t)sizeof(_peer_addr);
                        if (getpeername(_socket, (struct sockaddr*)&_peer_addr, &addr_len) == -1)
                        {
                            dassert(false, "getpeername failed, err = %s", strerror(errno));
                        }

                        _state = SS_CONNECTED;
                        dinfo("client session %s:%u connected", 
                            _remote_addr.name,
                            _remote_addr.port
                            );

                        set_connected();
                    }

                    //  send
                    if (_sending_msg)
                    {
                        do_write(_sending_msg);
                    }
                    else
                    {
                        on_write_completed(nullptr); // send next msg if there is.
                    }
                }

                if (events & EPOLLIN)
                {
                    // recv
                    do_read();
                }
            };
        }

        void hpc_rpc_client_session::on_failure()
        {
            _state = SS_CLOSED;
            close();
            on_disconnected();
        }

        void hpc_rpc_client_session::connect()
        {
            session_state closed_state = SS_CLOSED;

            if (!_state.compare_exchange_strong(closed_state, SS_CONNECTING))
                return;
            
            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(_remote_addr.ip);
            addr.sin_port = htons(_remote_addr.port);

            int rt = ::connect(_socket, (struct sockaddr*)&addr, (int)sizeof(addr));
            if (rt == -1)
            {
                if (errno != EINPROGRESS)
                {
                    dwarn("connect failed, err = %s", strerror(errno));
                    on_failure();
                }
                else
                {
                    // later notification in epoll_wait
                }
            }
            else
            {
                socklen_t addr_len = (socklen_t)sizeof(_peer_addr);
                if (getpeername(_socket, (struct sockaddr*)&_peer_addr, &addr_len) == -1)
                {
                    dassert(false, "getpeername failed, err = %s", strerror(errno));
                }

                _state = SS_CONNECTED;
                dinfo("client session %s:%u connected", 
                            _remote_addr.name,
                            _remote_addr.port
                         );

                set_connected();
            }
        }

        hpc_rpc_server_session::hpc_rpc_server_session(
            socket_t sock,
            std::shared_ptr<dsn::message_parser>& parser,
            connection_oriented_network& net,
            const dsn_address_t& remote_addr
            )
            : rpc_server_session(net, remote_addr), hpc_rpc_session(sock, parser)
        {
            socklen_t addr_len = (socklen_t)sizeof(_peer_addr);
            if (getpeername(_socket, (struct sockaddr*)&_peer_addr, &addr_len) == -1)
            {
                dassert(false, "getpeername failed, err = %s", strerror(errno));
            }

            _ready_event = [this](int err, uint32_t length, uintptr_t lolp_or_events)
            {
                uint32_t events = (uint32_t)lolp_or_events;

                if ((events & EPOLLHUP) || (events & EPOLLRDHUP) || (events & EPOLLERR))
                {
                    on_failure();
                    return;
                }

                if (events & EPOLLIN)
                {
                    do_read();
                }

                if (events & EPOLLOUT)
                {
                    if (_sending_msg)
                    {
                        do_write(_sending_msg);
                    }
                    else
                    {
                        on_write_completed(nullptr); // send next msg if there is.
                    }
                }
            };
        }
    }
}

# endif
