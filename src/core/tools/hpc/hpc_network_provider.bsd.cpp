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

# if defined(__APPLE__) || defined(__FreeBSD__)

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

            int reuse = 1;
            if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (char*)&reuse, sizeof(int)) == -1)
            {
                dwarn("setsockopt SO_REUSEADDR failed, err = %s", strerror(errno));
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

            int keepalive = 1;
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

            _looper = get_io_looper(node(), ctx.queue, ctx.mode);

            dassert(channel == RPC_CHANNEL_TCP || channel == RPC_CHANNEL_UDP,
                "invalid given channel %s", channel.to_string());

            char hostname[128];
            gethostname(hostname, sizeof(hostname));
            _address = ::dsn::rpc_address(HOST_TYPE_IPV4, hostname, port);

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

                // bind for accept
                _looper->bind_io_handle((dsn_handle_t)(intptr_t)_listen_fd, &_accept_event.callback,
                    EVFILT_READ,
                    nullptr // network_provider is a global object
                    );
            }

            return ERR_OK;
        }

        rpc_session_ptr hpc_network_provider::create_client_session(const ::dsn::rpc_address& server_addr)
        {
            auto matcher = new_client_matcher();
            auto parser = new_message_parser();

            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = 0;

            auto sock = create_tcp_socket(&addr);
            dassert(sock != -1, "create client tcp socket failed!");
            auto client = new hpc_rpc_session(sock, parser, *this, server_addr, matcher);
            rpc_session_ptr c(client);
            client->bind_looper(_looper, true);
            return c;
        }

        void hpc_network_provider::do_accept()
        {
            while (true)
            {
                struct sockaddr_in addr;
                socklen_t addr_len = (socklen_t)sizeof(addr);
                socket_t s = ::accept(_listen_fd, (struct sockaddr*)&addr, &addr_len);
                if (s != -1)
                {
                    ::dsn::rpc_address client_addr(ntohl(addr.sin_addr.s_addr), ntohs(addr.sin_port));

                    auto parser = new_message_parser();
                    auto rs = new hpc_rpc_session(s, parser, *this, client_addr);
                    rpc_session_ptr s1(rs);

                    rs->bind_looper(_looper);
                    this->on_server_session_accepted(s1);
                }
                else
                {
                    if (errno != EAGAIN && errno != EWOULDBLOCK)
                    {
                        derror("accept failed, err = %s", strerror(errno));
                    }                    
                    break;
                }
            }
        }

        void hpc_rpc_session::bind_looper(io_looper* looper, bool delay)
        {
            static short filters[] = { EVFILT_READ, EVFILT_WRITE };
            _looper = looper;
            if (!delay)
            {
                // bind for send/recv
                for (short filter : filters)
                {
                    looper->bind_io_handle((dsn_handle_t)(intptr_t)_socket, &_ready_event,
                        filter,
                        this
                        );
                }
            }   
        }

        void hpc_rpc_session::do_read(int read_next)
        {
            utils::auto_lock<utils::ex_lock_nr> l(_send_lock);

            while (true)
            {
                char* ptr = (char*)_parser->read_buffer_ptr((int)read_next);
                int remaining = _parser->read_buffer_capacity();

                int sz = recv(_socket, ptr, remaining, 0);
                int err = errno;
                dinfo("(s = %d) call recv on %s:%hu, return %d, err = %s",
                    _socket,
                    _remote_addr.name(),
                    _remote_addr.port(),
                    sz,
                    strerror(err)
                    );

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
                    if (err != EAGAIN && err != EWOULDBLOCK)
                    {
                        derror("(s = %d) recv failed, err = %s", _socket, strerror(err));
                        on_failure();
                    }
                    break;
                }
            }
        }

        void hpc_rpc_session::do_safe_write(message_ex* msg)
        {
            utils::auto_lock<utils::ex_lock_nr> l(_send_lock);

            if (nullptr == msg)
            {
                if (_sending_msg)
                {
                    do_write(_sending_msg);
                }
                else
                {
                    _send_lock.unlock(); // avoid recursion
                    on_send_completed(nullptr); // send next msg if there is.
                    _send_lock.lock();
                }
            }
            else
            {
                do_write(msg);
            }
        }

        void hpc_rpc_session::do_write(message_ex* msg)
        {
            static_assert (sizeof(dsn_message_parser::send_buf) == sizeof(struct iovec), 
                "make sure they are compatible");

            dbg_dassert(msg != nullptr, "cannot send empty msg");
                        
            // new msg
            if (_sending_msg == nullptr)
            {
                _sending_msg = msg;
                _sending_next_offset = 0;
            }

            // continue old msg
            else
            {
                dassert(_sending_msg == msg, "only one sending msg is possible");
            }

            // prepare send buffer, make sure header is already in the buffer
            int total_length = 0;
            int buffer_count = _parser->get_send_buffers_count_and_total_length(msg, &total_length);
            auto buffers = (dsn_message_parser::send_buf*)alloca(buffer_count * sizeof(dsn_message_parser::send_buf));

            while (true)
            {
                int count = _parser->prepare_buffers_on_send(msg, _sending_next_offset, buffers);
                struct msghdr hdr;
                memset((void*)&hdr, 0, sizeof(hdr));
                hdr.msg_name = (void*)&_peer_addr;
                hdr.msg_namelen = (socklen_t)sizeof(_peer_addr);
                hdr.msg_iov = (struct iovec*)buffers;
                hdr.msg_iovlen = (size_t)count;

                int sz = sendmsg(_socket, &hdr, MSG_NOSIGNAL);
                int err = errno;
                dinfo("(s = %d) call sendmsg on %s:%hu, return %d, err = %s",
                    _socket,
                    _remote_addr.name(),
                    _remote_addr.port(),
                    sz,
                    strerror(err)
                    );

                if (sz < 0)
                {
                    if (err != EAGAIN && err != EWOULDBLOCK)
                    {
                        derror("(s = %d) sendmsg failed, err = %s", _socket, strerror(err));
                        on_failure();                        
                    }
                    else
                    {
                        // wait for epoll_wait notification
                    }
                    return;
                }
                else
                {
                    _sending_next_offset += sz;

                    if (_sending_next_offset < total_length)
                    {
                        // try next while(true) loop to continue sending current msg
                    }

                    // message completed, continue next message
                    else
                    {
                        _sending_msg = nullptr;

                        _send_lock.unlock(); // avoid recursion
                        // try next msg recursively
                        on_send_completed(msg);
                        _send_lock.lock();
                        return;
                    }
                }
            }
        }

        void hpc_rpc_session::close()
        {
            if (-1 != _socket)
            {
                ::close(_socket);
                dinfo("(s = %d) close socket %p", _socket, this);
                _socket = -1;
            }
        }

        void hpc_rpc_session::on_send_recv_events_ready(uintptr_t lolp_or_events)
        {
            struct kevent& ev = *((struct kevent*)lolp_or_events);
            // shutdown or send/recv error
            if ((ev.flags == EV_ERROR) || (ev.flags == EV_EOF))
            {
                dinfo("(s = %d) epoll failure on %s:%hu, events = %x",
                    _socket,
                    _remote_addr.name(),
                    _remote_addr.port(),
                    ev.filter
                    );
                on_failure();
                return;
            }

            //  send
            if (ev.filter == EVFILT_WRITE)
            {
                dinfo("(s = %d) epoll EPOLLOUT on %s:%hu, events = %x",
                    _socket,
                    _remote_addr.name(),
                    _remote_addr.port(),
                    ev.filter
                    );

                do_safe_write(nullptr);
            }

            // recv
            if (ev.filter == EVFILT_READ)
            {
                dinfo("(s = %d) epoll EPOLLIN on %s:%hu, events = %x",
                    _socket,
                    _remote_addr.name(),
                    _remote_addr.port(),
                    ev.filter 
                    );

                do_read();
            }
        }

        // client
        hpc_rpc_session::hpc_rpc_session(
            socket_t sock,
            std::shared_ptr<dsn::message_parser>& parser,
            connection_oriented_network& net,
            const ::dsn::rpc_address& remote_addr,
            rpc_client_matcher_ptr& matcher
            )
            : rpc_session(net, remote_addr, matcher),
             _socket(sock), _parser(parser)
        {
            dassert(sock != -1, "invalid given socket handle");
            _sending_msg = nullptr;
            _sending_next_offset = 0;
            _looper = nullptr;

            memset((void*)&_peer_addr, 0, sizeof(_peer_addr));
            _peer_addr.sin_family = AF_INET;
            _peer_addr.sin_addr.s_addr = INADDR_ANY;
            _peer_addr.sin_port = 0;

            _ready_event = [this](int err, uint32_t length, uintptr_t lolp_or_events)
            {
                if (is_connecting())
                    this->on_connect_events_ready(lolp_or_events);
                else
                    this->on_send_recv_events_ready(lolp_or_events);
            };
        }

        void hpc_rpc_session::on_connect_events_ready(uintptr_t lolp_or_events)
        {
            dassert(is_connecting(), "session must be connecting at this time");

            struct kevent& ev = *((struct kevent*)lolp_or_events);
            dinfo("(s = %d) epoll for connect to %s:%hu, events = %x",
                _socket,
                _remote_addr.name(),
                _remote_addr.port(),
                ev.filter
                );

            if ((ev.filter == EVFILT_WRITE)
                && (ev.flags != EV_ERROR)
                && (ev.flags != EV_EOF)
                )
            {
                socklen_t addr_len = (socklen_t)sizeof(_peer_addr);
                if (getpeername(_socket, (struct sockaddr*)&_peer_addr, &addr_len) == -1)
                {
                    dassert(false, "(s = %d) (client) getpeername failed, err = %s",
                        _socket, strerror(errno));
                }

                dinfo("(s = %d) client session %s:%hu connected",
                    _socket,
                    _remote_addr.name(),
                    _remote_addr.port()
                    );

                set_connected();
                
                struct kevent e;
                static short filters[] = { EVFILT_READ, EVFILT_WRITE };
                for (auto filter : filters)
                {
                    EV_SET(&e, (int)(intptr_t)_looper->native_handle(), filter, (EV_ADD | EV_CLEAR), 0, 0, (void*)&_ready_event);

                    //TODO where is the ctx?
                    if (_looper->bind_io_handle(
                        (dsn_handle_t)(intptr_t)_socket,
                        &_ready_event,
                        filter
                        ) != ERR_OK)
                    {
                        on_failure();
                        return;
                    }
                }

                // start first round send
                do_safe_write(nullptr);
            }
            else
            {
                int err = 0;
                socklen_t err_len = (socklen_t)sizeof(err);

                if (getsockopt(_socket, SOL_SOCKET, SO_ERROR, (void*)&err, &err_len) < 0)
                {
                    dassert(false, "getsockopt for SO_ERROR failed, err = %s", strerror(errno));
                }

                derror("(s = %d) connect failed (in epoll), err = %s", _socket, strerror(err));
                on_failure();
            }
        }

        void hpc_rpc_session::on_failure()
        {
            _looper->unbind_io_handle((dsn_handle_t)(intptr_t)_socket, &_ready_event);
            if (on_disconnected())
                close();            
        }

        void hpc_rpc_session::connect()
        {
            if (!try_connecting())
                return;
            
            dassert(_socket != -1, "invalid given socket handle");

            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(_remote_addr.ip());
            addr.sin_port = htons(_remote_addr.port());

            int rt = ::connect(_socket, (struct sockaddr*)&addr, (int)sizeof(addr));
            int err = errno;
            dinfo("(s = %d) call connect to %s:%hu, return %d, err = %s",
                _socket,
                _remote_addr.name(),
                _remote_addr.port(),
                rt,
                strerror(err)
                );

            if (rt == -1 && err != EINPROGRESS)
            {
                dwarn("(s = %d) connect failed, err = %s", _socket, strerror(err));
                on_failure();
                return;
            }

            // bind for connect
            _looper->bind_io_handle((dsn_handle_t)(intptr_t)_socket, &_ready_event,
                EVFILT_WRITE,
                this
                );
        }

        // server
        hpc_rpc_session::hpc_rpc_session(
            socket_t sock,
            std::shared_ptr<dsn::message_parser>& parser,
            connection_oriented_network& net,
            const ::dsn::rpc_address& remote_addr
            )
            : rpc_session(net, remote_addr),
            _socket(sock), _parser(parser)
        {
            dassert(sock != -1, "invalid given socket handle");
            _sending_msg = nullptr;
            _sending_next_offset = 0;
            _looper = nullptr;

            memset((void*)&_peer_addr, 0, sizeof(_peer_addr));
            _peer_addr.sin_family = AF_INET;
            _peer_addr.sin_addr.s_addr = INADDR_ANY;
            _peer_addr.sin_port = 0;

            socklen_t addr_len = (socklen_t)sizeof(_peer_addr);
            if (getpeername(_socket, (struct sockaddr*)&_peer_addr, &addr_len) == -1)
            {
                dassert(false, "(server) getpeername failed, err = %s", strerror(errno));
            }

            _ready_event = [this](int err, uint32_t length, uintptr_t lolp_or_events)
            {
                uint32_t events = (uint32_t)lolp_or_events;
                dinfo("(s = %d) (server) epoll for send/recv to %s:%hu, events = %x",
                    _socket,
                    _remote_addr.name(),
                    _remote_addr.port(),
                    events
                    );
                this->on_send_recv_events_ready(events);
            };
        }
    }
}

# endif
