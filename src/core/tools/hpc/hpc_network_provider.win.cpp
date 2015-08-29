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
# ifdef _WIN32

# include "hpc_network_provider.h"
# include <MSWSock.h>
# include "mix_all_io_looper.h"

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
            socket_t s = INVALID_SOCKET;
            if ((s = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0, 0, WSA_FLAG_OVERLAPPED)) == INVALID_SOCKET)
            {
                dwarn("WSASocket failed, err = %d", ::GetLastError());
                return INVALID_SOCKET;
            }

            int reuse = 1;
            if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (char*)&reuse, sizeof(int)) == -1)
            {
                dwarn("setsockopt SO_REUSEADDR failed, err = %s", strerror(errno));
            }

            int nodelay = 1;
            if (setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (char*)&nodelay, sizeof(int)) != 0)
            {
                dwarn("setsockopt TCP_NODELAY failed, err = %d", ::GetLastError());
            }

            int isopt = 1;
            if (setsockopt(s, SOL_SOCKET, SO_DONTLINGER, (char*)&isopt, sizeof(int)) != 0)
            {
                dwarn("setsockopt SO_DONTLINGER failed, err = %d", ::GetLastError());
            }
            
            int buflen = 8 * 1024 * 1024;
            if (setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char*)&buflen, sizeof(buflen)) != 0)
            {
                dwarn("setsockopt SO_SNDBUF failed, err = %d", ::GetLastError());
            }

            buflen = 8*1024*1024;
            if (setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char*)&buflen, sizeof(buflen)) != 0)
            {
                dwarn("setsockopt SO_RCVBUF failed, err = %d", ::GetLastError());
            }

            int keepalive = 1;
            if (setsockopt(s, SOL_SOCKET, SO_KEEPALIVE, (char*)&keepalive, sizeof(keepalive)) != 0)
            {
                dwarn("setsockopt SO_KEEPALIVE failed, err = %d", ::GetLastError());
            }
            
            if (addr != 0)
            {
                if (bind(s, (struct sockaddr*)addr, sizeof(*addr)) != 0)
                {
                    derror("bind failed, err = %d", ::GetLastError());
                    closesocket(s);
                    return INVALID_SOCKET;
                }
            }

            return s;
        }

        static LPFN_ACCEPTEX s_lpfnAcceptEx = NULL;
        static LPFN_CONNECTEX s_lpfnConnectEx = NULL;
        static LPFN_GETACCEPTEXSOCKADDRS s_lpfnGetAcceptExSockaddrs = NULL;

        static void load_socket_functions()
        {
            if (s_lpfnGetAcceptExSockaddrs != NULL)
                return;

            socket_t s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
            if (s == INVALID_SOCKET)
            {
                dassert(false, "create Socket Failed, err = %d", ::GetLastError());
            }

            GUID GuidAcceptEx = WSAID_ACCEPTEX;
            GUID GuidConnectEx = WSAID_CONNECTEX;
            GUID GuidGetAcceptExSockaddrs = WSAID_GETACCEPTEXSOCKADDRS;
            DWORD dwBytes;

            // Load the AcceptEx function into memory using WSAIoctl.
            // The WSAIoctl function is an extension of the ioctlsocket()
            // function that can use overlapped I/O. The function's 3rd
            // through 6th parameters are input and output buffers where
            // we pass the pointer to our AcceptEx function. This is used
            // so that we can call the AcceptEx function directly, rather
            // than refer to the Mswsock.lib library.
            int rt = WSAIoctl(s,
                SIO_GET_EXTENSION_FUNCTION_POINTER,
                &GuidAcceptEx,
                sizeof(GuidAcceptEx),
                &s_lpfnAcceptEx,
                sizeof(s_lpfnAcceptEx),
                &dwBytes,
                NULL,
                NULL);
            if (rt == SOCKET_ERROR)
            {
                dwarn("WSAIoctl for AcceptEx failed, err = %d", ::WSAGetLastError());
                closesocket(s);
                return;
            }


            rt = WSAIoctl(s,
                SIO_GET_EXTENSION_FUNCTION_POINTER,
                &GuidConnectEx,
                sizeof(GuidConnectEx),
                &s_lpfnConnectEx,
                sizeof(s_lpfnConnectEx),
                &dwBytes,
                NULL,
                NULL);
            if (rt == SOCKET_ERROR)
            {
                dwarn("WSAIoctl for ConnectEx failed, err = %d", ::WSAGetLastError());
                closesocket(s);
                return;
            }

            rt = WSAIoctl(s,
                SIO_GET_EXTENSION_FUNCTION_POINTER,
                &GuidGetAcceptExSockaddrs,
                sizeof(GuidGetAcceptExSockaddrs),
                &s_lpfnGetAcceptExSockaddrs,
                sizeof(s_lpfnGetAcceptExSockaddrs),
                &dwBytes,
                NULL,
                NULL);
            if (rt == SOCKET_ERROR)
            {
                dwarn("WSAIoctl for GetAcceptExSockaddrs failed, err = %d", ::WSAGetLastError());
                closesocket(s);
                return;
            }

            closesocket(s);
        }

        hpc_network_provider::hpc_network_provider(rpc_engine* srv, network* inner_provider)
            : connection_oriented_network(srv, inner_provider)
        {
            load_socket_functions();
            _listen_fd = INVALID_SOCKET;
            _looper = nullptr;
        }
        
        error_code hpc_network_provider::start(rpc_channel channel, int port, bool client_only, io_modifer& ctx)
        {
            if (_listen_fd != INVALID_SOCKET)
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
                if (_listen_fd == INVALID_SOCKET)
                {
                    dassert(false, "");
                }

                int forcereuse = 1;
                if (setsockopt(_listen_fd, SOL_SOCKET, SO_REUSEADDR, 
                    (char*)&forcereuse, sizeof(forcereuse)) != 0)
                {
                    dwarn("setsockopt SO_REUSEDADDR failed, err = %d", ::GetLastError());
                }

                _looper->bind_io_handle((dsn_handle_t)_listen_fd, &_accept_event.callback);

                if (listen(_listen_fd, SOMAXCONN) != 0)
                {
                    dwarn("listen failed, err = %d", ::GetLastError());
                    return ERR_NETWORK_START_FAILED;
                }
                
                do_accept();
            }
            
            return ERR_OK;
        }

        rpc_session_ptr hpc_network_provider::create_client_session(const dsn_address_t& server_addr)
        {
            auto matcher = new_client_matcher();
            auto parser = new_message_parser();

            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = 0;

            auto sock = create_tcp_socket(&addr);
            auto client = new hpc_rpc_session(sock, parser, *this, server_addr, matcher);
            rpc_session_ptr c(client);
            client->bind_looper(_looper);
            return c;
        }

        void hpc_network_provider::do_accept()
        {
            socket_t s = create_tcp_socket(nullptr);
            dassert(s != INVALID_SOCKET, "cannot create socket for accept");

            _accept_sock = s;            
            _accept_event.callback = [this](int err, uint32_t size, uintptr_t lpolp)
            {
                //dinfo("accept completed, err = %d, size = %u", err, size);
                dassert(&_accept_event.olp == (LPOVERLAPPED)lpolp, "must be this exact overlap");
                if (err == ERROR_SUCCESS)
                {
                    setsockopt(_accept_sock,
                        SOL_SOCKET,
                        SO_UPDATE_ACCEPT_CONTEXT,
                        (char *)&_listen_fd,
                        sizeof(_listen_fd)
                        );

                    struct sockaddr_in addr;
                    memset((void*)&addr, 0, sizeof(addr));

                    addr.sin_family = AF_INET;
                    addr.sin_addr.s_addr = INADDR_ANY;
                    addr.sin_port = 0;

                    int addr_len = sizeof(addr);
                    if (getpeername(_accept_sock, (struct sockaddr*)&addr, &addr_len)
                        == SOCKET_ERROR)
                    {
                        dassert(false, "getpeername failed, err = %d", ::WSAGetLastError());
                    }

                    dsn_address_t client_addr;
                    dsn_address_build_ipv4(&client_addr, ntohl(addr.sin_addr.s_addr), ntohs(addr.sin_port));

                    auto parser = new_message_parser();
                    auto s = new hpc_rpc_session(_accept_sock, parser, *this, client_addr);
                    rpc_session_ptr s1(s);
                    s->bind_looper(_looper);

                    this->on_server_session_accepted(s1);

                    s->do_read();
                }
                else
                {
                    closesocket(_accept_sock);
                }

                do_accept();
            };
            memset(&_accept_event.olp, 0, sizeof(_accept_event.olp));

            DWORD bytes;
            BOOL rt = s_lpfnAcceptEx(
                _listen_fd, s,
                _accept_buffer,
                0,
                (sizeof(struct sockaddr_in) + 16),
                (sizeof(struct sockaddr_in) + 16),
                &bytes,
                &_accept_event.olp
                );

            if (!rt && (WSAGetLastError() != ERROR_IO_PENDING))
            {
                dassert(false, "AcceptEx failed, err = %d", ::WSAGetLastError());
                closesocket(s);
            }
        }
                
        io_loop_callback s_ready_event = 
            [](int err, uint32_t length, uintptr_t lolp)
            {
                auto evt = CONTAINING_RECORD(lolp, hpc_network_provider::ready_event, olp);
                evt->callback(err, length, lolp);
            };
        
        void hpc_rpc_session::bind_looper(io_looper* looper, bool delay)
        {
            looper->bind_io_handle((dsn_handle_t)_socket, &s_ready_event);
        }

        void hpc_rpc_session::do_read(int sz)
        {
            add_ref();
            _read_event.callback = [this](int err, uint32_t length, uintptr_t lolp)
            {
                //dinfo("WSARecv completed, err = %d, size = %u", err, length);
                dassert((LPOVERLAPPED)lolp == &_read_event.olp, "must be exact this overlapped");
                if (err != ERROR_SUCCESS)
                {
                    dwarn("WSARecv failed, err = %d", err);
                    on_failure();
                }
                else
                {
                    int read_next;
                    message_ex* msg = _parser->get_message_on_receive((int)length, read_next);

                    while (msg != nullptr)
                    {
                        this->on_read_completed(msg);
                        msg = _parser->get_message_on_receive(0, read_next);
                    }

                    do_read(read_next);
                }

                release_ref();
            };
            memset(&_read_event.olp, 0, sizeof(_read_event.olp));

            WSABUF buf[1];

            void* ptr = _parser->read_buffer_ptr((int)sz);
            int remaining = _parser->read_buffer_capacity();
            buf[0].buf = (char*)ptr;
            buf[0].len = remaining;

            DWORD bytes = 0;
            DWORD flag = 0;
            int rt = WSARecv(
                _socket,
                buf,
                1,
                &bytes,
                &flag,
                &_read_event.olp,
                NULL
                );
            
            if (SOCKET_ERROR == rt && (WSAGetLastError() != ERROR_IO_PENDING))
            {
                dwarn("WSARecv failed, err = %d", ::WSAGetLastError());
                release_ref();
                on_failure();
            }

            //dinfo("WSARecv called, err = %d", rt);
        }

        void hpc_rpc_session::do_write(message_ex* msg)
        {
            add_ref();
            
            _write_event.callback = [this](int err, uint32_t length, uintptr_t lolp)
            {
                dassert((LPOVERLAPPED)lolp == &_write_event.olp, "must be exact this overlapped");
                if (err != ERROR_SUCCESS)
                {
                    dwarn("WSASend failed, err = %d", err);
                    on_failure();
                }
                else
                {
                    _sending_next_offset += length;

                    int total_length = 0;
                    int buffer_count = _parser->get_send_buffers_count_and_total_length(_sending_msg, &total_length);

                    // message completed, continue next message
                    if (_sending_next_offset == total_length)
                    {
                        auto lmsg = _sending_msg;
                        _sending_msg = nullptr;
                        on_send_completed(lmsg);
                    }
                    else
                        do_write(_sending_msg);
                }

                release_ref();
            };
            memset(&_write_event.olp, 0, sizeof(_write_event.olp));
            
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
            
            static_assert (sizeof(dsn_message_parser::send_buf) == sizeof(WSABUF), "make sure they are compatible");

            DWORD bytes = 0;
            int rt = WSASend(
                _socket,
                (LPWSABUF)buffers,
                (DWORD)buffer_count,
                &bytes,
                0,
                &_write_event.olp,
                NULL
                );
            
            if (SOCKET_ERROR == rt && (WSAGetLastError() != ERROR_IO_PENDING))
            {
                dwarn("WSASend failed, err = %d", ::WSAGetLastError());
                release_ref();
                on_failure();
            }
            
            //dinfo("WSASend called, err = %d", rt);
        }

        void hpc_rpc_session::close()
        {
            closesocket(_socket);
        }

        hpc_rpc_session::hpc_rpc_session(
            socket_t sock,
            std::shared_ptr<dsn::message_parser>& parser,
            connection_oriented_network& net,
            const dsn_address_t& remote_addr,
            rpc_client_matcher_ptr& matcher
            )
            : rpc_session(net, remote_addr, matcher),
            _socket(sock), _parser(parser)
        {
            _sending_msg = nullptr;
            _sending_next_offset = 0;
        }

        void hpc_rpc_session::on_failure()
        {
            if (on_disconnected())
                close();
        }

        void hpc_rpc_session::connect()
        {
            if (!try_connecting())
                return;
                        
            _connect_event.callback = [this](int err, uint32_t io_size, uintptr_t lpolp)
            {
                //dinfo("ConnectEx completed, err = %d, size = %u", err, io_size);
                if (err != ERROR_SUCCESS)
                {
                    dwarn("ConnectEx failed, err = %d", err);
                    this->on_failure();
                }
                else
                {
                    dinfo("client session %s:%hu connected",
                        _remote_addr.name,
                        _remote_addr.port
                        );

                    set_connected();
                    on_send_completed(nullptr);
                    do_read();
                }
                this->release_ref(); // added before ConnectEx
            };
            memset(&_connect_event.olp, 0, sizeof(_connect_event.olp));

            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(_remote_addr.ip);
            addr.sin_port = htons(_remote_addr.port);

            this->add_ref(); // released in _connect_event.callback
            BOOL rt = s_lpfnConnectEx(
                _socket,
                (struct sockaddr*)&addr,
                (int)sizeof(addr),
                0,
                0,
                0,
                &_connect_event.olp
                );

            if (!rt && (WSAGetLastError() != ERROR_IO_PENDING))
            {
                dwarn("ConnectEx failed, err = %d", ::WSAGetLastError());
                this->release_ref();

                on_failure();
            }
        }

        hpc_rpc_session::hpc_rpc_session(
            socket_t sock,
            std::shared_ptr<dsn::message_parser>& parser,
            connection_oriented_network& net,
            const dsn_address_t& remote_addr
            )
            : rpc_session(net, remote_addr),
            _socket(sock), _parser(parser)
        {
            _sending_msg = nullptr;
            _sending_next_offset = 0;
        }
    }
}

# endif
