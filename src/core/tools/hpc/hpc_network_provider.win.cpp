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
        static SOCKET create_tcp_socket(sockaddr_in* addr)
        {
            SOCKET s = INVALID_SOCKET;
            if ((s = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0, 0, WSA_FLAG_OVERLAPPED)) == INVALID_SOCKET)
            {
                dwarn("WSASocket failed, err = %d", ::GetLastError());
                return INVALID_SOCKET;
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
            
            //streaming data using overlapped I/O should set the send buffer to zero
            int buflen = 0;
            if (setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char*)&buflen, sizeof(buflen)) != 0)
            {
                dwarn("setsockopt SO_SNDBUF failed, err = %d", ::GetLastError());
            }

            buflen = 8*1024*1024;
            if (setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char*)&buflen, sizeof(buflen)) != 0)
            {
                dwarn("setsockopt SO_RCVBUF failed, err = %d", ::GetLastError());
            }

            int keepalive = 0;
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

            SOCKET s = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
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
            : connection_oriented_network(srv, inner_provider), _callback(this)
        {
            load_socket_functions();
            _listen_fd = INVALID_SOCKET;
            _looper = get_io_looper(node());
        }

        error_code hpc_network_provider::start(rpc_channel channel, int port, bool client_only)
        {
            if (_listen_fd != INVALID_SOCKET)
                return ERR_SERVICE_ALREADY_RUNNING;
            
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
                    dwarn("setsockopt SO_REUSEDADDR failed, err = %d");
                }

                if (listen(_listen_fd, SOMAXCONN) != 0)
                {
                    dwarn("listen failed, err = %d", ::GetLastError());
                    return ERR_NETWORK_START_FAILED;
                }
                
                get_looper()->bind_io_handle((dsn_handle_t)_listen_fd, &_callback);

                do_accept();
            }
            
            return ERR_OK;
        }

        rpc_client_session_ptr hpc_network_provider::create_client_session(const dsn_address_t& server_addr)
        {
            auto matcher = new_client_matcher();
            auto parser = new_message_parser();
            auto sock = create_tcp_socket(nullptr);

            get_looper()->bind_io_handle((dsn_handle_t)sock, &_callback);

            return new hpc_rpc_client_session(sock, parser, *this, server_addr, matcher);
        }

        void hpc_network_provider::do_accept()
        {
            SOCKET s = create_tcp_socket(nullptr);
            dassert(s != INVALID_SOCKET, "cannot create socket for accept");

            get_looper()->bind_io_handle((dsn_handle_t)s, &_callback);

            _accept_event.s = s;
            _accept_event.callback = [this](int err, uint32_t size)
            {
                this->on_accepted(err, size);
            };
            memset(&_accept_event.olp, 0, sizeof(_accept_event.olp));

            DWORD bytes;
            BOOL rt = s_lpfnAcceptEx(
                _listen_fd, s,
                _accept_event.buffer,
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

        void hpc_network_provider::on_accepted(int err, uint32_t size)
        {
            if (err == ERROR_SUCCESS)
            {
                struct sockaddr_in addr;
                int addr_len = sizeof(addr);
                if (getsockname(_accept_event.s, (struct sockaddr*)&addr, &addr_len) != 0)
                {
                    dassert(false, "getsockname failed, err = %d", ::WSAGetLastError());
                }

                dsn_address_t client_addr;
                dsn_address_build_ipv4(&client_addr, ntohl(addr.sin_addr.s_addr), ntohs(addr.sin_port));
                
                auto parser = new_message_parser();
                rpc_server_session_ptr s = new hpc_rpc_server_session(_accept_event.s, parser, *this, client_addr);
                this->on_server_session_accepted(s);
            }
            else
            {
                closesocket(_accept_event.s);
            }

            do_accept();
        }
        
        hpc_rpc_session::hpc_rpc_session(
            SOCKET sock,
            std::shared_ptr<dsn::message_parser>& parser
            )
            : _rw_fd(sock), _parser(parser)
        {

        }

        void hpc_rpc_session::do_read(int sz)
        {
            add_reference();

            _read_event.callback = [this](int err, uint32_t length)
            {
                if (err != ERROR_SUCCESS)
                {
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

                release_reference();
            };
            memset(&_read_event.olp, 0, sizeof(_read_event.olp));

            WSABUF buf[1];

            void* ptr = _parser->read_buffer_ptr((int)sz);
            int remaining = _parser->read_buffer_capacity();
            buf[0].buf = (char*)ptr;
            buf[0].len = remaining;

            BOOL rt = WSARecv(
                _rw_fd,
                buf,
                1,
                NULL,
                NULL,
                &_read_event.olp,
                NULL
                );

            if (!rt && (WSAGetLastError() != ERROR_IO_PENDING))
            {
                dassert(false, "WSARecv failed, err = %d", ::WSAGetLastError());
                release_reference();
                on_failure();
            }
        }

        void hpc_rpc_session::do_write(message_ex* msg)
        {
            add_reference();
            _read_event.callback = [this, msg](int err, uint32_t length)
            {
                if (err != ERROR_SUCCESS)
                {
                    on_failure();
                }
                else
                {
                    on_write_completed(msg);
                }

                release_reference();
            };

            // make sure header is already in the buffer
            std::vector<dsn_message_parser::send_buf> buffers;
            _parser->prepare_buffers_on_send(msg, buffers);

            static_assert (sizeof(dsn_message_parser::send_buf) == sizeof(WSABUF), "make sure they are compatible");

            BOOL rt = WSASend(
                _rw_fd,
                (LPWSABUF)&buffers[0],
                (DWORD)buffers.size(),
                NULL,
                NULL,
                &_read_event.olp,
                NULL
                );

            if (!rt && (WSAGetLastError() != ERROR_IO_PENDING))
            {
                dassert(false, "WSASend failed, err = %d", ::WSAGetLastError());
                release_reference();
                on_failure();
            }
        }

        void hpc_rpc_session::close()
        {
            closesocket(_rw_fd);
            on_closed();
        }

        hpc_rpc_client_session::hpc_rpc_client_session(
            SOCKET sock,
            std::shared_ptr<dsn::message_parser>& parser,
            connection_oriented_network& net,
            const dsn_address_t& remote_addr,
            rpc_client_matcher_ptr& matcher
            )
            : rpc_client_session(net, remote_addr, matcher), hpc_rpc_session(sock, parser), _socket(sock)
        {
            _reconnect_count = 0;
            _state = SS_CLOSED;
        }

        void hpc_rpc_client_session::on_failure()
        {
            _state = SS_CLOSED;

            if (_reconnect_count++ > 3)
            {
                closesocket(_socket);
                on_disconnected();
                return;
            }

            connect();
        }

        void hpc_rpc_client_session::connect()
        {
            session_state closed_state = SS_CLOSED;

            if (!_state.compare_exchange_strong(closed_state, SS_CONNECTING))
                return;

            auto evt = new hpc_network_provider::completion_event;
            evt->callback = [this, evt](int err, uint32_t io_size)
            {
                if (err != ERROR_SUCCESS)
                {
                    this->on_failure();
                }
                else
                {
                    _reconnect_count = 0;
                    _state = SS_CONNECTED;

                    dinfo("client session %s:%hu connected",
                        _remote_addr.name,
                        _remote_addr.port
                        );

                    send_messages();                    
                    do_read();
                }
                this->release_ref(); // added before ConnectEx
                delete evt;
            };

            memset(&evt->olp, 0, sizeof(evt->olp));

            struct sockaddr_in addr;
            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = htonl(_remote_addr.ip);
            addr.sin_port = htons(_remote_addr.port);

            this->add_ref(); // released in evt->callback
            BOOL rt = s_lpfnConnectEx(
                _socket,
                (struct sockaddr*)&addr,
                (int)sizeof(addr),
                nullptr,
                0,
                nullptr,
                &evt->olp
                );

            if (!rt && (WSAGetLastError() != ERROR_IO_PENDING))
            {
                dassert(false, "ConnectEx failed, err = %d", ::WSAGetLastError());
                closesocket(_socket);
                this->release_ref();
                delete evt;

                on_failure();
            }
        }

        hpc_rpc_server_session::hpc_rpc_server_session(
            SOCKET sock,
            std::shared_ptr<dsn::message_parser>& parser,
            connection_oriented_network& net,
            const dsn_address_t& remote_addr
            )
            : rpc_server_session(net, remote_addr), hpc_rpc_session(sock, parser)
        {
        }

    }
}

# endif
