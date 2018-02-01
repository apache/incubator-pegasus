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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#ifdef _WIN32

#include "hpc_network_provider.h"
#include <MSWSock.h>
#include "mix_all_io_looper.h"

namespace dsn {
namespace tools {
static socket_t create_tcp_socket(sockaddr_in *addr)
{
    socket_t s = INVALID_SOCKET;
    if ((s = WSASocket(AF_INET, SOCK_STREAM, IPPROTO_TCP, 0, 0, WSA_FLAG_OVERLAPPED)) ==
        INVALID_SOCKET) {
        dwarn("WSASocket failed, err = %d", ::GetLastError());
        return INVALID_SOCKET;
    }

    int reuse = 1;
    if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse, sizeof(int)) == -1) {
        dwarn("setsockopt SO_REUSEADDR failed, err = %s", strerror(errno));
    }

    int nodelay = 1;
    if (setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (char *)&nodelay, sizeof(int)) != 0) {
        dwarn("setsockopt TCP_NODELAY failed, err = %d", ::GetLastError());
    }

    int isopt = 1;
    if (setsockopt(s, SOL_SOCKET, SO_DONTLINGER, (char *)&isopt, sizeof(int)) != 0) {
        dwarn("setsockopt SO_DONTLINGER failed, err = %d", ::GetLastError());
    }

    int buflen = 8 * 1024 * 1024;
    if (setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char *)&buflen, sizeof(buflen)) != 0) {
        dwarn("setsockopt SO_SNDBUF failed, err = %d", ::GetLastError());
    }

    buflen = 8 * 1024 * 1024;
    if (setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char *)&buflen, sizeof(buflen)) != 0) {
        dwarn("setsockopt SO_RCVBUF failed, err = %d", ::GetLastError());
    }

    int keepalive = 1;
    if (setsockopt(s, SOL_SOCKET, SO_KEEPALIVE, (char *)&keepalive, sizeof(keepalive)) != 0) {
        dwarn("setsockopt SO_KEEPALIVE failed, err = %d", ::GetLastError());
    }

    if (addr != 0) {
        if (bind(s, (struct sockaddr *)addr, sizeof(*addr)) != 0) {
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
    if (s == INVALID_SOCKET) {
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
    if (rt == SOCKET_ERROR) {
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
    if (rt == SOCKET_ERROR) {
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
    if (rt == SOCKET_ERROR) {
        dwarn("WSAIoctl for GetAcceptExSockaddrs failed, err = %d", ::WSAGetLastError());
        closesocket(s);
        return;
    }

    closesocket(s);
}

hpc_network_provider::hpc_network_provider(rpc_engine *srv, network *inner_provider)
    : connection_oriented_network(srv, inner_provider)
{
    load_socket_functions();
    _listen_fd = INVALID_SOCKET;
    _looper = nullptr;
    _max_buffer_block_count_per_send = 64;
}

error_code
hpc_network_provider::start(rpc_channel channel, int port, bool client_only, io_modifer &ctx)
{
    if (_listen_fd != INVALID_SOCKET)
        return ERR_SERVICE_ALREADY_RUNNING;

    _looper = get_io_looper(node(), ctx.queue, ctx.mode);

    dassert(channel == RPC_CHANNEL_TCP || channel == RPC_CHANNEL_UDP,
            "invalid given channel %s",
            channel.to_string());

    _address.assign_ipv4(get_local_ipv4(), port);

    if (!client_only) {
        struct sockaddr_in addr;
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = INADDR_ANY;
        addr.sin_port = htons(port);

        _listen_fd = create_tcp_socket(&addr);
        if (_listen_fd == INVALID_SOCKET) {
            dassert(false, "");
        }

        int forcereuse = 1;
        if (setsockopt(
                _listen_fd, SOL_SOCKET, SO_REUSEADDR, (char *)&forcereuse, sizeof(forcereuse)) !=
            0) {
            dwarn("setsockopt SO_REUSEDADDR failed, err = %d", ::GetLastError());
        }

        _looper->bind_io_handle((dsn_handle_t)_listen_fd, &_accept_event.callback);

        if (listen(_listen_fd, SOMAXCONN) != 0) {
            dwarn("listen failed, err = %d", ::GetLastError());
            return ERR_NETWORK_START_FAILED;
        }

        do_accept();
    }

    return ERR_OK;
}

rpc_session_ptr hpc_network_provider::create_client_session(::dsn::rpc_address server_addr)
{
    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = 0;

    auto sock = create_tcp_socket(&addr);
    message_parser_ptr parser(new_message_parser(_client_hdr_format));
    auto client = new hpc_rpc_session(sock, parser, *this, server_addr, true);
    rpc_session_ptr c(client);
    client->bind_looper(_looper);
    return c;
}

void hpc_network_provider::do_accept()
{
    socket_t s = create_tcp_socket(nullptr);
    dassert(s != INVALID_SOCKET, "cannot create socket for accept");

    _accept_sock = s;
    _accept_event.callback = [this](int err, uint32_t size, uintptr_t lpolp) {
        // dinfo("accept completed, err = %d, size = %u", err, size);
        dassert(&_accept_event.olp == (LPOVERLAPPED)lpolp, "must be this exact overlap");
        if (err == ERROR_SUCCESS) {
            setsockopt(_accept_sock,
                       SOL_SOCKET,
                       SO_UPDATE_ACCEPT_CONTEXT,
                       (char *)&_listen_fd,
                       sizeof(_listen_fd));

            struct sockaddr_in addr;
            memset((void *)&addr, 0, sizeof(addr));

            addr.sin_family = AF_INET;
            addr.sin_addr.s_addr = INADDR_ANY;
            addr.sin_port = 0;

            int addr_len = sizeof(addr);
            if (getpeername(_accept_sock, (struct sockaddr *)&addr, &addr_len) == SOCKET_ERROR) {
                dassert(false, "getpeername failed, err = %d", ::WSAGetLastError());
            }

            ::dsn::rpc_address client_addr(ntohl(addr.sin_addr.s_addr), ntohs(addr.sin_port));
            message_parser_ptr null_parser;
            auto s = new hpc_rpc_session(_accept_sock, null_parser, *this, client_addr, false);
            rpc_session_ptr s1(s);
            s->bind_looper(_looper);

            this->on_server_session_accepted(s1);

            s->start_read_next();
        } else {
            closesocket(_accept_sock);
        }

        do_accept();
    };
    memset(&_accept_event.olp, 0, sizeof(_accept_event.olp));

    DWORD bytes;
    BOOL rt = s_lpfnAcceptEx(_listen_fd,
                             s,
                             _accept_buffer,
                             0,
                             (sizeof(struct sockaddr_in) + 16),
                             (sizeof(struct sockaddr_in) + 16),
                             &bytes,
                             &_accept_event.olp);

    if (!rt && (WSAGetLastError() != ERROR_IO_PENDING)) {
        dassert(false, "AcceptEx failed, err = %d", ::WSAGetLastError());
        closesocket(s);
    }
}

io_loop_callback s_ready_event = [](int err, uint32_t length, uintptr_t lolp) {
    auto evt = CONTAINING_RECORD(lolp, hpc_network_provider::ready_event, olp);
    evt->callback(err, length, lolp);
};

void hpc_rpc_session::bind_looper(io_looper *looper, bool delay)
{
    looper->bind_io_handle((dsn_handle_t)_socket, &s_ready_event);
}

void hpc_rpc_session::do_read(int read_next)
{
    add_ref();
    _read_event.callback = [this](int err, uint32_t length, uintptr_t lolp) {
        // dinfo("WSARecv completed, err = %d, size = %u", err, length);
        dassert((LPOVERLAPPED)lolp == &_read_event.olp, "must be exact this overlapped");
        if (err != ERROR_SUCCESS) {
            dwarn("WSARecv failed, err = %d", err);
            on_failure();
        } else {
            _reader.mark_read(length);

            int read_next = -1;

            if (!_parser) {
                read_next = prepare_parser();
            }

            if (_parser) {
                message_ex *msg = _parser->get_message_on_receive(&_reader, read_next);

                while (msg != nullptr) {
                    this->on_read_completed(msg);
                    msg = _parser->get_message_on_receive(&_reader, read_next);
                }
            }

            if (read_next == -1) {
                derror(
                    "(s = %d) recv failed on %s, parse failed", _socket, _remote_addr.to_string());
                on_failure();
            } else {
                start_read_next(read_next);
            }
        }

        release_ref();
    };
    memset(&_read_event.olp, 0, sizeof(_read_event.olp));

    WSABUF buf[1];

    void *ptr = _reader.read_buffer_ptr(read_next);
    int remaining = _reader.read_buffer_capacity();
    buf[0].buf = (char *)ptr;
    buf[0].len = remaining;

    DWORD bytes = 0;
    DWORD flag = 0;
    int rt = WSARecv(_socket, buf, 1, &bytes, &flag, &_read_event.olp, NULL);

    if (SOCKET_ERROR == rt && (WSAGetLastError() != ERROR_IO_PENDING)) {
        dwarn("WSARecv failed, err = %d", ::WSAGetLastError());
        release_ref();
        on_failure();
    }

    // dinfo("WSARecv called, err = %d", rt);
}

void hpc_rpc_session::do_write(uint64_t sig)
{
    add_ref();

    _write_event.callback = [this](int err, uint32_t length, uintptr_t lolp) {
        dassert((LPOVERLAPPED)lolp == &_write_event.olp, "must be exact this overlapped");
        if (err != ERROR_SUCCESS) {
            dwarn("WSASend failed, err = %d", err);
            on_failure(true);
        } else {
            int len = (int)length;
            int buf_i = _sending_buffer_start_index;
            while (len > 0) {
                auto &buf = _sending_buffers[buf_i];
                if (len >= (int)buf.sz) {
                    buf_i++;
                    len -= (int)buf.sz;
                } else {
                    buf.buf = (char *)buf.buf + len;
                    buf.sz -= (uint32_t)len;
                    break;
                }
            }
            _sending_buffer_start_index = buf_i;

            // message completed, continue next message
            if (_sending_buffer_start_index == (int)_sending_buffers.size()) {
                dassert(len == 0, "buffer must be sent completely");
                auto sig = _sending_signature;
                _sending_signature = 0;
                on_send_completed(sig);
            } else
                do_write(_sending_signature);
        }

        release_ref();
    };
    memset(&_write_event.olp, 0, sizeof(_write_event.olp));

    // new msg
    if (_sending_signature != sig) {
        dassert(_sending_signature == 0, "only one sending msg is possible");
        _sending_signature = sig;
        _sending_buffer_start_index = 0;
    }

    // continue old msg
    else {
        dassert(_sending_signature == sig, "only one sending msg is possible");
    }

    int buffer_count = (int)_sending_buffers.size() - _sending_buffer_start_index;
    static_assert(sizeof(message_parser::send_buf) == sizeof(WSABUF),
                  "make sure they are compatible");

    DWORD bytes = 0;
    int rt = WSASend(_socket,
                     (LPWSABUF)&_sending_buffers[_sending_buffer_start_index],
                     (DWORD)buffer_count,
                     &bytes,
                     0,
                     &_write_event.olp,
                     NULL);

    if (SOCKET_ERROR == rt && (WSAGetLastError() != ERROR_IO_PENDING)) {
        dwarn("WSASend failed, err = %d", ::WSAGetLastError());
        release_ref();
        on_failure(true);
    }

    // dinfo("WSASend called, err = %d", rt);
}

void hpc_rpc_session::close()
{
    if (0 != _socket) {
        closesocket(_socket);
        _socket = 0;
    }
}

hpc_rpc_session::hpc_rpc_session(socket_t sock,
                                 message_parser_ptr &parser,
                                 connection_oriented_network &net,
                                 ::dsn::rpc_address remote_addr,
                                 bool is_client)
    : rpc_session(net, remote_addr, parser, is_client), _socket(sock)
{
    _sending_signature = 0;
    _sending_buffer_start_index = 0;
}

void hpc_rpc_session::on_failure(bool is_write)
{
    if (on_disconnected(is_write))
        close();
}

void hpc_rpc_session::connect()
{
    if (!try_connecting())
        return;

    _connect_event.callback = [this](int err, uint32_t io_size, uintptr_t lpolp) {
        // dinfo("ConnectEx completed, err = %d, size = %u", err, io_size);
        if (err != ERROR_SUCCESS) {
            dwarn("ConnectEx failed, err = %d", err);
            this->on_failure(true);
        } else {
            dinfo("client session %s connected", _remote_addr.to_string());

            set_connected();
            on_send_completed();
            start_read_next();
        }
        this->release_ref(); // added before ConnectEx
    };
    memset(&_connect_event.olp, 0, sizeof(_connect_event.olp));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(_remote_addr.ip());
    addr.sin_port = htons(_remote_addr.port());

    this->add_ref(); // released in _connect_event.callback
    BOOL rt = s_lpfnConnectEx(
        _socket, (struct sockaddr *)&addr, (int)sizeof(addr), 0, 0, 0, &_connect_event.olp);

    if (!rt && (WSAGetLastError() != ERROR_IO_PENDING)) {
        dwarn("ConnectEx failed, err = %d", ::WSAGetLastError());
        this->release_ref();

        on_failure(true);
    }
}
}
}

#endif
