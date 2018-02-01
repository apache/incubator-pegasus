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
 *     Network using kqueue on BSD.
 *
 * Revision history:
 *     2015-09-06, HX Lin(linmajia@live.com), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#if defined(__APPLE__) || defined(__FreeBSD__)

#include "hpc_network_provider.h"
#include "mix_all_io_looper.h"
#include <netinet/tcp.h>

namespace dsn {
namespace tools {
static socket_t create_tcp_socket(sockaddr_in *addr)
{
    socket_t s = -1;
    if ((s = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP)) == -1) {
        dwarn("socket failed, err = %s", strerror(errno));
        return -1;
    }

    int reuse = 1;
    if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (char *)&reuse, sizeof(int)) == -1) {
        dwarn("setsockopt SO_REUSEADDR failed, err = %s", strerror(errno));
    }

    int nodelay = 1;
    if (setsockopt(s, IPPROTO_TCP, TCP_NODELAY, (char *)&nodelay, sizeof(int)) != 0) {
        dwarn("setsockopt TCP_NODELAY failed, err = %s", strerror(errno));
    }

    int buflen = 8 * 1024 * 1024;
    if (setsockopt(s, SOL_SOCKET, SO_SNDBUF, (char *)&buflen, sizeof(buflen)) != 0) {
        dwarn("setsockopt SO_SNDBUF failed, err = %s", strerror(errno));
    }

    buflen = 8 * 1024 * 1024;
    if (setsockopt(s, SOL_SOCKET, SO_RCVBUF, (char *)&buflen, sizeof(buflen)) != 0) {
        dwarn("setsockopt SO_RCVBUF failed, err = %s", strerror(errno));
    }

    int keepalive = 1;
    if (setsockopt(s, SOL_SOCKET, SO_KEEPALIVE, (char *)&keepalive, sizeof(keepalive)) != 0) {
        dwarn("setsockopt SO_KEEPALIVE failed, err = %s", strerror(errno));
    }

    if (addr != 0) {
        if (bind(s, (struct sockaddr *)addr, sizeof(*addr)) != 0) {
            derror("bind failed, err = %s", strerror(errno));
            ::close(s);
            return -1;
        }
    }

    return s;
}

hpc_network_provider::hpc_network_provider(rpc_engine *srv, network *inner_provider)
    : connection_oriented_network(srv, inner_provider)
{
    _listen_fd = -1;
    _looper = nullptr;
    _max_buffer_block_count_per_send = 1; // TODO: after fixing we can increase it
}

error_code
hpc_network_provider::start(rpc_channel channel, int port, bool client_only, io_modifer &ctx)
{
    if (_listen_fd != -1)
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
        if (_listen_fd == -1) {
            dassert(false, "cannot create listen socket");
        }

        int forcereuse = 1;
        if (setsockopt(
                _listen_fd, SOL_SOCKET, SO_REUSEADDR, (char *)&forcereuse, sizeof(forcereuse)) !=
            0) {
            dwarn("setsockopt SO_REUSEDADDR failed, err = %s", strerror(errno));
        }

        if (listen(_listen_fd, SOMAXCONN) != 0) {
            dwarn("listen failed, err = %s", strerror(errno));
            return ERR_NETWORK_START_FAILED;
        }

        _accept_event.callback = [this](int err, uint32_t size, uintptr_t lpolp) {
            this->do_accept();
        };

        // bind for accept
        _looper->bind_io_handle((dsn_handle_t)(intptr_t)_listen_fd,
                                &_accept_event.callback,
                                EVFILT_READ,
                                nullptr // network_provider is a global object
                                );
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
    dassert(sock != -1, "create client tcp socket failed!");
    message_parser_ptr parser(new_message_parser(_client_hdr_format));
    auto client = new hpc_rpc_session(sock, parser, *this, server_addr, true);
    rpc_session_ptr c(client);
    client->bind_looper(_looper, true);
    return c;
}

void hpc_network_provider::do_accept()
{
    while (true) {
        struct sockaddr_in addr;
        socklen_t addr_len = (socklen_t)sizeof(addr);
        socket_t s = ::accept(_listen_fd, (struct sockaddr *)&addr, &addr_len);
        if (s != -1) {
            ::dsn::rpc_address client_addr(ntohl(addr.sin_addr.s_addr), ntohs(addr.sin_port));
            message_parser_ptr null_parser;
            auto rs = new hpc_rpc_session(s, null_parser, *this, client_addr, false);
            rpc_session_ptr s1(rs);

            rs->bind_looper(_looper);
            this->on_server_session_accepted(s1);
        } else {
            if (errno != EAGAIN && errno != EWOULDBLOCK) {
                derror("accept failed, err = %s", strerror(errno));
            }
            break;
        }
    }
}

void hpc_rpc_session::bind_looper(io_looper *looper, bool delay)
{
    _looper = looper;
    if (!delay) {
        // bind for send/recv
        looper->bind_io_handle(
            (dsn_handle_t)(intptr_t)_socket, &_ready_event, EVFILT_READ_WRITE, this);
    }
}

void hpc_rpc_session::do_read(int read_next)
{
    utils::auto_lock<utils::ex_lock_nr> l(_send_lock);

    while (true) {
        char *ptr = _reader.read_buffer_ptr(read_next);
        int remaining = _reader.read_buffer_capacity();

        int length = recv(_socket, ptr, remaining, 0);
        int err = errno;
        dinfo("(s = %d) call recv on %s, return %d, err = %s",
              _socket,
              _remote_addr.to_string(),
              length,
              strerror(err));

        if (length > 0) {
            _reader.mark_read(length);

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
                break;
            }
        } else {
            if (err != EAGAIN && err != EWOULDBLOCK) {
                derror("(s = %d) recv failed on %s, err = %s",
                       _socket,
                       _remote_addr.to_string(),
                       strerror(err));
                on_failure();
            }
            break;
        }
    }
}

void hpc_rpc_session::do_safe_write(uint64_t sig)
{
    utils::auto_lock<utils::ex_lock_nr> l(_send_lock);

    if (0 == sig) {
        if (_sending_signature) {
            do_write(_sending_signature);
        } else {
            _send_lock.unlock(); // avoid recursion
            on_send_completed(); // send next msg if there is.
            _send_lock.lock();
        }
    } else {
        do_write(sig);
    }
}

void hpc_rpc_session::do_write(uint64_t sig)
{
    int flags;

    static_assert(sizeof(dsn_message_parser::send_buf) == sizeof(struct iovec),
                  "make sure they are compatible");

    dbg_dassert(sig != 0, "cannot send empty msg");

    // new msg
    if (_sending_signature == 0) {
        _sending_signature = sig;
        _sending_buffer_start_index = 0;
    }

    // continue old msg
    else {
        dassert(_sending_signature == sig, "only one sending msg is possible");
    }

    flags =
#ifdef __APPLE__
        SO_NOSIGPIPE
#else
        MSG_NOSIGNAL
#endif
        ;

    // prepare send buffer, make sure header is already in the buffer
    while (true) {
        int buffer_count = (int)_sending_buffers.size() - _sending_buffer_start_index;
        struct msghdr hdr;
        memset((void *)&hdr, 0, sizeof(hdr));
        hdr.msg_name = (void *)&_peer_addr;
        hdr.msg_namelen = (socklen_t)sizeof(_peer_addr);
        hdr.msg_iov = (struct iovec *)&_sending_buffers[_sending_buffer_start_index];
        hdr.msg_iovlen = (size_t)buffer_count;

        int sz = sendmsg(_socket, &hdr, flags);
        int err = errno;
        dinfo("(s = %d) call sendmsg on %s, return %d, err = %s",
              _socket,
              _remote_addr.to_string(),
              sz,
              strerror(err));

        if (sz < 0) {
            if (err != EAGAIN && err != EWOULDBLOCK) {
                derror("(s = %d) sendmsg failed, err = %s", _socket, strerror(err));
                on_failure(true);
            } else {
                // wait for epoll_wait notification
            }
            return;
        } else {
            int len = (int)sz;
            int buf_i = _sending_buffer_start_index;
            while (len > 0) {
                auto &buf = _sending_buffers[buf_i];
                if (len >= (int)buf.sz) {
                    buf_i++;
                    len -= (int)buf.sz;
                } else {
                    buf.buf = (char *)buf.buf + len;
                    buf.sz -= len;
                    break;
                }
            }
            _sending_buffer_start_index = buf_i;

            // message completed, continue next message
            if (_sending_buffer_start_index == (int)_sending_buffers.size()) {
                dassert(len == 0, "buffer must be sent completely");
                auto csig = _sending_signature;
                _sending_signature = 0;

                _send_lock.unlock(); // avoid recursion
                // try next msg recursively
                on_send_completed(csig);
                _send_lock.lock();
                return;
            }

            else {
                // try next while(true) loop to continue sending current msg
            }
        }
    }
}

void hpc_rpc_session::close()
{
    if (-1 != _socket) {
        ::close(_socket);
        dinfo("(s = %d) close socket %p", _socket, this);
        _socket = -1;
    }
}

void hpc_rpc_session::on_send_recv_events_ready(uintptr_t lolp_or_events)
{
    struct kevent &e = *((struct kevent *)lolp_or_events);
    // shutdown or send/recv error
    if (((e.flags & EV_ERROR) != 0) || ((e.flags & EV_EOF) != 0)) {
        dinfo("(s = %d) kevent failure on %s, events = 0x%x",
              _socket,
              _remote_addr.to_string(),
              e.filter);
        on_failure();
        return;
    }

    //  send
    if (e.filter == EVFILT_WRITE) {
        dinfo("(s = %d) kevent EVFILT_WRITE on %s, events = 0x%x",
              _socket,
              _remote_addr.to_string(),
              e.filter);

        do_safe_write(0);
    }

    // recv
    if (e.filter == EVFILT_READ) {
        dinfo("(s = %d) kevent EVFILT_READ on %s, events = 0x%x",
              _socket,
              _remote_addr.to_string(),
              e.filter);

        start_read_next();
    }
}

// client
hpc_rpc_session::hpc_rpc_session(socket_t sock,
                                 message_parser_ptr &parser,
                                 connection_oriented_network &net,
                                 ::dsn::rpc_address remote_addr,
                                 bool is_client)
    : rpc_session(net, remote_addr, parser, is_client), _socket(sock)
{
    dassert(sock != -1, "invalid given socket handle");
    _sending_signature = 0;
    _sending_buffer_start_index = 0;
    _looper = nullptr;

    memset((void *)&_peer_addr, 0, sizeof(_peer_addr));
    _peer_addr.sin_family = AF_INET;
    _peer_addr.sin_addr.s_addr = INADDR_ANY;
    _peer_addr.sin_port = 0;

    if (is_client) {
        _ready_event = [this](int err, uint32_t length, uintptr_t lolp_or_events) {
            if (is_connecting())
                this->on_connect_events_ready(lolp_or_events);
            else
                this->on_send_recv_events_ready(lolp_or_events);
        };
    } else {
        socklen_t addr_len = (socklen_t)sizeof(_peer_addr);
        if (getpeername(_socket, (struct sockaddr *)&_peer_addr, &addr_len) == -1) {
            derror("(server) getpeername failed, err = %s", strerror(errno));
        }

        _ready_event = [this](int err, uint32_t length, uintptr_t lolp_or_events) {
            struct kevent &e = *((struct kevent *)lolp_or_events);
            dinfo("(s = %d) (server) epoll for send/recv to %s, events = %d",
                  _socket,
                  _remote_addr.to_string(),
                  e.filter);
            this->on_send_recv_events_ready(lolp_or_events);
        };
    }
}

void hpc_rpc_session::on_connect_events_ready(uintptr_t lolp_or_events)
{
    dassert(is_connecting(), "session must be connecting at this time");

    struct kevent &e = *((struct kevent *)lolp_or_events);
    dinfo("(s = %d) epoll for connect to %s, events = 0x%x",
          _socket,
          _remote_addr.to_string(),
          e.filter);

    if ((e.filter == EVFILT_WRITE) && ((e.flags & EV_ERROR) == 0) && ((e.flags & EV_EOF) == 0)) {
        socklen_t addr_len = (socklen_t)sizeof(_peer_addr);
        if (getpeername(_socket, (struct sockaddr *)&_peer_addr, &addr_len) == -1) {
            derror("(s = %d) (client) getpeername failed, err = %s", _socket, strerror(errno));
            on_failure();
            return;
        }

        dinfo("(s = %d) client session %s connected", _socket, _remote_addr.to_string());

        set_connected();

        if (_looper->bind_io_handle(
                (dsn_handle_t)(intptr_t)_socket, &_ready_event, EVFILT_READ_WRITE) != ERR_OK) {
            on_failure();
            return;
        }

        // start first round send
        do_safe_write(0);
    } else {
        int err = 0;
        socklen_t err_len = (socklen_t)sizeof(err);

        if (getsockopt(_socket, SOL_SOCKET, SO_ERROR, (void *)&err, &err_len) < 0) {
            derror("getsockopt for SO_ERROR failed, err = %s", strerror(errno));
        }

        derror("(s = %d) connect failed (in epoll), err = %s", _socket, strerror(err));
        on_failure(true);
    }
}

void hpc_rpc_session::on_failure(bool is_write)
{
    if (_socket != -1) {
        _looper->unbind_io_handle((dsn_handle_t)(intptr_t)_socket, &_ready_event);
    }
    if (on_disconnected(is_write))
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

    int rt = ::connect(_socket, (struct sockaddr *)&addr, (int)sizeof(addr));
    int err = errno;
    dinfo("(s = %d) call connect to %s, return %d, err = %s",
          _socket,
          _remote_addr.to_string(),
          rt,
          strerror(err));

    if (rt == -1 && err != EINPROGRESS) {
        dwarn("(s = %d) connect failed, err = %s", _socket, strerror(err));
        on_failure(true);
        return;
    }

    // bind for connect
    _looper->bind_io_handle((dsn_handle_t)(intptr_t)_socket, &_ready_event, EVFILT_WRITE, this);
}
}
}

#endif
