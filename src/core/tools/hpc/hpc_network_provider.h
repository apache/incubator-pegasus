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

#pragma once

#ifdef _WIN32
#include <Winsock2.h>
typedef SOCKET socket_t;
#else
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <arpa/inet.h>
typedef int socket_t;
#if defined(__FreeBSD__)
#include <netinet/in.h>
#endif
#endif

#include <dsn/tool_api.h>
#include "io_looper.h"

namespace dsn {
namespace tools {

class hpc_network_provider : public connection_oriented_network
{
public:
    hpc_network_provider(rpc_engine *srv, network *inner_provider);

    virtual error_code start(rpc_channel channel, int port, bool client_only, io_modifer &ctx);
    virtual ::dsn::rpc_address address() { return _address; }
    virtual rpc_session_ptr create_client_session(::dsn::rpc_address server_addr);

private:
    socket_t _listen_fd;
    ::dsn::rpc_address _address;
    io_looper *_looper;

private:
    void do_accept();

public:
    struct ready_event
    {
#ifdef _WIN32
        OVERLAPPED olp;
#endif
        io_loop_callback callback;
    };

private:
    ready_event _accept_event;
#ifdef _WIN32
    socket_t _accept_sock;
    char _accept_buffer[1024];
#endif
};

class hpc_rpc_session : public rpc_session
{
public:
    virtual void connect() override;
    virtual void close() override;

public:
    hpc_rpc_session(socket_t sock,
                    message_parser_ptr &parser,
                    connection_oriented_network &net,
                    ::dsn::rpc_address remote_addr,
                    bool is_client);

    virtual void send(uint64_t signature) override
    {
#ifdef _WIN32
        do_write(signature);
#else
        do_safe_write(signature);
#endif
    }

    void bind_looper(io_looper *looper, bool delay = false);
    virtual void do_read(int read_next) override;

private:
    void do_write(uint64_t signature);
    void on_failure(bool is_write = false);
    void on_read_completed(message_ex *msg)
    {
        if (!on_recv_message(msg, 0)) {
            on_failure(false);
        }
    }

protected:
    socket_t _socket;
    uint64_t _sending_signature;
    int _sending_buffer_start_index;

#ifdef _WIN32
    hpc_network_provider::ready_event _read_event;
    hpc_network_provider::ready_event _write_event;
    hpc_network_provider::ready_event _connect_event;
#else
    io_loop_callback _ready_event;
    struct sockaddr_in _peer_addr;
    io_looper *_looper;

    // due to the bad design of EPOLLET, we need to
    // use locks to avoid concurrent send/recv
    ::dsn::utils::ex_lock_nr _send_lock;
    ::dsn::utils::ex_lock_nr _recv_lock;

    void on_connect_events_ready(uintptr_t lolp_or_events);
    void on_send_recv_events_ready(uintptr_t lolp_or_events);
    void do_safe_write(uint64_t signature);
#endif
};
}
}
