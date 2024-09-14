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

#include <stddef.h>
#include <memory>
#include <thread>
#include <vector>

#include "boost/asio/io_service.hpp"
#include "boost/asio/ip/tcp.hpp"
#include "boost/asio/ip/udp.hpp"
#include "rpc/message_parser.h"
#include "rpc/network.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"
#include "rpc/rpc_message.h"
#include "task/task_spec.h"
#include "utils/error_code.h"
#include "utils/synchronize.h"

namespace dsn {
class rpc_engine;

namespace tools {

/// asio_network_provider is a wrapper of Asio library for rDSN to accept a connection and create
/// sockets. Each io_service only allows one thread polling, so the operations of the single socket
/// are always done in a single thread. we create many io_service instances to take advantage of the
/// multi-core capabilities of the processor, and use the round-robin scheme to decide which
/// io_service for socket to choose.
///
///    +-----------------------------------------------+
///    |Linux kernel                                   |
///    | +-----------+   +-----------+   +-----------+ |
///    | |  Epoll1   |   |   Epoll2  |   |   Epoll3  | |
///    | |           |   |           |   |           | |
///    | | rfd 1,2,3 |   | rfd 4,5,6 |   | rfd 7,8,9 | |
///    | |           |   |           |   |           | |
///    | +-----^-----+   +-----^-----+   +-----^-----+ |
///    +-------|---------------|---------------|-------+
///       +-----------+   +-----------+   +-----------+
///       |  polling  |   |  polling  |   |  polling  |
///       | +-------+ |   | +-------+ |   | +-------+ |
///       | |Thread1| |   | |Thread2| |   | |Thread3| |
///       | +-------+ |   | +-------+ |   | +-------+ |
///       |io_service1|   |io_service2|   |io_service3|
///       +-----------+   +-----------+   +-----------+

class asio_network_provider : public connection_oriented_network
{
public:
    asio_network_provider(rpc_engine *srv, network *inner_provider);

    ~asio_network_provider() override;

    virtual error_code start(rpc_channel channel, int port, bool client_only) override;
    const ::dsn::rpc_address &address() const override { return _address; }
    const ::dsn::host_port &host_port() const override { return _hp; }
    virtual rpc_session_ptr create_client_session(::dsn::rpc_address server_addr) override;

private:
    void do_accept();
    boost::asio::io_service &get_io_service();

private:
    friend class asio_rpc_session;
    friend class asio_network_provider_test;

    std::shared_ptr<boost::asio::ip::tcp::acceptor> _acceptor;
    std::vector<std::unique_ptr<boost::asio::io_service>> _io_services;
    std::vector<std::shared_ptr<std::thread>> _workers;
    ::dsn::rpc_address _address;
    // NOTE: '_hp' is possible to be invalid if '_address' can not be reverse resolved.
    ::dsn::host_port _hp;
};

// TODO(Tangyanzhao): change the network model like asio_network_provider
class asio_udp_provider : public network
{
public:
    asio_udp_provider(rpc_engine *srv, network *inner_provider);

    ~asio_udp_provider() override;

    void send_message(message_ex *request) override;

    virtual error_code start(rpc_channel channel, int port, bool client_only) override;

    const ::dsn::rpc_address &address() const override { return _address; }

    const ::dsn::host_port &host_port() const override { return _hp; }

    virtual void inject_drop_message(message_ex *msg, bool is_send) override
    {
        // nothing to do for UDP
    }

private:
    void do_receive();

    // create parser on demand
    message_parser *get_message_parser(network_header_format hdr_format);

    bool _is_client;
    boost::asio::io_service _io_service;
    std::shared_ptr<boost::asio::ip::udp::socket> _socket;
    std::vector<std::shared_ptr<std::thread>> _workers;
    ::dsn::rpc_address _address;
    ::dsn::host_port _hp;
    message_reader _recv_reader;

    ::dsn::utils::ex_lock_nr _lock; // [
    message_parser **_parsers;
    // ]

    static const size_t max_udp_packet_size;
};

} // namespace tools
} // namespace dsn
