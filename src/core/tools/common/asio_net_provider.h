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

#include <dsn/tool_api.h>
#include <boost/asio.hpp>

namespace dsn {
namespace tools {

class asio_network_provider : public connection_oriented_network
{
public:
    asio_network_provider(rpc_engine *srv, network *inner_provider);

    ~asio_network_provider() override;

    virtual error_code start(rpc_channel channel, int port, bool client_only) override;
    virtual ::dsn::rpc_address address() override { return _address; }
    virtual rpc_session_ptr create_client_session(::dsn::rpc_address server_addr) override;

private:
    void do_accept();

private:
    friend class asio_rpc_session;

    std::shared_ptr<boost::asio::ip::tcp::acceptor> _acceptor;
    boost::asio::io_service _io_service;
    std::vector<std::shared_ptr<std::thread>> _workers;
    ::dsn::rpc_address _address;
};

class asio_udp_provider : public network
{
public:
    asio_udp_provider(rpc_engine *srv, network *inner_provider);

    ~asio_udp_provider() override;

    void send_message(message_ex *request) override;

    virtual error_code start(rpc_channel channel, int port, bool client_only) override;

    virtual ::dsn::rpc_address address() override { return _address; }

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
    message_reader _recv_reader;

    ::dsn::utils::ex_lock_nr _lock; // [
    message_parser **_parsers;
    // ]

    static const size_t max_udp_packet_size = 1000;
};

} // namespace tools
} // namespace dsn
