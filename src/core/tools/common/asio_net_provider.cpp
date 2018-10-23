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

#include <dsn/utility/rand.h>

#include "asio_net_provider.h"
#include "asio_rpc_session.h"

namespace dsn {
namespace tools {

asio_network_provider::asio_network_provider(rpc_engine *srv, network *inner_provider)
    : connection_oriented_network(srv, inner_provider)
{
    _acceptor = nullptr;
}

asio_network_provider::~asio_network_provider()
{
    if (_acceptor) {
        _acceptor->close();
    }
    _io_service.stop();
    for (auto &w : _workers) {
        w->join();
    }
}

error_code asio_network_provider::start(rpc_channel channel, int port, bool client_only)
{
    if (_acceptor != nullptr)
        return ERR_SERVICE_ALREADY_RUNNING;

    int io_service_worker_count =
        (int)dsn_config_get_value_uint64("network",
                                         "io_service_worker_count",
                                         1,
                                         "thread number for io service (timer and boost network)");
    for (int i = 0; i < io_service_worker_count; i++) {
        _workers.push_back(std::make_shared<std::thread>([this, i]() {
            task::set_tls_dsn_context(node(), nullptr);

            const char *name = ::dsn::tools::get_service_node_name(node());
            char buffer[128];
            sprintf(buffer, "%s.asio.%d", name, i);
            task_worker::set_name(buffer);

            boost::asio::io_service::work work(_io_service);
            boost::system::error_code ec;
            _io_service.run(ec);
            if (ec) {
                dassert(false, "boost::asio::io_service run failed: err(%s)", ec.message().data());
            }
        }));
    }

    _acceptor = nullptr;

    dassert(channel == RPC_CHANNEL_TCP || channel == RPC_CHANNEL_UDP,
            "invalid given channel %s",
            channel.to_string());

    _address.assign_ipv4(get_local_ipv4(), port);

    if (!client_only) {
        auto v4_addr = boost::asio::ip::address_v4::any(); //(ntohl(_address.ip));
        ::boost::asio::ip::tcp::endpoint endpoint(v4_addr, _address.port());
        boost::system::error_code ec;
        _acceptor.reset(new boost::asio::ip::tcp::acceptor(_io_service));
        _acceptor->open(endpoint.protocol(), ec);
        if (ec) {
            derror("asio tcp acceptor open failed, error = %s", ec.message().c_str());
            _acceptor.reset();
            return ERR_NETWORK_INIT_FAILED;
        }
        _acceptor->set_option(boost::asio::socket_base::reuse_address(true));
        _acceptor->bind(endpoint, ec);
        if (ec) {
            derror("asio tcp acceptor bind failed, error = %s", ec.message().c_str());
            _acceptor.reset();
            return ERR_NETWORK_INIT_FAILED;
        }
        int backlog = boost::asio::socket_base::max_connections;
        _acceptor->listen(backlog, ec);
        if (ec) {
            derror("asio tcp acceptor listen failed, port = %u, error = %s",
                   _address.port(),
                   ec.message().c_str());
            _acceptor.reset();
            return ERR_NETWORK_INIT_FAILED;
        }
        do_accept();
    }

    return ERR_OK;
}

rpc_session_ptr asio_network_provider::create_client_session(::dsn::rpc_address server_addr)
{
    auto sock = std::shared_ptr<boost::asio::ip::tcp::socket>(
        new boost::asio::ip::tcp::socket(_io_service));
    message_parser_ptr parser(new_message_parser(_client_hdr_format));
    return rpc_session_ptr(new asio_rpc_session(*this, server_addr, sock, parser, true));
}

void asio_network_provider::do_accept()
{
    auto socket = std::shared_ptr<boost::asio::ip::tcp::socket>(
        new boost::asio::ip::tcp::socket(_io_service));

    _acceptor->async_accept(*socket, [this, socket](boost::system::error_code ec) {
        if (!ec) {
            auto remote = socket->remote_endpoint(ec);
            if (ec) {
                derror("failed to get the remote endpoint: %s", ec.message().data());
            } else {
                auto ip = remote.address().to_v4().to_ulong();
                auto port = remote.port();
                ::dsn::rpc_address client_addr(ip, port);

                message_parser_ptr null_parser;
                rpc_session_ptr s =
                    new asio_rpc_session(*this,
                                         client_addr,
                                         (std::shared_ptr<boost::asio::ip::tcp::socket> &)socket,
                                         null_parser,
                                         false);
                on_server_session_accepted(s);

                // we should start read immediately after the rpc session is completely created.
                s->start_read_next();
            }
        }

        do_accept();
    });
}

void asio_udp_provider::send_message(message_ex *request)
{
    auto parser = get_message_parser(request->hdr_format);
    parser->prepare_on_send(request);
    auto lcount = parser->get_buffer_count_on_send(request);
    std::unique_ptr<message_parser::send_buf[]> bufs(new message_parser::send_buf[lcount]);
    auto rcount = parser->get_buffers_on_send(request, bufs.get());
    dassert(lcount >= rcount, "%d VS %d", lcount, rcount);

    size_t tlen = 0, offset = 0;
    for (int i = 0; i < rcount; i++) {
        tlen += bufs[i].sz;
    }
    dassert(tlen <= max_udp_packet_size, "the message is too large to send via a udp channel");

    std::unique_ptr<char[]> packet_buffer(new char[tlen]);
    for (int i = 0; i < rcount; i++) {
        memcpy(&packet_buffer[offset], bufs[i].buf, bufs[i].sz);
        offset += bufs[i].sz;
    };

    ::boost::asio::ip::udp::endpoint ep(::boost::asio::ip::address_v4(request->to_address.ip()),
                                        request->to_address.port());
    _socket->async_send_to(
        ::boost::asio::buffer(packet_buffer.get(), tlen),
        ep,
        [=](const boost::system::error_code &error, std::size_t bytes_transferred) {
            if (error) {
                dwarn("send udp packet to ep %s:%d failed, message = %s",
                      ep.address().to_string().c_str(),
                      ep.port(),
                      error.message().c_str());
                // we do not handle failure here, rpc matcher would handle timeouts
            }
        });
}

asio_udp_provider::asio_udp_provider(rpc_engine *srv, network *inner_provider)
    : network(srv, inner_provider), _is_client(false), _recv_reader(_message_buffer_block_size)
{
    _parsers = new message_parser *[network_header_format::max_value() + 1];
    memset(_parsers, 0, sizeof(message_parser *) * (network_header_format::max_value() + 1));
}

asio_udp_provider::~asio_udp_provider()
{
    for (int i = 0; i <= network_header_format::max_value(); i++) {
        if (_parsers[i] != nullptr) {
            delete _parsers[i];
            _parsers[i] = nullptr;
        }
    }
    delete[] _parsers;
    _parsers = nullptr;

    _io_service.stop();
    for (auto &w : _workers) {
        w->join();
    }
}

message_parser *asio_udp_provider::get_message_parser(network_header_format hdr_format)
{
    if (_parsers[hdr_format] == nullptr) {
        utils::auto_lock<utils::ex_lock_nr> l(_lock);
        if (_parsers[hdr_format] == nullptr) // double check
        {
            _parsers[hdr_format] = new_message_parser(hdr_format);
        }
    }
    return _parsers[hdr_format];
}

void asio_udp_provider::do_receive()
{
    std::shared_ptr<::boost::asio::ip::udp::endpoint> send_endpoint(
        new ::boost::asio::ip::udp::endpoint);

    _recv_reader.truncate_read();
    auto buffer_ptr = _recv_reader.read_buffer_ptr(max_udp_packet_size);
    dassert(_recv_reader.read_buffer_capacity() >= max_udp_packet_size,
            "failed to load enough buffer in parser");

    _socket->async_receive_from(
        ::boost::asio::buffer(buffer_ptr, max_udp_packet_size),
        *send_endpoint,
        [this, send_endpoint](const boost::system::error_code &error,
                              std::size_t bytes_transferred) {
            if (!!error) {
                derror(
                    "%s: asio udp read failed: %s", _address.to_string(), error.message().c_str());
                do_receive();
                return;
            }

            if (bytes_transferred < sizeof(uint32_t)) {
                derror("%s: asio udp read failed: too short message", _address.to_string());
                do_receive();
                return;
            }

            auto hdr_format = message_parser::get_header_type(_recv_reader._buffer.data());
            if (NET_HDR_INVALID == hdr_format) {
                derror("%s: asio udp read failed: invalid header type '%s'",
                       _address.to_string(),
                       message_parser::get_debug_string(_recv_reader._buffer.data()).c_str());
                do_receive();
                return;
            }

            auto parser = get_message_parser(hdr_format);
            parser->reset();

            _recv_reader.mark_read(bytes_transferred);

            int read_next = -1;

            message_ex *msg = parser->get_message_on_receive(&_recv_reader, read_next);
            if (msg == nullptr) {
                derror("%s: asio udp read failed: invalid udp packet", _address.to_string());
                do_receive();
                return;
            }

            msg->to_address = _address;
            if (msg->header->context.u.is_request) {
                on_recv_request(msg, 0);
            } else {
                on_recv_reply(msg->header->id, msg, 0);
            }

            do_receive();
        });
}

error_code asio_udp_provider::start(rpc_channel channel, int port, bool client_only)
{
    _is_client = client_only;
    int io_service_worker_count =
        (int)dsn_config_get_value_uint64("network",
                                         "io_service_worker_count",
                                         1,
                                         "thread number for io service (timer and boost network)");

    dassert(channel == RPC_CHANNEL_UDP, "invalid given channel %s", channel.to_string());

    if (client_only) {
        do {
            // FIXME: we actually do not need to set a random port for client if the rpc_engine is
            // refactored
            _address.assign_ipv4(get_local_ipv4(),
                                 std::numeric_limits<uint16_t>::max() -
                                     rand::next_u64(std::numeric_limits<uint64_t>::min(),
                                                    std::numeric_limits<uint64_t>::max()) %
                                         5000);
            ::boost::asio::ip::udp::endpoint endpoint(boost::asio::ip::address_v4::any(),
                                                      _address.port());
            boost::system::error_code ec;
            _socket.reset(new ::boost::asio::ip::udp::socket(_io_service));
            _socket->open(endpoint.protocol(), ec);
            if (ec) {
                dwarn("asio udp socket open failed, error = %s", ec.message().c_str());
                _socket.reset();
                continue;
            }
            _socket->bind(endpoint, ec);
            if (ec) {
                dwarn("asio udp socket bind failed, port = %u, error = %s",
                      _address.port(),
                      ec.message().c_str());
                _socket.reset();
                continue;
            }
            break;
        } while (true);
    } else {
        _address.assign_ipv4(get_local_ipv4(), port);
        ::boost::asio::ip::udp::endpoint endpoint(boost::asio::ip::address_v4::any(),
                                                  _address.port());
        boost::system::error_code ec;
        _socket.reset(new ::boost::asio::ip::udp::socket(_io_service));
        _socket->open(endpoint.protocol(), ec);
        if (ec) {
            dwarn("asio udp socket open failed, error = %s", ec.message().c_str());
            _socket.reset();
            return ERR_NETWORK_INIT_FAILED;
        }
        _socket->bind(endpoint, ec);
        if (ec) {
            dwarn("asio udp socket bind failed, port = %u, error = %s",
                  _address.port(),
                  ec.message().c_str());
            _socket.reset();
            return ERR_NETWORK_INIT_FAILED;
        }
    }

    for (int i = 0; i < io_service_worker_count; i++) {
        _workers.push_back(std::make_shared<std::thread>([this, i]() {
            task::set_tls_dsn_context(node(), nullptr);

            const char *name = ::dsn::tools::get_service_node_name(node());
            char buffer[128];
            sprintf(buffer, "%s.asio.udp.%d.%d", name, (int)(this->address().port()), i);
            task_worker::set_name(buffer);

            boost::asio::io_service::work work(_io_service);
            boost::system::error_code ec;
            _io_service.run(ec);
            if (ec) {
                dassert(false, "boost::asio::io_service run failed: err(%s)", ec.message().data());
            }
        }));
    }

    do_receive();

    return ERR_OK;
}
} // namespace tools
} // namespace dsn
