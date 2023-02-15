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

#include "utils/rand.h"
#include <memory>

#include "asio_net_provider.h"
#include "asio_rpc_session.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace tools {

DSN_DEFINE_uint32(network,
                  io_service_worker_count,
                  1,
                  "thread number for io service (timer and boost network)");

const int threads_per_event_loop = 1;

asio_network_provider::asio_network_provider(rpc_engine *srv, network *inner_provider)
    : connection_oriented_network(srv, inner_provider), _acceptor(nullptr)
{
    for (auto i = 0; i < FLAGS_io_service_worker_count; i++) {
        // Using thread-local operation queues in single-threaded use cases (i.e. when
        // concurrency_hint is 1) to eliminate a lock/unlock pair.
        _io_services.emplace_back(
            std::make_unique<boost::asio::io_service>(threads_per_event_loop));
    }
}

asio_network_provider::~asio_network_provider()
{
    if (_acceptor) {
        _acceptor->close();
    }
    for (auto &io_service : _io_services) {
        io_service->stop();
    }

    for (auto &w : _workers) {
        w->join();
    }
}

error_code asio_network_provider::start(rpc_channel channel, int port, bool client_only)
{
    if (_acceptor != nullptr)
        return ERR_SERVICE_ALREADY_RUNNING;

    for (int i = 0; i < FLAGS_io_service_worker_count; i++) {
        _workers.push_back(std::make_shared<std::thread>([this, i]() {
            task::set_tls_dsn_context(node(), nullptr);

            const char *name = ::dsn::tools::get_service_node_name(node());
            char buffer[128];
            sprintf(buffer, "%s.asio.%d", name, i);
            task_worker::set_name(buffer);

            boost::asio::io_service::work work(*_io_services[i]);
            boost::system::error_code ec;
            _io_services[i]->run(ec);
            CHECK(!ec, "boost::asio::io_service run failed: err({})", ec.message());
        }));
    }

    _acceptor = nullptr;

    CHECK(channel == RPC_CHANNEL_TCP || channel == RPC_CHANNEL_UDP,
          "invalid given channel {}",
          channel);

    _address.assign_ipv4(get_local_ipv4(), port);

    if (!client_only) {
        auto v4_addr = boost::asio::ip::address_v4::any(); //(ntohl(_address.ip));
        ::boost::asio::ip::tcp::endpoint endpoint(v4_addr, _address.port());
        boost::system::error_code ec;
        _acceptor.reset(new boost::asio::ip::tcp::acceptor(get_io_service()));
        _acceptor->open(endpoint.protocol(), ec);
        if (ec) {
            LOG_ERROR("asio tcp acceptor open failed, error = {}", ec.message());
            _acceptor.reset();
            return ERR_NETWORK_INIT_FAILED;
        }
        _acceptor->set_option(boost::asio::socket_base::reuse_address(true));
        _acceptor->bind(endpoint, ec);
        if (ec) {
            LOG_ERROR("asio tcp acceptor bind failed, error = {}", ec.message());
            _acceptor.reset();
            return ERR_NETWORK_INIT_FAILED;
        }
        int backlog = boost::asio::socket_base::max_connections;
        _acceptor->listen(backlog, ec);
        if (ec) {
            LOG_ERROR("asio tcp acceptor listen failed, port = {}, error = {}",
                      _address.port(),
                      ec.message());
            _acceptor.reset();
            return ERR_NETWORK_INIT_FAILED;
        }
        do_accept();
    }

    return ERR_OK;
}

rpc_session_ptr asio_network_provider::create_client_session(::dsn::rpc_address server_addr)
{
    auto sock = std::make_shared<boost::asio::ip::tcp::socket>(get_io_service());
    message_parser_ptr parser(new_message_parser(_client_hdr_format));
    return rpc_session_ptr(new asio_rpc_session(*this, server_addr, sock, parser, true));
}

void asio_network_provider::do_accept()
{
    auto socket = std::make_shared<boost::asio::ip::tcp::socket>(get_io_service());

    _acceptor->async_accept(*socket, [this, socket](boost::system::error_code ec) {
        if (!ec) {
            auto remote = socket->remote_endpoint(ec);
            if (ec) {
                LOG_ERROR("failed to get the remote endpoint: {}", ec.message());
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

                // when server connection threshold is hit, close the session, otherwise accept it
                if (check_if_conn_threshold_exceeded(s->remote_address())) {
                    LOG_WARNING("close rpc connection from {} to {} due to hitting server "
                                "connection threshold per ip",
                                s->remote_address(),
                                address());
                    s->close();
                } else {
                    on_server_session_accepted(s);

                    // we should start read immediately after the rpc session is completely created.
                    s->start_read_next();
                }
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
    CHECK_GE(lcount, rcount);

    size_t tlen = 0, offset = 0;
    for (int i = 0; i < rcount; i++) {
        tlen += bufs[i].sz;
    }
    CHECK_LE_MSG(tlen, max_udp_packet_size, "the message is too large to send via a udp channel");

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
                LOG_WARNING("send udp packet to ep {}:{} failed, message = {}",
                            ep.address(),
                            ep.port(),
                            error.message());
                // we do not handle failure here, rpc matcher would handle timeouts
            }
        });
    request->add_ref();
    request->release_ref();
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
    CHECK_GE_MSG(_recv_reader.read_buffer_capacity(),
                 max_udp_packet_size,
                 "failed to load enough buffer in parser");

    _socket->async_receive_from(
        ::boost::asio::buffer(buffer_ptr, max_udp_packet_size),
        *send_endpoint,
        [this, send_endpoint](const boost::system::error_code &error,
                              std::size_t bytes_transferred) {
            if (!!error) {
                LOG_ERROR("{}: asio udp read failed: {}", _address, error.message());
                do_receive();
                return;
            }

            if (bytes_transferred < sizeof(uint32_t)) {
                LOG_ERROR("{}: asio udp read failed: too short message", _address);
                do_receive();
                return;
            }

            auto hdr_format = message_parser::get_header_type(_recv_reader._buffer.data());
            if (NET_HDR_INVALID == hdr_format) {
                LOG_ERROR("{}: asio udp read failed: invalid header type '{}'",
                          _address,
                          message_parser::get_debug_string(_recv_reader._buffer.data()));
                do_receive();
                return;
            }

            auto parser = get_message_parser(hdr_format);
            parser->reset();

            _recv_reader.mark_read(bytes_transferred);

            int read_next = -1;

            message_ex *msg = parser->get_message_on_receive(&_recv_reader, read_next);
            if (msg == nullptr) {
                LOG_ERROR("{}: asio udp read failed: invalid udp packet", _address);
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
    CHECK_EQ(channel, RPC_CHANNEL_UDP);

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
                LOG_ERROR("asio udp socket open failed, error = {}", ec.message());
                _socket.reset();
                continue;
            }
            _socket->bind(endpoint, ec);
            if (ec) {
                LOG_ERROR("asio udp socket bind failed, port = {}, error = {}",
                          _address.port(),
                          ec.message());
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
            LOG_ERROR("asio udp socket open failed, error = {}", ec.message());
            _socket.reset();
            return ERR_NETWORK_INIT_FAILED;
        }
        _socket->bind(endpoint, ec);
        if (ec) {
            LOG_ERROR("asio udp socket bind failed, port = {}, error = {}",
                      _address.port(),
                      ec.message());
            _socket.reset();
            return ERR_NETWORK_INIT_FAILED;
        }
    }

    for (int i = 0; i < FLAGS_io_service_worker_count; i++) {
        _workers.push_back(std::make_shared<std::thread>([this, i]() {
            task::set_tls_dsn_context(node(), nullptr);

            const char *name = ::dsn::tools::get_service_node_name(node());
            char buffer[128];
            sprintf(buffer, "%s.asio.udp.%d.%d", name, (int)(this->address().port()), i);
            task_worker::set_name(buffer);

            boost::asio::io_service::work work(_io_service);
            boost::system::error_code ec;
            _io_service.run(ec);
            CHECK(!ec, "boost::asio::io_service run failed: err({})", ec.message());
        }));
    }

    do_receive();

    return ERR_OK;
}

// use a round-robin scheme to choose the next io_service to use.
boost::asio::io_service &asio_network_provider::get_io_service()
{
    return *_io_services[rand::next_u32(0, FLAGS_io_service_worker_count - 1)];
}

} // namespace tools
} // namespace dsn
