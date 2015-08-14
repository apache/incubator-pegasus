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
#include "net_provider.h"
#include "net_client_session.h"
#include "net_server_session.h"

namespace dsn {
    namespace tools{

        asio_network_provider::asio_network_provider(rpc_engine* srv, network* inner_provider)
            : connection_oriented_network(srv, inner_provider)
        {
            int io_service_worker_count = config()->get_value<int>("network", "io_service_worker_count", 1,
                "thread number for io service (timer and boost network)");
            for (int i = 0; i < io_service_worker_count; i++)
            {
                _workers.push_back(std::shared_ptr<std::thread>(new std::thread([this]()
                {
                    task::set_current_worker(nullptr, node());

                    boost::asio::io_service::work work(_io_service);
                    _io_service.run();
                })));
            }

            _acceptor = nullptr;
            _socket.reset(new boost::asio::ip::tcp::socket(_io_service));

        }

        error_code asio_network_provider::start(rpc_channel channel, int port, bool client_only)
        {
            if (_acceptor != nullptr)
                return ERR_SERVICE_ALREADY_RUNNING;
            
            dassert(channel == RPC_CHANNEL_TCP || channel == RPC_CHANNEL_UDP, "invalid given channel %s", channel.to_string());

            dsn_address_build(&_address, boost::asio::ip::host_name().c_str(), port);

            if (!client_only)
            {
                auto v4_addr = boost::asio::ip::address_v4::any(); //(ntohl(_address.ip));
                ::boost::asio::ip::tcp::endpoint ep(v4_addr, _address.port);

                try
                {
                    _acceptor.reset(new boost::asio::ip::tcp::acceptor(_io_service, ep, true));
                    do_accept();
                }
                catch (boost::system::system_error& err)
                {
                    printf("boost asio listen on port %u failed, err: %s\n", port, err.what());
                    return ERR_ADDRESS_ALREADY_USED;
                }
            }            

            return ERR_OK;
        }

        rpc_client_session_ptr asio_network_provider::create_client_session(const dsn_address_t& server_addr)
        {
            auto matcher = new_client_matcher();
            auto parser = new_message_parser();
            auto sock = boost::asio::ip::tcp::socket(_io_service);
            return rpc_client_session_ptr(new net_client_session(*this, sock, server_addr, matcher, parser, _io_service));
        }

        void asio_network_provider::do_accept()
        {
            _acceptor->async_accept(*_socket,
                [this](boost::system::error_code ec)
            {
                if (!ec)
                {
                    dsn_address_t client_addr;
                    client_addr.ip = htonl(_socket->remote_endpoint().address().to_v4().to_ulong());
                    client_addr.port = _socket->remote_endpoint().port();

                    // TODO: convert ip to host name
                    strncpy(client_addr.name, _socket->remote_endpoint().address().to_string().c_str(),
                        sizeof(client_addr.name) / sizeof(char));

                    auto parser = new_message_parser();
                    auto sock = std::move(*_socket);
                    auto s = rpc_server_session_ptr(new net_server_session(*this, client_addr, sock, parser, _io_service));
                    this->on_server_session_accepted(s);
                }

                do_accept();
            });
        }
    }
}
