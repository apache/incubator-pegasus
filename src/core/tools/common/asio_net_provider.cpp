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

#include "asio_net_provider.h"
#include "asio_rpc_session.h"

namespace dsn {
    namespace tools{

        asio_network_provider::asio_network_provider(rpc_engine* srv, network* inner_provider)
            : connection_oriented_network(srv, inner_provider)
        {
            _acceptor = nullptr;
        }

        error_code asio_network_provider::start(rpc_channel channel, int port, bool client_only, io_modifer& ctx)
        {
            if (_acceptor != nullptr)
                return ERR_SERVICE_ALREADY_RUNNING;

            int io_service_worker_count = config()->get_value<int>("network", "io_service_worker_count", 1,
                "thread number for io service (timer and boost network)");
            for (int i = 0; i < io_service_worker_count; i++)
            {
                _workers.push_back(std::shared_ptr<std::thread>(new std::thread([this, ctx, i]()
                {
                    task::set_tls_dsn_context(node(), nullptr, ctx.queue);

                    const char* name = ::dsn::tools::get_service_node_name(node());
                    char buffer[128];
                    sprintf(buffer, "%s.asio.%d", name, i);
                    task_worker::set_name(buffer);

                    boost::asio::io_service::work work(_io_service);
                    _io_service.run();
                })));
            }

            _acceptor = nullptr;
                        
            dassert(channel == RPC_CHANNEL_TCP || channel == RPC_CHANNEL_UDP, "invalid given channel %s", channel.to_string());

            _address.assign_ipv4(get_local_ipv4(), port);

            if (!client_only)
            {
                auto v4_addr = boost::asio::ip::address_v4::any(); //(ntohl(_address.ip));
                ::boost::asio::ip::tcp::endpoint ep(v4_addr, _address.port());

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

        rpc_session_ptr asio_network_provider::create_client_session(::dsn::rpc_address server_addr)
        {
            auto sock = std::shared_ptr<boost::asio::ip::tcp::socket>(new boost::asio::ip::tcp::socket(_io_service));
            return rpc_session_ptr(new asio_rpc_session(*this, server_addr, sock, new_message_parser(), true));
        }

        void asio_network_provider::do_accept()
        {
            auto socket = std::shared_ptr<boost::asio::ip::tcp::socket>(
                new boost::asio::ip::tcp::socket(_io_service));

            _acceptor->async_accept(*socket,
                [this, socket](boost::system::error_code ec)
            {
                if (!ec)
                {
                    auto ip = socket->remote_endpoint().address().to_v4().to_ulong();
                    auto port = socket->remote_endpoint().port();
                    ::dsn::rpc_address client_addr(ip, port);

                    rpc_session_ptr s = new asio_rpc_session(*this, client_addr, 
                        (std::shared_ptr<boost::asio::ip::tcp::socket>&)socket,
                        new_message_parser(), false);
                    this->on_server_session_accepted(s);
                }

                do_accept();
            });
        }

        void asio_udp_provider::send_message(message_ex* request)
        {
            // prepare parser as there will be concurrent send-message-s
            auto pr = get_message_parser_info();
            auto parser_place = alloca(pr.second);
            auto parser = pr.first(parser_place, _message_buffer_block_size, true);

            int tlen;
            auto count = parser->get_send_buffers_count_and_total_length(request, &tlen);
            dassert(tlen < max_udp_packet_size, "the message is too large to send via a udp channel");
            std::unique_ptr<message_parser::send_buf[]> bufs(new message_parser::send_buf[count]);
            parser->prepare_buffers_on_send(request, 0, bufs.get());

            // end using parser
            parser->~message_parser();

            std::unique_ptr<char[]> packet_buffer(new char[tlen]);
            size_t offset = 0;
            for (int i = 0; i < count; i ++)
            {
                memcpy(&packet_buffer[offset], bufs[i].buf, bufs[i].sz);
                offset += bufs[i].sz;
            };
            ::boost::asio::ip::udp::endpoint ep(::boost::asio::ip::address_v4(request->to_address.ip()), request->to_address.port());
            _socket->async_send_to(::boost::asio::buffer(packet_buffer.get(), tlen), ep,
                [=](const boost::system::error_code& error, std::size_t bytes_transferred)
                {
                    if (error) {
                        dwarn("send udp packet to ep %s:%d failed, message = %s", ep.address().to_string().c_str(), ep.port(), error.message().c_str());
                        //we do not handle failure here, rpc matcher would handle timeouts
                    }
                });
        }

        void asio_udp_provider::do_receive()
        {
            std::shared_ptr< ::boost::asio::ip::udp::endpoint> send_endpoint(new ::boost::asio::ip::udp::endpoint);

            _recv_parser->truncate_read();
            auto buffer_ptr = _recv_parser->read_buffer_ptr(max_udp_packet_size);
            dassert(_recv_parser->read_buffer_capacity() >= max_udp_packet_size, "failed to load enough buffer in parser");

            _socket->async_receive_from(
                ::boost::asio::buffer(buffer_ptr, max_udp_packet_size),
                *send_endpoint,
                [this, send_endpoint](const boost::system::error_code& error, std::size_t bytes_transferred)
                {
                    if (!error) 
                    {
                        int read_next;
                        auto message = _recv_parser->get_message_on_receive(bytes_transferred, read_next);

                        if (message == nullptr)
                        {
                            derror("invalid udp packet");
                        }
                    
                        message->from_address.assign_ipv4(send_endpoint->address().to_v4().to_ulong(), send_endpoint->port());
                        message->to_address = address();

                        if (!_is_client)
                        {
                            on_recv_request(message, 0);
                        }
                        else
                        {
                            on_recv_reply(message, 0);
                        }
                    }

                    do_receive();
                }
            );
        }

        error_code asio_udp_provider::start(rpc_channel channel, int port, bool client_only, io_modifer& ctx)
        {
            _is_client = client_only;
            int io_service_worker_count = config()->get_value<int>("network", "io_service_worker_count", 1,
                                                                   "thread number for io service (timer and boost network)");
           
            dassert(channel == RPC_CHANNEL_UDP, "invalid given channel %s", channel.to_string());

            if (client_only)
            {
                do
                {
                    //FIXME: we actually do not need to set a random port for clinet if the rpc_engine is refactored
                    _address.assign_ipv4(get_local_ipv4(), std::numeric_limits<uint16_t>::max() - 
                        dsn_random64(std::numeric_limits<uint64_t>::min(),
                                     std::numeric_limits<uint64_t>::max()) % 5000);
                    ::boost::asio::ip::udp::endpoint ep(boost::asio::ip::address_v4::any(), _address.port());
                    try
                    {
                        _socket.reset(new ::boost::asio::ip::udp::socket(_io_service, ep));
                        break;
                    }
                    catch (boost::system::system_error& err)
                    {
                        derror("boost asio listen on port %u failed, err: %s\n", port, err.what());
                    }
                } while (true);
            }
            else
            {
                _address.assign_ipv4(get_local_ipv4(), port);
                ::boost::asio::ip::udp::endpoint ep(boost::asio::ip::address_v4::any(), _address.port());
                try
                {
                    _socket.reset(new ::boost::asio::ip::udp::socket(_io_service, ep));
                }
                catch (boost::system::system_error& err)
                {
                    derror("boost asio listen on port %u failed, err: %s\n", port, err.what());
                    return ERR_ADDRESS_ALREADY_USED;
                }
            }

            for (int i = 0; i < io_service_worker_count; i++)
            {
                _workers.push_back(std::shared_ptr<std::thread>(new std::thread([this, ctx, i]()
                {
                    task::set_tls_dsn_context(node(), nullptr, ctx.queue);

                    const char* name = ::dsn::tools::get_service_node_name(node());
                    char buffer[128];
                    sprintf(buffer, "%s.asio.udp.%d.%d", name, (int)(this->address().port()), i);
                    task_worker::set_name(buffer);

                    boost::asio::io_service::work work(_io_service);
                    _io_service.run();
                })));
            }

            do_receive();
            return ERR_OK;
        }
    }
}
