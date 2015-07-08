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
# include "net_io.h"
# include <dsn/internal/logging.h>
# include "shared_io_service.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "net.boost.asio"

namespace dsn {
    namespace tools {

        net_io::net_io(
            const end_point& remote_addr,
            boost::asio::ip::tcp::socket& socket,
            std::shared_ptr<dsn::message_parser>& parser
            )
            :
            _io_service(shared_io_service::instance().ios),
            _socket(std::move(socket)),
            _sq("net_io.send.queue"),
            _remote_addr(remote_addr),
            _parser(parser)
        {
            set_options();
        }

        net_io::~net_io()
        {
        }

        void net_io::set_options()
        {
            if (_socket.is_open())
            {
                try {
                    boost::asio::socket_base::send_buffer_size option, option2(16 * 1024 * 1024);
                    _socket.get_option(option);
                    int old = option.value();
                    _socket.set_option(option2);
                    _socket.get_option(option);

                    /*ddebug("boost asio send buffer size is %u, set as 16MB, now is %u",
                    old, option.value());*/

                    boost::asio::socket_base::receive_buffer_size option3, option4(16 * 1024 * 1024);
                    _socket.get_option(option3);
                    old = option3.value();
                    _socket.set_option(option4);
                    _socket.get_option(option3);
                    /*ddebug("boost asio recv buffer size is %u, set as 16MB, now is %u",
                    old, option.value());*/
                }
                catch (std::exception& ex)
                {
                    dwarn("network session %s:%d set socket option failed, err = %s",
                        _remote_addr.to_ip_string().c_str(),
                        static_cast<int>(_remote_addr.port),
                        ex.what()
                        );
                }
            }
        }

        void net_io::close()
        {
            try {
                _socket.shutdown(boost::asio::socket_base::shutdown_type::shutdown_both);
            }
            catch (std::exception& ex)
            {
                ex;
                /*dwarn("network session %s:%d exits failed, err = %s",
                    _remote_addr.to_ip_string().c_str(),
                    static_cast<int>_remote_addr.port,
                    ex.what()
                    );*/
            }

            _socket.close();
            on_closed();
        }

        void net_io::do_read(size_t sz)
        {
            add_reference();

            void* ptr = _parser->read_buffer_ptr((int)sz);
            int remaining = _parser->read_buffer_capacity();

            _socket.async_read_some(boost::asio::buffer(ptr, remaining),
                [this](boost::system::error_code ec, std::size_t length)
            {
                if (!!ec)
                {
                    on_failure();
                }
                else
                {
                    int read_next;
                    message_ptr msg = _parser->get_message_on_receive((int)length, read_next);

                    while (msg != nullptr)
                    {
                        this->on_message_read(msg);
                        msg = _parser->get_message_on_receive(0, read_next);
                    }
                     
                    do_read(read_next);
                }

                release_reference();
            });
        }

        void net_io::do_write()
        {
            auto msg = _sq.peek();
            if (nullptr == msg.get())
                return;

            std::vector<blob> buffers;
            _parser->prepare_buffers_for_send(msg, buffers);

            std::vector<boost::asio::const_buffer> buffers2;
            for (auto& b : buffers)
            {
                buffers2.push_back(boost::asio::const_buffer(b.data(), b.length()));
            }

            add_reference();
            boost::asio::async_write(_socket, buffers2,
                [this, msg](boost::system::error_code ec, std::size_t length)
            {
                if (!!ec)
                {
                    on_failure();
                }
                else
                {
                    auto smsg = _sq.dequeue_peeked();
                    dassert(smsg == msg, "sent msg must be the first msg in send queue");
                    //dinfo("network message sent, rpc_id = %016llx", msg->header().rpc_id);

                    do_write();
                }

                release_reference();
            });
        }

        void net_io::write(message_ptr& msg)
        {
            _sq.enqueue(msg, task_spec::get(msg->header().local_rpc_code)->priority);
            do_write();
        }

        // ------------------------------------------------------------
        
        client_net_io::client_net_io(const end_point& remote_addr,
            boost::asio::ip::tcp::socket& socket,
            std::shared_ptr<dsn::message_parser>& parser)
            :
            net_io(remote_addr, socket, parser), 
            _state(SS_CLOSED),
            _reconnect_count(0)
        {
        }

        void client_net_io::write(message_ptr& msg)
        {
            _sq.enqueue(msg, task_spec::get(msg->header().local_rpc_code)->priority);

            // not connected
            if (SS_CONNECTED != _state)
            {
                return;
            }

            do_write();
        }

        void client_net_io::on_failure()
        {
            _state = SS_CLOSED;

            if (_reconnect_count++ > 3)
            {
                close();
                return;
            }

            connect();
        }

        void client_net_io::connect()
        {
            session_state closed_state = SS_CLOSED;

            if (_state.compare_exchange_strong(closed_state, SS_CONNECTING))
            {
                boost::asio::ip::tcp::endpoint ep(
                    boost::asio::ip::address_v4(ntohl(_remote_addr.ip)), _remote_addr.port);

                add_reference();
                _socket.async_connect(ep, [this](boost::system::error_code ec)
                {
                    if (!ec)
                    {
                        _reconnect_count = 0;
                        _state = SS_CONNECTED;

                        dinfo("client session %s:%d connected",
                            _remote_addr.name.c_str(),
                            static_cast<int>(_remote_addr.port)
                            );

                        set_options();

                        do_write();
                        do_read();                        
                    }
                    else
                    {
                        derror("network client session connect failed, error = %s",
                            ec.message().c_str()
                            );
                        on_failure();
                    }
                    release_reference();
                });
            }
        }


    }
}