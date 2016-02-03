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

# include "asio_rpc_session.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "asio.rpc.session"

namespace dsn {
    namespace tools {


        asio_rpc_session::~asio_rpc_session()
        {
        }

        void asio_rpc_session::set_options()
        {
            if (_socket->is_open())
            {
                try {
                    boost::asio::socket_base::send_buffer_size option, option2(16 * 1024 * 1024);
                    _socket->get_option(option);
                    int old = option.value();
                    _socket->set_option(option2);
                    _socket->get_option(option);

                    dinfo("boost asio send buffer size is %u, set as 16MB, now is %u",
                            old, option.value());

                    boost::asio::socket_base::receive_buffer_size option3, option4(16 * 1024 * 1024);
                    _socket->get_option(option3);
                    old = option3.value();
                    _socket->set_option(option4);
                    _socket->get_option(option3);

                    dinfo("boost asio recv buffer size is %u, set as 16MB, now is %u",
                            old, option.value());
                }
                catch (std::exception& ex)
                {
                    dwarn("network session 0x%x:%hu set socket option failed, err = %s",
                        remote_address().ip(),
                        remote_address().port(),
                        ex.what()
                        );
                }
            }
        }

        void asio_rpc_session::do_read(int sz)
        {
            add_ref();

            void* ptr = _parser->read_buffer_ptr((int)sz);
            int remaining = _parser->read_buffer_capacity();

            _socket->async_read_some(boost::asio::buffer(ptr, remaining),
                [this](boost::system::error_code ec, std::size_t length)
            {
                if (!!ec)
                {
                    derror("asio read from %s failed: %s", _remote_addr.to_string(), ec.message().c_str());
                    on_failure();
                }
                else
                {
                    int read_next;
                    message_ex* msg = _parser->get_message_on_receive((int)length, read_next);

                    while (msg != nullptr)
                    {
                        this->on_message_read(msg);
                        msg = _parser->get_message_on_receive(0, read_next);
                    }
                     
                    start_read_next(read_next);
                }

                release_ref();
            });
        }
        
        void asio_rpc_session::write(uint64_t signature)
        {
            std::vector<boost::asio::const_buffer> buffers2;
            int bcount = (int)_sending_buffers.size();
            
            // prepare buffers
            buffers2.resize(bcount);            
            for (int i = 0; i < bcount; i++)
            {
                buffers2[i] = boost::asio::const_buffer(_sending_buffers[i].buf, _sending_buffers[i].sz);
            }

            add_ref();
            boost::asio::async_write(*_socket, buffers2,
                [this, signature](boost::system::error_code ec, std::size_t length)
            {
                if (!!ec)
                {
                    derror("asio write to %s failed: %s", _remote_addr.to_string(), ec.message().c_str());
                    on_failure(true);
                }
                else
                {
                    on_send_completed(signature);
                }

                release_ref();
            });
        }
        
        asio_rpc_session::asio_rpc_session(
            asio_network_provider& net,
            ::dsn::rpc_address remote_addr,
            std::shared_ptr<boost::asio::ip::tcp::socket>& socket,
            std::unique_ptr<message_parser>&& parser,
            bool is_client
            )
            :
            rpc_session(net, remote_addr, std::move(parser), is_client),
            _socket(socket)
        {
            set_options();
            if (!is_client) start_read_next();
        }
        
        void asio_rpc_session::on_failure(bool is_write)
        {
            if (on_disconnected(is_write))
            {
                try {
                    _socket->shutdown(boost::asio::socket_base::shutdown_type::shutdown_both);
                    _socket->close();
                }
                catch (std::exception& /*ex*/)
                {
                    /*dwarn("network session %s exits failed, err = %s",
                    remote_address().to_ip_string().c_str(),
                    static_cast<int>remote_address().port(),
                    ex.what()
                    );*/
                }
            }
        }

        void asio_rpc_session::connect()
        {
            if (try_connecting())
            {
                boost::asio::ip::tcp::endpoint ep(
                    boost::asio::ip::address_v4(_remote_addr.ip()), _remote_addr.port());

                add_ref();
                _socket->async_connect(ep, [this](boost::system::error_code ec)
                {
                    if (!ec)
                    {
                        dinfo("client session %s connected",
                            _remote_addr.to_string()
                            );

                        set_options();
                        set_connected();
                        on_send_completed();
                        start_read_next();
                    }
                    else
                    {
                        derror("client session connect to %s failed, error = %s",
                            _remote_addr.to_string(),
                            ec.message().c_str()
                            );
                        on_failure(true);
                    }
                    release_ref();
                });
            }
        }   
    }
}
