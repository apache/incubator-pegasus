/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
# include "shared_io_service.h"
# include "net_server_session.h"
# include <dsn/internal/logging.h>

# define __TITLE__ "net.session"

namespace dsn {
    namespace tools {
        net_server_session::net_server_session(asio_network_provider& net, const end_point& remote_addr,
            boost::asio::ip::tcp::socket socket)
            : _net(net), 
            _io_service(shared_io_service::instance().ios),
            _socket(std::move(socket)),
            rpc_server_session(net, remote_addr),
            _sq("net_server_session.send.queue")
        {
            do_read_header();
        }

        net_server_session::~net_server_session()
        {
            close();
        }

        void net_server_session::on_failure()
        {
            close();
        }
        
        void net_server_session::close()
        {
            try {
                _socket.shutdown(boost::asio::socket_base::shutdown_type::shutdown_both);
            }
            catch (std::exception& ex)
            {
                dwarn("network session %s:%u exits failed, err = %s",
                    remote_address().to_ip_string().c_str(),
                    (int)remote_address().port,
                    ex.what()
                    );
            }

            _socket.close();
            on_disconnected();
        }

        void net_server_session::do_read_header()
        {
            rpc_server_session_ptr sp = this;
            boost::asio::async_read(_socket,
                boost::asio::buffer((void*)&_read_msg_hdr, message_header::serialized_size()),
                [this, sp](boost::system::error_code ec, std::size_t length)
            {
                if (!ec && message_header::is_right_header((char*)&_read_msg_hdr))
                {
                    dassert(length == message_header::serialized_size(), "");
                    do_read_body();
                }
                else
                {
                    derror("network server session read message header failed, error = %s, sz = %d",
                        ec.message().c_str(), length
                        );
                    on_failure();
                }
            });
        }

        void net_server_session::do_read_body()
        {
            int body_sz = message_header::get_body_length((char*)&_read_msg_hdr); 
            dassert(body_sz > 0, "");
            int sz = message_header::serialized_size() + body_sz;
            auto buf = std::shared_ptr<char>((char*)malloc(sz));
            _read_buffer.assign(buf, 0, sz);
            memcpy((void*)_read_buffer.data(), (const void*)&_read_msg_hdr, message_header::serialized_size());

            rpc_server_session_ptr sp = this;
            boost::asio::async_read(_socket,
                boost::asio::buffer((char*)_read_buffer.data() + message_header::serialized_size(), body_sz),
                [this, body_sz, sp](boost::system::error_code ec, std::size_t length)
            {
                if (!ec)
                {
                    message_ptr msg = new message(_read_buffer, true);
                    dassert(msg->header().body_length == body_sz, "");
                    
                    if (msg->is_right_body())
                    {
                        this->on_recv_request(msg);
                    }
                    else
                    {
                        derror("invalid request body (type = %s, body len = %u, skip ...",
                            msg->header().rpc_name,
                            msg->header().body_length
                            );
                    }

                    do_read_header();
                }
                else
                {
                    derror("network server session read message failed, error = %s, sz = %d",
                        ec.message().c_str(), length
                        );
                    on_failure();
                }
            });
        }



        void net_server_session::do_write()
        {
            auto msg = _sq.peek();
            if (nullptr == msg.get())
                return;

            std::vector<utils::blob> buffers;
            msg->get_output_buffers(buffers);

            std::vector<boost::asio::const_buffer> buffers2;
            for (auto& b : buffers)
            {
                buffers2.push_back(boost::asio::const_buffer(b.data(), b.length()));
            }

            rpc_server_session_ptr sp = this;
            boost::asio::async_write(_socket, buffers2,
                [this, msg, sp](boost::system::error_code ec, std::size_t length)
            {
                if (ec)
                {
                    derror("network server session write message failed, error = %s, sz = %d",
                        ec.message().c_str(), length
                        );
                    on_failure();
                }
                else
                {
                    dassert(length == msg->total_size(), "");

                    auto smsg = _sq.dequeue_peeked();
                    dassert(smsg == msg, "sent msg must be the first msg in send queue");

                    do_write();
                }
            });
        }

        void net_server_session::write(message_ptr& msg)
        {
            _sq.enqueue(msg, task_spec::get(msg->header().local_rpc_code)->priority);
            do_write();
        }
    }
}


