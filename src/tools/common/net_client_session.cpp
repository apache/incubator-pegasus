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
# include "net_client_session.h"
# include <rdsn/internal/logging.h>

# define __TITLE__ "net.session"

namespace rdsn {
    namespace tools {
        net_client_session::net_client_session(asio_network_provider& net, const end_point& remote_addr, std::shared_ptr<rpc_client_matcher>& matcher)
            : _net(net), 
            _io_service(net._io_service),
            _socket(net._io_service),
            _state(SS_CLOSED),
            rpc_client_session(net, remote_addr, matcher),
            _sq("net_client_session.send.queue")
        {
            _reconnect_count = 0;
        }
        
        net_client_session::~net_client_session()
        {
            close();
        }
                
        void net_client_session::close()
        {
            try 
            {
                _socket.shutdown(boost::asio::socket_base::shutdown_type::shutdown_both);    
            }
            catch (std::exception& ex)
            {
                rwarn("network session %s:%u exits failed, err = %s",
                    remote_address().to_ip_string().c_str(),
                    (int)remote_address().port,
                    ex.what()
                    );
            }

            _socket.close();
            on_disconnected();
        }

        void net_client_session::on_failure()
        {
            _state = SS_CLOSED;
            
            if (_reconnect_count++ > 3)
            {
                close();
                return;
            }

            // TODO: delay and connect
            connect();
        }

        void net_client_session::connect()
        {
            session_state closed_state = SS_CLOSED;
            
            if (_state.compare_exchange_strong(closed_state, SS_CONNECTING))
            {
                boost::asio::ip::tcp::endpoint ep(boost::asio::ip::address_v4(ntohl(remote_address().ip)), remote_address().port);

                rpc_client_session_ptr sp = this;
                _socket.async_connect(ep, [this, sp](boost::system::error_code ec)
                {
                    if (!ec)
                    {
                        _reconnect_count = 0;
                        _state = SS_CONNECTED;
                        do_read_header();
                    }
                    else
                    {
                        rerror("network client session connect failed, error = %s",
                            ec.message().c_str()
                            );
                        on_failure();
                    }
                });
            }
        }

        void net_client_session::do_read_header()
        {
            rpc_client_session_ptr sp = this;
            boost::asio::async_read(_socket,
                boost::asio::buffer((void*)&_read_msg_hdr, message_header::serialized_size()),
                [this, sp](boost::system::error_code ec, std::size_t length)
            {
                if (!ec && message_header::is_right_header((char*)&_read_msg_hdr))
                {
                    rassert(length == message_header::serialized_size(), "");
                    do_read_body();
                }
                else
                {
                    rerror("network client session read message header failed, error = %s, sz = %d",
                        ec.message().c_str(), length
                        );
                    on_failure();
                }
            });
        }

        void net_client_session::do_read_body()
        {
            int body_sz = message_header::get_body_length((char*)&_read_msg_hdr);
            int sz = message_header::serialized_size() + body_sz;
            auto buf = std::shared_ptr<char>((char*)malloc(sz));
            _read_buffer.assign(buf, 0, sz);
            memcpy((void*)_read_buffer.data(), (const void*)&_read_msg_hdr, message_header::serialized_size());

            rpc_client_session_ptr sp = this;
            boost::asio::async_read(_socket,
                boost::asio::buffer((char*)_read_buffer.data() + message_header::serialized_size(), body_sz),
                [this, body_sz, sp](boost::system::error_code ec, std::size_t length)
            {
                if (!ec)
                {
                    message_ptr msg = new message(_read_buffer, true);
                    rassert(msg->header().body_length == body_sz, "");

                    if (msg->is_right_body())
                    {
                        this->on_recv_reply(msg->header().id, msg);
                    }
                    else
                    {
                        rerror("invalid response body (type = %s, body len = %u, skip ...",
                            msg->header().rpc_name,
                            msg->header().body_length
                            );
                    }

                    do_read_header();
                }
                else
                {
                    rerror("network client session read message failed, error = %s, sz = %d",
                        ec.message().c_str(), length
                        );
                    on_failure();
                }
            });
        }

        void net_client_session::do_write()
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

            rpc_client_session_ptr sp = this;
            boost::asio::async_write(_socket, buffers2,
                [this, msg, sp](boost::system::error_code ec, std::size_t length)
            {
                if (ec)
                {
                    rerror("network client session write message failed, error = %s, sz = %d",
                        ec.message().c_str(), length
                        );
                    on_failure();
                }
                else
                {
                    rassert(length == msg->total_size(), "");

                    auto smsg = _sq.dequeue_peeked();
                    rassert(smsg == msg, "sent msg must be the first msg in send queue");

                    do_write();
                }
            });
        }

        void net_client_session::write(message_ptr& msg)
        {
            _sq.enqueue(msg, task_spec::get(msg->header().local_rpc_code)->priority);

            // not connected
            if (SS_CONNECTED != _state)
            {
                return;
            }
            
            do_write();
        }
    }
}


