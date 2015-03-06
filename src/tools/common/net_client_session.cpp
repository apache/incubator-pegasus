# include "net_client_session.h"
# include <rdsn/internal/logging.h>

# define __TITLE__ "net.session"

namespace rdsn {
    namespace tools {
        net_client_session::net_client_session(asio_network_provider& net, const end_point& remote_addr, std::shared_ptr<rpc_client_matcher>& matcher)
            : _net(net), 
            _io_service(net._io_service),
            _socket(net._io_service),
            _read_msg_hdr((char*)malloc(message_header::serialized_size())),
            _state(SS_CLOSED),
            rpc_client_session(net, remote_addr, matcher)
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

                _socket.async_connect(ep, [this](boost::system::error_code ec)
                {
                    if (!ec)
                    {
                        _reconnect_count = 0;
                        _state = SS_CONNECTED;
                        do_read_header();
                    }
                    else
                    {
                        on_failure();
                    }
                });
            }
        }

        void net_client_session::do_read_header()
        {
            boost::asio::async_read(_socket,
                boost::asio::buffer(_read_msg_hdr.get(), message_header::serialized_size()),
                [this](boost::system::error_code ec, std::size_t /*length*/)
            {
                if (!ec && message_header::is_right_header(_read_msg_hdr.get()))
                {
                    do_read_body();
                }
                else
                {
                    on_failure();
                }
            });
        }

        void net_client_session::do_read_body()
        {
            int body_sz = message_header::get_body_length(_read_msg_hdr.get());
            int sz = message_header::serialized_size() + body_sz;
            auto buf = std::shared_ptr<char>((char*)malloc(sz));
            _read_buffer.assign(buf, 0, sz);
            memcpy((void*)_read_buffer.data(), _read_msg_hdr.get(), message_header::serialized_size());

            boost::asio::async_read(_socket,
                boost::asio::buffer((char*)_read_buffer.data() + message_header::serialized_size(), body_sz),
                [this](boost::system::error_code ec, std::size_t length)
            {
                if (!ec)
                {
                    message_ptr msg = new message(_read_buffer, true);
                    this->on_recv_reply(msg->header().id, msg);

                    do_read_header();
                }
                else
                {
                    rerror("network client read message failed, error = %s, read sz = %d",
                        ec.message().c_str(), length
                        );
                    on_failure();
                }
            });
        }


        void net_client_session::write(message_ptr& msg)
        {
            if (SS_CONNECTED != _state)
                return;

            std::vector<utils::blob> buffers;
            msg->get_output_buffers(buffers);

            std::vector<boost::asio::const_buffer> buffers2;
            for (auto& b : buffers)
            {
                buffers2.push_back(boost::asio::const_buffer(b.data(), b.length()));
            }
            
            boost::asio::async_write(_socket, buffers2,
                [this](boost::system::error_code ec, std::size_t /*length*/)
            {
                if (ec)
                {
                    on_failure();
                }
            });
        }
    }
}


