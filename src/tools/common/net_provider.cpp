#include "shared_io_service.h"
#include "net_provider.h"
#include "net_client_session.h"
#include "net_server_session.h"

namespace rdsn {
    namespace tools{

        asio_network_provider::asio_network_provider(rpc_engine* srv, network* inner_provider)
            : network(srv, inner_provider), _io_service(shared_io_service::instance().ios)
        {
            _acceptor = nullptr;
            _socket.reset(new boost::asio::ip::tcp::socket(_io_service));
        }

        error_code asio_network_provider::start(int port, bool client_only)
        {
            if (_acceptor != nullptr)
                return ERR_SERVICE_ALREADY_RUNNING;
            
            _address = end_point(boost::asio::ip::host_name().c_str(), port);

            if (!client_only)
            {
                auto v4_addr = boost::asio::ip::address_v4::any(); //(ntohl(_address.ip));
                ::boost::asio::ip::tcp::endpoint ep(v4_addr, _address.port);

                try
                {
                    _acceptor.reset(new boost::asio::ip::tcp::acceptor(_io_service, ep, false));
                    do_accept();
                }
                catch (boost::system::system_error& err)
                {
                    printf("boost asio listen on port %u failed, err: %s\n", port, err.what());
                    return ERR_ADDRESS_ALREADY_USED;
                }
            }            

            return ERR_SUCCESS;
        }

        std::shared_ptr<rpc_client_session> asio_network_provider::create_client_session(const end_point& server_addr)
        {
            auto matcher = new_client_matcher();
            return std::shared_ptr<rpc_client_session>(new net_client_session(*this, server_addr, matcher));
        }

        void asio_network_provider::do_accept()
        {
            _acceptor->async_accept(*_socket,
                [this](boost::system::error_code ec)
            {
                if (!ec)
                {
                    end_point client_addr;
                    client_addr.ip = htonl(_socket->remote_endpoint().address().to_v4().to_ulong());
                    client_addr.port = _socket->remote_endpoint().port();

                    // TODO: convert ip to host name
                    client_addr.name = _socket->remote_endpoint().address().to_string();

                    auto s = std::shared_ptr<rpc_server_session>(new net_server_session(*this, client_addr, std::move(*_socket)));
                    this->on_server_session_accepted(s);
                }

                do_accept();
            });
        }
    }
}