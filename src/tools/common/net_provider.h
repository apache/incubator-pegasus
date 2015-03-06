#pragma once

# include <boost/asio.hpp>
# include <rdsn/tool_api.h>

namespace rdsn {
    namespace tools {
        
        class asio_network_provider : public network
        {
        public:
            asio_network_provider(rpc_engine* srv, network* inner_provider);

			virtual error_code start(int port, bool client_only);
            virtual const end_point& address() { return _address;  }
            virtual std::shared_ptr<rpc_client_session> create_client_session(const end_point& server_addr);

        private:
            void do_accept();

        private:
            friend class net_server_session;
            friend class net_client_session;

            std::shared_ptr<boost::asio::ip::tcp::acceptor> _acceptor;
            std::shared_ptr<boost::asio::ip::tcp::socket>   _socket;
            boost::asio::io_service        &_io_service;
            end_point                      _address;
        };

    }
}