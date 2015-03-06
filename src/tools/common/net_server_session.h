#pragma once

# include "net_provider.h"
# include <rdsn/internal/priority_queue.h>

namespace rdsn {
    namespace tools {

        class asio_network_provider;
        class net_server_session
            : public rpc_server_session
        {
        public:
            net_server_session(asio_network_provider& net, const end_point& remote_addr,
                boost::asio::ip::tcp::socket socket);
            ~net_server_session();

            virtual void send(message_ptr& reply_msg) { return write(reply_msg); }

            void write(message_ptr& msg);
            void close();

        private:            
            void do_read_header();
            void do_read_body();
            void on_failure();

        protected:

            boost::asio::io_service      &_io_service;
            boost::asio::ip::tcp::socket _socket;
            boost::shared_ptr<char>      _read_msg_hdr;
            utils::blob                  _read_buffer;
            asio_network_provider        &_net;
        };
    }
}

