#pragma once

# include "net_provider.h"

namespace rdsn {
    namespace tools {

        class asio_network_provider;
        class net_client_session
            : public rpc_client_session
        {
        public:
            net_client_session(asio_network_provider& net, const end_point& remote_addr, std::shared_ptr<rpc_client_matcher>& matcher);
            ~net_client_session();

            void write(message_ptr& msg);
            void close();

            virtual void connect();
            virtual void send(message_ptr& msg) { return write(msg); }

        private:
            void do_read_header();
            void do_read_body();
            void on_failure();

        protected:
            enum session_state
            {
                SS_CONNECTING,
                SS_CONNECTED,
                SS_CLOSED
            };

            boost::asio::io_service      &_io_service;
            boost::asio::ip::tcp::socket _socket;
            boost::shared_ptr<char>      _read_msg_hdr;
            utils::blob                  _read_buffer;
            asio_network_provider        &_net;
            std::atomic<session_state>   _state;
            int                          _reconnect_count;
        };
    }
}

