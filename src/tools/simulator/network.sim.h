#pragma once

#include <rdsn/tool_api.h>

namespace rdsn { namespace tools {

    class sim_network_provider;
    class sim_client_session : public rpc_client_session
    {
    public:
        sim_client_session(sim_network_provider& net, const end_point& remote_addr, std::shared_ptr<rpc_client_matcher>& matcher);

        virtual void connect();
        virtual void send(message_ptr& msg);
    };

    class sim_server_session : public rpc_server_session
    {
    public:
        sim_server_session(sim_network_provider& net, const end_point& remote_addr);

        virtual void send(message_ptr& reply_msg);
    };

    class sim_network_provider : public network
    {
    public:
        sim_network_provider(rpc_engine* rpc, network* inner_provider);
        ~sim_network_provider(void) {}

		virtual error_code start(int port, bool client_only);
    
        virtual const end_point& address() { return _primary_address; }

        virtual std::shared_ptr<rpc_client_session> create_client_session(const end_point& server_addr)
        {
            auto matcher = new_client_matcher();
            return std::shared_ptr<rpc_client_session>(new sim_client_session(*this, server_addr, matcher));
        }

        uint32_t net_delay_milliseconds() const;

    private:
        end_point    _primary_address;
        uint32_t      _minMessageDelayMicroseconds;
        uint32_t      _maxMessageDelayMicroseconds;
    };

    //------------- inline implementations -------------


}} // end namespace

