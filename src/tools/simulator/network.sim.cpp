#include <boost/asio.hpp>
#include <rdsn/service_api.h>
#include <rdsn/internal/singleton_store.h>
#include "network.sim.h" 

#define __TITLE__ "net.provider.sim"

namespace rdsn { namespace tools {

    static utils::singleton_store<end_point, sim_network_provider*> s_switch; // multiple machines connect to the same switch

    sim_client_session::sim_client_session(sim_network_provider& net, const end_point& remote_addr, std::shared_ptr<rpc_client_matcher>& matcher)
        : rpc_client_session(net, remote_addr, matcher)
    {}

    void sim_client_session::connect() 
    {
        // nothing to do
    }

    void sim_client_session::send(message_ptr& msg)
    {
        sim_network_provider* rnet = nullptr;
        if (!s_switch.get(msg->header().to_address, rnet))
        {
            rdsn_warn("cannot find destination node %s:%u in simulator", 
                msg->header().to_address.name.c_str(), 
                (int)msg->header().to_address.port
                );
            return;
        }
        
        auto server_session = rnet->get_server_session(_net.address());
        if (nullptr == server_session)
        {
            server_session.reset(new sim_server_session(*rnet, _net.address()));
            rnet->on_server_session_accepted(server_session);
        }

        message_ptr recv_msg(new message(msg->get_output_buffer()));
        server_session->on_recv_request(recv_msg, 
            recv_msg->header().from_address == recv_msg->header().to_address ?
            0 : rnet->net_delay_milliseconds()
            );
    }

    sim_server_session::sim_server_session(sim_network_provider& net, const end_point& remote_addr)
        : rpc_server_session(net, remote_addr)
    {
    }

    void sim_server_session::send(message_ptr& reply_msg)
    {
        sim_network_provider* rnet = nullptr;
        if (!s_switch.get(reply_msg->header().to_address, rnet))
        {
            rdsn_warn("cannot find destination node %s:%u in simulator",
                reply_msg->header().to_address.name.c_str(),
                (int)reply_msg->header().to_address.port
                );
            return;
        }

        auto client_session = rnet->get_client_session(reply_msg->header().from_address);
        if (nullptr != client_session)
        {
            message_ptr recv_msg(new message(reply_msg->get_output_buffer()));
            client_session->on_recv_reply(recv_msg->header().id, recv_msg,
                recv_msg->header().from_address == recv_msg->header().to_address ?
                0 : rnet->net_delay_milliseconds()
                );
        }
        else
        {
            rdsn_warn("cannot find origination client for %s:%u @ %s:%u in simulator",
                reply_msg->header().from_address.name.c_str(),
                (int)reply_msg->header().from_address.port,
                reply_msg->header().to_address.name.c_str(),
                (int)reply_msg->header().to_address.port
                );
        }
    }

    sim_network_provider::sim_network_provider(rpc_engine* rpc, network* inner_provider)
    : network(rpc, inner_provider), _primary_address("localhost", 1)
    {
        _minMessageDelayMicroseconds = 1;
        _maxMessageDelayMicroseconds = 100000;

        auto config = tool_app::get_service_spec().config;
        if (config != NULL)
        {
            _minMessageDelayMicroseconds = config->get_value<uint32_t>("rdsn.simulation", "MinMessageDelayMicroseconds", _minMessageDelayMicroseconds);
            _maxMessageDelayMicroseconds = config->get_value<uint32_t>("rdsn.simulation", "MaxMessageDelayMicroseconds", _maxMessageDelayMicroseconds);
        }
    }

	error_code sim_network_provider::start(int port, bool client_only)
    { 
		client_only;
        _primary_address.port = port;
        if (s_switch.put(_primary_address, this))
            return ERR_SUCCESS;
        else
            return ERR_ADDRESS_ALREADY_USED;
    }

    uint32_t sim_network_provider::net_delay_milliseconds() const
    {
        return (int)rdsn::service::env::random32(_minMessageDelayMicroseconds, _maxMessageDelayMicroseconds) / 1000;
    }    
}} // end namespace
