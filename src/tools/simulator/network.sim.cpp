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
#include <boost/asio.hpp>
#include <dsn/service_api.h>
#include <dsn/internal/singleton_store.h>
#include "network.sim.h" 

#define __TITLE__ "net.provider.sim"

namespace dsn { namespace tools {

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
            dwarn("cannot find destination node %s:%u in simulator", 
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
            dwarn("cannot find destination node %s:%u in simulator",
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
            dwarn("cannot find origination client for %s:%u @ %s:%u in simulator",
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
            _minMessageDelayMicroseconds = config->get_value<uint32_t>("dsn.simulation", "MinMessageDelayMicroseconds", _minMessageDelayMicroseconds);
            _maxMessageDelayMicroseconds = config->get_value<uint32_t>("dsn.simulation", "MaxMessageDelayMicroseconds", _maxMessageDelayMicroseconds);
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
        return (int)dsn::service::env::random32(_minMessageDelayMicroseconds, _maxMessageDelayMicroseconds) / 1000;
    }    
}} // end namespace
