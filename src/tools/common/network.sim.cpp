/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus (rDSN) -=- 
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
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

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "net.provider.sim"

namespace dsn { namespace tools {

    // multiple machines connect to the same switch, 10 should be >= than rpc_channel::max_value() + 1
    static utils::singleton_store<end_point, sim_network_provider*> s_switch[10]; 

    sim_client_session::sim_client_session(sim_network_provider& net, const end_point& remote_addr, rpc_client_matcher_ptr& matcher)
        : rpc_client_session(net, remote_addr, matcher)
    {}

    void sim_client_session::connect() 
    {
        // nothing to do
    }

    void sim_client_session::send(message_ptr& msg)
    {
        sim_network_provider* rnet = nullptr;
        if (!s_switch[task_spec::get(msg->header().local_rpc_code)->rpc_call_channel].get(msg->header().to_address, rnet))
        {
            dwarn("cannot find destination node %s:%d in simulator", 
                msg->header().to_address.name.c_str(), 
                static_cast<int>(msg->header().to_address.port)
                );
            return;
        }
        
        auto server_session = rnet->get_server_session(_net.address());
        if (nullptr == server_session)
        {
            rpc_client_session_ptr cptr = this;
            server_session.reset(new sim_server_session(*rnet, _net.address(), cptr));
            rnet->on_server_session_accepted(server_session);
        }

        message_ptr recv_msg(new message(msg->writer().get_buffer()));
        recv_msg->header().from_address = msg->header().from_address;
        recv_msg->header().to_address = msg->header().to_address;

        server_session->on_recv_request(recv_msg, 
            recv_msg->header().from_address == recv_msg->header().to_address ?
            0 : rnet->net_delay_milliseconds()
            );
    }

    sim_server_session::sim_server_session(sim_network_provider& net, const end_point& remote_addr, rpc_client_session_ptr& client)
        : rpc_server_session(net, remote_addr)
    {
        _client = client;
    }

    void sim_server_session::send(message_ptr& reply_msg)
    {
        message_ptr recv_msg(new message(reply_msg->writer().get_buffer()));
        recv_msg->header().from_address = reply_msg->header().from_address;
        recv_msg->header().to_address = reply_msg->header().to_address;

        _client->on_recv_reply(recv_msg->header().id, recv_msg,
            recv_msg->header().from_address == recv_msg->header().to_address ?
            0 : (static_cast<sim_network_provider*>(&_net))->net_delay_milliseconds()
            );
    }

    sim_network_provider::sim_network_provider(rpc_engine* rpc, network* inner_provider)
        : connection_oriented_network(rpc, inner_provider), _address("localhost", 1)
    {
        _min_message_delay_microseconds = 1;
        _max_message_delay_microseconds = 100000;

        auto config = tool_app::get_service_spec().config;
        if (config != NULL)
        {
            _min_message_delay_microseconds = config->get_value<uint32_t>("tools.simulator", "min_message_delay_microseconds", _min_message_delay_microseconds);
            _max_message_delay_microseconds = config->get_value<uint32_t>("tools.simulator", "max_message_delay_microseconds", _max_message_delay_microseconds);
        }
    }

    error_code sim_network_provider::start(rpc_channel channel, int port, bool client_only)
    { 
        dassert(channel == RPC_CHANNEL_TCP || channel == RPC_CHANNEL_UDP, "invalid given channel %s", channel.to_string());

        _address = end_point(boost::asio::ip::host_name().c_str(), port);

        end_point ep2 = end_point("localhost", port);
      
        if (!client_only)
        {
            if (s_switch[channel].put(_address, this))
            {
                s_switch[channel].put(ep2, this);
                return ERR_OK;
            }   
            else
                return ERR_ADDRESS_ALREADY_USED;
        }
        else
        {
            return ERR_OK;
        }
    }

    uint32_t sim_network_provider::net_delay_milliseconds() const
    {
        return static_cast<uint32_t>(dsn::service::env::random32(_min_message_delay_microseconds, _max_message_delay_microseconds)) / 1000;
    }    
}} // end namespace
