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
#include "meta_server_failure_detector.h"
#include "server_state.h"

#define __TITLE__ "MetaServer.FD"

meta_server_failure_detector::meta_server_failure_detector(server_state* state)
{
    _state = state;
    _is_primary = false;
}

meta_server_failure_detector::~meta_server_failure_detector(void)
{
}

void meta_server_failure_detector::on_worker_disconnected(const std::vector<end_point>& nodes)
{
    if (!is_primary())
    {
        return;
    }

    node_states states;
    for (auto& n : nodes)
    {
        states.push_back(std::make_pair(n, false));

        dwarn("client expired: %s:%hu", n.name.c_str(), n.port);
    }
    
    _state->set_node_state(states);
}

void meta_server_failure_detector::on_worker_connected(const end_point& node)
{
    if (!is_primary())
    {
        return;
    }

    node_states states;
    states.push_back(std::make_pair(node, true));

    dwarn("Client reconnected",
        "Client %s:%hu", node.name.c_str(), node.port);

    _state->set_node_state(states);
}

bool meta_server_failure_detector::set_primary(bool is_primary /*= false*/)
{
    bool bRet = true;
    if (is_primary && !_is_primary)
    {
        node_states ns;
        _state->get_node_state(ns);

        for (auto& pr : ns)
        {
            register_worker(pr.first, pr.second);
        }

        _is_primary = true;
    }

    if (!is_primary && _is_primary)
    {
        clear_workers();
        _is_primary = false;
    }

    return bRet;
}

bool meta_server_failure_detector::is_primary() const
{
    return _is_primary;
}

void meta_server_failure_detector::on_ping(const fd::beacon_msg& beacon, ::dsn::service::rpc_replier<fd::beacon_ack>& reply)
{
    fd::beacon_ack ack;
    ack.this_node = beacon.to;
    if (!is_primary())
    {
        end_point master;
        if (_state->get_meta_server_primary(master))
        {
            ack.time = beacon.time;
            ack.is_master = false;
            ack.primary_node = master;
        }
        else
        {
            ack.time = beacon.time;
            ack.is_master = false;
            ack.primary_node =  end_point::INVALID;
        }
    }
    else
    {
        failure_detector::on_ping_internal(beacon, ack);
        ack.primary_node = primary_address();
    }

    reply(ack);
}

