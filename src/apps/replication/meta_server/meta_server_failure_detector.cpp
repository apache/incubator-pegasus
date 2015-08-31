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
#include "meta_service.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "meta.server.FD"

meta_server_failure_detector::meta_server_failure_detector(server_state* state, meta_service* svc)
{
    _state = state;
    _svc = svc;
    _is_primary = false;
}

meta_server_failure_detector::~meta_server_failure_detector(void)
{
}

void meta_server_failure_detector::on_worker_disconnected(const std::vector<::dsn::rpc_address>& nodes)
{
    if (!is_primary())
    {
        return;
    }

    node_states states;
    for (auto& n : nodes)
    {
        states.push_back(std::make_pair(n, false));

        dwarn("client expired: %s:%hu", n.name(), n.port());
    }
    
    machine_fail_updates pris;
    _state->set_node_state(states, &pris);
    
    for (auto& pri : pris)
    {
        dinfo("%d.%d primary node for %s:%hu is gone, update configuration on meta server", 
            pri.first.app_id,
            pri.first.pidx,
            pri.second->node.name(),
            pri.second->node.port()
            );
        _svc->update_configuration(pri.second);
    }
}

void meta_server_failure_detector::on_worker_connected(const ::dsn::rpc_address& node)
{
    if (!is_primary())
    {
        return;
    }

    node_states states;
    states.push_back(std::make_pair(node, true));

    dwarn("Client reconnected",
        "Client %s:%hu", node.name(), node.port());

    _state->set_node_state(states, nullptr);
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

void meta_server_failure_detector::on_ping(const fd::beacon_msg& beacon, ::dsn::rpc_replier<fd::beacon_ack>& reply)
{
    fd::beacon_ack ack;
    ack.this_node = beacon.to;
    if (!is_primary())
    {
        ::dsn::rpc_address master;
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
            ack.primary_node.set_invalid();
        }
    }
    else
    {
        failure_detector::on_ping_internal(beacon, ack);
        ack.primary_node = primary_address();
    }

    reply(ack);
}

