/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

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
#include "server_state.h"


server_state::server_state(void)
{
    _leader_index = -1;
    init_app();
}

server_state::~server_state(void)
{
}

void server_state::init_app()
{
    app_state app;
    app.app_id = 1;
    app.app_name = "TestTable";
    app.app_type = "simple_kv";
    app.partition_count = 1;
    for (int i = 0; i < app.partition_count; i++)
    {
        partition_configuration ps;
        ps.app_type = app.app_type;
        ps.ballot = 0;
        ps.gpid.app_id = app.app_id;
        ps.gpid.pidx = i;
        ps.last_committed_decree = 0;
        ps.max_replica_count = 3;

        app.partitions.push_back(ps);
    }

    zauto_write_lock l(_lock);
    _apps.push_back(app);
}

void server_state::get_node_state(__out_param node_states& nodes)
{
    zauto_read_lock l(_lock);
    for (auto it = _nodes.begin(); it != _nodes.end(); it++)
    {
        nodes.push_back(std::make_pair(it->first, it->second.is_alive));
    }
}

void server_state::set_node_state(const node_states& nodes)
{
    zauto_write_lock l(_lock);
    for (auto& itr : nodes)
    {
        auto it = _nodes.find(itr.first);
        if (it != _nodes.end())
            it->second.is_alive = itr.second;
        else
        {
            node_state n;
            n.address = itr.first;
            n.is_alive = itr.second;

            _nodes[itr.first] = n;
        }
    }
}

bool server_state::get_meta_server_primary(__out_param end_point& node)
{
    zauto_read_lock l(_meta_lock);
    if (-1 == _leader_index)
        return false;
    else
    {
        node = _meta_servers[_leader_index];
        return true;
    }
}

void server_state::add_meta_node(const end_point& node)
{
    zauto_write_lock l(_meta_lock);
    
    _meta_servers.push_back(node);
    if (1 == _meta_servers.size())
        _leader_index = 0;
}

void server_state::remove_meta_node(const end_point& node)
{
    zauto_write_lock l(_meta_lock);
    
    int i = -1;
    for (auto it = _meta_servers.begin(); it != _meta_servers.end(); it++)
    {
        i++;
        if (*it == node)
        {
            _meta_servers.erase(it);
            if (_meta_servers.size() == 0)
                _leader_index = -1;

            else if (i == _leader_index)
            {
                _leader_index = env::random32(0, (uint32_t)_meta_servers.size() - 1);
            }
            return;
        }
    }

    dassert (false, "cannot find node '%s:%u' in server state", node.name.c_str(), static_cast<int>(node.port));
}

void server_state::switch_meta_primary()
{
    zauto_write_lock l(_meta_lock);
    if (1 == _meta_servers.size())
        return;

    while (true)
    {
        int r = env::random32(0, (uint32_t)_meta_servers.size() - 1);
        if (r != _leader_index)
        {
            _leader_index = r;
            return;
        }
    }
}

// partition server & client => meta server
void server_state::query_configuration_by_node(configuration_query_by_node_request& request, __out_param configuration_query_by_node_response& response)
{
    zauto_read_lock l(_lock);
    auto it = _nodes.find(request.node);
    if (it == _nodes.end())
    {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
    else
    {
        response.err = ERR_SUCCESS;

        for (auto& p : it->second.partitions)
        {
            response.partitions.push_back(_apps[p.app_id - 1].partitions[p.pidx]);
        }
    }
}

void server_state::query_configuration_by_index(configuration_query_by_index_request& request, __out_param configuration_query_by_index_response& response)
{
    zauto_read_lock l(_lock);

    for (size_t i = 0; i < _apps.size(); i++)
    {
        app_state& kv = _apps[i];
        if (kv.app_name == request.app_name)
        {
            response.err = ERR_SUCCESS;
            app_state& app = kv;
            for (auto& idx : request.partition_indices)
            {
                if (idx < app.partition_count)
                { 
                    response.partitions.push_back(app.partitions[idx]);
                }
            }
            return;
        }
    }

    response.err = ERR_OBJECT_NOT_FOUND;
}

void server_state::update_configuration(configuration_update_request& request, __out_param configuration_update_response& response)
{
    zauto_write_lock l(_lock);

    app_state& app = _apps[request.config.gpid.app_id - 1];
    partition_configuration& old = app.partitions[request.config.gpid.pidx];
    if (old.ballot + 1 == request.config.ballot)
    {
        response.err = ERR_SUCCESS;

        old = request.config;
        response.config = request.config;

        node_state& node = _nodes[request.node];

        switch (request.type)
        {
        case CT_ASSIGN_PRIMARY:
            node.partitions.insert(old.gpid);
            node.primaries.insert(old.gpid);
            break;
        case CT_ADD_SECONDARY:
            node.partitions.insert(old.gpid);
            break;
        case CT_DOWNGRADE_TO_SECONDARY:
            node.primaries.erase(old.gpid);
            break;
        case CT_DOWNGRADE_TO_INACTIVE:
        case CT_REMOVE:
            node.partitions.erase(old.gpid);
            node.primaries.erase(old.gpid);
            break;
        case CT_UPGRADE_TO_SECONDARY:
            node.partitions.insert(old.gpid);
            break;
        default:
            dassert (false, "invalid config type %x", static_cast<int>(request.type));
        }
    }
    else
    {
        response.err = ERR_INVALID_VERSION;
        response.config = old;
    }
}
