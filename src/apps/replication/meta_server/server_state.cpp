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
#include "server_state.h"


server_state::server_state(void)
{
    _leaderIndex = -1;
    InitApp();
}

server_state::~server_state(void)
{
}

void server_state::InitApp()
{
    AppState app;
    app.AppId = 0;
    app.AppName = "TestTable";
    app.AppType = "SimpleKV";
    app.PartitionCount = 1;
    for (int i = 0; i < app.PartitionCount; i++)
    {
        partition_configuration ps;
        ps.app_type = app.AppType;
        ps.ballot = 0;
        ps.gpid.tableId = app.AppId;
        ps.gpid.pidx = i;
        ps.lastCommittedDecree = 0;

        app.Partitions.push_back(ps);
    }

    zauto_write_lock l(_lock);
    _apps.push_back(app);
}

void server_state::GetNodeState(__out NodeStates& nodes)
{
    zauto_read_lock l(_lock);
    for (auto it = _nodes.begin(); it != _nodes.end(); it++)
    {
        nodes.push_back(std::make_pair(it->first, it->second.IsAlive));
    }
}

void server_state::SetNodeState(const NodeStates& nodes)
{
    zauto_write_lock l(_lock);
    for (auto& itr : nodes)
    {
        auto it = _nodes.find(itr.first);
        if (it != _nodes.end())
            it->second.IsAlive = itr.second;
        else
        {
            NodeState n;
            n.address = itr.first;
            n.IsAlive = itr.second;

            _nodes[itr.first] = n;
        }
    }
}

bool server_state::GetMetaServerPrimary(__out end_point& node)
{
    zauto_read_lock l(_metaLock);
    if (-1 == _leaderIndex)
        return false;
    else
    {
        node = _metaServers[_leaderIndex];
        return true;
    }
}

void server_state::AddMetaNode(const end_point& node)
{
    zauto_write_lock l(_metaLock);
    
    _metaServers.push_back(node);
    if (1 == _metaServers.size())
        _leaderIndex = 0;
}

void server_state::RemoveMetaNode(const end_point& node)
{
    zauto_write_lock l(_metaLock);
    
    int i = -1;
    for (auto it = _metaServers.begin(); it != _metaServers.end(); it++)
    {
        i++;
        if (*it == node)
        {
            _metaServers.erase(it);
            if (_metaServers.size() == 0)
                _leaderIndex = -1;

            else if (i == _leaderIndex)
            {
                _leaderIndex = env::random32(0, (uint32_t)_metaServers.size() - 1);
            }
            return;
        }
    }

    rassert(false, "cannot find node '%s:%u' in server state", node.name.c_str(), (int)node.port);
}

void server_state::SwitchMetaPrimary()
{
    zauto_write_lock l(_metaLock);
    if (1 == _metaServers.size())
        return;

    while (true)
    {
        int r = env::random32(0, (uint32_t)_metaServers.size() - 1);
        if (r != _leaderIndex)
        {
            _leaderIndex = r;
            return;
        }
    }
}

// partition server & client => meta server
void server_state::OnQueryConfig(ConfigurationNodeQueryRequest& request, __out ConfigurationNodeQueryResponse& response)
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

        for (auto& p : it->second.Partitions)
        {
            response.partitions.push_back(_apps[p.tableId].Partitions[p.pidx]);
        }
    }
}

void server_state::DoQueryConfigurationByIndexRequest(QueryConfigurationByIndexRequest& request, __out QueryConfigurationByIndexResponse& response)
{
    zauto_read_lock l(_lock);

    for (size_t i = 0; i < _apps.size(); i++)
    {
        AppState& kv = _apps[i];
        if (kv.AppName == request.app_name)
        {
            response.err = ERR_SUCCESS;
            AppState& app = kv;
            for (auto& idx : request.parIdxes)
            {
                if (idx < (unsigned int)app.PartitionCount)
                { 
                    response.partitions.push_back(app.Partitions[idx]);
                }
            }
            return;
        }
    }

    response.err = ERR_OBJECT_NOT_FOUND;
}

void server_state::update_configuration(configuration_update_request& request, __out ConfigurationUpdateResponse& response)
{
    zauto_write_lock l(_lock);

    AppState& app = _apps[request.config.gpid.tableId];
    partition_configuration& old = app.Partitions[request.config.gpid.pidx];
    if (old.ballot + 1 == request.config.ballot)
    {
        response.err = ERR_SUCCESS;

        old = request.config;
        response.config = request.config;

        NodeState& node = _nodes[request.node];

        switch (request.type)
        {
        case CT_ASSIGN_PRIMARY:
            node.Partitions.insert(old.gpid);
            node.Primaries.insert(old.gpid);
            break;
        case CT_ADD_SECONDARY:
            node.Partitions.insert(old.gpid);
            break;
        case CT_DOWNGRADE_TO_SECONDARY:
            node.Primaries.erase(old.gpid);
            break;
        case CT_DOWNGRADE_TO_INACTIVE:
        case CT_REMOVE:
            node.Partitions.erase(old.gpid);
            node.Primaries.erase(old.gpid);
            break;
        case CT_UPGRADE_TO_SECONDARY:
            node.Partitions.insert(old.gpid);
            break;
        default:
            rassert(false, "invalid config type %x", (int)request.type);
        }
    }
    else
    {
        response.err = ERR_INVALID_VERSION;
        response.config = old;
    }
}
