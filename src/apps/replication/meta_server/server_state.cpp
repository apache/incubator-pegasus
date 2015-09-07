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
# include "server_state.h"
# include <sstream>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "meta.server.state"

void marshall(binary_writer& writer, const app_state& val)
{
    marshall(writer, val.app_type);
    marshall(writer, val.app_name);
    marshall(writer, val.app_id);
    marshall(writer, val.partition_count);
    marshall(writer, val.partitions);
}

void unmarshall(binary_reader& reader, /*out*/ app_state& val)
{
    unmarshall(reader, val.app_type);
    unmarshall(reader, val.app_name);
    unmarshall(reader, val.app_id);
    unmarshall(reader, val.partition_count);
    unmarshall(reader, val.partitions);
}


server_state::server_state(void)
{
    _leader_index = -1;
    _node_live_count = 0;
    _freeze = true;
    _node_live_percentage_threshold_for_update = 65;
}

server_state::~server_state(void)
{
}


void server_state::load(const char* chk_point)
{
    FILE* fp = ::fopen(chk_point, "rb");

    int32_t len;
    ::fread((void*)&len, sizeof(int32_t), 1, fp);

    std::shared_ptr<char> buffer(new char[len]);
    ::fread((void*)buffer.get(), len, 1, fp);

    blob bb(buffer, 0, len);
    binary_reader reader(bb);
    unmarshall(reader, _apps);

    ::fclose(fp);

    dassert(_apps.size() == 1, "");
    auto& app = _apps[0];
    for (int i = 0; i < app.partition_count; i++)
    {
        auto& ps = app.partitions[i];

        if (ps.primary.is_invalid() == false)
        {
            _nodes[ps.primary].primaries.insert(ps.gpid);
            _nodes[ps.primary].partitions.insert(ps.gpid);
        }
        
        for (auto& ep : ps.secondaries)
        {
            dassert(ep.is_invalid() == false, "");
            _nodes[ep].partitions.insert(ps.gpid);
        }
    }

    for (auto& node : _nodes)
    {
        node.second.address = node.first;
        node.second.is_alive = true;
        _node_live_count++;
    }

    for (auto& app : _apps)
    {
        for (auto& par : app.partitions)
        {
            check_consistency(par.gpid);
        }
    }
}

void server_state::save(const char* chk_point)
{
    binary_writer writer;
    marshall(writer, _apps);

    FILE* fp = ::fopen(chk_point, "wb+");

    int32_t len = writer.total_size();
    ::fwrite((const void*)&len, sizeof(len), 1, fp);

    std::vector<blob> bbs;
    writer.get_buffers(bbs);

    for (auto& bb : bbs)
    {
        ::fwrite((const void*)bb.data(), bb.length(), 1, fp);
    }

    ::fclose(fp);
}

void server_state::init_app()
{
    zauto_write_lock l(_lock);
    if (_apps.size() > 0)
        return;

    app_state app;
    app.app_id = 1;
    app.app_name = dsn_config_get_value_string("replication.app",
        "app_name", "", "replication app name");
    dassert(app.app_name.length() > 0, "'[replication.app] app_name' not specified");
    app.app_type = dsn_config_get_value_string("replication.app",
        "app_type", "", "replication app type-name");
    dassert(app.app_type.length() > 0, "'[replication.app] app_type' not specified");
    app.partition_count = (int)dsn_config_get_value_uint64("replication.app", 
        "partition_count", 1, "how many partitions the app should have");

    int32_t max_replica_count = (int)dsn_config_get_value_uint64("replication.app",
        "max_replica_count", 3, "maximum replica count for each partition");
    for (int i = 0; i < app.partition_count; i++)
    {
        partition_configuration ps;
        ps.app_type = app.app_type;
        ps.ballot = 0;
        ps.gpid.app_id = app.app_id;
        ps.gpid.pidx = i;
        ps.last_committed_decree = 0;
        ps.max_replica_count = max_replica_count;
        ps.primary.set_invalid();
        
        app.partitions.push_back(ps);
    }
    
    _apps.push_back(app);
}

void server_state::get_node_state(/*out*/ node_states& nodes)
{
    zauto_read_lock l(_lock);
    for (auto it = _nodes.begin(); it != _nodes.end(); it++)
    {
        nodes.push_back(std::make_pair(it->first, it->second.is_alive));
    }
}

void server_state::set_node_state(const node_states& nodes, /*out*/ machine_fail_updates* pris)
{
    zauto_write_lock l(_lock);

    auto old_lc = _node_live_count;
    
    for (auto& itr : nodes)
    {
        dassert(itr.first.is_invalid() == false, "");

        auto it = _nodes.find(itr.first);
        if (it != _nodes.end())
        {
            bool old = it->second.is_alive;
            it->second.is_alive = itr.second;
            
            if (!old && itr.second)
                _node_live_count++;
            else if (old && !itr.second)
            {
                _node_live_count--;

                if (pris)
                {
                    for (auto& pri : it->second.primaries)
                    {
                        app_state& app = _apps[pri.app_id - 1];
                        partition_configuration& old = app.partitions[pri.pidx];

                        dassert(old.primary == it->first, "");

                        auto request = std::shared_ptr<configuration_update_request>(new configuration_update_request());
                        request->node = old.primary;
                        request->type = CT_DOWNGRADE_TO_INACTIVE;
                        request->config = old;
                        request->config.ballot++;
                        request->config.primary.set_invalid();

                        (*pris)[pri] = request;
                    }
                }
            }   
        }   
        else
        {
            node_state n;
            n.address = itr.first;
            n.is_alive = itr.second;

            _nodes[itr.first] = n;

            if (n.is_alive)
                _node_live_count++;
        }
    }

    if (_node_live_count != old_lc)
    {
        _freeze = (_node_live_count * 100 < _node_live_percentage_threshold_for_update * static_cast<int>(_nodes.size()));
        dinfo("live replica server # changes from %d to %d, freeze = %s", old_lc, _node_live_count, _freeze ? "true":"false");
    }
}

void server_state::unfree_if_possible_on_start()
{
    zauto_write_lock l(_lock);
    _freeze = (_node_live_count * 100 < _node_live_percentage_threshold_for_update * static_cast<int>(_nodes.size()));
    dinfo("live replica server # is %d, freeze = %s", _node_live_count, _freeze ? "true" : "false");
}

bool server_state::get_meta_server_primary(/*out*/ ::dsn::rpc_address& node)
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

void server_state::add_meta_node(const ::dsn::rpc_address& node)
{
    zauto_write_lock l(_meta_lock);
    
    _meta_servers.push_back(node);
    if (1 == _meta_servers.size())
        _leader_index = 0;
}

void server_state::remove_meta_node(const ::dsn::rpc_address& node)
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
                _leader_index = dsn_random32(0, (uint32_t)_meta_servers.size() - 1);
            }
            return;
        }
    }

    dassert (false, "cannot find node '%s:%hu' in server state", node.name(), node.port());
}

void server_state::switch_meta_primary()
{
    zauto_write_lock l(_meta_lock);
    if (1 == _meta_servers.size())
        return;

    while (true)
    {
        int r = dsn_random32(0, (uint32_t)_meta_servers.size() - 1);
        if (r != _leader_index)
        {
            _leader_index = r;
            return;
        }
    }
}

// partition server & client => meta server
void server_state::query_configuration_by_node(configuration_query_by_node_request& request, /*out*/ configuration_query_by_node_response& response)
{
    zauto_read_lock l(_lock);
    auto it = _nodes.find(request.node);
    if (it == _nodes.end())
    {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
    else
    {
        response.err = ERR_OK;

        for (auto& p : it->second.partitions)
        {
            response.partitions.push_back(_apps[p.app_id - 1].partitions[p.pidx]);
        }
    }
}

void server_state::query_configuration_by_gpid(global_partition_id id, /*out*/ partition_configuration& config)
{
    zauto_read_lock l(_lock);
    config = _apps[id.app_id - 1].partitions[id.pidx];
}

void server_state::query_configuration_by_index(configuration_query_by_index_request& request, /*out*/ configuration_query_by_index_response& response)
{
    zauto_read_lock l(_lock);

    for (size_t i = 0; i < _apps.size(); i++)
    {
        app_state& kv = _apps[i];
        if (kv.app_name == request.app_name)
        {
            response.err = ERR_OK;
            response.app_id = static_cast<int>(i) + 1;
            response.partition_count = kv.partition_count;
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

void server_state::update_configuration(configuration_update_request& request, /*out*/ configuration_update_response& response)
{
    zauto_write_lock l(_lock);

    update_configuration_internal(request, response);
}

void server_state::update_configuration_internal(configuration_update_request& request, /*out*/ configuration_update_response& response)
{
    app_state& app = _apps[request.config.gpid.app_id - 1];
    partition_configuration& old = app.partitions[request.config.gpid.pidx];
    if (old.ballot + 1 == request.config.ballot)
    {
        response.err = ERR_OK;
        response.config = request.config;
        
        auto it = _nodes.find(request.node);
        dassert(it != _nodes.end(), "");
        node_state& node = it->second;

        switch (request.type)
        {
        case CT_ASSIGN_PRIMARY:
# ifdef _DEBUG
            dassert(old.primary != request.node, "");
            dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) == old.secondaries.end(), "");
# endif
            node.partitions.insert(old.gpid);
            node.primaries.insert(old.gpid);
            break; 
        case CT_UPGRADE_TO_PRIMARY:
# ifdef _DEBUG
            dassert(old.primary != request.node, "");
            dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) != old.secondaries.end(), "");
# endif
            node.partitions.insert(old.gpid);
            node.primaries.insert(old.gpid);
            break;
        case CT_ADD_SECONDARY:
            dassert(false, "invalid execution flow");
            break;
        case CT_DOWNGRADE_TO_SECONDARY:
# ifdef _DEBUG
            dassert(old.primary == request.node, "");
            dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) == old.secondaries.end(), "");
# endif
            node.primaries.erase(old.gpid);
            break;
        case CT_DOWNGRADE_TO_INACTIVE:
        case CT_REMOVE:
# ifdef _DEBUG
            dassert(old.primary == request.node || 
                std::find(old.secondaries.begin(), old.secondaries.end(), request.node) != old.secondaries.end(), "");
# endif
            if (request.node == old.primary)
            {
                node.primaries.erase(old.gpid);
            }
            node.partitions.erase(old.gpid);            
            break;
        case CT_UPGRADE_TO_SECONDARY:
# ifdef _DEBUG
            dassert(old.primary != request.node, "");
            dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) == old.secondaries.end(), "");
# endif
            node.partitions.insert(old.gpid);
            break;
        default:
            dassert(false, "invalid config type %x", static_cast<int>(request.type));
        }
        
        // update to new config
        old = request.config;

        std::stringstream cf;
        cf << "{primary:" << request.config.primary.name() << ":" << request.config.primary.port() << ", secondaries = [";
        for (auto& s : request.config.secondaries)
        {
            cf << s.name() << ":" << s.port() << ",";
        }
        cf << "]}";

        ddebug("%d.%d metaupdateok to ballot %lld, type = %s, config = %s",
            request.config.gpid.app_id,
            request.config.gpid.pidx,
            request.config.ballot,
            enum_to_string(request.type),
            cf.str().c_str()
            );
    }
    else
    {
        response.err = ERR_INVALID_VERSION;
        response.config = old;
    }

#ifdef _DEBUG
    check_consistency(request.config.gpid);
#endif
}

void server_state::check_consistency(global_partition_id gpid)
{
    app_state& app = _apps[gpid.app_id - 1];
    partition_configuration& config = app.partitions[gpid.pidx];

    if (config.primary.is_invalid() == false)
    {
        auto it = _nodes.find(config.primary);
        dassert(it != _nodes.end(), "");
        dassert(it->second.primaries.find(gpid) != it->second.primaries.end(), "");
        dassert(it->second.partitions.find(gpid) != it->second.partitions.end(), "");
    }
    
    for (auto& ep : config.secondaries)
    {
        auto it = _nodes.find(ep);
        dassert(it != _nodes.end(), "");
        dassert(it->second.partitions.find(gpid) != it->second.partitions.end(), "");
    }

    int lc = 0;
    for (auto& ep : _nodes)
    {
        if (ep.second.is_alive)
            lc++;
    }    
    dassert(_node_live_count == lc, "");
}