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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include "server_state.h"
#include <dsn/internal/factory_store.h>
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
    : ::dsn::serverlet<server_state>("meta.server.state")
{
    _node_live_count = 0;
    _freeze = true;
    _node_live_percentage_threshold_for_update = 65;
}

server_state::~server_state(void)
{
}

DEFINE_TASK_CODE(LPC_META_STATE_SVC_CALLBACK, TASK_PRIORITY_COMMON, THREAD_POOL_META_SERVER);

void server_state::initialize()
{
    _storage = dsn::utils::factory_store<::dsn::dist::meta_state_service>::create(
        "meta_state_service_simple",  // TODO: read config
        PROVIDER_TYPE_MAIN
        );

    auto err = _storage->initialize();
    dassert(err == ERR_OK, "meta state service initialization failed, err = %s", err.to_string());
}

error_code server_state::on_become_leader()
{
    _apps.clear();
    _nodes.clear();
    auto err = sync_apps_from_remote_storage();
    if (err == ERR_OBJECT_NOT_FOUND)
        err = initialize_apps();
    return err;
}

error_code server_state::initialize_apps()
{
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

    error_code err = ERR_OK;
    clientlet tracker;

    // create root /apps node
    std::string path = "/apps";
    auto t = _storage->create_node(path, LPC_META_STATE_SVC_CALLBACK, 
        [&err](error_code ec)
        {err = ec; }
    );
    t->wait();

    if (err != ERR_NODE_ALREADY_EXIST && err != ERR_OK)
    {
        derror("create root node /apps in meta store failed, err = %s",
            err.to_string());
        return err;
    }

    // create /apps/app_name node
    std::stringstream ss;
    ss << "/apps/" << app.app_name;
    path = ss.str();

    binary_writer writer;
    marshall(writer, app);
    
    blob value = writer.get_buffer();

    _storage->create_node(path, LPC_META_STATE_SVC_CALLBACK,
        [&err, this, &app, &tracker](error_code ec)
        {
            if (ec == ERR_OK)
            {
                int32_t max_replica_count = (int)dsn_config_get_value_uint64("replication.app",
                    "max_replica_count", 3, "maximum replica count for each partition");

                for (int i = 0; i < app.partition_count; i++)
                {
                    std::stringstream ss;
                    ss << "/apps/" << app.app_name << "/" << i;
                    std::string path = ss.str();

                    partition_configuration ps;
                    ps.app_type = app.app_type;
                    ps.ballot = 0;
                    ps.gpid.app_id = app.app_id;
                    ps.gpid.pidx = i;
                    ps.last_committed_decree = 0;
                    ps.max_replica_count = max_replica_count;
                    ps.primary.set_invalid();

                    app.partitions.push_back(ps);

                    binary_writer writer;
                    marshall(writer, ps);

                    blob value = writer.get_buffer();

                    _storage->create_node(path, LPC_META_STATE_SVC_CALLBACK,
                        [&err, this, &app](error_code ec)
                        {
                            if (ec == ERR_OK)
                            {

                            }
                            else
                            {
                                err = ec;
                            }
                        },
                        value,
                        &tracker
                        );
                }
                
            }
            else
            {
                err = ec;
            }
        },
        value,
        &tracker
        );
    
    // wait for all those tasks completed
    dsn_task_tracker_wait_all(tracker.tracker());

    if (err == ERR_OK)
    {
        _apps.push_back(app);
    }

    return err;
}

error_code server_state::sync_apps_from_remote_storage()
{
    error_code err = ERR_OK;
    clientlet tracker(1);
    // get all apps
    std::string root = "/apps";
    _storage->get_children(root, LPC_META_STATE_SVC_CALLBACK,
        [&](error_code ec, std::vector<std::string>&& apps)
        {
            if (ec == ERR_OK)
            {
                // get app info
                for (auto& s : apps)
                {
                    auto app_path = root + "/" + s;
                    _storage->get_data(
                        app_path,
                        LPC_META_STATE_SVC_CALLBACK,
                        [this, app_path, &err, &tracker](error_code ec, blob&& value)
                    {
                        if (ec == ERR_OK)
                        {
                            binary_reader reader(value);
                            app_state state;
                            unmarshall(reader, state);

                            int app_id = state.app_id;
                            dassert(app_id != 0, "invalid app id");
                            {
                                zauto_write_lock l(_lock);
                                if (app_id > _apps.size())
                                {
                                    _apps.resize(app_id);
                                }
                                _apps[app_id - 1] = state;
                                _apps[app_id - 1].partitions.resize(state.partition_count);
                            }

                            // get partition info
                            for (int i = 0; i < state.partition_count; i++)
                            {
                                std::stringstream ss;
                                ss << app_path << "/" << i;
                                auto par_path = ss.str();
                                _storage->get_data(
                                    par_path,
                                    LPC_META_STATE_SVC_CALLBACK,
                                    [this, app_id, i, &err](error_code ec, blob&& value)
                                    {
                                        if (ec == ERR_OK)
                                        {
                                            binary_reader reader(value);
                                            partition_configuration pc;
                                            unmarshall(reader, pc);
                                            zauto_write_lock l(_lock);
                                            _apps[app_id - 1].partitions[i] = pc;
                                        }
                                        else
                                        {
                                            derror("get partition info from meta state service failed, err = %s",
                                                ec.to_string());
                                            err = ec;
                                        }
                                    },
                                    &tracker
                                    );
                            }
                        }
                        else
                        {
                            derror("get app info from meta state service failed, err = %s",
                                ec.to_string());
                            err = ec;
                        }
                    },
                        &tracker
                        );
                }
            }
            else
            {
                derror("get root info from meta state service failed, err = %s",
                    ec.to_string());
                err = ec;
            }
        },
        &tracker
        );

    // wait for all those tasks completed
    dsn_task_tracker_wait_all(tracker.tracker());

    if (err == ERR_OK)
    {
        if (_apps.size() == 0)
            return ERR_OBJECT_NOT_FOUND;

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

    return err;
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

// partition server & client => meta server
void server_state::query_configuration_by_node(const configuration_query_by_node_request& request, /*out*/ configuration_query_by_node_response& response)
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

void server_state::query_configuration_by_index(const configuration_query_by_index_request& request, /*out*/ configuration_query_by_index_response& response)
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

DEFINE_TASK_CODE(LPC_META_SERVER_STATE_UPDATE_CALLBACK, TASK_PRIORITY_HIGH, THREAD_POOL_META_SERVER)

void server_state::update_configuration(
    std::shared_ptr<configuration_update_request>& req,
    dsn_message_t request_msg, 
    std::function<void()> callback)
{
    bool write;
    configuration_update_response response; 
    std::string partition_path;

    {
        zauto_read_lock l(_lock);
        app_state& app = _apps[req->config.gpid.app_id - 1];
        partition_configuration& old = app.partitions[req->config.gpid.pidx];
        if (old.ballot + 1 != req->config.ballot)
        {
            write = false;   
            response.err = ERR_INVALID_VERSION;
            response.config = old;
        }
        else
        {
            write = true;
            std::stringstream ss;
            ss << "/apps/" << app.app_name << "/" << old.gpid.pidx;
            partition_path = ss.str();
            req->config.last_drops = old.last_drops;
        }
    }

    if (!write)
    {
        if (request_msg)
        {
            reply(request_msg, response);
        }
        else
        {
            dwarn("meta state update failed and request msg is nullptr");
        }
    }
    else
    {
        if (request_msg)
        {
            dsn_msg_add_ref(request_msg);
        }

        // maintain dropouts
        switch (req->type)
        {
        case CT_ASSIGN_PRIMARY:
        case CT_ADD_SECONDARY:
        case CT_UPGRADE_TO_SECONDARY:
            maintain_drops(req->config.last_drops, req->node, true);
            break;
        case CT_DOWNGRADE_TO_INACTIVE:
        case CT_REMOVE:
            maintain_drops(req->config.last_drops, req->node, false);
            break;
        }
        
        binary_writer writer;
        marshall(writer, req->config);

        blob new_config_blob = writer.get_buffer();
        _storage->set_data(
            partition_path,
            new_config_blob,
            LPC_META_SERVER_STATE_UPDATE_CALLBACK,
            [this, new_config_blob, req, request_msg, callback](error_code ec)
            {                
                storage_work_item wi;
                wi.ballot = req->config.ballot;
                wi.req = req;
                wi.msg = request_msg;
                wi.callback = callback;

                {
                    zauto_lock l(_pending_requests_lock);
                    _pending_requests[wi.ballot] = wi;
                }

                exec_pending_requests();
            }
        );
    }
}

void server_state::exec_pending_requests()
{
    do
    {
        storage_work_item wi;
        {
            zauto_lock l(_pending_requests_lock);
            if (_pending_requests.size() == 0)
                return;

            storage_work_item& fwi = _pending_requests.begin()->second;

            {
                zauto_read_lock l(_lock);
                app_state& app = _apps[fwi.req->config.gpid.app_id - 1];
                partition_configuration& old = app.partitions[fwi.req->config.gpid.pidx];
                if (old.ballot + 1 != fwi.req->config.ballot)
                {
                    return;
                }
            }

            wi = fwi;
            _pending_requests.erase(_pending_requests.begin());
        }
        
        configuration_update_response resp;
        {
            zauto_write_lock l(_lock);
            update_configuration_internal(*wi.req, resp);
        }

        if (wi.msg)
        {
            reply(wi.msg, resp);
            dsn_msg_release_ref(wi.msg);
        }

        if (wi.callback)
        {
            wi.callback();
        }

    } while (true);
}

/*static*/ void server_state::maintain_drops(/*inout*/ std::vector<rpc_address>& drops, const rpc_address& node, bool is_add)
{
    auto it = std::find(drops.begin(), drops.end(), node);
    if (is_add)
    {
        if (it != drops.end())
            drops.erase(it);
    }
    else
    {        
        if (it == drops.end())
        {
            drops.push_back(node);
            if (drops.size() > 3)
                drops.erase(drops.begin());
        }
        else
        {
            dassert(false, "the node cannot be in drops set before this update", node.to_string());
        }
    }
}

void server_state::update_configuration_internal(const configuration_update_request& request, /*out*/ configuration_update_response& response)
{
    app_state& app = _apps[request.config.gpid.app_id - 1];
    partition_configuration& old = app.partitions[request.config.gpid.pidx];
    if (old.ballot + 1 != request.config.ballot)
    {
        response.err = ERR_INVALID_VERSION;
        response.config = old;
    }
    else
    {
        // TODO: update _storage first

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
            dassert(false, "invalid config type 0x%x", static_cast<int>(request.type));
        }
        
        // maintain dropouts
        auto drops = old.last_drops; 
        switch (request.type)
        {
        case CT_ASSIGN_PRIMARY:
        case CT_ADD_SECONDARY:
        case CT_UPGRADE_TO_SECONDARY:
            maintain_drops(drops, request.node, true);
            break;
        case CT_DOWNGRADE_TO_INACTIVE:
        case CT_REMOVE:
            maintain_drops(drops, request.node, false);
            break;
        }
        
        // update to new config        
        old = request.config;
        old.last_drops = drops;
        
        std::stringstream cf;
        cf << "{primary:" << request.config.primary.to_string() << ", secondaries = [";
        for (auto& s : request.config.secondaries)
        {
            cf << s.to_string() << ",";
        }
        cf << "], drops = [";
        for (auto& s : drops)
        {
            cf << s.to_string() << ",";
        }
        cf << "]}";

        ddebug("%d.%d metaupdateok to ballot %lld, type = %s, node = %s, config = %s",
            request.config.gpid.app_id,
            request.config.gpid.pidx,
            request.config.ballot,
            enum_to_string(request.type),
            request.node.to_string(),
            cf.str().c_str()
            );
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

        auto it2 = std::find(config.last_drops.begin(), config.last_drops.end(), config.primary);
        dassert(it2 == config.last_drops.end(), "");
    }
    
    for (auto& ep : config.secondaries)
    {
        auto it = _nodes.find(ep);
        dassert(it != _nodes.end(), "");
        dassert(it->second.partitions.find(gpid) != it->second.partitions.end(), "");

        auto it2 = std::find(config.last_drops.begin(), config.last_drops.end(), ep);
        dassert(it2 == config.last_drops.end(), "");
    }

    int lc = 0;
    for (auto& ep : _nodes)
    {
        if (ep.second.is_alive)
            lc++;
    }    
    dassert(_node_live_count == lc, "");
}