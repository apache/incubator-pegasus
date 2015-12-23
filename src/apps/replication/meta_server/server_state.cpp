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
# include <dsn/internal/factory_store.h>
# include <sstream>
# include <cinttypes>
# include <boost/lexical_cast.hpp>

# include <rapidjson/document.h>
# include <rapidjson/prettywriter.h>
# include <rapidjson/writer.h>
# include <rapidjson/stringbuffer.h>

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

/**
 * app_state:
 * {
 *   "app_type": "whatever",
 *   "app_id": 11,
 *   "app_name": "you like",
 *   "partition_count": 2323,
 * }
 */
void marshall_json(blob& output, const app_state& app)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

    writer.StartObject();
    writer.String("app_type"); writer.String(app.app_type.c_str());
    writer.String("app_name"); writer.String(app.app_name.c_str());
    writer.String("app_id"); writer.Int(app.app_id);
    writer.String("partition_count"); writer.Int(app.partition_count);
    writer.EndObject();

    std::shared_ptr<char> outptr(new char[buffer.GetSize()], [](char* ptr){ delete []ptr; } );
    memcpy(outptr.get(), buffer.GetString(), buffer.GetSize());
    output.assign(outptr, 0, buffer.GetSize());
}

static const char* partition_status_str[] = {
    "inactive", "error", "primary", "secondary",
    "potential_secondary", "invalid", nullptr
};
const char* get_partition_status_string(partition_status ps)
{
    return partition_status_str[ps];
}
partition_status get_partition_status(const char* status_str)
{
    for (int i=0; partition_status_str[i]!=nullptr; ++i)
        if (strcmp(partition_status_str[i], status_str)==0)
            return (partition_status)i;
    return PS_INVALID;
}
/**
 * Example partition config:
 * {"ballot": 1, "last_committed_decree": 2, "max_replica_count": 3,
 *  "entries": [{"addr": "xxx.xxx.xxx.xxx:12345", "partition_status": "primary"},
 *              {"addr": "xxx.xxx.xxx.xxb:23424", "partition_status": "secondary"}
 *             ]}
 * type of role: primary/secondary/potential primary/potential secondary/inactive
 **/
void marshall_json(blob& output, const partition_configuration& pc)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

    auto end_point_gen_json = [&writer](const ::dsn::rpc_address& ep, partition_status ps) {
        if (ep.is_invalid())
            return;
        writer.StartObject();
        writer.String("addr"); writer.String( ep.to_string() );
        writer.String("partition_status"); writer.String( get_partition_status_string(ps) );
        writer.EndObject();
    };

    writer.StartObject();

    writer.String("app_type"); writer.String(pc.app_type.c_str());

    std::stringstream gpid;
    gpid << pc.gpid.app_id << "." << pc.gpid.pidx;
    writer.String("gpid"); writer.String( gpid.str().c_str() );

    writer.String("ballot"); writer.Int64(pc.ballot);
    writer.String("max_replica_count"); writer.Int(pc.max_replica_count);
    writer.String("last_committed_decree"); writer.Int64(pc.last_committed_decree);

    writer.String("entries");
    writer.StartArray();
    end_point_gen_json(pc.primary, PS_PRIMARY);
    for (unsigned int i=0; i!=pc.secondaries.size(); ++i)
        end_point_gen_json(pc.secondaries[i], PS_SECONDARY);
    for (unsigned int i=0; i!=pc.last_drops.size(); ++i)
        end_point_gen_json(pc.last_drops[i], PS_INACTIVE);
    writer.EndArray();

    writer.EndObject();

    std::shared_ptr<char> outptr(new char[buffer.GetSize()], [](char* ptr){ delete []ptr; } );
    memcpy(outptr.get(), buffer.GetString(), buffer.GetSize());
    output.assign(outptr, 0, buffer.GetSize());
}

void unmarshall(binary_reader& reader, /*out*/ app_state& val)
{
    unmarshall(reader, val.app_type);
    unmarshall(reader, val.app_name);
    unmarshall(reader, val.app_id);
    unmarshall(reader, val.partition_count);
    unmarshall(reader, val.partitions);
}

void unmarshall_json(const blob& buf, app_state& app)
{
    rapidjson::Document doc;
    std::string input(buf.data(), buf.length());
    if (input.empty() || doc.Parse(input.c_str()).HasParseError() )
        return;

    app.app_type = doc["app_type"].GetString();
    app.app_id = doc["app_id"].GetInt();
    app.app_name = doc["app_name"].GetString();
    app.partition_count = doc["partition_count"].GetInt();

    partition_configuration pc;
    pc.app_type = app.app_type;
    pc.ballot = 0;
    pc.gpid.app_id = app.app_id;
    pc.last_committed_decree = 0;
    pc.max_replica_count = 3;

    app.partitions.assign(app.partition_count, pc);
    for (unsigned int i=0; i!=app.partition_count; ++i)
        app.partitions[i].gpid.pidx = i;
}

void unmarshall_json(const blob& buf, partition_configuration& pc)
{
    rapidjson::Document doc;
    std::string input(buf.data(), buf.length());

    if ( input.empty() || doc.Parse(input.c_str()).HasParseError())
        return;

    pc.app_type = doc["app_type"].GetString();
    sscanf(doc["gpid"].GetString(), "%d.%d", &pc.gpid.app_id, &pc.gpid.pidx);
    pc.ballot = doc["ballot"].GetInt64();
    pc.max_replica_count = doc["max_replica_count"].GetInt();
    pc.last_committed_decree = doc["last_committed_decree"].GetInt();
    pc.primary.set_invalid();

    rapidjson::Value& entries = doc["entries"];
    for (rapidjson::SizeType i=0; i<entries.Size(); ++i) {
        rapidjson::Value& val = entries[i];
        ::dsn::rpc_address ep;
        ep.from_string_ipv4(val["addr"].GetString());
        partition_status ps = get_partition_status(val["partition_status"].GetString());
        switch (ps) {
        case PS_PRIMARY:
            pc.primary = ep;
            break;
        case PS_SECONDARY:
            pc.secondaries.push_back(ep);
            break;
        default:
            pc.last_drops.push_back(ep);
            break;
        }
    }
}

server_state::server_state()
    : ::dsn::serverlet<server_state>("meta.server.state"), _cli_json_state_handle(nullptr)
{
    _node_live_count = 0;
    _node_live_percentage_threshold_for_update = 65;
    _freeze = true;
    _storage = nullptr;
}

server_state::~server_state()
{
    if (_storage != nullptr)
    {
        delete _storage;
        _storage = nullptr;
    }
    if (_cli_json_state_handle != nullptr)
    {
        dsn_cli_deregister(_cli_json_state_handle);
        _cli_json_state_handle = nullptr;
    }
}

DEFINE_TASK_CODE(LPC_META_STATE_SVC_CALLBACK, TASK_PRIORITY_COMMON, THREAD_POOL_META_SERVER);

error_code server_state::initialize()
{
    const char* cluster_root = dsn_config_get_value_string(
        "meta_server",
        "cluster_root",
        "/",
        "cluster root of meta server"
        );
    const char* meta_state_service_type = dsn_config_get_value_string(
        "meta_server",
        "meta_state_service_type",
        "meta_state_service_simple",
        "meta_state_service provider type"
        );
    const char* meta_state_service_parameters = dsn_config_get_value_string(
        "meta_server",
        "meta_state_service_parameters",
        "",
        "meta_state_service provider parameters"
        );

    // prepare parameters
    std::vector<std::string> args;
    dsn::utils::split_args(meta_state_service_parameters, args);
    int argc = static_cast<int>(args.size());
    std::vector<const char*> args_ptr;
    args_ptr.resize(argc);
    for (int i = argc - 1; i >= 0; i--)
    {
        args_ptr[i] = args[i].c_str();
    }

    // create storage
    _storage = dsn::utils::factory_store< ::dsn::dist::meta_state_service>::create(
        meta_state_service_type,
        PROVIDER_TYPE_MAIN
        );
    error_code err = _storage->initialize(argc, argc > 0 ? &args_ptr[0] : nullptr);
    if (err != ERR_OK)
    {
        derror("init meta_state_service failed, err = %s", err.to_string());
        return err;
    }

    // prepare cluster root
    std::vector<std::string> slices;
    utils::split_args(cluster_root, slices, '/');
    std::string current = "";
    for (unsigned int i = 0; i != slices.size(); ++i)
    {
        current = join_path(current, slices[i]);
        task_ptr tsk = _storage->create_node(current, LPC_META_STATE_SVC_CALLBACK,
            [&err](error_code ec)
            {
                err = ec;
            }
        );
        tsk->wait();
        if (err != ERR_OK && err != ERR_NODE_ALREADY_EXIST)
        {
            derror("create node failed, node_path = %s, err = %s", current.c_str(), err.to_string());
            return err;
        }
    }
    _cluster_root = current.empty() ? "/" : current;

    dassert(_cli_json_state_handle == nullptr, "server state is initialized twice");
    _cli_json_state_handle = dsn_cli_app_register("info", "get info of nodes and apps on meta_server", "", this, &static_cli_json_state, &static_cli_json_state_cleanup);
    dassert(_cli_json_state_handle != nullptr, "register cil handler failed, maybe it has been registered");

    ddebug("init server_state succeed, cluster_root = %s", _cluster_root.c_str());
    return ERR_OK;
}

error_code server_state::on_become_leader()
{
    _apps.clear();
    _nodes.clear();
    _pending_requests.clear();
    auto err = sync_apps_from_remote_storage();
    if (err == ERR_OBJECT_NOT_FOUND)
        err = initialize_apps();
    return err;
}

std::string server_state::join_path(const std::string& input1, const std::string& input2)
{
    size_t pos1 = input1.size(); // last_valid_pos + 1
    while (pos1 > 0 && input1[pos1-1] == '/') pos1--;
    size_t pos2 = 0; // first non '/' position
    while (pos2 < input2.size() && input2[pos2] == '/') pos2++;
    return input1.substr(0, pos1) + "/" + input2.substr(pos2);
}

error_code server_state::initialize_apps()
{
    ddebug("start to do initialize");
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

    // create  cluster_root/apps node
    std::string apps_path = join_path(_cluster_root, "apps");
    auto t = _storage->create_node(apps_path, LPC_META_STATE_SVC_CALLBACK,
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

    // create cluster_root/apps/app_name node
    std::string app_path = join_path(apps_path, app.app_name);
    blob value;
    marshall_json(value, app);

    _storage->create_node(app_path, LPC_META_STATE_SVC_CALLBACK,
        [&err, this, &app, &tracker, &app_path](error_code ec)
        {
            err = ec;
            if (ec == ERR_OK)
            {
                int32_t max_replica_count = (int)dsn_config_get_value_uint64("replication.app",
                    "max_replica_count", 3, "maximum replica count for each partition");

                for (int i = 0; i < app.partition_count; i++)
                {
                    std::string partition_path = join_path(app_path, boost::lexical_cast<std::string>(i));

                    partition_configuration ps;
                    ps.app_type = app.app_type;
                    ps.ballot = 0;
                    ps.gpid.app_id = app.app_id;
                    ps.gpid.pidx = i;
                    ps.last_committed_decree = 0;
                    ps.max_replica_count = max_replica_count;
                    ps.primary.set_invalid();

                    app.partitions.push_back(ps);

                    blob value;
                    marshall_json(value, ps);

                    _storage->create_node(partition_path, LPC_META_STATE_SVC_CALLBACK,
                        [&err, this, &app](error_code ec)
                        {
                            err = ec;
                        },
                        value,
                        &tracker
                        );
                }
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
    ddebug("start to do sync");
    error_code err = ERR_OK;
    clientlet tracker(1);
    // get all apps

    std::string app_root = join_path(_cluster_root, "apps");
    _storage->get_children(app_root, LPC_META_STATE_SVC_CALLBACK,
        [&](error_code ec, const std::vector<std::string>& apps)
        {
            err = ec;
            if (ec == ERR_OK)
            {
                // get app info
                for (auto& s : apps)
                {
                    auto app_path = join_path(app_root, s);
                    _storage->get_data(
                        app_path,
                        LPC_META_STATE_SVC_CALLBACK,
                        [this, app_path, &err, &tracker](error_code ec, const blob& value)
                    {
                        err = ec;
                        if (ec == ERR_OK)
                        {
                            app_state state;
                            unmarshall_json(value, state);
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
                                auto par_path = join_path(app_path, boost::lexical_cast<std::string>(i));
                                _storage->get_data(
                                    par_path,
                                    LPC_META_STATE_SVC_CALLBACK,
                                    [this, app_id, i, par_path, &err](error_code ec, const blob& value)
                                    {
                                        err = ec;
                                        if (ec == ERR_OK)
                                        {
                                            partition_configuration pc;
                                            unmarshall_json(value, pc);
                                            zauto_write_lock l(_lock);
                                            _apps[app_id - 1].partitions[i] = pc;
                                        }
                                        else
                                        {
                                            derror("get partition info from meta state service failed, path = %s, err = %s",
                                                par_path.c_str(), ec.to_string());
                                        }
                                    },
                                    &tracker
                                    );
                            }
                        }
                        else
                        {
                            derror("get app info from meta state service failed, path = %s, err = %s",
                                app_path.c_str(), ec.to_string());
                        }
                    },
                        &tracker
                        );
                }
            }
            else
            {
                derror("get app list from meta state service failed, path = %s, err = %s",
                    app_root.c_str(), ec.to_string());
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

        for (app_state& app: _apps) 
        {
            for (int i = 0; i < app.partition_count; i++)
            {
                auto& ps = app.partitions[i];
                auto refresh_state = [this](rpc_address addr, bool is_primary_role, global_partition_id gpid) -> bool {
                    auto result_pair = this->_nodes.insert( std::make_pair(addr, node_state()) );
                    node_state& ns = result_pair.first->second;
                    ns.address = addr;
                    ns.partitions.insert(gpid);
                    if ( is_primary_role )
                        ns.primaries.insert(gpid);
                    
                    if (result_pair.second==false || ns.is_alive==false) {
                        this->_node_live_count++;
                        ns.is_alive = true;
                    }
                };
                
                if ( !ps.primary.is_invalid() )
                    refresh_state(ps.primary, true, ps.gpid);
                for (auto& ep: ps.secondaries)
                {
                    dassert(ep.is_invalid() == false, "");
                    refresh_state(ep, false, ps.gpid);
                }
            }
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

void server_state::set_config_change_subscriber_for_test(config_change_subscriber subscriber)
{
    _config_change_subscriber = subscriber;
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
        if (partition_configuration_equal(old, req->config))
        {
            // duplicate request
            dwarn("received duplicate update configuration request from %s, gpid = %d.%d, ballot = %" PRId64,
                  req->node.to_string(), old.gpid.app_id, old.gpid.pidx, old.ballot);
            write = false;
            response.err = ERR_OK;
            response.config = old;
        }
        else if (old.ballot + 1 != req->config.ballot)
        {
            dwarn("received invalid update configuration request from %s, gpid = %d.%d, ballot = %" PRId64 ", cur_ballot = %" PRId64,
                  req->node.to_string(), old.gpid.app_id, old.gpid.pidx, req->config.ballot, old.ballot);
            write = false;   
            response.err = ERR_INVALID_VERSION;
            response.config = old;
        }
        else
        {
            write = true;
            auto apps_path = join_path(_cluster_root, "apps");
            auto app_path = join_path(apps_path, app.app_name);
            partition_path = join_path(app_path, boost::lexical_cast<std::string>(old.gpid.pidx));
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

        blob new_config_blob;
        marshall_json(new_config_blob, req->config);
        _storage->set_data(
            partition_path,
            new_config_blob,
            LPC_META_SERVER_STATE_UPDATE_CALLBACK,
            [this, new_config_blob, req, request_msg, callback](error_code ec)
            {
                // TODO: should check error_code first

                global_partition_id gpid = req->config.gpid;
                storage_work_item wi;
                wi.ballot = req->config.ballot;
                wi.req = req;
                wi.msg = request_msg;
                wi.callback = callback;

                {
                    zauto_lock l(_pending_requests_lock);
                    _pending_requests[gpid][wi.ballot] = wi;
                }

                exec_pending_requests(gpid);
            }
        );
    }
}

void server_state::exec_pending_requests(global_partition_id gpid)
{
    do
    {
        storage_work_item wi;
        {
            zauto_lock l(_pending_requests_lock);
            auto& part = _pending_requests[gpid];
            if (part.size() == 0)
                return;

            storage_work_item& fwi = part.begin()->second;
            {
                zauto_read_lock l2(_lock);
                app_state& app = _apps[gpid.app_id - 1];
                partition_configuration& old = app.partitions[gpid.pidx];
                if (old.ballot + 1 != fwi.ballot)
                {
                    return;
                }
            }

            wi = fwi;
            part.erase(part.begin());
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

        ddebug("%d.%d meta update ok to ballot %" PRId64 ", type = %s, node = %s, config = %s",
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

    if (_config_change_subscriber)
    {
        _config_change_subscriber(_apps);
    }
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

bool server_state::partition_configuration_equal(const partition_configuration& pc1, const partition_configuration& pc2)
{
    // last_drops is not considered into equality check
    return pc1.ballot == pc2.ballot &&
           pc1.gpid.app_id == pc2.gpid.app_id &&
           pc1.gpid.pidx == pc2.gpid.pidx &&
           pc1.app_type == pc2.app_type &&
           pc1.max_replica_count == pc2.max_replica_count &&
           pc1.primary == pc2.primary &&
           pc1.secondaries == pc2.secondaries &&
           pc1.last_committed_decree == pc2.last_committed_decree;
}

void server_state::json_state(std::stringstream& out) const
{
    zauto_read_lock _(_lock);
    JSON_DICT_ENTRIES(out, *this, _nodes, _apps);
}

void server_state::static_cli_json_state(void* context, int argc, const char** argv, dsn_cli_reply* reply)
{
    auto _server_state = reinterpret_cast<server_state*>(context);
    std::stringstream out;
    _server_state->json_state(out);
    auto danglingstring = new std::string(std::move(out.str()));
    reply->message = danglingstring->c_str();
    reply->size = danglingstring->size();
    reply->context = danglingstring;
}

void server_state::static_cli_json_state_cleanup(dsn_cli_reply reply)
{
    dassert(reply.context != nullptr, "corrupted cli reply context");
    auto danglingstring = reinterpret_cast<std::string*>(reply.context);
    dassert(reply.message == danglingstring->c_str(), "corrupted cli reply message");
    delete danglingstring;
}
