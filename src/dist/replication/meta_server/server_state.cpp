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

# include "../zookeeper/zookeeper_session_mgr.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "meta.server.state"

# include "dump_file.h"
# include "meta_service.h"

int32_t server_state::_default_max_replica_count = 3;

/// misc functions for server state
void marshall(binary_writer& writer, const app_state& val)
{
    marshall(writer, val.status);
    marshall(writer, val.app_type);
    marshall(writer, val.app_name);
    marshall(writer, val.app_id);
    marshall(writer, val.partition_count);
    marshall(writer, val.partitions);
}

void marshall_json(blob& output, const app_state& app, bool available_status)
{
    rapidjson::StringBuffer buffer;
    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

    writer.StartObject();
    writer.String("app_type"); writer.String(app.app_type.c_str());
    writer.String("app_name"); writer.String(app.app_name.c_str());
    writer.String("app_id"); writer.Int(app.app_id);
    writer.String("partition_count"); writer.Int(app.partition_count);
    writer.String("status"); writer.String(available_status?"available":"dropped");
    writer.EndObject();

    std::shared_ptr<char> outptr(new char[buffer.GetSize()], [](char* ptr){ delete []ptr; } );
    memcpy(outptr.get(), buffer.GetString(), buffer.GetSize());
    output.assign(outptr, 0, buffer.GetSize());
}

void marshall_json(blob& output, const partition_configuration& pc)
{
    std::stringstream out;
    json_encode(out, pc);
    std::string buffer = out.str();
    std::shared_ptr<char> outptr(new char[buffer.size()], std::default_delete<char[]>{});
    memcpy(outptr.get(), buffer.c_str(), buffer.size());
    output.assign(outptr, 0, buffer.size());
}

void unmarshall(binary_reader& reader, /*out*/ app_state& val)
{
    unmarshall(reader, val.status);
    unmarshall(reader, val.app_type);
    unmarshall(reader, val.app_name);
    unmarshall(reader, val.app_id);
    unmarshall(reader, val.partition_count);
    unmarshall(reader, val.partitions);
}

void init_partition_configuration(/*out*/partition_configuration& pc, /*in*/const app_state& app, int32_t max_replica_count)
{
    pc.app_type = app.app_type;
    pc.ballot = 0;
    pc.gpid.app_id = app.app_id;
    pc.last_committed_decree = 0;
    pc.max_replica_count = max_replica_count;
    pc.primary.set_invalid();
    pc.secondaries.clear();
    pc.last_drops.clear();
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
    app.status = strcmp(doc["status"].GetString(), "available")==0?AS_AVAILABLE:AS_DROPPED;
    partition_configuration pc;
    init_partition_configuration(pc, app, server_state::_default_max_replica_count);
    app.partitions.assign(app.partition_count, pc);
    for (unsigned int i=0; i!=app.partition_count; ++i)
        app.partitions[i].gpid.pidx = i;
    app.partition_assists.resize(app.partition_count);
}

void unmarshall_json(const blob& buf, partition_configuration& pc)
{
    rapidjson::Document doc;
    std::string input(buf.data(), buf.length());

    dinfo("partition config json: %s", input.c_str());
    if ( input.empty() || doc.Parse(input.c_str()).HasParseError())
        return;

    pc.app_type = doc["app_type"].GetString();
    pc.gpid.app_id = doc["gpid"]["app_id"].GetInt();
    pc.gpid.pidx = doc["gpid"]["pidx"].GetInt();
    pc.ballot = doc["ballot"].GetInt64();
    pc.max_replica_count = doc["max_replica_count"].GetInt();
    pc.last_committed_decree = doc["last_committed_decree"].GetInt();

    pc.primary.set_invalid();
    pc.primary.from_string_ipv4(doc["primary"].GetString());

    pc.secondaries.resize( doc["secondaries"].Size() );
    for (unsigned int i=0; i!=pc.secondaries.size(); ++i)
        pc.secondaries[i].from_string_ipv4(doc["secondaries"][i].GetString());

    pc.last_drops.resize( doc["last_drops"].Size() );
    for (unsigned int i=0; i!=pc.last_drops.size(); ++i)
        pc.last_drops[i].from_string_ipv4(doc["last_drops"][i].GetString());
}

void maintain_drops(/*inout*/ std::vector<rpc_address>& drops, const rpc_address& node, bool is_add)
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

std::string join_path(const std::string& input1, const std::string& input2)
{
    size_t pos1 = input1.size(); // last_valid_pos + 1
    while (pos1 > 0 && input1[pos1-1] == '/') pos1--;
    size_t pos2 = 0; // first non '/' position
    while (pos2 < input2.size() && input2[pos2] == '/') pos2++;
    return input1.substr(0, pos1) + "/" + input2.substr(pos2);
}

/// server state member functions
server_state::server_state(meta_service* meta_svc)
    : ::dsn::serverlet<server_state>("meta.server.state"),
    _meta_svc(meta_svc), _cli_json_state_handle(nullptr), _cli_dump_handle(nullptr)
{
    _node_live_count = 0;
    _node_live_percentage_threshold_for_update = 50;
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

error_code server_state::dump_from_remote_storage(const char *format, const char *local_path, bool sync_immediately)
{
    error_code ec;
    std::shared_ptr<dump_file> file = dump_file::open_file(local_path, true);
    if (file == nullptr)
    {
        derror("open file failed, file(%s)", local_path);
        return ERR_FILE_OPERATION_FAILED;
    }

    if (sync_immediately && ((ec=sync_apps_from_remote_storage())!=ERR_OK)) {
        if (ec == ERR_OBJECT_NOT_FOUND) {
            dwarn("remote storage is empty, just stop the dump");
            return ERR_OK;
        }
        else {
            derror("sync from remote storage failed, err(%s)", ec.to_string());
            return ec;
        }
    }

    file->append_buffer(format, strlen(format));

    size_t apps_count = _apps.size();
    for (size_t i=0; i!=apps_count; ++i)
    {
        app_state snapshot;
        {
            zauto_read_lock l(_lock);
            snapshot = _apps[i];
        }
        if (snapshot.status != AS_AVAILABLE)
        {
            snapshot.partition_count = 0;
            snapshot.partitions.clear();
            snapshot.partition_assists.clear();
        }

        if (strcmp(format, "json") == 0)
        {
            blob data;
            marshall_json(data, snapshot, snapshot.status==AS_AVAILABLE);
            file->append_buffer(data);
            for (const partition_configuration& pc: snapshot.partitions)
            {
                marshall_json(data, pc);
                file->append_buffer(data);
            }
        }
        else if (strcmp(format, "binary") == 0)
        {
            binary_writer writer;
            marshall(writer, snapshot);
            file->append_buffer(writer.get_buffer());
        }
        else
            return ERR_INVALID_PARAMETERS;
    }
    return ERR_OK;
}

error_code server_state::restore_from_local_storage(const char* local_path, bool write_back_to_remote_storage)
{
    error_code ec;

    std::shared_ptr<dump_file> file = dump_file::open_file(local_path, false);
    if (file == nullptr)
    {
        derror("open file failed, file(%s)", local_path);
        return ERR_FILE_OPERATION_FAILED;
    }

    blob data;
    dassert(file->read_next_buffer(data)==1, "read format header fail");
    _apps.clear();

    if ( memcmp(data.data(), "json", 4)==0 )
    {
        while ( true )
        {
            int ans = file->read_next_buffer(data);
            dassert(ans != -1, "read file failed");
            if (ans == 0) //file end
                break;

            _apps.push_back( app_state() );
            app_state& app = _apps.back();
            unmarshall_json(data, app);

            app.partitions.resize(app.partition_count);
            app.partition_assists.resize(app.partition_count);
            for (unsigned int i=0; i!=app.partition_count; ++i)
            {
                ans = file->read_next_buffer(data);
                dassert(ans == 1, "unexpect read buffer, ret(%d)", ans);
                unmarshall_json(data, app.partitions[i]);
                dassert(app.partitions[i].gpid.pidx==i, "uncorrect partition data, gpid(%d.%d), appname(%s)", app.app_id, i, app.app_name.c_str());
            }
        }
    }
    else if ( memcmp(data.data(), "binary", 6)==0 )
    {
        while (true)
        {
            int ans = file->read_next_buffer(data);
            dassert(ans != -1, "read file failed");
            if (ans == 0) break;

            _apps.push_back( app_state() );
            binary_reader reader(data);
            unmarshall(reader, _apps.back());
        }
    }
    else
    {
        dassert(false, "unsupported format");
    }

    if (write_back_to_remote_storage)
    {
        ec = sync_apps_to_remote_storage();
    }

    for (const app_state& app: _apps)
    {
        while (app.status!=AS_DROPPED && app.available_partitions.load() != app.partition_count)
            std::this_thread::sleep_for(std::chrono::seconds(1));
    }
    return ec;
}

DEFINE_TASK_CODE(LPC_META_STATE_SVC_CALLBACK, TASK_PRIORITY_COMMON, THREAD_POOL_META_SERVER);

error_code server_state::initialize()
{
    _default_max_replica_count = (int32_t)dsn_config_get_value_uint64(
        "meta_server",
        "default_max_replica_count",
        3,
        "the default value of max_replica_count for all apps, default is 3");

    _node_live_percentage_threshold_for_update = (int32_t)dsn_config_get_value_uint64(
        "meta_server",
        "node_live_percentage_threshold_for_update",
        50,
        "if live_node_count * 100 < total_node_count * node_live_percentage_threshold_for_update, then freeze the cluster; default is 50");

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
    ddebug("create meta_state_service: %s", meta_state_service_type);
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
    _apps_root = join_path(_cluster_root, "apps");
    dassert(_cli_json_state_handle == nullptr, "server state is initialized twice");
    _cli_json_state_handle = dsn_cli_app_register("info", "get info of nodes and apps on meta_server", "", this, &static_cli_json_state, &static_cli_json_state_cleanup);
    dassert(_cli_json_state_handle != nullptr, "register cil handler failed, maybe it has been registered");

    _cli_dump_handle = dsn_cli_app_register(
        "dump",
        "dump app_states of meta server to local file",
        "usage: -f|--format [json|binary] -t|--target target_file",
        this,
        &static_cli_dump_app_states,
        &static_cli_dump_app_states_cleanup
    );
    dassert(_cli_dump_handle != nullptr, "register cli handler failed");

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

std::string server_state::get_app_path(const app_state &app) const
{
    return _apps_root + "/" + boost::lexical_cast<std::string>(app.app_id);
}

std::string server_state::get_partition_path(const app_state &app, int partition_id) const
{
    std::stringstream oss;
    oss << _apps_root << "/" << app.app_id
        << "/" << partition_id;
    return oss.str();
}

std::string server_state::get_partition_path(const global_partition_id& gpid) const
{
    std::stringstream oss;
    oss << _apps_root << "/" << gpid.app_id
        << "/" << gpid.pidx;
    return oss.str();
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
    app.status = AS_CREATING;
    app.available_partitions.store(0);
    _apps.push_back(app);

    partition_configuration pc;
    init_partition_configuration(pc, app,
        dsn_config_get_value_uint64("replication.app", "max_replica_count", 3, "max replica count in app"));

    std::vector<partition_configuration>& partitions = _apps.back().partitions;
    partitions.resize(app.partition_count, pc);
    _apps.back().partition_assists.resize(app.partition_count);
    for (unsigned int i=0; i!=partitions.size(); ++i)
        partitions[i].gpid.pidx = i;

    error_code err = sync_apps_to_remote_storage();
    if (err != ERR_OK)
    {
        _apps.pop_back();
        return err;
    }
    return ERR_OK;
}

error_code server_state::sync_apps_to_remote_storage()
{
    // create  cluster_root/apps node
    std::string& apps_path = _apps_root;
    error_code err;
    auto t = _storage->create_node(apps_path, LPC_META_STATE_SVC_CALLBACK,
        [&err](error_code ec)
        { err = ec; }
    );
    t->wait();

    if (err != ERR_NODE_ALREADY_EXIST && err != ERR_OK)
    {
        derror("create root node /apps in meta store failed, err = %s",
            err.to_string());
        return err;
    }

    // the caller of sync_apps_to_remote_storage must ensure the
    // app_status is creating
    err = ERR_OK;
    clientlet tracker(1);
    for (const app_state& app: _apps)
    {
        std::string path = get_app_path(app);
        blob value;
        marshall_json(value, app, true);
        _storage->create_node(path,
            LPC_META_STATE_SVC_CALLBACK,
            [&err, path](error_code ec)
            {
                if (ec != ERR_OK && ec != ERR_NODE_ALREADY_EXIST)
                {
                    dwarn("create app node failed, path(%s) reason(%s)", path.c_str(), ec.to_string());
                    err = ec;
                }
                else
                {
                    ddebug("create app node %s ok", path.c_str());
                }
            },
            value,
            &tracker);
    }

    dsn_task_tracker_wait_all(tracker.tracker());
    if (err != ERR_OK)
        return err;

    for (app_state& app: _apps)
    {
        if (app.status == AS_DROPPED)
            continue;
        for (unsigned int i=0; i!=app.partition_count; ++i)
            init_app_partition_node(app.app_id, i);
    }
    return ERR_OK;
}

error_code server_state::sync_apps_from_remote_storage()
{
    ddebug("start to do sync");
    error_code err = ERR_OK;
    clientlet tracker(1);
    // get all apps

    std::string& app_root = _apps_root;
    _storage->get_children(app_root, LPC_META_STATE_SVC_CALLBACK,
        [&](error_code ec, const std::vector<std::string>& apps)
        {
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

                                if (state.status == AS_DROPPED)
                                {
                                    _apps[app_id - 1] = state;
                                    return;
                                }
                                else
                                {
                                    state.status = AS_CREATING;
                                    state.available_partitions.store(0);
                                    _apps[app_id - 1] = state;
                                    _apps[app_id - 1].partitions.resize(state.partition_count);
                                    _apps[app_id - 1].partition_assists.resize(state.partition_count);
                                }
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
                                        if (ec == ERR_OK)
                                        {
                                            partition_configuration pc;
                                            unmarshall_json(value, pc);
                                            zauto_write_lock l(_lock);
                                            _apps[app_id - 1].partitions[i] = pc;
                                            dassert(pc.gpid.app_id == app_id && pc.gpid.pidx == i, "invalid partition config");

                                            _apps[app_id -1].available_partitions++;
                                        }
                                        else if (ec == ERR_OBJECT_NOT_FOUND)
                                        {
                                            dwarn("partition node %s not exist on remote storage, may half create before", par_path.c_str());
                                            init_partition_configuration(_apps[app_id - 1].partitions[i],
                                                _apps[app_id - 1],
                                                _default_max_replica_count);
                                            _apps[app_id - 1].partitions[i].gpid.pidx = i;
                                            init_app_partition_node(app_id, i);
                                        }
                                        else
                                        {
                                            derror("get partition node failed, reason(%s)", ec.to_string());
                                            err = ec;
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
                            err = ec;
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

        //all the app is creating right now
        for (app_state& app: _apps)
        {
            if (app.available_partitions == app.partition_count)
            {
                app.status = AS_AVAILABLE;
            }
        }

        for (app_state& app: _apps) 
        {
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
    for (auto it = _nodes.begin(); it != _nodes.end(); ++it)
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
        _freeze = set_freeze();
        ddebug("live replica server # changes from %d to %d, freeze = %s", old_lc, _node_live_count, _freeze ? "true":"false");
    }
}

void server_state::apply_cache_nodes()
{
    node_states alive_list;
    for (auto& node: _cache_alive_nodes)
        alive_list.push_back( std::make_pair(node, true) );
    set_node_state(alive_list, nullptr);
}

void server_state::unfree_if_possible_on_start()
{
    zauto_write_lock l(_lock);
    _freeze = set_freeze();
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
    int32_t index = get_app_index(request.app_name.c_str());
    if ( -1 == index) {
        response.err = ERR_OBJECT_NOT_FOUND;
        return;
    }

    app_state& app = _apps[index];
    if ( app.status != AS_AVAILABLE ) {
        response.err = ERR_INVALID_STATE;
        return;
    }

    response.err = ERR_OK;
    response.app_id = app.app_id;
    response.partition_count = app.partition_count;

    for (const int32_t& index: request.partition_indices) {
        if (index>=0 && index<app.partitions.size())
            response.partitions.push_back( app.partitions[index]);
    }
    if (response.partitions.empty())
        response.partitions = app.partitions;
}

int32_t server_state::get_app_index(const char *app_name) const
{
    for (const app_state& app: _apps)
        if ( strcmp(app.app_name.c_str(), app_name) == 0 && app.status!=AS_DROPPED)
            return app.app_id-1;
    return -1;
}

DEFINE_TASK_CODE(LPC_META_SERVER_STATE_UPDATE_CALLBACK, TASK_PRIORITY_HIGH, THREAD_POOL_META_SERVER)
DEFINE_TASK_CODE(LPC_CM_UPDATE_CONFIGURATION, TASK_PRIORITY_HIGH, THREAD_POOL_META_SERVER)

void server_state::init_app_partition_node(int app_id, int pidx)
{
    auto on_create_app_partition = [this, app_id, pidx](error_code ec)
    {
        app_state& app = _apps[app_id - 1];
        if (ERR_OK==ec || ERR_NODE_ALREADY_EXIST==ec)
        {
            int avail_count = ++app.available_partitions;
            if (avail_count == app.partition_count)
            {
                ddebug("partition init finished, the app(name:%s, id:%d) is available", app.app_name.c_str(), app.app_id);
                zauto_write_lock l(_lock);
                app.status = AS_AVAILABLE;
            }
            else
            {
                dinfo("available partition are %d, add partition node gpid(%d.%d) ok", avail_count, app_id, pidx);
            }
        }
        else if (ERR_TIMEOUT == ec)
        {
            dwarn("create partition node failed, gpid(%d.%d), retry later", app_id, pidx);
            //TODO: add parameter of the retry time interval in config file
            tasking::enqueue(LPC_META_SERVER_STATE_UPDATE_CALLBACK,
                             this,
                             std::bind(&server_state::init_app_partition_node, this, app_id, pidx),
                             0,
                             std::chrono::milliseconds(1000));
        }
        else
        {
            dassert(false, "we can't handle this error in init app partition nodes err(%s), gpid(%d.%d)", ec.to_string(), app_id, pidx);
        }
    };

    app_state& app = _apps[app_id - 1];
    std::string app_partition_path = get_partition_path(app, pidx);
    blob value;
    marshall_json(value, app.partitions[pidx]);
    _storage->create_node(app_partition_path,
        LPC_META_SERVER_STATE_UPDATE_CALLBACK,
        on_create_app_partition,
        value);
}

void server_state::initialize_app(app_state& app, dsn_message_t msg)
{
    dsn_msg_add_ref(msg);
    auto on_create_app_root = [this, msg, &app](error_code ec)
    {
        configuration_create_app_response resp;
        if (ERR_OK==ec || ERR_NODE_ALREADY_EXIST==ec)
        {
            dinfo("create app on storage service ok, name: %s, appid %" PRId32 "", app.app_name.c_str(), app.app_id);
            resp.appid = app.app_id;
            resp.err = ERR_OK;
            reply(msg, resp);
            for (unsigned int i=0; i!=app.partition_count; ++i)
            {
                init_app_partition_node(app.app_id, i);
            }
        }
        else if (ERR_TIMEOUT == ec)
        {
            dwarn("the storage service is not available currently, just ignore this request");
            {
                zauto_write_lock l(_lock);
                app.status = AS_CREATE_FAILED;
            }
        }
        else
        {
            dassert(false, "we can't handle this right now, err(%s)", ec.to_string());
        }
        dsn_msg_release_ref(msg);
    };

    blob value;
    std::string app_dir = get_app_path(app);
    marshall_json(value, app, true);
    _storage->create_node(
        app_dir,
        LPC_META_SERVER_STATE_UPDATE_CALLBACK,
        on_create_app_root,
        value);
}

void server_state::create_app(dsn_message_t msg)
{
    configuration_create_app_request request;
    configuration_create_app_response response;
    bool will_create_app = false;
    int32_t index;
    ::unmarshall(msg ,request);
    
    ddebug("create app request, name(%s), type(%s), partition_count(%d), replica_count(%d)",
           request.app_name.c_str(),
           request.options.app_type.c_str(),
           request.options.partition_count,
           request.options.replica_count);

    auto option_match_check = [](const create_app_options& opt, const app_state& exist_app) {
        return opt.partition_count==exist_app.partition_count &&
               opt.app_type==exist_app.app_type;
    };

    {
        zauto_write_lock l(_lock);
        index = get_app_index(request.app_name.c_str());
        /* so we can't store the data on meta_state_service with app_name, but app_id */
        if (index != -1 && _apps[index].status!=AS_DROPPED)
        {
            app_state& exist_app = _apps[index];
            switch (exist_app.status)
            {
            case AS_AVAILABLE:
                if (!request.options.success_if_exist || !option_match_check(request.options, exist_app))
                    response.err = ERR_INVALID_PARAMETERS;
                else {
                    response.err = ERR_OK;
                    response.appid = exist_app.app_id;
                }
                break;
            case AS_CREATING:
                response.err = ERR_BUSY_CREATING;
                break;
            case AS_CREATE_FAILED:
                exist_app.status = AS_CREATING;
                will_create_app = true;
                break;
            case AS_DROPPING:
            case AS_DROP_FAILED:
                response.err = ERR_BUSY_DROPPING;
            default:
                break;
            }
        }
        else {
            will_create_app = true;
            index = _apps.size();
            _apps.push_back(app_state());
            app_state& app = _apps.back();

            //the app_id is started from 1!!!
            app.app_id = index + 1;
            app.app_name = request.app_name;
            app.app_type = request.options.app_type;
            app.partition_count = request.options.partition_count;
            app.available_partitions.store(0);

            partition_configuration pc;
            init_partition_configuration(pc, app, request.options.replica_count);

            app.partitions.resize(app.partition_count, pc);
            app.partition_assists.resize(app.partition_count);
            for (int i=0; i!=app.partitions.size(); ++i)
                app.partitions[i].gpid.pidx = i;

            app.status = AS_CREATING;
        }
    }

    if ( will_create_app ) {
        initialize_app(_apps[index], msg);
    }
    else
        reply(msg, response);
}

void server_state::do_app_drop(app_state& app, dsn_message_t msg)
{
    blob value;
    marshall_json(value, app, false);

    std::string app_path = get_app_path(app);

    dsn_msg_add_ref(msg);
    auto after_set_app_dropped = [this, &app, msg](error_code ec) {
        configuration_drop_app_response response;
        if (ERR_OK == ec || ERR_OBJECT_NOT_FOUND == ec)
        {
            {
                zauto_write_lock l(_lock);
                app.status = AS_DROPPED;
                for (partition_configuration& pc: app.partitions) {
                    if (!pc.primary.is_invalid() && _nodes.find(pc.primary)!=_nodes.end()) {
                        _nodes[pc.primary].primaries.erase(pc.gpid);
                        pc.primary.set_invalid();
                    }
                    for (dsn::rpc_address& addr: pc.secondaries) {
                        if (_nodes.find(addr) != _nodes.end()) {
                            _nodes[addr].partitions.erase(pc.gpid);
                        }
                    }
                    pc.secondaries.clear();
                    ++pc.ballot;
                }
            }
            response.err = ERR_OK;
            reply(msg, response);
            dinfo("drop table(id:%d, name:%s) finished", app.app_id, app.app_name.c_str());
        }
        else if (ERR_TIMEOUT == ec)
        {
            dinfo("drop table(id:%d, name:%s) timeout, ignore request", app.app_id, app.app_name.c_str());
            zauto_write_lock l(_lock);
            app.status = AS_DROP_FAILED;
        }
        else
        {
            dassert(false, "we can't handle this, error(%s)", ec.to_string());
        }
        dsn_msg_release_ref(msg);
    };    
    _storage->set_data(app_path,
        value,
        LPC_META_SERVER_STATE_UPDATE_CALLBACK,
        after_set_app_dropped);
}

void server_state::drop_app(dsn_message_t msg)
{
    configuration_drop_app_request request;
    configuration_drop_app_response response;
    int32_t index;
    bool do_dropping = false;
    ::unmarshall(msg, request);

    ddebug("drop app request, name(%s)", request.app_name.c_str());

    {
        zauto_write_lock l(_lock);
        index = get_app_index(request.app_name.c_str());
        if (index == -1 || _apps[index].status == AS_DROPPED) {
            response.err = request.options.success_if_not_exist?ERR_OK:ERR_APP_NOT_EXIST;
        }
        else {
            switch (_apps[index].status)
            {
            case AS_AVAILABLE:
            case AS_DROP_FAILED:
            case AS_CREATE_FAILED:
                do_dropping = true;
                _apps[index].status = AS_DROPPING;
                break;
            case AS_CREATING:
                response.err = ERR_BUSY_CREATING;
                break;
            case AS_DROPPING:
                response.err = ERR_BUSY_DROPPING;
                break;
            default:
                break;
            }
        }
    }
    if (do_dropping)
    {
        do_app_drop(_apps[index], msg);
    }
    else
        reply(msg, response);
}

void server_state::list_apps(dsn_message_t msg)
{
    configuration_list_apps_request request;
    configuration_list_apps_response response;
    ::unmarshall(msg, request);

    ddebug("list app request, status(%d)", request.status);
    {
        zauto_read_lock l(_lock);
        for (const app_state& app: _apps)
        {
            if ( request.status == AS_INVALID || request.status == app.status)
            {
                dsn::replication::app_info info;
                info.app_id = app.app_id;
                info.status = app.status;
                info.app_type = app.app_type;
                info.app_name = app.app_name;
                info.partition_count = app.partition_count;
                response.infos.push_back(info);
            }
        }
        response.err = dsn::ERR_OK;
    }
    reply(msg, response);
}

void server_state::list_nodes(dsn_message_t msg)
{
    configuration_list_nodes_request request;
    configuration_list_nodes_response response;
    ::unmarshall(msg, request);
    {
        zauto_read_lock l(_lock);
        for (auto& node: _nodes)
        {
            node_status status = node.second.is_alive ? NS_ALIVE : NS_UNALIVE;
            if (request.status == NS_INVALID || request.status == status)
            {
                dsn::replication::node_info info;
                info.status = status;
                info.address = node.first;
                response.infos.push_back(info);
            }
        }
        response.err = dsn::ERR_OK;
    }
    reply(msg, response);
}

void server_state::cluster_info(dsn_message_t msg)
{
    configuration_cluster_info_request request;
    configuration_cluster_info_response response;
    ::unmarshall(msg, request);
    {
        response.keys.push_back("meta_servers");
        std::vector<rpc_address> servers;
        replication_app_client_base::load_meta_servers(servers);
        std::ostringstream oss;
        for (size_t i = 0; i < servers.size(); ++i)
        {
            if (i != 0)
                oss << ", ";
            oss << servers[i].to_string();
        }
        response.values.push_back(oss.str());
        response.keys.push_back("primary_meta_server");
        response.values.push_back(_meta_svc->get_primary().to_string());
        response.keys.push_back("zookeeper_servers");
        response.values.push_back(::dsn::dist::zookeeper_session_mgr::instance().zoo_hosts());
        response.keys.push_back("zookeeper_cluster_root");
        response.values.push_back(_cluster_root);
        response.err = dsn::ERR_OK;
    }
    reply(msg, response);
}

void server_state::update_configuration(
    std::shared_ptr<configuration_update_request>& req,
    dsn_message_t request_msg, 
    std::function<void()> callback)
{
    bool write;
    bool retry;
    int delay_ms = 0;

    configuration_update_response response;
    {
        zauto_write_lock l(_lock);
        app_state& app = _apps[req->config.gpid.app_id - 1];
        partition_configuration& old = app.partitions[req->config.gpid.pidx];

        //For a dropped table on meta server, all the update configuration request is invalid
        if (app.status == AS_DROPPED)
        {
            dwarn("app_id(%d) is dropped, update request(from:%s, cfg_type:%s, ballot:%s) is ignored",
                  app.app_id, req->node.to_string(), enum_to_string(req->type), req->config.ballot);
            write = false;
            retry = false;

            response.err = ERR_INVALID_VERSION;
            response.config = old;
        }
        else if (is_partition_config_equal(old, req->config))
        {
            // duplicate request
            dwarn("received duplicate update configuration request from %s, gpid = %d.%d, ballot = %" PRId64,
                  req->node.to_string(), old.gpid.app_id, old.gpid.pidx, old.ballot);
            write = false;
            retry = false;
            response.err = ERR_OK;
            response.config = old;
        }
        else if (old.ballot + 1 != req->config.ballot)
        {
            dwarn("received invalid update configuration request from %s, gpid = %d.%d, ballot = %" PRId64 ", cur_ballot = %" PRId64,
                  req->node.to_string(), old.gpid.app_id, old.gpid.pidx, req->config.ballot, old.ballot);
            write = false;
            if (nullptr == request_msg)
            {
                //for meta server's remove primary request, we must always try to execute it
                req->config.ballot = old.ballot + 1;
                retry = true;
            }
            response.err = ERR_INVALID_VERSION;
            response.config = old;
        }
        else if (_pending_requests.find(req->config.gpid) != _pending_requests.end())
        {
            dwarn("A pending request exists, ignore the request. configuration request from %s, gpid = %d.%d, ballot = %" PRId64,
                  req->node.to_string(), old.gpid.app_id, old.gpid.pidx, req->config.ballot);
            if (nullptr == request_msg)
            {
                //for meta server's remove primary request, we must always try to execute it
                retry = true;
                delay_ms = 1000;
            }
            else
            {
                //for request from replica server, we just adandon it.
                //because we asssume the replica server will resend this
                return;
            }
        }
        else
        {
            write = true;
            req->config.last_drops = old.last_drops;

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
                //only maintain last drops for primary
                if (req->config.primary.is_invalid())
                    maintain_drops(req->config.last_drops, req->node, false);
                break;
            }
            _pending_requests.emplace(req->config.gpid, callback);
        }
    }

    if (!write)
    {
        if (request_msg)
        {
            reply(request_msg, response);
        }
        else if (retry)
        {
            tasking::enqueue(
                LPC_CM_UPDATE_CONFIGURATION,
                nullptr,
                [this, cb = std::move(callback), req_ptr = std::move(req)]() mutable
                {
                    this->update_configuration(req_ptr, nullptr, cb);
                },
                0,
                std::chrono::milliseconds(delay_ms));
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
        update_configuration_on_remote(req, request_msg);
    }
}

void server_state::update_configuration_on_remote(const std::shared_ptr<configuration_update_request>& req, dsn_message_t request_msg)
{
    std::string partition_path = get_partition_path(req->config.gpid);
    blob config;
    marshall_json(config, req->config);

    _storage->set_data(
        partition_path,
        config,
        LPC_META_SERVER_STATE_UPDATE_CALLBACK,
        [this, req, request_msg](error_code ec)
        {
            if (ec == ERR_OK)
            {
                exec_pending_request(req, request_msg);
            }
            else if (ec == ERR_TIMEOUT)
            {
                // well, if we have message need to resposne,
                // we just ignore it. After all, the sender will
                // resend it
                if (request_msg)
                {
                    zauto_write_lock l(_lock);
                    _pending_requests.erase(req->config.gpid);
                    dsn_msg_release_ref(request_msg);
                }
                else
                {
                    tasking::enqueue(LPC_META_SERVER_STATE_UPDATE_CALLBACK,
                        this,
                        std::bind(&server_state::update_configuration_on_remote, this, req, nullptr),
                        0,
                        std::chrono::milliseconds(1000));
                }
            }
            else
            {
                dassert(false, "we can't handle this, err(%s)", ec.to_string());
            }
        }
    );
}

void server_state::exec_pending_request(const std::shared_ptr<configuration_update_request>& req, dsn_message_t request_msg)
{
    configuration_update_response resp;
    std::function<void ()> callback;
    {
        zauto_write_lock l(_lock);
        auto iter = _pending_requests.find(req->config.gpid);
        callback = std::move(iter->second);
        dassert(iter != _pending_requests.end(), "request for gpid(%d.%d) is removed unexpectedly", req->config.gpid.app_id, req->config.gpid.pidx);
        update_configuration_internal(*req, resp);
        _pending_requests.erase(iter);
    }
    if (request_msg)
    {
        reply(request_msg, resp);
        dsn_msg_release_ref(request_msg);
    }
    if (callback)
    {
        callback();
    }
}

void server_state::update_configuration_internal(const configuration_update_request& request, /*out*/ configuration_update_response& response)
{
    app_state& app = _apps[request.config.gpid.app_id - 1];
    partition_configuration& old = app.partitions[request.config.gpid.pidx];
    if (app.status == AS_DROPPED)
    {
        response.err = ERR_INVALID_VERSION;
        response.config = old;
        dwarn("table is dropped when trying to update config(node: %s, config_type: %s, ballot: %" PRId64 ")",
              request.node.to_string(), enum_to_string(request.type), request.config.ballot);
    }
    else if (old.ballot + 1 != request.config.ballot)
    {
        dassert(false, "");
    }
    else
    {
        response.err = ERR_OK;
        response.config = request.config;
        
        auto it = _nodes.find(request.node);
        dassert(it != _nodes.end(), "");
        node_state& node = it->second;

        switch (request.type)
        {
        case CT_ASSIGN_PRIMARY:
# ifndef NDEBUG
            dassert(old.primary != request.node, "");
            dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) == old.secondaries.end(), "");
# endif
            node.partitions.insert(old.gpid);
            node.primaries.insert(old.gpid);
            break; 
        case CT_UPGRADE_TO_PRIMARY:
# ifndef NDEBUG
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
# ifndef NDEBUG
            dassert(old.primary == request.node, "");
            dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) == old.secondaries.end(), "");
# endif
            node.primaries.erase(old.gpid);
            break;
        case CT_DOWNGRADE_TO_INACTIVE:
        case CT_REMOVE:
# ifndef NDEBUG
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
# ifndef NDEBUG
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
            if (request.config.primary.is_invalid())
                maintain_drops(drops, request.node, false);
            break;
        }
        
        // update to new config        
        old = request.config;
        old.last_drops = drops;
        
        if (request.type == CT_DOWNGRADE_TO_INACTIVE || request.type == CT_REMOVE)
        {
            partition_assist_info& assist_info = app.partition_assists[old.gpid.pidx];
            // erase duplicate entries
            auto it = assist_info.history_queue.begin();
            while (it != assist_info.history_queue.end())
            {
                if (it->addr == request.node)
                {
                    it = assist_info.history_queue.erase(it);
                }
                else
                {
                    it++;
                }
            }
            // push to back
            dropout_history d = { request.node, dsn_now_ms() };
            assist_info.history_queue.push_back(d);
            // limit history_queue size
            while (assist_info.history_queue.size() > 10)
            {
                assist_info.history_queue.pop_front();
            }
        }

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
    
#ifndef NDEBUG
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

void server_state::static_cli_dump_app_states(void *context, int argc, const char **argv, dsn_cli_reply *reply)
{
    server_state* _this = reinterpret_cast<server_state*>(context);
    std::string* dump_result;
    if (argc != 4)
    {
        dump_result = new std::string("invalid command parameter");
    }
    else
    {
        const char* format = nullptr;
        const char* target_file = nullptr;
        for (int i=0; i<argc; i+=2)
        {
            if (strcmp(argv[i], "-f") == 0 || strcmp(argv[i], "--format") == 0)
                format = argv[i+1];
            else if (strcmp(argv[i], "-t") == 0 || strcmp(argv[i], "--target") == 0)
                target_file = argv[i+1];
        }

        if (format==nullptr || target_file==nullptr)
        {
            dump_result = new std::string("invalid command parameter");
        }
        else {
            error_code ec = _this->dump_from_remote_storage(format, target_file, false);
            dump_result = new std::string("execute result: ");
            dump_result->append(ec.to_string());
        }
    }

    reply->message = dump_result->c_str();
    reply->size = dump_result->size();
    reply->context = dump_result;
}

void server_state::static_cli_dump_app_states_cleanup(dsn_cli_reply reply)
{
    dassert(reply.context != nullptr, "corrupted cli context");
    std::string* dump_result = reinterpret_cast<std::string*>(reply.context);
    delete dump_result;
}
