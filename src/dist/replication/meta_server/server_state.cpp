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
# include <rapidjson/writer.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "meta.server.state"

# include "dump_file.h"

int32_t server_state::_default_max_replica_count = 3;

void init_partition_configuration(/*out*/partition_configuration& pc, /*in*/const app_state& app, int32_t max_replica_count)
{
    pc.ballot = 0;
    pc.pid.set_app_id(app.info.app_id);
    pc.last_committed_decree = 0;
    pc.max_replica_count = max_replica_count;
    pc.primary.set_invalid();
    pc.secondaries.clear();
    pc.last_drops.clear();
}

void maintain_drops(/*inout*/ std::vector<rpc_address>& drops, const rpc_address& node, bool is_add)
{
    auto it = find(drops.begin(), drops.end(), node);
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

void maintain_drops(/*inout*/ std::vector<rpc_address>& drops, const configuration_update_request& req)
{
    switch (req.type)
    {
    case config_type::CT_ASSIGN_PRIMARY:
    case config_type::CT_ADD_SECONDARY:
    case config_type::CT_UPGRADE_TO_SECONDARY:
    case config_type::CT_ADD_SECONDARY_FOR_LB:
        maintain_drops(drops, req.node, true);
        break;
    case config_type::CT_DOWNGRADE_TO_INACTIVE:
    case config_type::CT_REMOVE:
        if (req.config.primary.is_invalid())
            maintain_drops(drops, req.node, false);
        break;
    case config_type::CT_UPGRADE_TO_PRIMARY:
    case config_type::CT_DOWNGRADE_TO_SECONDARY:
        break;
    default:
        dassert(false, "unhandled request type for drops maintainance");
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
server_state::server_state()
    : serverlet<server_state>("meta.server.state"), _cli_json_state_handle(nullptr), _cli_dump_handle(nullptr)
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
        if (snapshot.info.status != app_status::AS_AVAILABLE)
        {
            snapshot.info.partition_count = 0;
            snapshot.partitions.clear();
        }

        if (strcmp(format, "json") == 0)
        {
            binary_writer writer;
            marshall(writer, snapshot, DSF_THRIFT_JSON);
            file->append_buffer(writer.get_buffer());
        }
        else if (strcmp(format, "binary") == 0)
        {
            binary_writer writer;
            marshall(writer, snapshot, DSF_THRIFT_BINARY);
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

    binary_reader reader(data);
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

            unmarshall(reader, app, DSF_THRIFT_JSON);

            app.partitions.resize(app.info.partition_count);
            for (unsigned int i=0; i!=app.info.partition_count; ++i)
            {
                ans = file->read_next_buffer(data);
                dassert(ans == 1, "unexpect read buffer, ret(%d)", ans);

                binary_reader reader(data);
                unmarshall(reader, app.partitions[i], DSF_THRIFT_JSON);

                dassert(app.partitions[i].pid.get_partition_index()==i, "uncorrect partition data, gpid(%d.%d), appname(%s)", app.info.app_id, i, app.info.app_name.c_str());
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
            unmarshall(reader, _apps.back(), DSF_THRIFT_BINARY);
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

    for (app_state& app: _apps)
    {
        while (app.info.status != app_status::AS_DROPPED && app.available_partitions.atom().load() != app.info.partition_count)
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
        "the default value of max_replica_count for all apps");

    _min_live_node_count_for_unfreeze = (int32_t)dsn_config_get_value_uint64(
        "meta_server", 
        "min_live_node_count_for_unfreeze",
        3,
        "minimum live node count without which the state is freezed"
        );

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
    utils::split_args(meta_state_service_parameters, args);
    int argc = static_cast<int>(args.size());
    std::vector<const char*> args_ptr;
    args_ptr.resize(argc);
    for (int i = argc - 1; i >= 0; i--)
    {
        args_ptr[i] = args[i].c_str();
    }

    // create storage
    _storage = utils::factory_store< dist::meta_state_service>::create(
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
    return _apps_root + "/" + boost::lexical_cast<std::string>(app.info.app_id);
}

std::string server_state::get_partition_path(const app_state &app, int partition_id) const
{
    std::stringstream oss;
    oss << _apps_root << "/" << app.info.app_id
        << "/" << partition_id;
    return oss.str();
}

std::string server_state::get_partition_path(const gpid& gpid) const
{
    std::stringstream oss;
    oss << _apps_root << "/" << gpid.get_app_id()
        << "/" << gpid.get_partition_index();
    return oss.str();
}

error_code server_state::initialize_apps()
{
    const char* sections[10240];
    int scount, used_count = sizeof(sections)/sizeof(const char*);
    scount = dsn_config_get_all_sections(sections, &used_count);
    dassert(scount == used_count, "too many sections (>10240) defined in config files");

    ddebug("start to do initialize");

    for (int i = 0; i < used_count; i++)
    {
        if (strstr(sections[i], "meta_server.apps") == sections[i] 
            // legacy hack
            || strcmp(sections[i], "replication.app") == 0)
        {
            const char* s = sections[i];

            app_state app;
            app.info.app_id = 1 + _apps.size();
            app.info.app_name = dsn_config_get_value_string(s,
                "app_name", "", "app name");
            dassert(app.info.app_name.length() > 0, "'[%s] app_name' not specified", s);
            app.info.app_type = dsn_config_get_value_string(s,
                "app_type", "", "app type-name");
            dassert(app.info.app_type.length() > 0, "'[%s] app_type' not specified", s);
            app.info.partition_count = (int)dsn_config_get_value_uint64(s,
                "partition_count", 1, "how many partitions the app should have");
            app.info.status = app_status::AS_CREATING;
            app.available_partitions.atom().store(0);

            app.info.is_stateful = dsn_config_get_value_bool(s, "stateful",
                true, "whether this is a stateful app");

            // TODO: setup envs

            _apps.push_back(app);

            partition_configuration pc;
            init_partition_configuration(pc, app,
                dsn_config_get_value_uint64(s, "max_replica_count", 3, "max replica count in app"));

            std::vector<partition_configuration>& partitions = _apps.back().partitions;
            partitions.resize(app.info.partition_count, pc);
            for (unsigned int i = 0; i != partitions.size(); ++i)
                partitions[i].pid.set_partition_index(i);
        }
    }

    error_code err = sync_apps_to_remote_storage();
    if (err != ERR_OK)
    {
        _apps.clear();
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
        binary_writer writer;

        marshall(writer, app, DSF_THRIFT_JSON);

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
            writer.get_buffer(),
            &tracker);
    }

    dsn_task_tracker_wait_all(tracker.tracker());
    if (err != ERR_OK)
        return err;

    for (app_state& app: _apps)
    {
        if (app.info.status == app_status::AS_DROPPED)
            continue;
        for (unsigned int i=0; i!=app.info.partition_count; ++i)
            init_app_partition_node(app.info.app_id, i);
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
                            binary_reader reader(value);
                            unmarshall(reader, state, DSF_THRIFT_JSON);

                            int app_id = state.info.app_id;
                            dassert(app_id != 0, "invalid app id");

                            {
                                zauto_write_lock l(_lock);
                                if (app_id > _apps.size())
                                {
                                    _apps.resize(app_id);
                                }

                                if (state.info.status == app_status::AS_DROPPED)
                                {
                                    _apps[app_id - 1].info.status = app_status::AS_DROPPED;
                                    return;
                                }
                                else
                                {
                                    state.info.status = app_status::AS_CREATING;
                                    state.available_partitions.atom().store(0);
                                    _apps[app_id - 1] = state;
                                    _apps[app_id - 1].partitions.resize(state.info.partition_count);
                                }
                            }

                            // get partition info
                            for (int i = 0; i < state.info.partition_count; i++)
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
                                            binary_reader reader(value);
                                            unmarshall(reader, pc, DSF_THRIFT_JSON);
# ifndef NDEBUG
                                            this->check_consistency(_apps[app_id - 1].info.is_stateful, pc);
# endif

                                            zauto_write_lock l(_lock);
                                            _apps[app_id - 1].partitions[i] = pc;
                                            dassert(pc.pid.get_app_id() == app_id && pc.pid.get_partition_index() == i, "invalid partition config");
                                            
                                            _apps[app_id -1].available_partitions.atom()++;
                                        }
                                        else if (ec == ERR_OBJECT_NOT_FOUND)
                                        {
                                            dwarn("partition node %s not exist on remote storage, may half create before", par_path.c_str());
                                            init_partition_configuration(_apps[app_id - 1].partitions[i],
                                                _apps[app_id - 1],
                                                _default_max_replica_count);
                                            _apps[app_id - 1].partitions[i].pid.set_partition_index(i);
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
            if (app.available_partitions.atom().load() == app.info.partition_count)
            {
                app.info.status = app_status::AS_AVAILABLE;
            }
        }

        for (app_state& app: _apps) 
        {
            for (int i = 0; i < app.info.partition_count; i++)
            {
                auto& ps = app.partitions[i];

                if (ps.primary.is_invalid() == false)
                {
                    _nodes[ps.primary].primaries.insert(ps.pid);
                    _nodes[ps.primary].partitions.insert(ps.pid);
                }

                for (auto& ep : ps.secondaries)
                {
                    dassert(ep.is_invalid() == false, "");
                    _nodes[ep].partitions.insert(ps.pid);
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
                check_consistency(par.pid);
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
                        app_state& app = _apps[pri.get_app_id() - 1];
                        partition_configuration& old = app.partitions[pri.get_partition_index()];

                        dassert(old.primary == it->first, "");

                        auto request = std::shared_ptr<configuration_update_request>(new configuration_update_request());
                        request->info = app.info;
                        request->node = old.primary;
                        request->type = config_type::CT_DOWNGRADE_TO_INACTIVE;
                        request->config = old;
                        request->config.ballot++;
                        request->config.primary.set_invalid();

                        (*pris)[pri] = request;
                    }

                    for (auto& pri : it->second.partitions)
                    {
                        app_state& app = _apps[pri.get_app_id() - 1];                        

                        // skip primary
                        if (!app.info.is_stateful)
                        {
                            partition_configuration& old = app.partitions[pri.get_partition_index()];
                            auto request = std::shared_ptr<configuration_update_request>(new configuration_update_request());
                            request->info = app.info;
                            request->type = config_type::CT_REMOVE;
                            request->host_node = it->first;

                            for (int i = 0; i < (int)old.secondaries.size(); i++)
                            {
                                if (old.secondaries[i] == it->first)
                                {
                                    request->node = old.last_drops[i];
                                    break;
                                }
                            }

                            dassert(!request->node.is_invalid(), "");
                            
                            request->config = old;

                            (*pris)[pri] = request;
                        }
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
        dinfo("live replica server # changes from %d to %d, freeze = %s", old_lc, _node_live_count, _freeze ? "true":"false");
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
            configuration_update_request req;
            req.config = _apps[p.get_app_id() - 1].partitions[p.get_partition_index()];
            req.info = _apps[p.get_app_id() - 1].info;
            response.partitions.push_back(req);
        }
    }
}

void server_state::query_configuration_by_gpid(gpid id, /*out*/ partition_configuration& config)
{
    zauto_read_lock l(_lock);
    config = _apps[id.get_app_id() - 1].partitions[id.get_partition_index()];
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
    if ( app.info.status != app_status::AS_AVAILABLE ) {
        response.err = ERR_INVALID_STATE;
        return;
    }

    response.err = ERR_OK;
    response.app_id = app.info.app_id;
    response.partition_count = app.info.partition_count;
    response.is_stateful = app.info.is_stateful;

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
        if ( strcmp(app.info.app_name.c_str(), app_name) == 0 && app.info.status != app_status::AS_DROPPED)
            return app.info.app_id-1;
    return -1;
}

DEFINE_TASK_CODE(LPC_META_SERVER_STATE_UPDATE_CALLBACK, TASK_PRIORITY_HIGH, THREAD_POOL_META_SERVER)
DEFINE_TASK_CODE(LPC_CM_UPDATE_CONFIGURATION, TASK_PRIORITY_HIGH, THREAD_POOL_META_SERVER)

void server_state::init_app_partition_node(int app_id, int partition_index)
{
    auto on_create_app_partition = [this, app_id, partition_index](error_code ec)
    {
        app_state& app = _apps[app_id - 1];
        if (ERR_OK==ec || ERR_NODE_ALREADY_EXIST==ec)
        {
            int avail_count = ++app.available_partitions.atom();
            if (avail_count == app.info.partition_count)
            {
                ddebug("partition init finished, the app(name:%s, id:%d) is available", app.info.app_name.c_str(), app.info.app_id);
                zauto_write_lock l(_lock);
                app.info.status = app_status::AS_AVAILABLE;
            }
            else
            {
                dinfo("available partition are %d, add partition node gpid(%d.%d) ok", avail_count, app_id, partition_index);
            }
        }
        else if (ERR_TIMEOUT == ec)
        {
            dwarn("create partition node failed, gpid(%d.%d), retry later", app_id, partition_index);
            //TODO: add parameter of the retry time interval in config file
            tasking::enqueue(LPC_META_SERVER_STATE_UPDATE_CALLBACK,
                             this,
                             std::bind(&server_state::init_app_partition_node, this, app_id, partition_index),
                             0,
                             std::chrono::milliseconds(1000));
        }
        else
        {
            dassert(false, "we can't handle this error in init app partition nodes err(%s), gpid(%d.%d)", ec.to_string(), app_id, partition_index);
        }
    };

    app_state& app = _apps[app_id - 1];
    std::string app_partition_path = get_partition_path(app, partition_index);
    binary_writer writer;
    marshall(writer, app.partitions[partition_index], DSF_THRIFT_JSON);
    _storage->create_node(app_partition_path,
        LPC_META_SERVER_STATE_UPDATE_CALLBACK,
        on_create_app_partition,
        writer.get_buffer()
        );
}

void server_state::initialize_app(app_state& app, dsn_message_t msg)
{
    if (msg) dsn_msg_add_ref(msg);
    auto on_create_app_root = [this, msg, &app](error_code ec)
    {
        configuration_create_app_response resp;
        if (ERR_OK==ec || ERR_NODE_ALREADY_EXIST==ec)
        {
            dinfo("create app on storage service ok, name: %s, appid %" PRId32 "", app.info.app_name.c_str(), app.info.app_id);
            resp.appid = app.info.app_id;
            resp.err = ERR_OK;
            if (msg) reply(msg, resp);
            for (unsigned int i=0; i!=app.info.partition_count; ++i)
            {
                init_app_partition_node(app.info.app_id, i);
            }
        }
        else if (ERR_TIMEOUT == ec)
        {
            dwarn("the storage service is not available currently, just ignore this request");
            {
                zauto_write_lock l(_lock);
                app.info.status = app_status::AS_CREATE_FAILED;
            }
        }
        else
        {
            dassert(false, "we can't handle this right now, err(%s)", ec.to_string());
        }
        if (msg) dsn_msg_release_ref(msg);
    };

    binary_writer writer;
    std::string app_dir = get_app_path(app);
    marshall(writer, app, DSF_THRIFT_JSON);
    _storage->create_node(
        app_dir,
        LPC_META_SERVER_STATE_UPDATE_CALLBACK,
        on_create_app_root,
        writer.get_buffer()
        );
}

void server_state::create_app(configuration_create_app_request& request, /*out*/ configuration_create_app_response& response)
{
    int32_t index;
    bool will_create_app = false;

    ddebug("create app request, name(%s), type(%s), partition_count(%d), replica_count(%d), stateful(%s), envs(%d kvs)",
           request.app_name.c_str(),
           request.options.app_type.c_str(),
           request.options.partition_count,
           request.options.replica_count,
           request.options.is_stateful ? "true" : "false",
           (int)request.options.envs.size()
        );

    auto option_match_check = [](const create_app_options& opt, const app_state& exist_app) {
        return opt.partition_count==exist_app.info.partition_count &&
               opt.app_type==exist_app.info.app_type;
    };

    {
        zauto_write_lock l(_lock);
        index = get_app_index(request.app_name.c_str());
        /* so we can't store the data on meta_state_service with app_name, but app_id */
        if (index != -1 && _apps[index].info.status!= app_status::AS_DROPPED)
        {
            app_state& exist_app = _apps[index];
            response.appid = exist_app.info.app_id;

            switch (exist_app.info.status)
            {
            case app_status::AS_AVAILABLE:
                if (!request.options.success_if_exist || !option_match_check(request.options, exist_app))
                    response.err = ERR_INVALID_PARAMETERS;
                else {
                    response.err = ERR_OK;                    
                }
                break;
            case app_status::AS_CREATING:
                response.err = ERR_BUSY_CREATING;
                break;
            case app_status::AS_CREATE_FAILED:
                exist_app.info.status = app_status::AS_CREATING;
                will_create_app = true;
                response.err = ERR_IO_PENDING;
                break;
            case app_status::AS_DROPPING:
            case app_status::AS_DROP_FAILED:
                response.err = ERR_BUSY_DROPPING;
            default:
                break;
            }
        }
        else 
        {
            will_create_app = true;            
            index = _apps.size();
            response.err = ERR_IO_PENDING;
            response.appid = index;

            _apps.push_back(app_state());
            app_state& app = _apps.back();

            //the app_id is started from 1!!!
            app.info.app_id = index + 1;
            app.info.app_name = request.app_name;
            app.info.envs = request.options.envs;
            app.info.is_stateful = request.options.is_stateful;
            app.info.app_type = request.options.app_type;
            app.info.partition_count = request.options.partition_count;
            app.available_partitions.atom().store(0);

            partition_configuration pc;
            init_partition_configuration(pc, app, request.options.replica_count);

            app.partitions.resize(app.info.partition_count, pc);
            for (int i = 0; i != app.partitions.size(); ++i)
                app.partitions[i].pid.set_partition_index(i);

            app.info.status = app_status::AS_CREATING;
        }
    }

    if ( will_create_app ) 
    {
        initialize_app(_apps[index], nullptr);
    }
}

void server_state::do_app_drop(app_state& app, dsn_message_t msg)
{
    binary_writer writer;
    marshall(writer, app, DSF_THRIFT_JSON);

    std::string app_path = get_app_path(app);

    if (msg) dsn_msg_add_ref(msg);
    auto after_set_app_dropped = [this, &app, msg](error_code ec) {
        configuration_drop_app_response response;
        if (ERR_OK == ec || ERR_OBJECT_NOT_FOUND == ec)
        {
            {
                zauto_write_lock l(_lock);
                app.info.status = app_status::AS_DROPPED;
                for (partition_configuration& pc: app.partitions) {
                    if (!pc.primary.is_invalid() && _nodes.find(pc.primary)!=_nodes.end()) {
                        _nodes[pc.primary].primaries.erase(pc.pid);
                        pc.primary.set_invalid();
                    }
                    for (rpc_address& addr: pc.secondaries) {
                        if (_nodes.find(addr) != _nodes.end()) {
                            _nodes[addr].partitions.erase(pc.pid);
                        }
                    }
                    pc.secondaries.clear();
                    ++pc.ballot;
                }
            }
            response.err = ERR_OK;
            if (msg) reply(msg, response);
            dinfo("drop table(id:%d, name:%s) finished", app.info.app_id, app.info.app_name.c_str());
        }
        else if (ERR_TIMEOUT == ec)
        {
            dinfo("drop table(id:%d, name:%s) timeout, ignore request", app.info.app_id, app.info.app_name.c_str());
            zauto_write_lock l(_lock);
            app.info.status = app_status::AS_DROP_FAILED;
        }
        else
        {
            dassert(false, "we can't handle this, error(%s)", ec.to_string());
        }
        if (msg) dsn_msg_release_ref(msg);
    };    
    _storage->set_data(app_path,
        writer.get_buffer(),
        LPC_META_SERVER_STATE_UPDATE_CALLBACK,
        after_set_app_dropped);
}

void server_state::drop_app(configuration_drop_app_request& request, /*out*/ configuration_drop_app_response& response)
{
    int32_t index;
    bool do_dropping = false;

    ddebug("drop app request, name(%s)", request.app_name.c_str());

    {
        zauto_write_lock l(_lock);
        index = get_app_index(request.app_name.c_str());
        if (index == -1 || _apps[index].info.status == app_status::AS_DROPPED)
        {
            response.err = request.options.success_if_not_exist?ERR_OK:ERR_APP_NOT_EXIST;
        }
        else
        {
            switch (_apps[index].info.status)
            {
            case app_status::AS_AVAILABLE:
            case app_status::AS_DROP_FAILED:
            case app_status::AS_CREATE_FAILED:
                do_dropping = true;
                response.err = ERR_IO_PENDING;
                _apps[index].info.status = app_status::AS_DROPPING;
                break;
            case app_status::AS_CREATING:
                response.err = ERR_BUSY_CREATING;
                break;
            case app_status::AS_DROPPING:
                response.err = ERR_BUSY_DROPPING;
                break;
            default:
                break;
            }
        }
    }

    if (do_dropping)
    {
        do_app_drop(_apps[index], nullptr);
    }
}

void server_state::list_apps(configuration_list_apps_request& request, /*out*/ configuration_list_apps_response& response)
{
    ddebug("list app request, status(%d)", request.status);
    {
        zauto_read_lock l(_lock);
        for (const app_state& app: _apps)
        {
            if ( request.status == app_status::AS_INVALID || request.status == app.info.status)
            {
                app_info info;
                info.app_id = app.info.app_id;
                info.status = app.info.status;
                info.app_type = app.info.app_type;
                info.app_name = app.info.app_name;
                info.partition_count = app.info.partition_count;
                info.is_stateful = app.info.is_stateful;
                info.envs = app.info.envs;
                response.infos.push_back(info);
            }
        }
        response.err = ERR_OK;
    }
}

void server_state::list_nodes(configuration_list_nodes_request& request, /*out*/ configuration_list_nodes_response& response)
{
    {
        zauto_read_lock l(_lock);
        for (auto& node: _nodes)
        {
            node_status::type status = node.second.is_alive ? node_status::NS_ALIVE : node_status::NS_UNALIVE;
            if (request.status == node_status::NS_INVALID || request.status == status)
            {
                node_info info;
                info.status = status;
                info.address = node.first;
                response.infos.push_back(info);
            }
        }
        response.err = ERR_OK;
    }
}

void server_state::update_configuration_on_remote(const std::shared_ptr<configuration_update_request>& req,
                                                  dsn_message_t request_msg)
{
# ifndef NDEBUG
    check_consistency(req->info.is_stateful, req->config);
# endif

    std::string partition_path = get_partition_path(req->config.pid);
    binary_writer writer;
    marshall(writer, req->config, DSF_THRIFT_JSON);

    _storage->set_data(
        partition_path,
        writer.get_buffer(),
        LPC_META_SERVER_STATE_UPDATE_CALLBACK,
        [this, req, request_msg](error_code ec)
        {
            gpid gpid = req->config.pid;
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
                    _pending_requests.erase(req->config.pid);
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

void server_state::update_configuration(
    std::shared_ptr<configuration_update_request>& req,
    dsn_message_t request_msg, 
    std::function<void()> callback)
{
    bool write = false;
    bool retry = false;
    int delay_ms = 0;

    configuration_update_response response;
    
    {        
        zauto_write_lock l(_lock);
        app_state& app = _apps[req->config.pid.get_app_id() - 1];
        partition_configuration& old = app.partitions[req->config.pid.get_partition_index()];
        
        // update for stateful service (via replication framework)
        if (app.info.is_stateful)
        {
            //For a dropped table on meta server, all the update configuration request is invalid
            if (app.info.status == app_status::AS_DROPPED)
            {
                dwarn("app_id(%d) is dropped, update request(from:%s, cfg_type:%s, ballot:%s) is ignored",
                    app.info.app_id, req->node.to_string(), enum_to_string(req->type), req->config.ballot);

                response.err = ERR_INVALID_VERSION;
                response.config = old;
            }
            else if (is_partition_config_equal(old, req->config))
            {
                // duplicate request
                dwarn("received duplicate update configuration request from %s, gpid = %d.%d, ballot = %" PRId64,
                    req->node.to_string(), old.pid.get_app_id(), old.pid.get_partition_index(), old.ballot);

                response.err = ERR_OK;
                response.config = old;
            }
            else if (old.ballot + 1 != req->config.ballot)
            {
                dwarn("received invalid update configuration request from %s, gpid = %d.%d, ballot = %" PRId64 ", cur_ballot = %" PRId64,
                    req->node.to_string(), old.pid.get_app_id(), old.pid.get_partition_index(), req->config.ballot, old.ballot);

                if (nullptr == request_msg)
                {
                    //for meta server's remove primary request, we must always try to execute it
                    req->config.ballot = old.ballot + 1;
                    retry = true;
                }
                response.err = ERR_INVALID_VERSION;
                response.config = old;
            }
            else if (_pending_requests.find(req->config.pid) != _pending_requests.end())
            {
                dwarn("A pending request exists, ignore the request. configuration request from %s, gpid = %d.%d, ballot = %" PRId64,
                    req->node.to_string(), old.pid.get_app_id(), old.pid.get_partition_index(), req->config.ballot);
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
            }
        }

        // update for stateless services
        else
        {            
            if (req->config.ballot != old.ballot)
            {
                dwarn("received invalid update configuration request from %s, gpid = %d.%d, ballot = %" PRId64 ", cur_ballot = %" PRId64,
                    req->node.to_string(), old.pid.get_app_id(), old.pid.get_partition_index(), req->config.ballot, old.ballot);

                response.err = ERR_INVALID_VERSION;
                response.config = old;
            }
            else if (_pending_requests.find(req->config.pid) != _pending_requests.end())
            {
                dwarn("A pending request exists, ignore the request. configuration request from %s, gpid = %d.%d, ballot = %" PRId64,
                    req->node.to_string(), old.pid.get_app_id(), old.pid.get_partition_index(), req->config.ballot);
                retry = true;
                delay_ms = 1000;
            }
            else
            {
                partition_configuration_stateless pcs(old);

                // remove
                if (config_type::CT_REMOVE == req->type)
                {
                    // not found
                    if (find(pcs.host_replicas().begin(), pcs.host_replicas().end(), req->host_node) == pcs.host_replicas().end() ||
                        find(pcs.worker_replicas().begin(), pcs.worker_replicas().end(), req->node) == pcs.worker_replicas().end())
                    {
                        response.err = ERR_OK;
                        response.config = old;
                    }
                    else
                    {
                        write = true;
                    }
                }
                
                // add
                else
                {
                    // added already
                    if (find(pcs.host_replicas().begin(), pcs.host_replicas().end(), req->host_node) != pcs.host_replicas().end())
                    {
                        response.err = ERR_OK;
                        response.config = old;
                    }

                    else
                    {
                        write = true;
                    }
                }

            }
        }

        if (write)
        {
            _pending_requests.emplace(req->config.pid, callback);

            if (req->info.is_stateful)
            {
                req->config.last_drops = old.last_drops;
                maintain_drops(req->config.last_drops, *req);
            }
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
                [this, cb = move(callback), req_ptr = move(req)]() mutable
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


void server_state::exec_pending_request(const std::shared_ptr<configuration_update_request>& req, dsn_message_t request_msg)
{
    configuration_update_response resp;
    std::function<void()> callback;
    {
        zauto_write_lock l(_lock);
        auto iter = _pending_requests.find(req->config.pid);
        callback = move(iter->second);
        dassert(iter != _pending_requests.end(), "request for gpid(%d.%d) is removed unexpectedly", 
            req->config.pid.get_app_id(), req->config.pid.get_partition_index()
            );
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
    app_state& app = _apps[request.config.pid.get_app_id() - 1];
    partition_configuration& old = app.partitions[request.config.pid.get_partition_index()];

    if (app.info.status == app_status::AS_DROPPED)
    {
        response.err = ERR_INVALID_VERSION;
        response.config = old;
        dwarn("table is dropped when trying to update config(node: %s, config_type: %s, ballot: %" PRId64 ")",
              request.node.to_string(), enum_to_string(request.type), request.config.ballot);
    }

    else if (app.info.is_stateful)
    {
        dassert(old.ballot + 1 == request.config.ballot, "");

        response.err = ERR_OK;
        response.config = request.config;

        auto it = _nodes.find(request.node);
        dassert(it != _nodes.end(), "");
        node_state& node = it->second;

        switch (request.type)
        {
        case config_type::CT_ASSIGN_PRIMARY:
# ifndef NDEBUG
            dassert(old.primary != request.node, "");
            dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) == old.secondaries.end(), "");
# endif
            node.partitions.insert(old.pid);
            node.primaries.insert(old.pid);
            break;
        case config_type::CT_UPGRADE_TO_PRIMARY:
# ifndef NDEBUG
            dassert(old.primary != request.node, "");
            dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) != old.secondaries.end(), "");
# endif
            node.partitions.insert(old.pid);
            node.primaries.insert(old.pid);
            break;
        case config_type::CT_ADD_SECONDARY:
            dassert(false, "invalid execution flow");
            break;
        case config_type::CT_DOWNGRADE_TO_SECONDARY:
# ifndef NDEBUG
            dassert(old.primary == request.node, "");
            dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) == old.secondaries.end(), "");
# endif
            node.primaries.erase(old.pid);
            break;
        case config_type::CT_DOWNGRADE_TO_INACTIVE:
        case config_type::CT_REMOVE:
# ifndef NDEBUG
            dassert(old.primary == request.node ||
                std::find(old.secondaries.begin(), old.secondaries.end(), request.node) != old.secondaries.end(), "");
# endif
            if (request.node == old.primary)
            {
                node.primaries.erase(old.pid);
            }
            node.partitions.erase(old.pid);
            break;
        case config_type::CT_UPGRADE_TO_SECONDARY:
# ifndef NDEBUG
            dassert(old.primary != request.node, "");
            dassert(std::find(old.secondaries.begin(), old.secondaries.end(), request.node) == old.secondaries.end(), "");
# endif
            node.partitions.insert(old.pid);
            break;
        default:
            dassert(false, "invalid config type 0x%x", static_cast<int>(request.type));
        }

        // maintain dropouts
        auto drops = old.last_drops;
        maintain_drops(drops, request);

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
            request.config.pid.get_app_id(),
            request.config.pid.get_partition_index(),
            request.config.ballot,
            enum_to_string(request.type),
            request.node.to_string(),
            cf.str().c_str()
            );
    }
    else
    {
        dassert(request.config.ballot == old.ballot, "");

        partition_configuration_stateless pcs(old);

        // remove
        if (config_type::CT_REMOVE == request.type)
        {
            // remove working node address
            auto it1 = std::remove(pcs.worker_replicas().begin(), pcs.worker_replicas().end(), request.node);
            dassert(it1 != pcs.worker_replicas().end(), "node must exit in the given vector");
            pcs.worker_replicas().erase(it1);

            // remove host node address
            it1 = std::remove(pcs.host_replicas().begin(), pcs.host_replicas().end(), request.host_node);
            dassert(it1 != pcs.host_replicas().end(), "node must exit in the given vector");
            pcs.host_replicas().erase(it1);

            auto it = _nodes.find(request.host_node);
            dassert(it != _nodes.end(), "");
            it->second.partitions.erase(request.config.pid);
        }

        // add
        else
        {
            // add
            if (std::find(pcs.host_replicas().begin(), pcs.host_replicas().end(), request.host_node) == pcs.host_replicas().end())
            {
                pcs.worker_replicas().emplace_back(request.node);
                pcs.host_replicas().emplace_back(request.host_node);

                auto it = _nodes.find(request.host_node);
                dassert(it != _nodes.end(), "");
                it->second.partitions.insert(request.config.pid);
            }
        }
        response.err = ERR_OK;
        response.config = old;

        std::stringstream cf;
        cf << "{replicas = [";
        for (size_t i = 0; i < pcs.host_replicas().size(); i++)
        {
            cf << pcs.host_replicas()[i].to_string() << " (work-port = " << pcs.worker_replicas()[i].port() << "),";
        }
        cf << "]}";

        ddebug("%d.%d meta update ok to ballot %" PRId64 ", type = %s, node = %s, config = %s",
            response.config.pid.get_app_id(),
            response.config.pid.get_partition_index(),
            response.config.ballot,
            enum_to_string(request.type),
            request.node.to_string(),
            cf.str().c_str()
            );
    }

#ifndef NDEBUG
    check_consistency(request.config.pid);
#endif

    if (_config_change_subscriber)
    {
        _config_change_subscriber(_apps);
    }
}

void server_state::check_consistency(bool is_stateful, const partition_configuration& config)
{
    if (is_stateful)
    {
        if (config.primary.is_invalid() == false)
        {
            auto it2 = find(config.last_drops.begin(), config.last_drops.end(), config.primary);
            dassert(it2 == config.last_drops.end(), "");
        }

        for (auto& ep : config.secondaries)
        {
            auto it2 = find(config.last_drops.begin(), config.last_drops.end(), ep);
            dassert(it2 == config.last_drops.end(), "");
        }
    }

    // stateless
    else
    {
        partition_configuration_stateless pcs((partition_configuration&)config);
        dassert(pcs.host_replicas().size() == pcs.worker_replicas().size(), "");
    }
}

void server_state::check_consistency(gpid gpid)
{
    app_state& app = _apps[gpid.get_app_id() - 1];
    partition_configuration& config = app.partitions[gpid.get_partition_index()];

    if (app.info.is_stateful)
    {
        if (config.primary.is_invalid() == false)
        {
            auto it = _nodes.find(config.primary);
            dassert(it != _nodes.end(), "");
            dassert(it->second.primaries.find(gpid) != it->second.primaries.end(), "");
            dassert(it->second.partitions.find(gpid) != it->second.partitions.end(), "");

            auto it2 = find(config.last_drops.begin(), config.last_drops.end(), config.primary);
            dassert(it2 == config.last_drops.end(), "");
        }

        for (auto& ep : config.secondaries)
        {
            auto it = _nodes.find(ep);
            dassert(it != _nodes.end(), "");
            dassert(it->second.partitions.find(gpid) != it->second.partitions.end(), "");

            auto it2 = find(config.last_drops.begin(), config.last_drops.end(), ep);
            dassert(it2 == config.last_drops.end(), "");
        }
    }

    // stateless
    else
    {
        partition_configuration_stateless pcs(config);
        dassert(pcs.host_replicas().size() == pcs.worker_replicas().size(), "");
        for (auto& ep : pcs.host_replicas())
        {
            auto it = _nodes.find(ep);
            dassert(it != _nodes.end(), "");
            dassert(it->second.partitions.find(gpid) != it->second.partitions.end(), "");
        }
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
    auto danglingstring = new std::string(move(out.str()));
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
