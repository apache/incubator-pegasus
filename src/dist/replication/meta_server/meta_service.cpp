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
#include <sys/stat.h>

#include <boost/lexical_cast.hpp>

#include <dsn/internal/factory_store.h>
#include <dsn/dist/meta_state_service.h>

#include "meta_service.h"
#include "server_state.h"
#include "meta_server_failure_detector.h"
#include "server_load_balancer.h"

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "meta.service"

namespace dsn { namespace replication {

meta_service::meta_service():
    serverlet("meta_service"),
    _failure_detector(nullptr),
    _started(false),
    _meta_ctrl_flags(0)
{
    _node_live_percentage_threshold_for_update = 65;
    _opts.initialize();
    _meta_opts.initialize();
    _state.reset(new server_state());
}

meta_service::~meta_service()
{
}

error_code meta_service::remote_storage_initialize()
{
    // create storage
    dsn::dist::meta_state_service* storage = dsn::utils::factory_store< ::dsn::dist::meta_state_service>::create(
        _meta_opts.meta_state_service_type.c_str(),
        PROVIDER_TYPE_MAIN
        );
    error_code err = storage->initialize(_meta_opts.meta_state_service_args);
    if (err != ERR_OK)
    {
        derror("init meta_state_service failed, err = %s", err.to_string());
        return err;
    }
    _storage.reset(storage);

    std::vector<std::string> slices;
    utils::split_args(_meta_opts.cluster_root.c_str(), slices, '/');
    std::string current = "";
    for (unsigned int i = 0; i != slices.size(); ++i)
    {
        current = meta_options::concat_path_unix_style(current, slices[i]);
        task_ptr tsk = _storage->create_node(current, LPC_META_CALLBACK,
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
    return ERR_OK;
}

void meta_service::set_node_state(const std::vector<rpc_address> &nodes, bool is_alive)
{
    zauto_write_lock l(_meta_lock);
    for (auto& node: nodes)
    {
        if (is_alive) {
            _alive_set.insert(node);
            _dead_set.erase(node);
        }
        else {
            _alive_set.erase(node);
            _dead_set.insert(node);
        }
    }
    if (!_started)
        return;
    for (const rpc_address& address: nodes) {
        tasking::enqueue(
            LPC_META_STATE_HIGH,
            nullptr,
            std::bind(&server_state::on_change_node_state, _state.get(), address, is_alive),
            server_state::s_state_write_hash
        );
    }
}

void meta_service::get_node_state(std::set<rpc_address> &node_set, bool is_alive)
{
    zauto_read_lock l(_meta_lock);
    if (is_alive)
        node_set = _alive_set;
    else
        node_set = _dead_set;
}

void meta_service::balancer_run()
{
    _state->check_all_partitions();
}

void meta_service::prepare_service_starting()
{
    zauto_write_lock l(_meta_lock);
    const meta_view view = _state->get_meta_view();
    for (auto& kv : *view.nodes)
    {
        if (_dead_set.find(kv.first) == _dead_set.end())
            _alive_set.insert(kv.first);
    }
}

void meta_service::service_starting()
{
    zauto_read_lock l(_meta_lock);

    _started = true;
    std::list< std::pair<rpc_address, bool> > nodes;
    for (const rpc_address& node: _alive_set) {
        nodes.push_back( std::make_pair(node, true) );
    }
    for (const rpc_address& node: _dead_set) {
        nodes.push_back( std::make_pair(node, false) );
    }
    for (auto& node_pair: nodes) {
        tasking::enqueue(
            LPC_META_STATE_HIGH,
            nullptr,
            std::bind(&server_state::on_change_node_state, _state.get(), node_pair.first, node_pair.second),
            server_state::s_state_write_hash
        );
    }

    tasking::enqueue_timer(
        LPC_META_STATE_NORMAL,
        nullptr,
        std::bind(&meta_service::balancer_run, this),
        std::chrono::milliseconds(_opts.lb_interval_ms),
        server_state::s_state_write_hash,
        std::chrono::milliseconds(_opts.lb_interval_ms)
    );
}

error_code meta_service::start()
{
    dassert(!_started, "meta service is already started");
    error_code err;

    err = remote_storage_initialize();
    if (err != ERR_OK)
    {
        derror("init remote storage failed, err = %s", err.to_string());
        return err;
    }
    ddebug("remote storage is successfully initialized");

    // we should start the FD service to response to the workers fd request
    _failure_detector.reset(new meta_server_failure_detector(this));
    err = _failure_detector->start(
        _opts.fd_check_interval_seconds,
        _opts.fd_beacon_interval_seconds,
        _opts.fd_lease_seconds,
        _opts.fd_grace_seconds,
        false
    );

    if (err != ERR_OK)
    {
        derror("start failure_detector failed, err = %s", err.to_string());
        return err;
    }
    ddebug("meta service failure detector is successfully started");

    //should register rpc handlers before acquiring leader lock, so that this meta service
    //can tell others who is the current leader
    register_rpc_handlers();
    
    _failure_detector->acquire_leader_lock();
    dassert(_failure_detector->is_primary(), "must be primary at this point");
    ddebug("%s got the primary lock, start to recover server state from remote storage", primary_address().to_string());

    _state->initialize(this, meta_options::concat_path_unix_style(_cluster_root, "apps"));
    while ((err = _state->initialize_data_structure()) != ERR_OK)
    {
        derror("recover server state failed, err = %s, retry ...", err.to_string());
    }
    _state->register_cli_commands();

    server_load_balancer* balancer = utils::factory_store<server_load_balancer>::create(
        _meta_opts.server_load_balancer_type.c_str(),
        PROVIDER_TYPE_MAIN,
        this);
    _balancer.reset(balancer);

    _failure_detector->sync_node_state_and_start_service();
    ddebug("start meta_service succeed");
    return ERR_OK;
}

void meta_service::register_rpc_handlers()
{
    register_rpc_handler(
        RPC_CM_QUERY_NODE_PARTITIONS,
        "query_configuration_by_node",
        &meta_service::on_query_configuration_by_node
        );
    register_rpc_handler(
        RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX,
        "query_configuration_by_index",
        &meta_service::on_query_configuration_by_index
        );
    register_rpc_handler(
        RPC_CM_UPDATE_PARTITION_CONFIGURATION,
        "update_configuration",
        &meta_service::on_update_configuration
        );
    register_rpc_handler(
        RPC_CM_CREATE_APP,
        "create_app",
        &meta_service::on_create_app
        );
    register_rpc_handler(
        RPC_CM_DROP_APP,
        "drop_app",
        &meta_service::on_drop_app
        );
    register_rpc_handler(
        RPC_CM_LIST_APPS,
        "list_apps",
        &meta_service::on_list_apps
        );
    register_rpc_handler(
        RPC_CM_LIST_NODES,
        "list_nodes",
        &meta_service::on_list_nodes
        );
    register_rpc_handler(
        RPC_CM_CLUSTER_INFO,
        "cluster_info",
        &meta_service::on_query_cluster_info
        );
    register_rpc_handler(
        RPC_CM_PROPOSE_BALANCER,
        "propose_balancer",
        &meta_service::on_propose_balancer
        );
    register_rpc_handler(
        RPC_CM_CONTROL_META,
        "control_meta",
        &meta_service::on_control_meta
        );
}

int meta_service::check_primary(dsn_message_t req)
{
    if (!_failure_detector->is_primary())
    {
        dsn_msg_options_t options;
        dsn_msg_get_options(req, &options);
        if (options.context.u.is_forward_not_supported)
            return -1;

        auto primary = _failure_detector->get_primary();
        dinfo("primary address: %s", primary.to_string());
        if (!primary.is_invalid())
        {
            dsn_rpc_forward(req, _failure_detector->get_primary().c_addr());
            return 0;
        }
    }

    return 1;
}

#define RPC_CHECK_STATUS(dsn_msg, response_struct)\
    dinfo("rpc %s called", __FUNCTION__);\
    int result = check_primary(dsn_msg);\
    if (result == 0) return;\
    if (result == -1 || !_started) {\
        response_struct.err = (result==-1)?ERR_FORWARD_TO_OTHERS:ERR_SERVICE_NOT_ACTIVE;\
        reply(dsn_msg, response_struct);\
        return;\
    }\

// table operations
void meta_service::on_create_app(dsn_message_t req)
{
    configuration_create_app_response response;
    RPC_CHECK_STATUS(req, response);

    dsn_msg_add_ref(req);
    tasking::enqueue(LPC_META_STATE_NORMAL, nullptr, std::bind(&server_state::create_app, _state.get(), req), server_state::s_state_write_hash);
}

void meta_service::on_drop_app(dsn_message_t req)
{
    configuration_drop_app_response response;
    RPC_CHECK_STATUS(req, response);

    dsn_msg_add_ref(req);
    tasking::enqueue(LPC_META_STATE_NORMAL, nullptr, std::bind(&server_state::drop_app, _state.get(), req), server_state::s_state_write_hash);
}

void meta_service::on_list_apps(dsn_message_t req)
{
    configuration_list_apps_response response;
    RPC_CHECK_STATUS(req, response);

    configuration_list_apps_request request;
    ::dsn::unmarshall(req, request);
    _state->list_apps(request, response);
    reply(req, response);
}

void meta_service::on_list_nodes(dsn_message_t req)
{
    configuration_list_nodes_response response;
    RPC_CHECK_STATUS(req, response);

    configuration_list_nodes_request request;
    dsn::unmarshall(req, request);

    {
        zauto_read_lock l(_meta_lock);
        dsn::replication::node_info info;
        if (request.status == node_status::NS_INVALID || request.status == node_status::NS_ALIVE)
        {
            info.status = node_status::NS_ALIVE;
            for (auto& node: _alive_set) {
                info.address = node;
                response.infos.push_back(info);
            }
        }
        if (request.status == node_status::NS_INVALID || request.status == node_status::NS_UNALIVE)
        {
            info.status = node_status::NS_UNALIVE;
            for (auto& node: _dead_set) {
                info.address = node;
                response.infos.push_back(info);
            }
        }
        response.err = dsn::ERR_OK;
    }

    reply(req, response);
}

void meta_service::on_query_cluster_info(dsn_message_t req)
{
    configuration_cluster_info_response response;
    RPC_CHECK_STATUS(req, response);

    configuration_cluster_info_request request;
    dsn::unmarshall(req, request);

    std::stringstream oss;
    response.keys.push_back("meta_servers");
    for (size_t i = 0; i < _opts.meta_servers.size(); ++i)
    {
        if (i != 0)
            oss << ", ";
        oss << _opts.meta_servers[i].to_string();
    }
    response.values.push_back(oss.str());
    response.keys.push_back("primary_meta_server");
    response.values.push_back(_failure_detector->get_primary().to_string());
    response.keys.push_back("remote_storage_cluster_root");
    response.values.push_back(_cluster_root);
    response.err = dsn::ERR_OK;

    reply(req, response);
}

// partition server & client => meta server
void meta_service::on_query_configuration_by_node(dsn_message_t msg)
{
    configuration_query_by_node_response response;
    RPC_CHECK_STATUS(msg, response);

    configuration_query_by_node_request request;
    dsn::unmarshall(msg, request);
    _state->query_configuration_by_node(request, response);
    reply(msg, response);    
}

void meta_service::on_query_configuration_by_index(dsn_message_t msg)
{
    configuration_query_by_index_response response;
    RPC_CHECK_STATUS(msg, response);

    configuration_query_by_index_request request;
    dsn::unmarshall(msg, request);
    _state->query_configuration_by_index(request, response);
    reply(msg, response);
}

void meta_service::on_update_configuration(dsn_message_t req)
{
    configuration_update_response response;
    RPC_CHECK_STATUS(req, response);

    std::shared_ptr<configuration_update_request> request = std::make_shared<configuration_update_request>();
    dsn::unmarshall(req, *request);

    if (is_service_freezed())
    {
        response.err = ERR_STATE_FREEZED;
        _state->query_configuration_by_gpid(request->config.pid, response.config);
        reply(req, response);
        return;
    }

    dsn_msg_add_ref(req);
    tasking::enqueue(LPC_META_STATE_HIGH,
        nullptr,
        std::bind(&server_state::on_update_configuration, _state.get(), request, req),
        server_state::s_state_write_hash
    );
}

void meta_service::on_control_meta(dsn_message_t req)
{
    configuration_meta_control_request request;
    configuration_balancer_response response;
    RPC_CHECK_STATUS(req, response);

    dsn::unmarshall(req, request);
    ddebug("get control meta rpc, flags(%d), type(%d), current flags(%d)", request.ctrl_flags, request.ctrl_type, _meta_ctrl_flags);
    {
        zauto_write_lock l(_meta_lock);
        switch (request.ctrl_type)
        {
        case meta_ctrl_type::meta_flags_and:
            _meta_ctrl_flags &= request.ctrl_flags;
            break;
        case meta_ctrl_type::meta_flags_or:
            _meta_ctrl_flags |= request.ctrl_flags;
            break;
        case meta_ctrl_type::meta_flags_overwrite:
            _meta_ctrl_flags = request.ctrl_flags;
            break;
        default:
            ddebug("invalid requst ctrl type: %d", request.ctrl_type);
            break;
        }
    }
    if (request.ctrl_flags&meta_ctrl_flags::ctrl_disable_replica_migration) {
        tasking::enqueue(LPC_META_STATE_NORMAL,
            nullptr,
            std::bind(&server_state::clear_proposals, _state.get()),
            server_state::s_state_write_hash
        );
    }
    response.err = ERR_OK;
    reply(req, response);
}

void meta_service::on_propose_balancer(dsn_message_t req)
{
    configuration_balancer_request request;
    configuration_balancer_response response;
    RPC_CHECK_STATUS(req, response);

    dsn::unmarshall(req, request);
    ddebug("get proposal balancer request, gpid(%d.%d)", request.gpid.get_app_id(), request.gpid.get_partition_index());
    _state->on_propose_balancer(request, response);
    reply(req, response);
}

}}
