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

#include "meta_service.h"
#include "server_state.h"
#include "meta_server_failure_detector.h"
#include "greedy_load_balancer.h"
#include <sys/stat.h>
#include <dsn/internal/factory_store.h>

# include <dsn/cpp/json_helper.h>

# include <rapidjson/document.h> 
# include <rapidjson/writer.h>
# include <rapidjson/stringbuffer.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "meta.service"

meta_service::meta_service()
    : serverlet("meta_service"), _failure_detector(nullptr), _balancer(nullptr), _started(false)
{
    _opts.initialize();
    // create in constructor because it may be used in checker before started
    _state = new server_state();
}

meta_service::~meta_service()
{
}

error_code meta_service::start()
{
    dassert(!_started, "meta service is already started");

    // init server state
    error_code err = _state->initialize();
    if (err != ERR_OK)
    {
        derror("init server_state failed, err = %s", err.to_string());
        return err;
    }
    ddebug("init server state succeed");

    // we should start the FD service to response to the workers fd request
    _failure_detector = new meta_server_failure_detector(_state, this);
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
    
    // should register rpc handlers before acquiring leader lock, so that this meta service
    // can tell others who is the current leader
    register_rpc_handlers();
    
    // become leader
    _failure_detector->acquire_leader_lock();
    dassert(_failure_detector->is_primary(), "must be primary at this point");
    ddebug("hahaha, I got the primary lock! now start to recover server state");

    // recover server state
    while ((err = _state->on_become_leader()) != ERR_OK)
    {
        derror("recover server state failed, err = %s, retry ...", err.to_string());
    }

    // create server load balancer
    // TODO: create per app server load balancer
    const char* server_load_balancer = dsn_config_get_value_string(
        "meta_server",
        "server_load_balancer_type",
        "simple_load_balancer",
        "server_load_balancer provider type"
        );
    
    _balancer = dsn::utils::factory_store< ::dsn::dist::server_load_balancer>::create(
        server_load_balancer,
        PROVIDER_TYPE_MAIN,
        _state
        );

    _failure_detector->sync_node_state_and_start_service();
    ddebug("start meta_service succeed");
    return ERR_OK;
}

static void __svc_cli_freeer__(dsn_cli_reply reply)
{
    std::string* s = (std::string*)reply.context;
    delete s;
}

void meta_service::register_rpc_handlers()
{
    register_rpc_handler(
        RPC_CM_QUERY_NODE_PARTITIONS,
        "RPC_CM_QUERY_NODE_PARTITIONS",
        &meta_service::on_query_configuration_by_node
        );

    register_rpc_handler(
        RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX,
        "RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX",
        &meta_service::on_query_configuration_by_index
        );

    register_rpc_handler(
        RPC_CM_UPDATE_PARTITION_CONFIGURATION,
        "RPC_CM_UPDATE_PARTITION_CONFIGURATION",
        &meta_service::on_update_configuration
        );

    register_rpc_handler(
        RPC_CM_MODIFY_REPLICA_CONFIG_COMMAND,
        "RPC_CM_MODIFY_REPLICA_CONFIG_COMMAND",
        &meta_service::on_modify_replica_config_explictly
        );

    register_rpc_handler(
        RPC_CM_CREATE_APP,
        "RPC_CM_CREATE_APP",
        &meta_service::on_create_app
        );

    register_rpc_handler(
        RPC_CM_DROP_APP,
        "RPC_CM_DROP_APP",
        &meta_service::on_drop_app
        );

    register_rpc_handler(
        RPC_CM_LIST_APPS,
        "RPC_CM_LIST_APPS",
        &meta_service::on_list_apps
        );

    register_rpc_handler(
        RPC_CM_LIST_NODES,
        "RPC_CM_LIST_NODES",
        &meta_service::on_list_nodes
        );

    register_rpc_handler(
        RPC_CM_CONTROL_BALANCER_MIGRATION,
        "RPC_CM_CONTROL_BALANCER_MIGRATION",
        &meta_service::on_control_balancer_migration);

    register_rpc_handler(
        RPC_CM_BALANCER_PROPOSAL,
        "RPC_CM_BALANCER_PROPOSAL",
        &meta_service::on_balancer_proposal);


    dsn_cli_app_register(
        "create_app",
        "create app on meta server (in json format)",
        "create app on meta server and auto-deployed in cluster",
        (void*)this,
        [](void *context, int argc, const char **argv, dsn_cli_reply *reply)
        {
            auto this_ = (meta_service*)context;
            this_->on_create_app_cli(context, argc, argv, reply);
        },
        __svc_cli_freeer__
        );

    dsn_cli_app_register(
        "drop_app",
        "drop app on meta server (in json format)",
        "drop app on meta server and auto-undeployed in cluster",
        (void*)this,
        [](void *context, int argc, const char **argv, dsn_cli_reply *reply)
        {
            auto this_ = (meta_service*)context;
            this_->on_drop_app_cli(context, argc, argv, reply);
        },
        __svc_cli_freeer__
        );

    dsn_cli_app_register(
        "list_apps",
        "list apps on meta server (in json format)",
        "list apps and their status on meta server",
        (void*)this,
        [](void *context, int argc, const char **argv, dsn_cli_reply *reply)
        {
            auto this_ = (meta_service*)context;
            this_->on_list_apps_cli(context, argc, argv, reply);
        },
        __svc_cli_freeer__
        );

    dsn_cli_app_register(
        "list_nodes",
        "list nodes on meta server (in json format)",
        "list nodes and their status on meta server",
        (void*)this,
        [](void *context, int argc, const char **argv, dsn_cli_reply *reply)
        {
            auto this_ = (meta_service*)context;
            this_->on_list_nodes_cli(context, argc, argv, reply);
        },
        __svc_cli_freeer__
        );

    dsn_cli_app_register(
        "query_config_by_app",
        "query app configurations on meta server (in json format)",
        "query app configurations on meta server with app id",
        (void*)this,
        [](void *context, int argc, const char **argv, dsn_cli_reply *reply)
        {
            auto this_ = (meta_service*)context;
            this_->on_query_config_by_app_cli(context, argc, argv, reply);
        },
        __svc_cli_freeer__
        );

    dsn_cli_app_register(
        "query_config_by_node",
        "query apps on one node (in json format)",
        "query apps on one node with node address",
        (void*)this,
        [](void *context, int argc, const char **argv, dsn_cli_reply *reply)
        {
            auto this_ = (meta_service*)context;
            this_->on_query_config_by_node_cli(context, argc, argv, reply);
        },
        __svc_cli_freeer__
        );
}

void meta_service::stop()
{
    _started = false;

    unregister_rpc_handler(RPC_CM_QUERY_NODE_PARTITIONS);
    unregister_rpc_handler(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX);
    unregister_rpc_handler(RPC_CM_UPDATE_PARTITION_CONFIGURATION);
    unregister_rpc_handler(RPC_CM_MODIFY_REPLICA_CONFIG_COMMAND);
    unregister_rpc_handler(RPC_CM_CREATE_APP);
    unregister_rpc_handler(RPC_CM_DROP_APP);
    unregister_rpc_handler(RPC_CM_CONTROL_BALANCER_MIGRATION);
    unregister_rpc_handler(RPC_CM_BALANCER_PROPOSAL);

    if (_balancer_timer != nullptr)
    {
        _balancer_timer->cancel(true);
    }

    if (_balancer != nullptr)
    {
        delete _balancer;
        _balancer = nullptr;
    }

    if (_failure_detector != nullptr)
    {
        _failure_detector->stop();
        delete _failure_detector;
        _failure_detector = nullptr;
    }

    if (_state != nullptr)
    {
        delete _state;
        _state = nullptr;
    }
}

void meta_service::start_load_balance()
{
    dassert(_balancer_timer == nullptr, "");

    _state->unfree_if_possible_on_start();
    _balancer_timer = tasking::enqueue_timer(LPC_LBM_RUN, this, [this] {on_load_balance_timer();},
        std::chrono::milliseconds(_opts.lb_interval_ms)
        );

    _started = true;
}

bool meta_service::check_primary(dsn_message_t req)
{
    if (!_failure_detector->is_primary())
    {
        auto primary = _failure_detector->get_primary();
        dinfo("primary address: %s", primary.to_string());
        if (!primary.is_invalid())
        {
            dsn_rpc_forward(req, _failure_detector->get_primary().c_addr());
            return false;
        }
    }

    return true;
}

#define META_STATUS_CHECK_ON_RPC(dsn_msg, response_struct)\
    dinfo("rpc %s called", __FUNCTION__);\
    if ( !check_primary(dsn_msg) )\
        return;\
    if ( !_started )\
    {\
        response_struct.err = ERR_SERVICE_NOT_ACTIVE;\
        reply(dsn_msg, response_struct);\
        return;\
    }\

// create app cli
inline error_code unmarshall_json(const char* json_str, const char* key, /*out*/ configuration_create_app_request& val)
{
    // TODO:
    // 
    return ::dsn::ERR_OK;
}

inline std::string marshall_json(const configuration_create_app_response& val)
{
    std::stringstream ss;
    JSON_DICT_ENTRIES(ss, val, err, appid);

    return std::move(ss.str());
}

void meta_service::on_create_app_cli(void *context, int argc, const char **argv, dsn_cli_reply *reply)
{
    dassert(context == this, "must called with local context");

    error_code err = ERR_INVALID_PARAMETERS;
    configuration_create_app_request req;
    configuration_create_app_response resp;
    
    if (argc == 0 || ERR_OK != (err = unmarshall_json(argv[0], "req", req)))
    {
        resp.err = err;
    }
    else
    {
        _state->create_app(req, resp);
    }

    std::string* resp_json = new std::string();
    *resp_json = std::move(marshall_json(resp));
    reply->context = resp_json;
    reply->message = (const char*)resp_json->c_str();
    reply->size = resp_json->size();
    return;
}

// drop app cli
inline error_code unmarshall_json(const char* json_str, const char* key, /*out*/ configuration_drop_app_request& val)
{
    // TODO:
    // 
    return ::dsn::ERR_OK;
}

inline std::string marshall_json(const configuration_drop_app_response& val)
{
    std::stringstream ss;
    JSON_DICT_ENTRIES(ss, val, err);

    return std::move(ss.str());
}

void meta_service::on_drop_app_cli(void *context, int argc, const char **argv, dsn_cli_reply *reply)
{
    dassert(context == this, "must called with local context");

    error_code err = ERR_INVALID_PARAMETERS;
    configuration_drop_app_request req;
    configuration_drop_app_response resp;

    if (argc == 0 || ERR_OK != (err = unmarshall_json(argv[0], "req", req)))
    {
        resp.err = err;
    }
    else
    {
        _state->drop_app(req, resp);
    }

    std::string* resp_json = new std::string();
    *resp_json = std::move(marshall_json(resp));
    reply->context = resp_json;
    reply->message = (const char*)resp_json->c_str();
    reply->size = resp_json->size();
    return;
}

// list_apps
inline std::string marshall_json(const configuration_list_apps_response& val)
{
    std::stringstream ss;
    JSON_DICT_ENTRIES(ss, val, err, infos);

    return std::move(ss.str());
}

void meta_service::on_list_apps_cli(void *context, int argc, const char **argv, dsn_cli_reply *reply)
{
    dassert(context == this, "must called with local context");

    configuration_list_apps_request req;
    configuration_list_apps_response resp;

    _state->list_apps(req, resp);

    std::string* resp_json = new std::string();
    *resp_json = std::move(marshall_json(resp));
    reply->context = resp_json;
    reply->message = (const char*)resp_json->c_str();
    reply->size = resp_json->size();
    return;
}

// list nodes
inline std::string marshall_json(const configuration_list_nodes_response& val)
{
    std::stringstream ss;
    JSON_DICT_ENTRIES(ss, val, err, infos);

    return std::move(ss.str());
}

void meta_service::on_list_nodes_cli(void *context, int argc, const char **argv, dsn_cli_reply *reply)
{
    dassert(context == this, "must called with local context");

    configuration_list_nodes_request req;
    configuration_list_nodes_response resp;

    _state->list_nodes(req, resp);

    std::string* resp_json = new std::string();
    *resp_json = std::move(marshall_json(resp));
    reply->context = resp_json;
    reply->message = (const char*)resp_json->c_str();
    reply->size = resp_json->size();
    return;
}


// query app config
inline error_code unmarshall_json(const char* json_str, const char* key, /*out*/ configuration_query_by_index_request& val)
{
    // TODO:
    // 
    return ::dsn::ERR_OK;
}

inline std::string marshall_json(const configuration_query_by_index_response& val)
{
    std::stringstream ss;
    JSON_DICT_ENTRIES(ss, val, err, app_id, partition_count, is_stateful, partitions);

    return std::move(ss.str());
}

void meta_service::on_query_config_by_app_cli(void *context, int argc, const char **argv, dsn_cli_reply *reply)
{
    dassert(context == this, "must called with local context");

    error_code err = ERR_INVALID_PARAMETERS;
    configuration_query_by_index_request req;
    configuration_query_by_index_response resp;

    if (argc == 0 || ERR_OK != (err = unmarshall_json(argv[0], "req", req)))
    {
        resp.err = err;
    }
    else
    {
        _state->query_configuration_by_index(req, resp);
    }

    std::string* resp_json = new std::string();
    *resp_json = std::move(marshall_json(resp));
    reply->context = resp_json;
    reply->message = (const char*)resp_json->c_str();
    reply->size = resp_json->size();
    return;
}

// query node config
inline error_code unmarshall_json(const char* json_str, const char* key, /*out*/ configuration_query_by_node_request& val)
{
    // TODO:
    // 
    return ::dsn::ERR_OK;
}

inline std::string marshall_json(const configuration_query_by_node_response& val)
{
    std::stringstream ss;
    JSON_DICT_ENTRIES(ss, val, err, partitions);

    return std::move(ss.str());
}

void meta_service::on_query_config_by_node_cli(void *context, int argc, const char **argv, dsn_cli_reply *reply)
{
    dassert(context == this, "must called with local context");

    error_code err = ERR_INVALID_PARAMETERS;
    configuration_query_by_node_request req;
    configuration_query_by_node_response resp;

    if (argc == 0 || ERR_OK != (err = unmarshall_json(argv[0], "req", req)))
    {
        resp.err = err;
    }
    else
    {
        _state->query_configuration_by_node(req, resp);
    }

    std::string* resp_json = new std::string();
    *resp_json = std::move(marshall_json(resp));
    reply->context = resp_json;
    reply->message = (const char*)resp_json->c_str();
    reply->size = resp_json->size();
    return;
}


// table operations
void meta_service::on_create_app(dsn_message_t req)
{
    configuration_create_app_response response;
    META_STATUS_CHECK_ON_RPC(req, response);

    configuration_create_app_request request;
    unmarshall(req, request);
    _state->create_app(request, response);
    reply(req, response);
}

void meta_service::on_drop_app(dsn_message_t req)
{
    configuration_drop_app_response response;
    META_STATUS_CHECK_ON_RPC(req, response);

    configuration_drop_app_request request;
    unmarshall(req, request);
    _state->drop_app(request, response);
    reply(req, response);
}

void meta_service::on_list_apps(dsn_message_t req)
{
    configuration_list_apps_response response;
    META_STATUS_CHECK_ON_RPC(req, response);

    configuration_list_apps_request request;
    _state->list_apps(request, response);
    reply(req, response);
}

void meta_service::on_list_nodes(dsn_message_t req)
{
    configuration_list_nodes_response response;
    META_STATUS_CHECK_ON_RPC(req, response);

    configuration_list_nodes_request request;
    _state->list_nodes(request, response);
    reply(req, response);
}

// partition server & client => meta server
void meta_service::on_query_configuration_by_node(dsn_message_t msg)
{
    configuration_query_by_node_request request;
    configuration_query_by_node_response response;
    META_STATUS_CHECK_ON_RPC(msg, response);

    ::unmarshall(msg, request);
    _state->query_configuration_by_node(request, response);
    reply(msg, response);    
}

void meta_service::on_query_configuration_by_index(dsn_message_t msg)
{
    configuration_query_by_index_request request;
    configuration_query_by_index_response response;
    META_STATUS_CHECK_ON_RPC(msg, response);

    ::unmarshall(msg, request);
    _state->query_configuration_by_index(request, response);
    reply(msg, response);
}

void meta_service::on_modify_replica_config_explictly(dsn_message_t req)
{
    if (!check_primary(req))
        return;

    global_partition_id gpid;
    rpc_address receiver;
    config_type type;
    rpc_address node;

    ::unmarshall(req, gpid);
    ::unmarshall(req, receiver);
    ::unmarshall(req, type);
    ::unmarshall(req, node);

    _balancer->explictly_send_proposal(gpid, receiver, type, node);
}

void meta_service::on_update_configuration(dsn_message_t req)
{
    configuration_update_response response;
    META_STATUS_CHECK_ON_RPC(req, response);

    std::shared_ptr<configuration_update_request> request(new configuration_update_request);
    ::unmarshall(req, *request);

    if (_state->freezed())
    {
        response.err = ERR_STATE_FREEZED;
        _state->query_configuration_by_gpid(request->config.gpid, response.config);
        reply(req, response);
        return;
    }
  
    global_partition_id gpid = request->config.gpid;
    _state->update_configuration(request, req, [this, gpid, request]() mutable
    {
        if (_started)
        {
            _balancer->on_config_changed(request);
            tasking::enqueue(LPC_LBM_RUN, this, std::bind(&meta_service::on_config_changed, this, gpid));
        }
    });
}

void meta_service::update_configuration_on_machine_failure(std::shared_ptr<configuration_update_request>& update)
{
    global_partition_id gpid = update->config.gpid;
    _state->update_configuration(update, nullptr, [this, gpid, update]() mutable
    {
        if (_started)
        {
            _balancer->on_config_changed(update);
            tasking::enqueue(LPC_LBM_RUN, this, std::bind(&meta_service::on_config_changed, this, gpid));
        }
    });
}

void meta_service::on_control_balancer_migration(dsn_message_t req)
{
    control_balancer_migration_request request;
    control_balancer_migration_response response;
    META_STATUS_CHECK_ON_RPC(req, response);

    ::unmarshall(req, request);
    _balancer->on_control_migration(request, response);
    reply(req, response);
}

void meta_service::on_balancer_proposal(dsn_message_t req)
{
    balancer_proposal_request request;
    balancer_proposal_response response;
    META_STATUS_CHECK_ON_RPC(req, response);

    ::unmarshall(req, request);
    dinfo("balancer proposal, gpid(%d.%d), type(%s), from(%s), to(%s)",
          request.gpid.app_id, request.gpid.pidx,
          enum_to_string(request.type),
          request.from_addr.to_string(),
          request.to_addr.to_string());
    _balancer->on_balancer_proposal(request, response);
    reply(req, response);
}

// local timers
void meta_service::on_load_balance_timer()
{
    if (!_started)
        return;

    if (_state->freezed())
        return;

    if (_failure_detector->is_primary())
    {
        _balancer->run();
    }
}

void meta_service::on_config_changed(global_partition_id gpid)
{
    if (_failure_detector->is_primary())
    {
        _balancer->run(gpid);
    }
}
