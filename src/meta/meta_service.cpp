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

// IWYU pragma: no_include <boost/detail/basic_pointerbuf.hpp>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <boost/lexical_cast.hpp>
#include <algorithm> // for std::remove_if
#include <chrono>
#include <cstdint>
#include <functional>
#include <ostream>
#include <string_view>
#include <unordered_map>
#include <utility>

#include "backup_types.h"
#include "bulk_load_types.h"
#include "common/common.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "dsn.layer2_types.h"
#include "duplication_types.h"
#include "meta/duplication/meta_duplication_service.h"
#include "meta/meta_backup_service.h"
#include "meta/meta_data.h"
#include "meta/meta_options.h"
#include "meta/meta_rpc_types.h"
#include "meta/meta_server_failure_detector.h"
#include "meta/meta_state_service.h"
#include "meta/meta_state_service_utils.h"
#include "meta/partition_guardian.h"
#include "meta_bulk_load_service.h"
#include "meta_service.h"
#include "meta_split_service.h"
#include "partition_split_types.h"
#include "ranger/ranger_resource_policy_manager.h"
#include "remote_cmd/remote_command.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_holder.h"
#include "server_load_balancer.h"
#include "server_state.h"
#include "task/async_calls.h"
#include "utils/autoref_ptr.h"
#include "utils/command_manager.h"
#include "utils/factory_store.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"

DSN_DECLARE_string(hosts_list);
DSN_DEFINE_bool(meta_server,
                recover_from_replica_server,
                false,
                "Whether to recover tables from replica servers when there is no "
                "data of the tables in remote storage");
DSN_DEFINE_bool(meta_server, cold_backup_disabled, true, "whether to disable cold backup");
DSN_DEFINE_bool(meta_server,
                enable_white_list,
                false,
                "whether to enable white list of replica servers");
DSN_DEFINE_uint64(meta_server,
                  min_live_node_count_for_unfreeze,
                  3,
                  "If the number of ALIVE nodes is less than this threshold, MetaServer will "
                  "also enter the 'freezed' protection state");
DSN_TAG_VARIABLE(min_live_node_count_for_unfreeze, FT_MUTABLE);
DSN_DEFINE_validator(min_live_node_count_for_unfreeze,
                     [](uint64_t min_live_node_count) -> bool { return min_live_node_count > 0; });

DSN_DEFINE_int32(replication,
                 lb_interval_ms,
                 10000,
                 "The interval milliseconds of meta server to execute load balance");
DSN_DEFINE_int32(meta_server,
                 node_live_percentage_threshold_for_update,
                 65,
                 "If the proportion of ALIVE nodes is less than this threshold, MetaServer will "
                 "enter the 'freezed' protection state");
DSN_DEFINE_validator(node_live_percentage_threshold_for_update,
                     [](int32_t value) -> bool { return value >= 0 && value <= 100; });
DSN_DEFINE_string(meta_server,
                  meta_state_service_type,
#ifdef MOCK_TEST
                  "meta_state_service_simple",
#else
                  "meta_state_service_zookeeper",
#endif
                  "The implementation class of metadata storage service");
DSN_DEFINE_string(meta_server,
                  cluster_root,
                  "/",
                  "The root of the cluster meta state service to be stored on remote storage. "
                  "Different meta servers in the same cluster need to be configured with the "
                  "same value, while different clusters using different values if they share "
                  "the same remote storage");
DSN_DEFINE_string(meta_server,
                  server_load_balancer_type,
                  "greedy_load_balancer",
                  "The implementation class of load balancer");
DSN_DEFINE_string(meta_server,
                  partition_guardian_type,
                  "partition_guardian",
                  "partition guardian provider");

DSN_DECLARE_bool(duplication_enabled);
DSN_DECLARE_int32(fd_beacon_interval_seconds);
DSN_DECLARE_int32(fd_check_interval_seconds);
DSN_DECLARE_int32(fd_grace_seconds);
DSN_DECLARE_int32(fd_lease_seconds);
DSN_DECLARE_string(cold_backup_root);

METRIC_DEFINE_counter(server,
                      replica_server_disconnections,
                      dsn::metric_unit::kDisconnections,
                      "The number of disconnections with replica servers");

METRIC_DEFINE_gauge_int64(server,
                          unalive_replica_servers,
                          dsn::metric_unit::kServers,
                          "The number of unalive replica servers");

METRIC_DEFINE_gauge_int64(server,
                          alive_replica_servers,
                          dsn::metric_unit::kServers,
                          "The number of alive replica servers");

namespace dsn {
namespace replication {

#define CHECK_APP_ID_STATUS_AND_AUTHZ(app_id)                                                      \
    do {                                                                                           \
        const auto &_app_id = (app_id);                                                            \
        const auto &_app = _state->get_app(_app_id);                                               \
        if (!_app) {                                                                               \
            rpc.response().err = ERR_INVALID_PARAMETERS;                                           \
            LOG_WARNING("reject request on app_id = {}", _app_id);                                 \
            return;                                                                                \
        }                                                                                          \
        const std::string &app_name = _app->app_name;                                              \
        if (!check_status_and_authz(rpc, nullptr, app_name)) {                                     \
            return;                                                                                \
        }                                                                                          \
    } while (0)

meta_service::meta_service()
    : serverlet("meta_service"),
      _failure_detector(nullptr),
      _started(false),
      _recovering(false),
      METRIC_VAR_INIT_server(replica_server_disconnections),
      METRIC_VAR_INIT_server(unalive_replica_servers),
      METRIC_VAR_INIT_server(alive_replica_servers)
{
    _opts.initialize();
    _meta_opts.initialize();
    _node_live_percentage_threshold_for_update = FLAGS_node_live_percentage_threshold_for_update;
    _state.reset(new server_state());
    _function_level.store(_meta_opts.meta_function_level_on_start);
    if (FLAGS_recover_from_replica_server) {
        LOG_INFO("enter recovery mode for [meta_server].recover_from_replica_server = true");
        _recovering = true;
        if (_meta_opts.meta_function_level_on_start > meta_function_level::fl_steady) {
            LOG_INFO("meta server function level changed to fl_steady under recovery mode");
            _function_level.store(meta_function_level::fl_steady);
        }
    }

    _meta_op_status.store(meta_op_status::FREE);
}

meta_service::~meta_service() { stop(); }

void meta_service::stop()
{
    zauto_write_lock l(_meta_lock);
    if (!_started.load()) {
        return;
    }
    _tracker.cancel_outstanding_tasks();
    _ctrl_node_live_percentage_threshold_for_update.reset();
    _failure_detector.reset();
    _balancer.reset();
    _partition_guardian.reset();
    _started = false;
}

bool meta_service::check_freeze() const
{
    zauto_lock l(_failure_detector->_lock);
    if (_alive_set.size() < FLAGS_min_live_node_count_for_unfreeze)
        return true;
    int total = _alive_set.size() + _dead_set.size();
    return _alive_set.size() * 100 < _node_live_percentage_threshold_for_update * total;
}

error_code meta_service::remote_storage_initialize()
{
    // create storage
    dsn::dist::meta_state_service *storage =
        dsn::utils::factory_store<::dsn::dist::meta_state_service>::create(
            FLAGS_meta_state_service_type, PROVIDER_TYPE_MAIN);
    error_code err = storage->initialize(_meta_opts.meta_state_service_args);
    if (err != ERR_OK) {
        LOG_ERROR("init meta_state_service failed, err = {}", err);
        return err;
    }
    _storage.reset(storage);
    _meta_storage.reset(new mss::meta_storage(_storage.get(), &_tracker));

    std::vector<std::string> slices;
    utils::split_args(FLAGS_cluster_root, slices, '/');
    std::string current = "";
    for (unsigned int i = 0; i != slices.size(); ++i) {
        current = utils::filesystem::concat_path_unix_style(current, slices[i]);
        task_ptr tsk =
            _storage->create_node(current, LPC_META_CALLBACK, [&err](error_code ec) { err = ec; });
        tsk->wait();
        if (err != ERR_OK && err != ERR_NODE_ALREADY_EXIST) {
            LOG_ERROR("create node failed, node_path = {}, err = {}", current, err);
            return err;
        }
    }
    _cluster_root = current.empty() ? "/" : current;

    LOG_INFO("init meta_state_service succeed, cluster_root = {}", _cluster_root);
    return ERR_OK;
}

// visited in protection of failure_detector::_lock
void meta_service::set_node_state(const std::vector<host_port> &nodes, bool is_alive)
{
    for (auto &node : nodes) {
        if (is_alive) {
            _alive_set.insert(node);
            _dead_set.erase(node);
        } else {
            _alive_set.erase(node);
            _dead_set.insert(node);
        }
    }

    METRIC_VAR_INCREMENT_BY(replica_server_disconnections, is_alive ? 0 : nodes.size());
    METRIC_VAR_SET(unalive_replica_servers, _dead_set.size());
    METRIC_VAR_SET(alive_replica_servers, _alive_set.size());

    if (!_started) {
        return;
    }
    for (const auto &hp : nodes) {
        tasking::enqueue(LPC_META_STATE_HIGH,
                         nullptr,
                         std::bind(&server_state::on_change_node_state, _state.get(), hp, is_alive),
                         server_state::sStateHash);
    }
}

void meta_service::get_node_state(/*out*/ std::map<host_port, bool> &all_nodes)
{
    zauto_lock l(_failure_detector->_lock);
    for (auto &node : _alive_set)
        all_nodes[node] = true;
    for (auto &node : _dead_set)
        all_nodes[node] = false;
}

void meta_service::balancer_run() { _state->check_all_partitions(); }

bool meta_service::try_lock_meta_op_status(meta_op_status op_status)
{
    meta_op_status expected = meta_op_status::FREE;
    if (!_meta_op_status.compare_exchange_strong(expected, op_status)) {
        LOG_ERROR("LOCK meta op status failed, meta "
                  "server is busy, current op status is {}",
                  enum_to_string(expected));
        return false;
    }

    LOG_INFO("LOCK meta op status to {}", enum_to_string(op_status));
    return true;
}

void meta_service::unlock_meta_op_status()
{
    LOG_INFO("UNLOCK meta op status from {}", enum_to_string(_meta_op_status.load()));
    _meta_op_status.store(meta_op_status::FREE);
}

void meta_service::register_ctrl_commands()
{
    _ctrl_node_live_percentage_threshold_for_update =
        dsn::command_manager::instance().register_int_command(
            _node_live_percentage_threshold_for_update,
            FLAGS_node_live_percentage_threshold_for_update,
            "meta.live_percentage",
            "node live percentage threshold for update",
            [](int32_t new_value) -> bool { return new_value >= 0 && new_value <= 100; });
}

void meta_service::start_service()
{
    zauto_lock l(_failure_detector->_lock);

    const meta_view view = _state->get_meta_view();
    for (auto &kv : *view.nodes) {
        if (_dead_set.find(kv.first) == _dead_set.end())
            _alive_set.insert(kv.first);
    }

    METRIC_VAR_SET(alive_replica_servers, _alive_set.size());

    for (const auto &node : _alive_set) {
        // sync alive set and the failure_detector
        _failure_detector->unregister_worker(node);
        _failure_detector->register_worker(node, true);
    }

    _ranger_resource_policy_manager =
        std::make_shared<ranger::ranger_resource_policy_manager>(this);

    _access_controller = security::create_meta_access_controller(_ranger_resource_policy_manager);

    _started = true;
    for (const auto &node : _alive_set) {
        tasking::enqueue(LPC_META_STATE_HIGH,
                         nullptr,
                         std::bind(&server_state::on_change_node_state, _state.get(), node, true),
                         server_state::sStateHash);
    }
    for (const auto &node : _dead_set) {
        tasking::enqueue(LPC_META_STATE_HIGH,
                         nullptr,
                         std::bind(&server_state::on_change_node_state, _state.get(), node, false),
                         server_state::sStateHash);
    }

    tasking::enqueue_timer(LPC_META_STATE_NORMAL,
                           nullptr,
                           std::bind(&meta_service::balancer_run, this),
                           std::chrono::milliseconds(FLAGS_lb_interval_ms),
                           server_state::sStateHash,
                           std::chrono::milliseconds(FLAGS_lb_interval_ms));

    if (!FLAGS_cold_backup_disabled) {
        LOG_INFO("start backup service");
        tasking::enqueue(LPC_DEFAULT_CALLBACK,
                         nullptr,
                         std::bind(&backup_service::start, _backup_handler.get()));
    }

    if (_bulk_load_svc) {
        LOG_INFO("start bulk load service");
        tasking::enqueue(LPC_META_CALLBACK, tracker(), [this]() {
            _bulk_load_svc->initialize_bulk_load_service();
        });
    }
}

// the start function is executed in threadpool default
error_code meta_service::start()
{
    CHECK(!_started, "meta service is already started");
    register_ctrl_commands();

    error_code err;

    err = remote_storage_initialize();
    dreturn_not_ok_logged(err, "init remote storage failed, err = {}", err);
    LOG_INFO("remote storage is successfully initialized");

    // start failure detector, and try to acquire the leader lock
    _failure_detector.reset(new meta_server_failure_detector(this));
    if (FLAGS_enable_white_list)
        _failure_detector->set_allow_list(_meta_opts.replica_white_list);
    _failure_detector->register_ctrl_commands();

    CHECK_GT_MSG(FLAGS_fd_grace_seconds, FLAGS_fd_lease_seconds, "");
    err = _failure_detector->start(FLAGS_fd_check_interval_seconds,
                                   FLAGS_fd_beacon_interval_seconds,
                                   FLAGS_fd_lease_seconds,
                                   FLAGS_fd_grace_seconds,
                                   FLAGS_enable_white_list);

    dreturn_not_ok_logged(err, "start failure_detector failed, err = {}", err);
    LOG_INFO("meta service failure detector is successfully started {}",
             FLAGS_enable_white_list ? "with whitelist enabled" : "");

    // should register rpc handlers before acquiring leader lock, so that this meta service
    // can tell others who is the current leader
    register_rpc_handlers();

    // start remote command service before acquiring leader lock,
    // so that the command line call can be handled
    dist::cmd::register_remote_command_rpc();

    _failure_detector->acquire_leader_lock();
    CHECK(_failure_detector->get_leader(nullptr), "must be primary at this point");
    LOG_INFO("{}({}) got the primary lock, start to recover server state from remote storage",
             dsn_primary_host_port(),
             dsn_primary_address());

    // initialize the load balancer
    server_load_balancer *balancer = utils::factory_store<server_load_balancer>::create(
        FLAGS_server_load_balancer_type, PROVIDER_TYPE_MAIN, this);
    _balancer.reset(balancer);
    // register control command to singleton-container for load balancer
    _balancer->register_ctrl_commands();

    partition_guardian *guardian = utils::factory_store<partition_guardian>::create(
        FLAGS_partition_guardian_type, PROVIDER_TYPE_MAIN, this);
    _partition_guardian.reset(guardian);
    _partition_guardian->register_ctrl_commands();

    // initializing the backup_handler should after remote_storage be initialized,
    // because we should use _cluster_root
    if (!FLAGS_cold_backup_disabled) {
        LOG_INFO("initialize backup handler");
        _backup_handler = std::make_shared<backup_service>(
            this,
            utils::filesystem::concat_path_unix_style(_cluster_root, "backup"),
            FLAGS_cold_backup_root,
            [](backup_service *bs) { return std::make_shared<policy_context>(bs); });
    }

    _bulk_load_svc = std::make_unique<bulk_load_service>(
        this, utils::filesystem::concat_path_unix_style(_cluster_root, "bulk_load"));

    // initialize the server_state
    _state->initialize(this, utils::filesystem::concat_path_unix_style(_cluster_root, "apps"));
    while ((err = _state->initialize_data_structure()) != ERR_OK) {
        if (err == ERR_OBJECT_NOT_FOUND && FLAGS_recover_from_replica_server) {
            LOG_INFO("can't find apps from remote storage, and "
                     "[meta_server].recover_from_replica_server = true, "
                     "administrator should recover this cluster manually later");
            return dsn::ERR_OK;
        }
        LOG_ERROR("initialize server state from remote storage failed, err = {}, retry ...", err);
    }

    _state->recover_from_max_replica_count_env();

    initialize_duplication_service();
    recover_duplication_from_meta_state();

    _split_svc = std::make_unique<meta_split_service>(this);

    _state->register_cli_commands();

    start_service();

    LOG_INFO("start meta_service succeed");

    return ERR_OK;
}

void meta_service::register_rpc_handlers()
{
    register_rpc_handler_with_rpc_holder(
        RPC_CM_CONFIG_SYNC, "config_sync", &meta_service::on_config_sync);
    register_rpc_handler_with_rpc_holder(RPC_CM_QUERY_PARTITION_CONFIG_BY_INDEX,
                                         "query_configuration_by_index",
                                         &meta_service::on_query_configuration_by_index);
    register_rpc_handler(RPC_CM_UPDATE_PARTITION_CONFIGURATION,
                         "update_configuration",
                         &meta_service::on_update_configuration);
    register_rpc_handler(RPC_CM_CREATE_APP, "create_app", &meta_service::on_create_app);
    register_rpc_handler(RPC_CM_DROP_APP, "drop_app", &meta_service::on_drop_app);
    register_rpc_handler(RPC_CM_RECALL_APP, "recall_app", &meta_service::on_recall_app);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_RENAME_APP, "rename_app", &meta_service::on_rename_app);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_LIST_APPS, "list_apps", &meta_service::on_list_apps);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_LIST_NODES, "list_nodes", &meta_service::on_list_nodes);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_CLUSTER_INFO, "cluster_info", &meta_service::on_query_cluster_info);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_PROPOSE_BALANCER, "propose_balancer", &meta_service::on_propose_balancer);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_CONTROL_META, "control_meta_level", &meta_service::on_control_meta_level);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_START_RECOVERY, "start_recovery", &meta_service::on_start_recovery);
    register_rpc_handler(RPC_CM_START_RESTORE, "start_restore", &meta_service::on_start_restore);
    register_rpc_handler(
        RPC_CM_ADD_BACKUP_POLICY, "add_backup_policy", &meta_service::on_add_backup_policy);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_QUERY_BACKUP_POLICY, "query_backup_policy", &meta_service::on_query_backup_policy);
    register_rpc_handler_with_rpc_holder(RPC_CM_MODIFY_BACKUP_POLICY,
                                         "modify_backup_policy",
                                         &meta_service::on_modify_backup_policy);
    register_rpc_handler_with_rpc_holder(RPC_CM_REPORT_RESTORE_STATUS,
                                         "report_restore_status",
                                         &meta_service::on_report_restore_status);
    register_rpc_handler_with_rpc_holder(RPC_CM_QUERY_RESTORE_STATUS,
                                         "query_restore_status",
                                         &meta_service::on_query_restore_status);
    register_duplication_rpc_handlers();
    register_rpc_handler_with_rpc_holder(
        RPC_CM_UPDATE_APP_ENV, "update_app_env(set/del/clear)", &meta_service::update_app_env);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_DDD_DIAGNOSE, "ddd_diagnose", &meta_service::ddd_diagnose);
    register_rpc_handler_with_rpc_holder(RPC_CM_START_PARTITION_SPLIT,
                                         "start_partition_split",
                                         &meta_service::on_start_partition_split);
    register_rpc_handler_with_rpc_holder(RPC_CM_CONTROL_PARTITION_SPLIT,
                                         "control_partition_split(pause/restart/cancel)",
                                         &meta_service::on_control_partition_split);
    register_rpc_handler_with_rpc_holder(RPC_CM_QUERY_PARTITION_SPLIT,
                                         "query_partition_split",
                                         &meta_service::on_query_partition_split);
    register_rpc_handler_with_rpc_holder(RPC_CM_REGISTER_CHILD_REPLICA,
                                         "register_child_on_meta",
                                         &meta_service::on_register_child_on_meta);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_NOTIFY_STOP_SPLIT, "notify_stop_split", &meta_service::on_notify_stop_split);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_QUERY_CHILD_STATE, "query_child_state", &meta_service::on_query_child_state);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_START_BULK_LOAD, "start_bulk_load", &meta_service::on_start_bulk_load);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_CONTROL_BULK_LOAD, "control_bulk_load", &meta_service::on_control_bulk_load);
    register_rpc_handler_with_rpc_holder(RPC_CM_QUERY_BULK_LOAD_STATUS,
                                         "query_bulk_load_status",
                                         &meta_service::on_query_bulk_load_status);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_CLEAR_BULK_LOAD, "clear_bulk_load", &meta_service::on_clear_bulk_load);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_START_BACKUP_APP, "start_backup_app", &meta_service::on_start_backup_app);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_QUERY_BACKUP_STATUS, "query_backup_status", &meta_service::on_query_backup_status);
    register_rpc_handler_with_rpc_holder(RPC_CM_START_MANUAL_COMPACT,
                                         "start_manual_compact",
                                         &meta_service::on_start_manual_compact);
    register_rpc_handler_with_rpc_holder(RPC_CM_QUERY_MANUAL_COMPACT_STATUS,
                                         "query_manual_compact_status",
                                         &meta_service::on_query_manual_compact_status);
    register_rpc_handler_with_rpc_holder(RPC_CM_GET_MAX_REPLICA_COUNT,
                                         "get_max_replica_count",
                                         &meta_service::on_get_max_replica_count);
    register_rpc_handler_with_rpc_holder(RPC_CM_SET_MAX_REPLICA_COUNT,
                                         "set_max_replica_count",
                                         &meta_service::on_set_max_replica_count);
    register_rpc_handler_with_rpc_holder(RPC_CM_GET_ATOMIC_IDEMPOTENT,
                                         "get_atomic_idempotent",
                                         &meta_service::on_get_atomic_idempotent);
    register_rpc_handler_with_rpc_holder(RPC_CM_SET_ATOMIC_IDEMPOTENT,
                                         "set_atomic_idempotent",
                                         &meta_service::on_set_atomic_idempotent);
}

meta_leader_state meta_service::check_leader(dsn::message_ex *req, dsn::host_port *forward_address)
{
    host_port leader;
    if (!_failure_detector->get_leader(&leader)) {
        if (!req->header->context.u.is_forward_supported) {
            if (forward_address != nullptr)
                *forward_address = leader;
            return meta_leader_state::kNotLeaderAndCannotForwardRpc;
        }

        LOG_DEBUG("leader address: {}", leader);
        if (leader) {
            dsn_rpc_forward(req, dsn::dns_resolver::instance().resolve_address(leader));
            return meta_leader_state::kNotLeaderAndCanForwardRpc;
        } else {
            if (forward_address != nullptr)
                forward_address->reset();
            return meta_leader_state::kNotLeaderAndCannotForwardRpc;
        }
    }
    return meta_leader_state::kIsLeader;
}

// table operations
void meta_service::on_create_app(dsn::message_ex *req)
{
    if (!check_status_and_authz_with_reply<configuration_create_app_request,
                                           configuration_create_app_response>(req)) {
        return;
    }

    req->add_ref();
    tasking::enqueue(LPC_META_STATE_NORMAL,
                     nullptr,
                     std::bind(&server_state::create_app, _state.get(), req),
                     server_state::sStateHash);
}

void meta_service::on_drop_app(dsn::message_ex *req)
{
    if (!check_status_and_authz_with_reply<configuration_drop_app_request,
                                           configuration_drop_app_response>(req)) {
        return;
    }

    req->add_ref();
    tasking::enqueue(LPC_META_STATE_NORMAL,
                     nullptr,
                     std::bind(&server_state::drop_app, _state.get(), req),
                     server_state::sStateHash);
}

void meta_service::on_rename_app(configuration_rename_app_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().old_app_name)) {
        return;
    }

    tasking::enqueue(LPC_META_STATE_NORMAL,
                     tracker(),
                     std::bind(&server_state::rename_app, _state.get(), rpc),
                     server_state::sStateHash);
}

void meta_service::on_recall_app(dsn::message_ex *req)
{
    configuration_recall_app_request request;
    configuration_recall_app_response response;
    dsn::message_ex *copied_req = message_ex::copy_message_no_reply(*req);
    dsn::unmarshall(copied_req, request);
    auto target_app = _state->get_app(request.app_id);
    if (!target_app) {
        response.err = ERR_APP_NOT_EXIST;
        reply(req, response);
        return;
    }
    const std::string &app_name = target_app->app_name;

    if (!check_status_and_authz_with_reply(req, response, app_name)) {
        return;
    }
    // check new_app_name reasonable.
    // when the Ranger ACL is enabled, ensure that the prefix of new_app_name is consistent with
    // old, or it is empty
    if (_access_controller->is_enable_ranger_acl() && !request.new_app_name.empty()) {
        std::string app_name_prefix = ranger::get_database_name_from_app_name(app_name);
        std::string new_app_name_prefix =
            ranger::get_database_name_from_app_name(request.new_app_name);
        if (app_name_prefix != new_app_name_prefix) {
            response.err = ERR_INVALID_PARAMETERS;
            reply(req, response);
            return;
        }
    }

    req->add_ref();
    tasking::enqueue(LPC_META_STATE_NORMAL,
                     nullptr,
                     std::bind(&server_state::recall_app, _state.get(), req),
                     server_state::sStateHash);
}

void meta_service::on_list_apps(configuration_list_apps_rpc rpc)
{
    if (!check_leader_status(rpc)) {
        return;
    }

    dsn::message_ex *msg = nullptr;
    if (_access_controller->is_enable_ranger_acl()) {
        msg = rpc.dsn_request();
    }
    _state->list_apps(rpc.request(), rpc.response(), msg);
}

void meta_service::on_list_nodes(configuration_list_nodes_rpc rpc)
{
    if (!check_status_and_authz(rpc)) {
        return;
    }

    configuration_list_nodes_response &response = rpc.response();
    const configuration_list_nodes_request &request = rpc.request();
    {
        zauto_lock l(_failure_detector->_lock);
        dsn::replication::node_info info;
        if (request.status == node_status::NS_INVALID || request.status == node_status::NS_ALIVE) {
            info.status = node_status::NS_ALIVE;
            for (auto &node : _alive_set) {
                SET_IP_AND_HOST_PORT_BY_DNS(info, node, node);
                response.infos.push_back(info);
            }
        }
        if (request.status == node_status::NS_INVALID ||
            request.status == node_status::NS_UNALIVE) {
            info.status = node_status::NS_UNALIVE;
            for (auto &node : _dead_set) {
                SET_IP_AND_HOST_PORT_BY_DNS(info, node, node);
                response.infos.push_back(info);
            }
        }
        response.err = dsn::ERR_OK;
    }
}

void meta_service::on_query_cluster_info(configuration_cluster_info_rpc rpc)
{
    if (!check_status_and_authz(rpc)) {
        return;
    }

    std::stringstream oss;
    configuration_cluster_info_response &response = rpc.response();
    response.keys.push_back("meta_servers");
    for (size_t i = 0; i < _opts.meta_servers.size(); ++i) {
        if (i != 0)
            oss << ",";
        oss << _opts.meta_servers[i];
    }

    response.values.push_back(oss.str());
    response.keys.push_back("primary_meta_server");
    response.values.push_back(dsn_primary_host_port().to_string());
    response.keys.push_back("zookeeper_hosts");
    response.values.push_back(FLAGS_hosts_list);
    response.keys.push_back("zookeeper_root");
    response.values.push_back(_cluster_root);
    response.keys.push_back("meta_function_level");
    response.values.push_back(
        _meta_function_level_VALUES_TO_NAMES.find(get_function_level())->second + 3);
    response.keys.push_back("balance_operation_count");
    std::vector<std::string> balance_operation_type;
    balance_operation_type.emplace_back(std::string("detail"));
    response.values.push_back(_balancer->get_balance_operation_count(balance_operation_type));
    double primary_stddev, total_stddev;
    _state->get_cluster_balance_score(primary_stddev, total_stddev);
    response.keys.push_back("primary_replica_count_stddev");
    response.values.push_back(fmt::format("{:.{}f}", primary_stddev, 2));
    response.keys.push_back("total_replica_count_stddev");
    response.values.push_back(fmt::format("{:.{}f}", total_stddev, 2));
    response.keys.push_back("cluster_name");
    response.values.push_back(get_current_cluster_name());
    response.err = dsn::ERR_OK;
}

// client => meta server
void meta_service::on_query_configuration_by_index(configuration_query_by_index_rpc rpc)
{
    query_cfg_response &response = rpc.response();
    host_port forward_hp;
    if (!check_status_and_authz(rpc, &forward_hp)) {
        if (forward_hp) {
            partition_configuration pc;
            SET_IP_AND_HOST_PORT_BY_DNS(pc, primary, forward_hp);
            response.partitions.push_back(std::move(pc));
        }
        return;
    }

    _state->query_configuration_by_index(rpc.request(), response);
    if (ERR_OK == response.err) {
        LOG_INFO("client {} queried an available app {} with appid {}",
                 rpc.dsn_request()->header->from_address,
                 rpc.request().app_name,
                 response.app_id);
    }
}

// partition sever => meta sever
// as get stale configuration is not allowed for partition server, we need to dispatch it to the
// meta state thread pool
void meta_service::on_config_sync(configuration_query_by_node_rpc rpc)
{
    if (!check_status_and_authz(rpc)) {
        return;
    }

    {
        // this code piece should be referenced together with meta_service::set_node_state.
        // In which, the replica server's failure event is dispatched to the meta_state_thread with
        // the protection
        // of failure_detector::_lock. Here we use this lock again, to make sure the config_sync rpc
        // AFTER the node dead is dispatch
        // AFTER the node dead event
        zauto_lock l(_failure_detector->_lock);
        tasking::enqueue(LPC_META_STATE_HIGH,
                         nullptr,
                         std::bind(&server_state::on_config_sync, _state.get(), rpc),
                         server_state::sStateHash);
    }
}

void meta_service::on_update_configuration(dsn::message_ex *req)
{
    configuration_update_response response;
    if (!check_status_and_authz_with_reply(req, response)) {
        return;
    }

    std::shared_ptr<configuration_update_request> request =
        std::make_shared<configuration_update_request>();
    dsn::unmarshall(req, *request);

    meta_function_level::type level = get_function_level();
    if (level <= meta_function_level::fl_freezed) {
        response.err = ERR_STATE_FREEZED;
        _state->query_configuration_by_gpid(request->config.pid, response.config);
        reply(req, response);

        LOG_INFO("refuse request {} coz meta function level is {}",
                 boost::lexical_cast<std::string>(*request),
                 _meta_function_level_VALUES_TO_NAMES.find(level)->second);
        return;
    }

    req->add_ref();
    tasking::enqueue(LPC_META_STATE_HIGH,
                     nullptr,
                     std::bind(&server_state::on_update_configuration, _state.get(), request, req),
                     server_state::sStateHash);
}

void meta_service::on_control_meta_level(configuration_meta_control_rpc rpc)
{
    if (!check_status_and_authz(rpc)) {
        return;
    }

    const configuration_meta_control_request &request = rpc.request();
    configuration_meta_control_response &response = rpc.response();
    response.err = ERR_OK;
    response.old_level = _function_level.load();
    if (request.level == meta_function_level::fl_invalid) {
        return;
    }

    if (request.level <= meta_function_level::fl_steady) {
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         nullptr,
                         std::bind(&server_state::clear_proposals, _state.get()),
                         server_state::sStateHash);
    }

    _function_level.store(request.level);
}

void meta_service::on_propose_balancer(configuration_balancer_rpc rpc)
{
    CHECK_APP_ID_STATUS_AND_AUTHZ(rpc.request().gpid.get_app_id());
    const configuration_balancer_request &request = rpc.request();
    LOG_INFO("get proposal balancer request, gpid({})", request.gpid);
    _state->on_propose_balancer(request, rpc.response());
}

void meta_service::on_start_recovery(configuration_recovery_rpc rpc)
{
    configuration_recovery_response &response = rpc.response();
    LOG_INFO("got start recovery request, start to do recovery");
    auto result = check_leader(rpc, nullptr);
    // request has been forwarded to others
    if (result == meta_leader_state::kNotLeaderAndCanForwardRpc) {
        return;
    }

    if (result == meta_leader_state::kNotLeaderAndCannotForwardRpc) {
        response.err = ERR_FORWARD_TO_OTHERS;
    } else {
        zauto_write_lock l(_meta_lock);
        if (_started.load()) {
            LOG_INFO("service({}({})) is already started, ignore the recovery request",
                     dsn_primary_host_port(),
                     dsn_primary_address());
            response.err = ERR_SERVICE_ALREADY_RUNNING;
        } else {
            _state->on_start_recovery(rpc.request(), response);
            if (response.err == dsn::ERR_OK) {
                _recovering = false;
                start_service();
            }
        }
    }
}

void meta_service::on_start_restore(dsn::message_ex *req)
{
    if (!check_status_and_authz_with_reply<configuration_restore_request,
                                           configuration_create_app_response>(req)) {
        return;
    }

    req->add_ref();
    tasking::enqueue(
        LPC_RESTORE_BACKGROUND, nullptr, std::bind(&server_state::restore_app, _state.get(), req));
}

void meta_service::on_add_backup_policy(dsn::message_ex *req)
{
    configuration_add_backup_policy_response response;
    if (!check_status_and_authz_with_reply(req, response)) {
        return;
    }

    if (_backup_handler == nullptr) {
        LOG_ERROR("meta doesn't enable backup service");
        response.err = ERR_SERVICE_NOT_ACTIVE;
        reply(req, response);
    } else {
        req->add_ref();
        tasking::enqueue(LPC_DEFAULT_CALLBACK,
                         nullptr,
                         std::bind(&backup_service::add_backup_policy, _backup_handler.get(), req));
    }
}

void meta_service::on_query_backup_policy(query_backup_policy_rpc policy_rpc)
{
    if (!check_status_and_authz(policy_rpc)) {
        return;
    }

    auto &response = policy_rpc.response();
    if (_backup_handler == nullptr) {
        LOG_ERROR("meta doesn't enable backup service");
        response.err = ERR_SERVICE_NOT_ACTIVE;
    } else {
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            nullptr,
            std::bind(&backup_service::query_backup_policy, _backup_handler.get(), policy_rpc));
    }
}

void meta_service::on_modify_backup_policy(configuration_modify_backup_policy_rpc rpc)
{
    if (!check_status_and_authz(rpc)) {
        return;
    }

    if (_backup_handler == nullptr) {
        LOG_ERROR("meta doesn't enable backup service");
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
    } else {
        tasking::enqueue(
            LPC_DEFAULT_CALLBACK,
            nullptr,
            std::bind(&backup_service::modify_backup_policy, _backup_handler.get(), rpc));
    }
}

void meta_service::on_report_restore_status(configuration_report_restore_status_rpc rpc)
{
    if (!check_status_and_authz(rpc)) {
        return;
    }

    tasking::enqueue(LPC_META_STATE_NORMAL,
                     nullptr,
                     std::bind(&server_state::on_recv_restore_report, _state.get(), rpc));
}

void meta_service::on_query_restore_status(configuration_query_restore_rpc rpc)
{
    CHECK_APP_ID_STATUS_AND_AUTHZ(rpc.request().restore_app_id);
    tasking::enqueue(LPC_META_STATE_NORMAL,
                     nullptr,
                     std::bind(&server_state::on_query_restore_status, _state.get(), rpc));
}

void meta_service::on_add_duplication(duplication_add_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }

    if (!_dup_svc) {
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }
    tasking::enqueue(
        LPC_META_STATE_NORMAL,
        tracker(),
        [this, rpc]() { _dup_svc->add_duplication(std::move(rpc)); },
        server_state::sStateHash);
}

void meta_service::on_modify_duplication(duplication_modify_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }

    if (!_dup_svc) {
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }
    tasking::enqueue(
        LPC_META_STATE_NORMAL,
        tracker(),
        [this, rpc]() { _dup_svc->modify_duplication(std::move(rpc)); },
        server_state::sStateHash);
}

void meta_service::on_query_duplication_info(duplication_query_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }

    if (_dup_svc) {
        _dup_svc->query_duplication_info(rpc.request(), rpc.response());
    } else {
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
    }
}

void meta_service::on_duplication_sync(duplication_sync_rpc rpc)
{
    if (!check_status_and_authz(rpc)) {
        return;
    }

    tasking::enqueue(
        LPC_META_STATE_NORMAL,
        tracker(),
        [this, rpc]() {
            if (_dup_svc) {
                _dup_svc->duplication_sync(std::move(rpc));
            } else {
                rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
            }
        },
        server_state::sStateHash);
}

void meta_service::on_list_duplication_info(duplication_list_rpc rpc)
{
    if (!check_status_and_authz(rpc)) {
        return;
    }

    if (!_dup_svc) {
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }

    _dup_svc->list_duplication_info(rpc.request(), rpc.response());
}

void meta_service::recover_duplication_from_meta_state()
{
    if (_dup_svc) {
        _dup_svc->recover_from_meta_state();
    }
}

void meta_service::register_duplication_rpc_handlers()
{
    register_rpc_handler_with_rpc_holder(
        RPC_CM_ADD_DUPLICATION, "add_duplication", &meta_service::on_add_duplication);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_MODIFY_DUPLICATION, "modify duplication", &meta_service::on_modify_duplication);
    register_rpc_handler_with_rpc_holder(RPC_CM_QUERY_DUPLICATION,
                                         "query duplication info",
                                         &meta_service::on_query_duplication_info);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_DUPLICATION_SYNC, "sync duplication", &meta_service::on_duplication_sync);
    register_rpc_handler_with_rpc_holder(
        RPC_CM_LIST_DUPLICATION, "list_duplication_info", &meta_service::on_list_duplication_info);
}

void meta_service::initialize_duplication_service()
{
    if (FLAGS_duplication_enabled) {
        _dup_svc = std::make_unique<meta_duplication_service>(_state.get(), this);
    }
}

void meta_service::update_app_env(app_env_rpc env_rpc)
{
    if (!check_status_and_authz(env_rpc, nullptr, env_rpc.request().app_name)) {
        return;
    }

    auto &response = env_rpc.response();
    app_env_operation::type op = env_rpc.request().op;
    switch (op) {
    case app_env_operation::type::APP_ENV_OP_SET:
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         nullptr,
                         std::bind(&server_state::set_app_envs, _state.get(), env_rpc));
        break;
    case app_env_operation::type::APP_ENV_OP_DEL:
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         nullptr,
                         std::bind(&server_state::del_app_envs, _state.get(), env_rpc));
        break;
    case app_env_operation::type::APP_ENV_OP_CLEAR:
        tasking::enqueue(LPC_META_STATE_NORMAL,
                         nullptr,
                         std::bind(&server_state::clear_app_envs, _state.get(), env_rpc));
        break;
    default: // app_env_operation::type::APP_ENV_OP_INVALID
        LOG_WARNING("recv a invalid update app_env request, just ignore");
        response.err = ERR_INVALID_PARAMETERS;
        response.hint_message =
            "recv a invalid update_app_env request with op = APP_ENV_OP_INVALID";
        break;
    }
}

void meta_service::ddd_diagnose(ddd_diagnose_rpc rpc)
{
    CHECK_APP_ID_STATUS_AND_AUTHZ(rpc.request().pid.get_app_id());
    auto &response = rpc.response();
    get_partition_guardian()->get_ddd_partitions(rpc.request().pid, response.partitions);
    response.err = ERR_OK;
}

void meta_service::on_start_partition_split(start_split_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }
    if (_split_svc == nullptr) {
        LOG_ERROR("meta doesn't support partition split");
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }
    tasking::enqueue(
        LPC_META_STATE_NORMAL,
        tracker(),
        [this, rpc]() { _split_svc->start_partition_split(std::move(rpc)); },
        server_state::sStateHash);
}

void meta_service::on_control_partition_split(control_split_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }

    if (_split_svc == nullptr) {
        LOG_ERROR("meta doesn't support partition split");
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }
    tasking::enqueue(
        LPC_META_STATE_NORMAL,
        tracker(),
        [this, rpc]() { _split_svc->control_partition_split(std::move(rpc)); },
        server_state::sStateHash);
}

void meta_service::on_query_partition_split(query_split_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }

    if (_split_svc == nullptr) {
        LOG_ERROR("meta doesn't support partition split");
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }
    _split_svc->query_partition_split(std::move(rpc));
}

void meta_service::on_register_child_on_meta(register_child_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app.app_name)) {
        return;
    }

    tasking::enqueue(
        LPC_META_STATE_NORMAL,
        tracker(),
        [this, rpc]() { _split_svc->register_child_on_meta(std::move(rpc)); },
        server_state::sStateHash);
}

void meta_service::on_notify_stop_split(notify_stop_split_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }
    if (_split_svc == nullptr) {
        LOG_ERROR("meta doesn't support partition split");
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }
    tasking::enqueue(
        LPC_META_STATE_NORMAL,
        tracker(),
        [this, rpc]() { _split_svc->notify_stop_split(std::move(rpc)); },
        server_state::sStateHash);
}

void meta_service::on_query_child_state(query_child_state_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }
    if (_split_svc == nullptr) {
        LOG_ERROR("meta doesn't support partition split");
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }
    _split_svc->query_child_state(std::move(rpc));
}

void meta_service::on_start_bulk_load(start_bulk_load_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }

    if (_bulk_load_svc == nullptr) {
        LOG_ERROR("meta doesn't support bulk load");
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }
    _bulk_load_svc->on_start_bulk_load(std::move(rpc));
}

void meta_service::on_control_bulk_load(control_bulk_load_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }

    if (_bulk_load_svc == nullptr) {
        LOG_ERROR("meta doesn't support bulk load");
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }
    tasking::enqueue(
        LPC_META_STATE_NORMAL,
        tracker(),
        [this, rpc]() { _bulk_load_svc->on_control_bulk_load(std::move(rpc)); },
        server_state::sStateHash);
}

void meta_service::on_query_bulk_load_status(query_bulk_load_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }

    if (_bulk_load_svc == nullptr) {
        LOG_ERROR("meta doesn't support bulk load");
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }
    _bulk_load_svc->on_query_bulk_load_status(std::move(rpc));
}

void meta_service::on_clear_bulk_load(clear_bulk_load_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }

    if (_bulk_load_svc == nullptr) {
        LOG_ERROR("meta doesn't support bulk load");
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }
    tasking::enqueue(
        LPC_META_STATE_NORMAL,
        tracker(),
        [this, rpc]() { _bulk_load_svc->on_clear_bulk_load(std::move(rpc)); },
        server_state::sStateHash);
}

void meta_service::on_start_backup_app(start_backup_app_rpc rpc)
{
    CHECK_APP_ID_STATUS_AND_AUTHZ(rpc.request().app_id);
    if (_backup_handler == nullptr) {
        LOG_ERROR("meta doesn't enable backup service");
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }
    _backup_handler->start_backup_app(std::move(rpc));
}

void meta_service::on_query_backup_status(query_backup_status_rpc rpc)
{
    CHECK_APP_ID_STATUS_AND_AUTHZ(rpc.request().app_id);
    if (_backup_handler == nullptr) {
        LOG_ERROR("meta doesn't enable backup service");
        rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        return;
    }
    _backup_handler->query_backup_status(std::move(rpc));
}

size_t meta_service::get_alive_node_count() const
{
    zauto_lock l(_failure_detector->_lock);
    return _alive_set.size();
}

void meta_service::on_start_manual_compact(start_manual_compact_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }
    tasking::enqueue(LPC_META_STATE_NORMAL,
                     nullptr,
                     std::bind(&server_state::on_start_manual_compact, _state.get(), rpc));
}

void meta_service::on_query_manual_compact_status(query_manual_compact_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }
    tasking::enqueue(LPC_META_STATE_NORMAL,
                     nullptr,
                     std::bind(&server_state::on_query_manual_compact_status, _state.get(), rpc));
}

// ThreadPool: THREAD_POOL_META_SERVER
void meta_service::on_get_max_replica_count(configuration_get_max_replica_count_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }

    tasking::enqueue(LPC_META_STATE_NORMAL,
                     tracker(),
                     std::bind(&server_state::get_max_replica_count, _state.get(), rpc),
                     server_state::sStateHash);
}

// ThreadPool: THREAD_POOL_META_SERVER
void meta_service::on_set_max_replica_count(configuration_set_max_replica_count_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }

    tasking::enqueue(LPC_META_STATE_NORMAL,
                     tracker(),
                     std::bind(&server_state::set_max_replica_count, _state.get(), rpc),
                     server_state::sStateHash);
}

// ThreadPool: THREAD_POOL_META_SERVER
void meta_service::on_get_atomic_idempotent(configuration_get_atomic_idempotent_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }

    tasking::enqueue(LPC_META_STATE_NORMAL,
                     tracker(),
                     std::bind(&server_state::get_atomic_idempotent, _state.get(), rpc),
                     server_state::sStateHash);
}

// ThreadPool: THREAD_POOL_META_SERVER
void meta_service::on_set_atomic_idempotent(configuration_set_atomic_idempotent_rpc rpc)
{
    if (!check_status_and_authz(rpc, nullptr, rpc.request().app_name)) {
        return;
    }

    tasking::enqueue(LPC_META_STATE_NORMAL,
                     tracker(),
                     std::bind(&server_state::set_atomic_idempotent, _state.get(), rpc),
                     server_state::sStateHash);
}

} // namespace replication
} // namespace dsn
