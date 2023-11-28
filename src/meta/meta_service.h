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

#pragma once

#include <fmt/core.h>
#include <stddef.h>
#include <stdint.h>
#include <atomic>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "block_service/block_service_manager.h"
#include "common/bulk_load_common.h"
#include "common/duplication_common.h"
#include "common/manual_compact.h"
#include "common/partition_split_common.h"
#include "common/replication_common.h"
#include "meta_admin_types.h"
#include "meta_options.h"
#include "meta_rpc_types.h"
#include "meta_server_failure_detector.h"
#include "perf_counter/perf_counter_wrapper.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/network.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/rpc/serialization.h"
#include "runtime/security/access_controller.h"
#include "runtime/serverlet.h"
#include "runtime/task/task.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_tracker.h"
#include "utils/autoref_ptr.h"
#include "utils/enum_helper.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/threadpool_code.h"
#include "utils/zlocks.h"

namespace dsn {
class command_deregister;

namespace ranger {
class ranger_resource_policy_manager;
} // namespace ranger
namespace dist {
class meta_state_service;
} // namespace dist

namespace replication {
class backup_service;
class bulk_load_service;
class meta_duplication_service;
class meta_split_service;
class partition_guardian;
class server_load_balancer;
class server_state;

namespace mss {
struct meta_storage;
} // namespace mss

namespace test {
class test_checker;
}

DEFINE_TASK_CODE(LPC_DEFAULT_CALLBACK, TASK_PRIORITY_COMMON, dsn::THREAD_POOL_DEFAULT)

enum class meta_op_status
{
    FREE = 0,
    RECALL,
    BALANCE,
    BACKUP,
    BULKLOAD,
    RESTORE,
    MANUAL_COMPACT,
    INVALID
};

ENUM_BEGIN(meta_op_status, meta_op_status::INVALID)
ENUM_REG(meta_op_status::FREE)
ENUM_REG(meta_op_status::RECALL)
ENUM_REG(meta_op_status::BALANCE)
ENUM_REG(meta_op_status::BACKUP)
ENUM_REG(meta_op_status::BULKLOAD)
ENUM_REG(meta_op_status::RESTORE)
ENUM_REG(meta_op_status::MANUAL_COMPACT)
ENUM_END(meta_op_status)

// The leader status of meta server
enum class meta_leader_state : int
{
    kIsLeader,                     // the meta is leader
    kNotLeaderAndCanForwardRpc,    // meta isn't leader, and rpc-msg can forward to others
    kNotLeaderAndCannotForwardRpc, // meta isn't leader, and rpc-msg can't forward to others
};

// Meta server service for EON (rDSN layer 2).
class meta_service : public serverlet<meta_service>
{
public:
    meta_service();
    virtual ~meta_service();

    error_code start();
    void stop();

    const replication_options &get_options() const { return _opts; }
    const meta_options &get_meta_options() const { return _meta_opts; }

    /// NOTE: prefer using mss::meta_storage instead.
    dist::meta_state_service *get_remote_storage() const { return _storage.get(); }
    mss::meta_storage *get_meta_storage() const { return _meta_storage.get(); }

    server_state *get_server_state() { return _state.get(); }
    security::access_controller *get_access_controller() { return _access_controller.get(); }
    server_load_balancer *get_balancer() { return _balancer.get(); }
    partition_guardian *get_partition_guardian() { return _partition_guardian.get(); }
    dist::block_service::block_service_manager &get_block_service_manager()
    {
        return _block_service_manager;
    }
    bulk_load_service *get_bulk_load_service() { return _bulk_load_svc.get(); }

    meta_function_level::type get_function_level()
    {
        meta_function_level::type level = _function_level.load();
        if (level > meta_function_level::fl_freezed && check_freeze()) {
            level = meta_function_level::fl_freezed;
        }
        return level;
    }
    void set_function_level(meta_function_level::type level) { _function_level.store(level); }

    template <typename TResponse>
    void reply_data(dsn::message_ex *request, const TResponse &data)
    {
        dsn::message_ex *response = request->create_response();
        dsn::marshall(response, data);
        reply_message(request, response);
    }

    virtual void reply_message(dsn::message_ex *, dsn::message_ex *response)
    {
        dsn_rpc_reply(response);
    }
    virtual void send_message(const rpc_address &target, dsn::message_ex *request)
    {
        dsn_rpc_call_one_way(target, request);
    }
    virtual void send_request(dsn::message_ex * /*req*/,
                              const rpc_address &target,
                              const rpc_response_task_ptr &callback)
    {
        dsn_rpc_call(target, callback);
    }

    // these two callbacks are running in fd's thread_pool, and in fd's lock
    void set_node_state(const std::vector<rpc_address> &nodes_list, bool is_alive);
    void get_node_state(/*out*/ std::map<rpc_address, bool> &all_nodes);

    void start_service();
    void balancer_run();

    dsn::task_tracker *tracker() { return &_tracker; }

    size_t get_alive_node_count() const;

    bool try_lock_meta_op_status(meta_op_status op_status);
    void unlock_meta_op_status();
    meta_op_status get_op_status() const { return _meta_op_status.load(); }

    std::string get_meta_list_string() const
    {
        std::string metas;
        for (const auto &node : _opts.meta_servers) {
            metas = fmt::format("{}{},", metas, node.to_string());
        }
        return metas.substr(0, metas.length() - 1);
    }

    std::string cluster_root() const { return _cluster_root; }

private:
    void register_rpc_handlers();
    void register_ctrl_commands();

    // client => meta server
    void on_query_configuration_by_index(configuration_query_by_index_rpc rpc);

    // partition server => meta server
    void on_config_sync(configuration_query_by_node_rpc rpc);

    // update configuration
    void on_propose_balancer(configuration_balancer_rpc rpc);
    void on_update_configuration(dsn::message_ex *req);

    // app operations
    void on_create_app(dsn::message_ex *req);
    void on_drop_app(dsn::message_ex *req);
    void on_recall_app(dsn::message_ex *req);
    void on_rename_app(configuration_rename_app_rpc rpc);
    void on_list_apps(configuration_list_apps_rpc rpc);
    void on_list_nodes(configuration_list_nodes_rpc rpc);

    // app env operations
    void update_app_env(app_env_rpc env_rpc);

    // ddd diagnose
    void ddd_diagnose(ddd_diagnose_rpc rpc);

    // cluster info
    void on_query_cluster_info(configuration_cluster_info_rpc rpc);

    // meta control
    void on_control_meta_level(configuration_meta_control_rpc rpc);
    void on_start_recovery(configuration_recovery_rpc rpc);

    // backup/restore
    void on_start_backup_app(start_backup_app_rpc rpc);
    void on_query_backup_status(query_backup_status_rpc rpc);
    void on_start_restore(dsn::message_ex *req);
    void on_add_backup_policy(dsn::message_ex *req);
    void on_query_backup_policy(query_backup_policy_rpc policy_rpc);
    void on_modify_backup_policy(configuration_modify_backup_policy_rpc rpc);
    void on_report_restore_status(configuration_report_restore_status_rpc rpc);
    void on_query_restore_status(configuration_query_restore_rpc rpc);

    // duplication
    void on_add_duplication(duplication_add_rpc rpc);
    void on_modify_duplication(duplication_modify_rpc rpc);
    void on_query_duplication_info(duplication_query_rpc rpc);
    void on_duplication_sync(duplication_sync_rpc rpc);
    void register_duplication_rpc_handlers();
    void recover_duplication_from_meta_state();
    void initialize_duplication_service();

    // split
    void on_start_partition_split(start_split_rpc rpc);
    void on_control_partition_split(control_split_rpc rpc);
    void on_query_partition_split(query_split_rpc rpc);
    void on_register_child_on_meta(register_child_rpc rpc);
    void on_notify_stop_split(notify_stop_split_rpc rpc);
    void on_query_child_state(query_child_state_rpc rpc);

    // bulk load
    void on_start_bulk_load(start_bulk_load_rpc rpc);
    void on_control_bulk_load(control_bulk_load_rpc rpc);
    void on_query_bulk_load_status(query_bulk_load_rpc rpc);
    void on_clear_bulk_load(clear_bulk_load_rpc rpc);

    // manual compaction
    void on_start_manual_compact(start_manual_compact_rpc rpc);
    void on_query_manual_compact_status(query_manual_compact_rpc rpc);

    // get/set max_replica_count of an app
    void on_get_max_replica_count(configuration_get_max_replica_count_rpc rpc);
    void on_set_max_replica_count(configuration_set_max_replica_count_rpc rpc);

    // if return 'kNotLeaderAndCannotForwardRpc' and 'forward_address' != nullptr, then return
    // leader by 'forward_address'.
    meta_leader_state check_leader(dsn::message_ex *req, dsn::rpc_address *forward_address);
    template <typename TRpcHolder>
    meta_leader_state check_leader(TRpcHolder rpc, /*out*/ rpc_address *forward_address);

    // app_name: when the Ranger ACL is enabled, some rpc requests need to verify the app_name
    // ret:
    //    false: rpc request check failed because check leader failed or ACL authentication failed
    //    true:  rpc request check and authentication succeed
    template <typename TRpcHolder>
    bool check_status_and_authz(TRpcHolder rpc,
                                /*out*/ rpc_address *forward_address = nullptr,
                                const std::string &app_name = "");

    // app_name: when the Ranger ACL is enabled, some rpc requests need to verify the app_name
    // ret:
    //    false: rpc request check failed because check leader failed or ACL authentication failed
    //    true:  rpc request check and authentication succeed
    template <typename TRespType>
    bool check_status_and_authz_with_reply(message_ex *req,
                                           TRespType &response_struct,
                                           const std::string &app_name = "");
    template <typename TReqType, typename TRespType>
    bool check_status_and_authz_with_reply(message_ex *msg);

    template <typename TRpcHolder>
    bool check_leader_status(TRpcHolder rpc, rpc_address *forward_address = nullptr);

    error_code remote_storage_initialize();
    bool check_freeze() const;

private:
    friend class backup_engine_test;
    friend class backup_service_test;
    friend class bulk_load_service_test;
    friend class meta_backup_service_test;
    friend class meta_backup_test_base;
    friend class meta_duplication_service;
    friend class meta_http_service;
    friend class meta_http_service_test;
    friend class meta_partition_guardian_test;
    friend class meta_service_test;
    friend class meta_service_test_app;
    friend class meta_split_service_test;
    friend class meta_test_base;
    friend class policy_context_test;
    friend class server_state_restore_test;
    friend class test::test_checker;
    friend class fake_receiver_meta_service;

    replication_options _opts;
    meta_options _meta_opts;
    uint64_t _node_live_percentage_threshold_for_update;
    std::unique_ptr<command_deregister> _ctrl_node_live_percentage_threshold_for_update;

    std::shared_ptr<server_state> _state;
    std::shared_ptr<meta_server_failure_detector> _failure_detector;

    std::shared_ptr<dist::meta_state_service> _storage;
    std::unique_ptr<mss::meta_storage> _meta_storage;

    std::shared_ptr<server_load_balancer> _balancer;
    std::shared_ptr<backup_service> _backup_handler;
    std::shared_ptr<partition_guardian> _partition_guardian;

    std::unique_ptr<meta_duplication_service> _dup_svc;

    std::unique_ptr<meta_split_service> _split_svc;

    std::unique_ptr<bulk_load_service> _bulk_load_svc;

    // handle all the block filesystems for current meta service
    // (in other words, current service node)
    dist::block_service::block_service_manager _block_service_manager;

    // [
    // this is protected by failure_detector::_lock
    std::set<rpc_address> _alive_set;
    std::set<rpc_address> _dead_set;
    // ]
    mutable zrwlock_nr _meta_lock;

    std::atomic_bool _started;
    std::atomic_bool _recovering;
    // reference replication.thrift for what the meta_function_level means
    std::atomic<meta_function_level::type> _function_level;

    std::string _cluster_root;

    perf_counter_wrapper _recent_disconnect_count;
    perf_counter_wrapper _unalive_nodes_count;
    perf_counter_wrapper _alive_nodes_count;

    dsn::task_tracker _tracker;

    std::shared_ptr<security::access_controller> _access_controller;

    // Use Apache Ranger for access control, which is nullptr when not use
    std::shared_ptr<ranger::ranger_resource_policy_manager> _ranger_resource_policy_manager;

    // indicate which operation is processeding in meta server
    std::atomic<meta_op_status> _meta_op_status;
};

template <typename TRpcHolder>
meta_leader_state meta_service::check_leader(TRpcHolder rpc, rpc_address *forward_address)
{
    dsn::rpc_address leader;
    if (!_failure_detector->get_leader(&leader)) {
        if (!rpc.dsn_request()->header->context.u.is_forward_supported) {
            if (forward_address != nullptr)
                *forward_address = leader;
            return meta_leader_state::kNotLeaderAndCannotForwardRpc;
        }

        LOG_DEBUG("leader address: {}", leader);
        if (!leader.is_invalid()) {
            rpc.forward(leader);
            return meta_leader_state::kNotLeaderAndCanForwardRpc;
        } else {
            if (forward_address != nullptr)
                forward_address->set_invalid();
            return meta_leader_state::kNotLeaderAndCannotForwardRpc;
        }
    }
    return meta_leader_state::kIsLeader;
}

template <typename TRpcHolder>
bool meta_service::check_leader_status(TRpcHolder rpc, rpc_address *forward_address)
{
    auto result = check_leader(rpc, forward_address);
    if (result == meta_leader_state::kNotLeaderAndCanForwardRpc)
        return false;
    if (result == meta_leader_state::kNotLeaderAndCannotForwardRpc || !_started) {
        if (result == meta_leader_state::kNotLeaderAndCannotForwardRpc) {
            rpc.response().err = ERR_FORWARD_TO_OTHERS;
        } else if (_recovering) {
            rpc.response().err = ERR_UNDER_RECOVERY;
        } else {
            rpc.response().err = ERR_SERVICE_NOT_ACTIVE;
        }
        LOG_INFO("reject request with {}", rpc.response().err);
        return false;
    }
    return true;
}

// when the Ranger ACL is enabled, only the leader meta_server will pull Ranger policy, so if it is
// not the leader, _access_controller may be a null pointer, or a new leader is elected, and the
// above policy information may be out of date.
template <typename TRpcHolder>
bool meta_service::check_status_and_authz(TRpcHolder rpc,
                                          rpc_address *forward_address,
                                          const std::string &app_name)
{
    if (!check_leader_status(rpc, forward_address)) {
        return false;
    }
    if (!_access_controller->allowed(rpc.dsn_request(), app_name)) {
        rpc.response().err = ERR_ACL_DENY;
        LOG_DEBUG("not authorized {} to operate on app({}) for user({})",
                  rpc.dsn_request()->rpc_code(),
                  app_name,
                  rpc.dsn_request()->io_session->get_client_username());
        return false;
    }
    return true;
}

template <typename TRespType>
bool meta_service::check_status_and_authz_with_reply(message_ex *req,
                                                     TRespType &response_struct,
                                                     const std::string &app_name)
{
    auto result = check_leader(req, nullptr);
    if (result == meta_leader_state::kNotLeaderAndCanForwardRpc) {
        return false;
    }
    if (result == meta_leader_state::kNotLeaderAndCannotForwardRpc || !_started) {
        if (result == meta_leader_state::kNotLeaderAndCannotForwardRpc) {
            response_struct.err = ERR_FORWARD_TO_OTHERS;
        } else if (_recovering) {
            response_struct.err = ERR_UNDER_RECOVERY;
        } else {
            response_struct.err = ERR_SERVICE_NOT_ACTIVE;
        }
        LOG_DEBUG("reject request with {}", response_struct.err);
        reply(req, response_struct);
        return false;
    }
    if (!_access_controller->allowed(req, app_name)) {
        response_struct.err = ERR_ACL_DENY;
        LOG_DEBUG("not authorized {} to operate on app({}) for user({})",
                  req->rpc_code(),
                  app_name,
                  req->io_session->get_client_username());
        reply(req, response_struct);
        return false;
    }
    return true;
}

template <typename TReqType, typename TRespType>
bool meta_service::check_status_and_authz_with_reply(message_ex *msg)
{
    TReqType req;
    TRespType resp;
    dsn::message_ex *copied_msg = message_ex::copy_message_no_reply(*msg);
    dsn::unmarshall(copied_msg, req);
    return check_status_and_authz_with_reply(msg, resp, req.app_name);
}

} // namespace replication
} // namespace dsn
