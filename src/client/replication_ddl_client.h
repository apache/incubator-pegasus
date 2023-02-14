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

#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "runtime/rpc/rpc_address.h"
#include "common/replication_other_types.h"
#include "common/replication.codes.h"

#include <cctype>
#include <string>
#include <map>
#include <vector>

#include "runtime/task/task_tracker.h"
#include "runtime/task/async_calls.h"
#include "utils/errors.h"

namespace dsn {
namespace replication {

class replication_ddl_client
{
public:
    replication_ddl_client(const std::vector<dsn::rpc_address> &meta_servers);
    ~replication_ddl_client();

    dsn::error_code create_app(const std::string &app_name,
                               const std::string &app_type,
                               int partition_count,
                               int replica_count,
                               const std::map<std::string, std::string> &envs,
                               bool is_stateless,
                               bool success_if_exist = true);

    // 'reserve_seconds' == 0 means use default value in configuration
    // FLAGS_hold_seconds_for_dropped_app.
    dsn::error_code drop_app(const std::string &app_name, int reserve_seconds);

    dsn::error_code recall_app(int32_t app_id, const std::string &new_app_name);

    error_with<configuration_rename_app_response> rename_app(const std::string &old_app_name,
                                                             const std::string &new_app_name);

    dsn::error_code list_apps(const dsn::app_status::type status,
                              bool show_all,
                              bool detailed,
                              bool json,
                              const std::string &file_name);

    dsn::error_code list_apps(const dsn::app_status::type status,
                              std::vector<::dsn::app_info> &apps);

    dsn::error_code list_nodes(const dsn::replication::node_status::type status,
                               bool detailed,
                               const std::string &file_name,
                               bool resolve_ip = false);

    dsn::error_code
    list_nodes(const dsn::replication::node_status::type status,
               std::map<dsn::rpc_address, dsn::replication::node_status::type> &nodes);

    dsn::error_code cluster_name(int64_t timeout_ms, std::string &cluster_name);

    dsn::error_code cluster_info(const std::string &file_name, bool resolve_ip, bool json);

    dsn::error_code list_app(const std::string &app_name,
                             bool detailed,
                             bool json,
                             const std::string &file_name,
                             bool resolve_ip = false);

    dsn::error_code list_app(const std::string &app_name,
                             int32_t &app_id,
                             int32_t &partition_count,
                             std::vector<partition_configuration> &partitions);

    dsn::replication::configuration_meta_control_response
    control_meta_function_level(meta_function_level::type level);

    dsn::error_code send_balancer_proposal(const configuration_balancer_request &request);

    dsn::error_code
    wait_app_ready(const std::string &app_name, int partition_count, int max_replica_count);

    dsn::error_code do_recovery(const std::vector<dsn::rpc_address> &replica_nodes,
                                int wait_seconds,
                                bool skip_bad_nodes,
                                bool skip_lost_partitions,
                                const std::string &outfile);

    error_with<duplication_add_response>
    add_dup(std::string app_name, std::string remote_address, bool is_duplicating_checkpoint);

    error_with<duplication_modify_response>
    change_dup_status(std::string app_name, int dupid, duplication_status::type status);
    error_with<duplication_modify_response>
    update_dup_fail_mode(std::string app_name, int dupid, duplication_fail_mode::type fmode);

    error_with<duplication_query_response> query_dup(std::string app_name);

    dsn::error_code do_restore(const std::string &backup_provider_name,
                               const std::string &cluster_name,
                               const std::string &policy_name,
                               int64_t timestamp /*backup_id*/,
                               const std::string &old_app_name,
                               int32_t old_app_id,
                               const std::string &new_app_name,
                               bool skip_bad_partition,
                               const std::string &restore_path = "");

    dsn::error_code query_restore(int32_t restore_app_id, bool detailed);

    dsn::error_code add_backup_policy(const std::string &policy_name,
                                      const std::string &backup_provider_type,
                                      const std::vector<int32_t> &app_ids,
                                      int64_t backup_interval_seconds,
                                      int32_t backup_history_cnt,
                                      const std::string &start_time);

    error_with<start_backup_app_response> backup_app(int32_t app_id,
                                                     const std::string &backup_provider_type,
                                                     const std::string &backup_path = "");

    error_with<query_backup_status_response> query_backup(int32_t app_id, int64_t backup_id);

    dsn::error_code ls_backup_policy();

    dsn::error_code disable_backup_policy(const std::string &policy_name);

    dsn::error_code enable_backup_policy(const std::string &policy_name);

    dsn::error_code query_backup_policy(const std::vector<std::string> &policy_names,
                                        int backup_info_cnt);

    dsn::error_code update_backup_policy(const std::string &policy_name,
                                         const std::vector<int32_t> &add_appids,
                                         const std::vector<int32_t> &removal_appids,
                                         int64_t new_backup_interval_sec,
                                         int32_t backup_history_count_to_keep = 0,
                                         const std::string &start_time = std::string());

    dsn::error_code get_app_envs(const std::string &app_name,
                                 std::map<std::string, std::string> &envs);
    error_with<configuration_update_app_env_response>
    set_app_envs(const std::string &app_name,
                 const std::vector<std::string> &keys,
                 const std::vector<std::string> &values);
    dsn::error_code del_app_envs(const std::string &app_name, const std::vector<std::string> &keys);
    // precondition:
    //  -- if clear_all = true, just ignore prefix
    //  -- if clear_all = false, then prefix must not be empty
    dsn::error_code
    clear_app_envs(const std::string &app_name, bool clear_all, const std::string &prefix);

    dsn::error_code ddd_diagnose(gpid pid, std::vector<ddd_partition_info> &ddd_partitions);

    void query_disk_info(
        const std::vector<dsn::rpc_address> &targets,
        const std::string &app_name,
        /*out*/ std::map<dsn::rpc_address, error_with<query_disk_info_response>> &resps);

    error_with<start_bulk_load_response> start_bulk_load(const std::string &app_name,
                                                         const std::string &cluster_name,
                                                         const std::string &file_provider_type,
                                                         const std::string &remote_root_path,
                                                         bool ingest_behind = false);

    error_with<control_bulk_load_response>
    control_bulk_load(const std::string &app_name, const bulk_load_control_type::type control_type);

    error_with<query_bulk_load_response> query_bulk_load(const std::string &app_name);

    error_with<clear_bulk_load_state_response> clear_bulk_load(const std::string &app_name);

    error_code detect_hotkey(const dsn::rpc_address &target,
                             detect_hotkey_request &req,
                             detect_hotkey_response &resp);

    // partition split
    error_with<start_partition_split_response> start_partition_split(const std::string &app_name,
                                                                     int partition_count);
    error_with<control_split_response> pause_partition_split(const std::string &app_name,
                                                             const int32_t parent_pidx);
    error_with<control_split_response> restart_partition_split(const std::string &app_name,
                                                               const int32_t parent_pidx);
    error_with<control_split_response> cancel_partition_split(const std::string &app_name,
                                                              const int32_t old_partition_count);
    error_with<control_split_response>
    control_partition_split(const std::string &app_name,
                            split_control_type::type control_type,
                            const int32_t parent_pidx,
                            const int32_t old_partition_count);

    error_with<query_split_response> query_partition_split(const std::string &app_name);

    error_with<add_new_disk_response> add_new_disk(const rpc_address &target_node,
                                                   const std::string &disk_str);

    error_with<start_app_manual_compact_response>
    start_app_manual_compact(const std::string &app_name,
                             bool bottommost = false,
                             const int32_t level = -1,
                             const int32_t max_count = 0);

    error_with<query_app_manual_compact_response>
    query_app_manual_compact(const std::string &app_name);

    error_with<configuration_get_max_replica_count_response>
    get_max_replica_count(const std::string &app_name);

    error_with<configuration_set_max_replica_count_response>
    set_max_replica_count(const std::string &app_name, int32_t max_replica_count);

private:
    bool static valid_app_char(int c);

    void end_meta_request(const rpc_response_task_ptr &callback,
                          int retry_times,
                          error_code err,
                          dsn::message_ex *request,
                          dsn::message_ex *resp);

    template <typename TRequest>
    rpc_response_task_ptr request_meta(dsn::task_code code,
                                       std::shared_ptr<TRequest> &req,
                                       int timeout_milliseconds = 0,
                                       int reply_thread_hash = 0)
    {
        dsn::message_ex *msg = dsn::message_ex::create_request(code, timeout_milliseconds);
        ::dsn::marshall(msg, *req);

        rpc_response_task_ptr task = ::dsn::rpc::create_rpc_response_task(
            msg, nullptr, empty_rpc_handler, reply_thread_hash);
        rpc::call(_meta_server,
                  msg,
                  &_tracker,
                  [this, task](
                      error_code err, dsn::message_ex *request, dsn::message_ex *response) mutable {
                      end_meta_request(std::move(task), 0, err, request, response);
                  });
        return task;
    }

    /// Send request to meta server synchronously.
    template <typename TRpcHolder, typename TResponse = typename TRpcHolder::response_type>
    error_with<TResponse> call_rpc_sync(TRpcHolder rpc, int reply_thread_hash = 0)
    {
        // Retry at maximum `MAX_RETRY` times when error occurred.
        static constexpr int MAX_RETRY = 2;
        error_code err = ERR_UNKNOWN;
        for (int retry = 0; retry < MAX_RETRY; retry++) {
            task_ptr task = rpc.call(_meta_server,
                                     &_tracker,
                                     [&err](error_code code) { err = code; },
                                     reply_thread_hash);
            task->wait();
            if (err == ERR_OK) {
                break;
            }
        }
        if (err != ERR_OK) {
            return error_s::make(err, "unable to send rpc to server");
        }
        return error_with<TResponse>(std::move(rpc.response()));
    }

    /// Send request to multi replica server synchronously.
    template <typename TRpcHolder, typename TResponse = typename TRpcHolder::response_type>
    void call_rpcs_sync(std::map<dsn::rpc_address, TRpcHolder> &rpcs,
                        std::map<dsn::rpc_address, error_with<TResponse>> &resps,
                        int reply_thread_hash = 0,
                        bool enable_retry = true)
    {
        dsn::task_tracker tracker;
        error_code err = ERR_UNKNOWN;
        for (auto &rpc : rpcs) {
            rpc.second.call(
                rpc.first, &tracker, [&err, &resps, &rpcs, &rpc](error_code code) mutable {
                    err = code;
                    if (err == dsn::ERR_OK) {
                        resps.emplace(rpc.first, std::move(rpc.second.response()));
                        rpcs.erase(rpc.first);
                    } else {
                        resps.emplace(
                            rpc.first,
                            std::move(error_s::make(err, "unable to send rpc to server")));
                    }
                });
        }
        tracker.wait_outstanding_tasks();

        if (enable_retry && rpcs.size() > 0) {
            std::map<dsn::rpc_address, dsn::error_with<TResponse>> retry_resps;
            call_rpcs_sync(rpcs, retry_resps, reply_thread_hash, false);
            for (auto &resp : retry_resps) {
                resps.emplace(resp.first, std::move(resp.second));
            }
        }
    }

private:
    dsn::rpc_address _meta_server;
    dsn::task_tracker _tracker;

    typedef rpc_holder<detect_hotkey_request, detect_hotkey_response> detect_hotkey_rpc;
    typedef rpc_holder<query_disk_info_request, query_disk_info_response> query_disk_info_rpc;
    typedef rpc_holder<add_new_disk_request, add_new_disk_response> add_new_disk_rpc;
};
} // namespace replication
} // namespace dsn
