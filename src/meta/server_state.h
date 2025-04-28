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

// IWYU pragma: no_include <boost/detail/basic_pointerbuf.hpp>
#include <boost/lexical_cast.hpp>
#include <gtest/gtest_prod.h>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <ostream>
#include <string>
#include <utility>
#include <vector>

#include "app_env_validator.h"
#include "common/gpid.h"
#include "common/manual_compact.h"
#include "gutil/map_util.h"
#include "meta/meta_rpc_types.h"
#include "meta_data.h"
#include "table_metrics.h"
#include "task/task.h"
#include "task/task_tracker.h"
#include "utils/error_code.h"
#include "utils/zlocks.h"

namespace dsn {
class app_info;
class blob;
class command_deregister;
class host_port;
class message_ex;
class partition_configuration;
class query_cfg_request;
class query_cfg_response;

namespace replication {
class configuration_balancer_request;
class configuration_balancer_response;
class configuration_create_app_request;
class configuration_list_apps_request;
class configuration_list_apps_response;
class configuration_proposal_action;
class configuration_recovery_request;
class configuration_recovery_response;
class configuration_restore_request;
class configuration_update_request;
class query_app_info_response;
class query_replica_info_response;

namespace test {
class test_checker;
}

typedef std::function<void(const app_mapper & /*new_config*/)> config_change_subscriber;
typedef std::function<void(const migration_list &)> replica_migration_subscriber;

class meta_service;

//
// Notes for server_state
//
// A. structure of remote storage
//
// the tree structure on remote storage are like this:
// _apps_root/<app-id1>/0
// _apps_root/<app-id1>/1
// ....
// _apps_root/<app-id1>/n
// _apps_root/<app-id2>/0
// ...
// the content in _apps_root/<app-id> is a json string for class "app-info"
// the content in _apps_root/<app-id>/<partition-id> is a json string for class
// "partition-configuration"
//
// B. app management
//
// When recving create-app request from the DDL client(let's say, NEW-APP-NAME with NEW-APP-ID),
// we first create the root-node for this app, i.e: _apps_root/<NEW-APP-ID>,
// then all the partition nodes of NEW-APP-NAME is created asynchronously with multiple tasks.
// Reasons for this are:
//    1. an app may have thousands/millions of replicas, creating all these things in a task is
//    time-consuming,
//       which may block the update-thread for a long time.
//    2. there are so many requests to the remote storage that it is very likely to timeout if all
//    requests are in a task
// For the same reason, dropping and recalling for an app also have similar implementations.
// Generally, an app may have serveral status of two kinds:
//    a. staging-status: creating, dropping, recalling
//    b. stable-status: available, dropped
//
// Notice: in remote storage, only stable status are stored. We can tell if an app-state is in
// staging by checking if
// all of its partitions are ready:
//    1. for available app, it should have as many partitions as app_info.partition_count AND
//       ALL of the partitions shouldn't have the flags DROPPED
//    2. for dropped app, All of the partitions should have the flags DROPPED
// If theses constraints are not satisfied, it means that some work are not finished.
// Meta-server will check these constraints and continue the creating/dropping/recalling work if
// necessary.
//
// C. persistence of meta data
// D. thread-model of meta server
// E. load balancer

class server_state
{
public:
    static const int sStateHash = 0;

public:
    server_state();
    ~server_state();

    void initialize(meta_service *meta_svc, const std::string &apps_root);
    error_code initialize_data_structure();
    void register_cli_commands();

    void lock_read(zauto_read_lock &other);
    void lock_write(zauto_write_lock &other);

    meta_view get_meta_view() { return {&_all_apps, &_nodes}; }

    std::shared_ptr<app_state> get_app(const std::string &app_name) const
    {
        return gutil::FindWithDefault(_exist_apps, app_name);
    }

    std::shared_ptr<app_state> get_app(int32_t app_id) const
    {
        return gutil::FindWithDefault(_all_apps, app_id);
    }

    void query_configuration_by_index(const query_cfg_request &request,
                                      /*out*/ query_cfg_response &response);
    bool query_configuration_by_gpid(const dsn::gpid id, /*out*/ partition_configuration &pc);

    // app options
    void create_app(dsn::message_ex *msg);
    void drop_app(dsn::message_ex *msg);
    void recall_app(dsn::message_ex *msg);
    void rename_app(configuration_rename_app_rpc rpc);
    void list_apps(const configuration_list_apps_request &request,
                   configuration_list_apps_response &response,
                   dsn::message_ex *msg = nullptr) const;
    void restore_app(dsn::message_ex *msg);

    // app env operations
    void set_app_envs(const app_env_rpc &env_rpc);
    void del_app_envs(const app_env_rpc &env_rpc);
    void clear_app_envs(const app_env_rpc &env_rpc);

    // update configuration
    void on_config_sync(configuration_query_by_node_rpc rpc);
    void on_update_configuration(std::shared_ptr<configuration_update_request> &request,
                                 dsn::message_ex *msg);

    // dump & restore
    error_code dump_from_remote_storage(const char *local_path, bool sync_immediately);
    error_code restore_from_local_storage(const char *local_path);

    void on_change_node_state(const host_port &node, bool is_alive);
    void on_propose_balancer(const configuration_balancer_request &request,
                             configuration_balancer_response &response);
    void on_start_recovery(const configuration_recovery_request &request,
                           configuration_recovery_response &response);
    void on_recv_restore_report(configuration_report_restore_status_rpc rpc);

    void on_query_restore_status(configuration_query_restore_rpc rpc);

    // manual compaction
    void on_start_manual_compact(start_manual_compact_rpc rpc);
    void on_query_manual_compact_status(query_manual_compact_rpc rpc);

    // get/set max_replica_count of an app
    void get_max_replica_count(configuration_get_max_replica_count_rpc rpc) const;
    void set_max_replica_count(configuration_set_max_replica_count_rpc rpc);
    void recover_from_max_replica_count_env();

    // Get `atomic_idempotent` of a table.
    void get_atomic_idempotent(configuration_get_atomic_idempotent_rpc rpc) const;

    // Set `atomic_idempotent` of a table.
    void set_atomic_idempotent(configuration_set_atomic_idempotent_rpc rpc);

    // return true if no need to do any actions
    bool check_all_partitions();
    void get_cluster_balance_score(double &primary_stddev /*out*/, double &total_stddev /*out*/);
    void clear_proposals();

    int count_staging_app();
    // for test
    void set_config_change_subscriber_for_test(config_change_subscriber subscriber);
    void set_replica_migration_subscriber_for_test(replica_migration_subscriber subscriber);

    task_tracker *tracker() { return &_tracker; }
    void wait_all_task() { _tracker.wait_outstanding_tasks(); }

    table_metric_entities &get_table_metric_entities() { return _table_metric_entities; }

private:
    FRIEND_TEST(backup_service_test, test_invalid_backup_request);

    //-1 means waiting forever
    bool spin_wait_staging(int timeout_seconds = -1);
    bool can_run_balancer();

    // user should lock it first
    void update_partition_metrics();

    error_code dump_app_states(const char *local_path,
                               const std::function<app_state *()> &iterator);
    error_code sync_apps_from_remote_storage();
    // sync local state to remote storage,
    // if return OK, all states are synced correctly, and all apps are in stable state
    // else indicate error that remote storage responses
    error_code sync_apps_to_remote_storage();

    error_code sync_apps_from_replica_nodes(const std::vector<dsn::host_port> &node_list,
                                            bool skip_bad_nodes,
                                            bool skip_lost_partitions,
                                            std::string &hint_message);
    void
    sync_app_from_backup_media(const configuration_restore_request &request,
                               std::function<void(dsn::error_code, const dsn::blob &)> &&callback);
    std::pair<dsn::error_code, std::shared_ptr<app_state>> restore_app_info(
        dsn::message_ex *msg, const configuration_restore_request &req, const dsn::blob &app_info);

    error_code initialize_default_apps();
    void initialize_node_state();

    void check_consistency(const dsn::gpid &gpid);

    error_code construct_apps(const std::vector<query_app_info_response> &query_app_responses,
                              const std::vector<dsn::host_port> &replica_nodes,
                              std::string &hint_message);
    error_code construct_partitions(
        const std::vector<query_replica_info_response> &query_replica_info_responses,
        const std::vector<dsn::host_port> &replica_nodes,
        bool skip_lost_partitions,
        std::string &hint_message);

    // Process the status carried in the environment variables of creating table request while
    // the table is at the status of AS_AVAILABLE, to update remote and local states and reply
    // to the master cluster.
    void process_create_follower_app_status(message_ex *msg,
                                            const configuration_create_app_request &request,
                                            const std::string &req_master_cluster,
                                            std::shared_ptr<app_state> &app);

    // Update the meta data with the new creating status both on the remote storage and local
    // memory.
    void update_create_follower_app_status(message_ex *msg,
                                           const std::string &old_status,
                                           const std::string &new_status,
                                           std::shared_ptr<app_state> &app);

    void do_app_create(std::shared_ptr<app_state> &app);
    void do_app_drop(std::shared_ptr<app_state> &app);
    void do_app_recall(std::shared_ptr<app_state> &app);
    void init_app_partition_node(std::shared_ptr<app_state> &app, int pidx, task_ptr callback);
    // do_update_app_info()
    //  -- ensure update app_info to remote storage succeed, if timeout, it will retry autoly
    void do_update_app_info(const std::string &app_path,
                            const app_info &info,
                            const std::function<void(error_code)> &cb);

    task_ptr
    update_configuration_on_remote(std::shared_ptr<configuration_update_request> &config_request);
    void
    on_update_configuration_on_remote_reply(error_code ec,
                                            std::shared_ptr<configuration_update_request> &request);
    void
    update_configuration_locally(app_state &app,
                                 std::shared_ptr<configuration_update_request> &config_request);
    void request_check(const partition_configuration &old_pc,
                       const configuration_update_request &request);
    void recall_partition(std::shared_ptr<app_state> &app, int pidx);
    void drop_partition(std::shared_ptr<app_state> &app, int pidx);
    void downgrade_primary_to_inactive(std::shared_ptr<app_state> &app, int pidx);
    void downgrade_secondary_to_inactive(std::shared_ptr<app_state> &app,
                                         int pidx,
                                         const host_port &node);
    void
    downgrade_stateless_nodes(std::shared_ptr<app_state> &app, int pidx, const host_port &node);
    void
    on_partition_node_dead(std::shared_ptr<app_state> &app, int pidx, const dsn::host_port &node);
    void send_proposal(const host_port &target, const configuration_update_request &proposal);
    void send_proposal(const configuration_proposal_action &action,
                       const partition_configuration &pc,
                       const app_state &app);

    // util function
    int32_t next_app_id() const
    {
        if (_all_apps.empty())
            return 1;
        // return the max_id + 1
        return ((--_all_apps.end())->first) + 1;
    }
    std::string get_app_path(const app_state &app) const
    {
        return _apps_root + "/" + boost::lexical_cast<std::string>(app.app_id);
    }
    std::string get_partition_path(const dsn::gpid &gpid) const
    {
        std::stringstream oss;
        oss << _apps_root << "/" << gpid.get_app_id() << "/" << gpid.get_partition_index();
        return oss.str();
    }
    std::string get_partition_path(const app_state &app, int partition_id) const
    {
        std::stringstream oss;
        oss << _apps_root << "/" << app.app_id << "/" << partition_id;
        return oss.str();
    }

    void process_one_partition(std::shared_ptr<app_state> &app);
    void transition_staging_state(std::shared_ptr<app_state> &app);

    // check whether a max replica count is valid especially for a new app
    bool validate_target_max_replica_count(int32_t max_replica_count,
                                           std::string &hint_message) const;
    bool validate_target_max_replica_count(int32_t max_replica_count) const;

    template <typename Response>
    std::shared_ptr<app_state> get_app_and_check_exist(const std::string &app_name,
                                                       Response &response) const;

    template <typename Response>
    bool check_max_replica_count_consistent(const std::shared_ptr<app_state> &app,
                                            Response &response) const;

    void set_max_replica_count_env_updating(std::shared_ptr<app_state> &app,
                                            configuration_set_max_replica_count_rpc rpc);
    using partition_callback = std::function<void(error_code, int32_t)>;
    void do_update_max_replica_count(std::shared_ptr<app_state> &app,
                                     configuration_set_max_replica_count_rpc rpc);
    void update_app_max_replica_count(std::shared_ptr<app_state> &app,
                                      configuration_set_max_replica_count_rpc rpc);
    void update_partition_max_replica_count(std::shared_ptr<app_state> &app,
                                            int32_t partition_index,
                                            int32_t new_max_replica_count,
                                            partition_callback on_partition_updated);
    task_ptr update_partition_max_replica_count_on_remote(std::shared_ptr<app_state> &app,
                                                          const partition_configuration &new_pc,
                                                          partition_callback on_partition_updated);
    void
    on_update_partition_max_replica_count_on_remote_reply(error_code ec,
                                                          std::shared_ptr<app_state> &app,
                                                          const partition_configuration &new_pc,
                                                          partition_callback on_partition_updated);
    void update_partition_max_replica_count_locally(std::shared_ptr<app_state> &app,
                                                    const partition_configuration &new_pc);

    void recover_all_partitions_max_replica_count(std::shared_ptr<app_state> &app,
                                                  int32_t max_replica_count,
                                                  dsn::task_tracker &tracker);
    void recover_app_max_replica_count(std::shared_ptr<app_state> &app,
                                       int32_t max_replica_count,
                                       dsn::task_tracker &tracker);

    // Update `atomic_idempotent` of given table on remote storage.
    //
    // Parameters:
    // - app: the given table.
    // - rpc: RPC request/response to change `atomic_idempotent`.
    void update_app_atomic_idempotent_on_remote(std::shared_ptr<app_state> &app,
                                                configuration_set_atomic_idempotent_rpc rpc);

    // Used for `on_start_manual_compaction`
    bool parse_compaction_envs(start_manual_compact_rpc rpc,
                               std::vector<std::string> &keys,
                               std::vector<std::string> &values);
    void update_compaction_envs_on_remote_storage(start_manual_compact_rpc rpc,
                                                  const std::vector<std::string> &keys,
                                                  const std::vector<std::string> &values);

    friend class bulk_load_service;
    friend class bulk_load_service_test;
    friend class meta_app_operation_test;
    friend class meta_duplication_service;
    friend class meta_duplication_service_test;
    friend class meta_partition_guardian_test;
    friend class meta_split_service;
    friend class meta_split_service_test;
    friend class meta_service_test_app;
    friend class server_state_test;
    friend class meta_test_base;
    friend class test::test_checker;
    friend class server_state_restore_test;
    friend class meta_app_compaction_test;

    FRIEND_TEST(meta_backup_service_test, test_add_backup_policy);
    FRIEND_TEST(policy_context_test, test_app_dropped_during_backup);
    FRIEND_TEST(policy_context_test, test_backup_failed);

    dsn::task_tracker _tracker;

    meta_service *_meta_svc;
    std::string _apps_root;

    mutable zrwlock_nr _lock;
    node_mapper _nodes;

    // available apps, dropping apps, creating apps: name -> app_state
    std::map<std::string, std::shared_ptr<app_state>> _exist_apps;
    //_exist_apps + dropped apps: app_id -> app_state
    app_mapper _all_apps;

    // for load balancer
    migration_list _temporary_list;

    // for test
    config_change_subscriber _config_change_subscriber;
    replica_migration_subscriber _replica_migration_subscriber;

    bool _add_secondary_enable_flow_control;
    int32_t _add_secondary_max_count_for_one_node;
    std::vector<std::unique_ptr<command_deregister>> _cmds;

    app_env_validator _app_env_validator;

    table_metric_entities _table_metric_entities;
};

} // namespace replication
} // namespace dsn
