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

#include <gtest/gtest_prod.h>
#include <stddef.h>
#include <stdint.h>
#include <atomic>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "block_service/block_service_manager.h"
#include "bulk_load_types.h"
#include "common/bulk_load_common.h"
#include "common/fs_manager.h"
#include "common/gpid.h"
#include "common/replication_common.h"
#include "common/replication_other_types.h"
#include "consensus_types.h"
#include "dsn.layer2_types.h"
#include "failure_detector/failure_detector_multimaster.h"
#include "metadata_types.h"
#include "partition_split_types.h"
#include "replica.h"
#include "replica/mutation_log.h"
#include "replica_admin_types.h"
#include "runtime/ranger/access_type.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_holder.h"
#include "runtime/security/access_controller.h"
#include "runtime/serverlet.h"
#include "runtime/task/task.h"
#include "runtime/task/task_code.h"
#include "runtime/task/task_tracker.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_utils.h"
#include "utils/metrics.h"
#include "utils/zlocks.h"

namespace dsn {
class command_deregister;
class message_ex;
class nfs_node;

namespace service {
class copy_request;
class copy_response;
class get_file_size_request;
class get_file_size_response;
} // namespace service

namespace replication {
class configuration_query_by_node_response;
class configuration_update_request;
class potential_secondary_context;

DSN_DECLARE_uint32(max_concurrent_manual_emergency_checkpointing_count);

typedef rpc_holder<group_check_response, learn_notify_response> learn_completion_notification_rpc;
typedef rpc_holder<group_check_request, group_check_response> group_check_rpc;
typedef rpc_holder<query_replica_decree_request, query_replica_decree_response>
    query_replica_decree_rpc;
typedef rpc_holder<learn_request, learn_response> query_last_checkpoint_info_rpc;
typedef rpc_holder<query_disk_info_request, query_disk_info_response> query_disk_info_rpc;
typedef rpc_holder<replica_disk_migrate_request, replica_disk_migrate_response>
    replica_disk_migrate_rpc;
typedef rpc_holder<notify_catch_up_request, notify_cacth_up_response> notify_catch_up_rpc;
typedef rpc_holder<update_child_group_partition_count_request,
                   update_child_group_partition_count_response>
    update_child_group_partition_count_rpc;
typedef rpc_holder<group_bulk_load_request, group_bulk_load_response> group_bulk_load_rpc;
typedef rpc_holder<detect_hotkey_request, detect_hotkey_response> detect_hotkey_rpc;
typedef rpc_holder<add_new_disk_request, add_new_disk_response> add_new_disk_rpc;

namespace test {
class test_checker;
}
class cold_backup_context;
class replica_split_manager;

typedef std::unordered_map<gpid, replica_ptr> replicas;
typedef std::function<void(
    ::dsn::rpc_address /*from*/, const replica_configuration & /*new_config*/, bool /*is_closing*/)>
    replica_state_subscriber;

class replica_stub;

typedef dsn::ref_ptr<replica_stub> replica_stub_ptr;

class duplication_sync_timer;
class replica_backup_server;

// The replica_stub is the *singleton* entry to access all replica managed in the same process
//   replica_stub(singleton) --> replica --> replication_app_base
class replica_stub : public serverlet<replica_stub>, public ref_counter
{
public:
    static bool s_not_exit_on_log_failure; // for test

public:
    replica_stub(replica_state_subscriber subscriber = nullptr, bool is_long_subscriber = true);
    ~replica_stub(void);

    //
    // initialization
    //
    void initialize(const replication_options &opts, bool clear = false);
    void initialize(bool clear = false);
    void set_options(const replication_options &opts) { _options = opts; }
    void open_service();
    void close();

    //
    //    requests from clients
    //
    void on_client_write(gpid id, dsn::message_ex *request);
    void on_client_read(gpid id, dsn::message_ex *request);

    //
    //    messages from meta server
    //
    void on_config_proposal(const configuration_update_request &proposal);
    void on_query_decree(query_replica_decree_rpc rpc);
    void on_query_replica_info(query_replica_info_rpc rpc);
    void on_query_app_info(query_app_info_rpc rpc);
    void on_bulk_load(bulk_load_rpc rpc);

    //
    //    messages from peers (primary or secondary)
    //        - prepare
    //        - commit
    //        - learn
    //        - bulk_load
    //
    void on_prepare(dsn::message_ex *request);
    void on_learn(dsn::message_ex *msg);
    void on_learn_completion_notification(learn_completion_notification_rpc rpc);
    void on_add_learner(const group_check_request &request);
    void on_remove(const replica_configuration &request);
    void on_group_check(group_check_rpc rpc);
    void on_group_bulk_load(group_bulk_load_rpc rpc);

    //
    //    local messages
    //
    void on_meta_server_connected();
    void on_meta_server_disconnected();
    void on_gc();
    void on_disk_stat();

    //
    //  routines published for test
    //
    void init_gc_for_test();
    void set_meta_server_disconnected_for_test() { on_meta_server_disconnected(); }
    void set_meta_server_connected_for_test(const configuration_query_by_node_response &config);
    void set_replica_state_subscriber_for_test(replica_state_subscriber subscriber,
                                               bool is_long_subscriber);

    //
    // common routines for inquiry
    //
    replica_ptr get_replica(gpid id) const;
    replication_options &options() { return _options; }
    const replication_options &options() const { return _options; }
    bool is_connected() const { return NS_Connected == _state; }
    virtual rpc_address get_meta_server_address() const { return _failure_detector->get_servers(); }
    rpc_address primary_address() const { return _primary_address; }

    //
    // helper methods
    //

    // execute command function on specified or all replicas.
    //   - if allow_empty_args = true and args is empty, then apply on all replicas.
    //   - if allow_empty_args = false, you should specify at least one argument.
    // each argument should be in format of:
    //     id1,id2... (where id is 'app_id' or 'app_id.partition_id')
    std::string exec_command_on_replica(const std::vector<std::string> &args,
                                        bool allow_empty_args,
                                        std::function<std::string(const replica_ptr &rep)> func);

    //
    // partition split
    //

    // called by parent partition, executed by child partition
    void create_child_replica(dsn::rpc_address primary_address,
                              app_info app,
                              ballot init_ballot,
                              gpid child_gpid,
                              gpid parent_gpid,
                              const std::string &parent_dir);

    // create a new replica instance if not found
    // return nullptr when failed to create new replica
    replica_ptr
    create_child_replica_if_not_found(gpid child_pid, app_info *app, const std::string &parent_dir);

    typedef std::function<void(replica_split_manager *split_mgr)> local_execution;

    // This function is used for partition split, caller(replica)
    // parent/child may want child/parent to execute function during partition split
    // if replica `pid` exists, will execute function `handler` and return ERR_OK, otherwise return
    // ERR_OBJECT_NOT_FOUND
    dsn::error_code split_replica_exec(dsn::task_code code, gpid pid, local_execution handler);

    // This function is used for partition split error handler
    void split_replica_error_handler(gpid pid, local_execution handler);

    // on primary parent partition, child notify itself has been caught up parent
    void on_notify_primary_split_catch_up(notify_catch_up_rpc rpc);

    // on child partition, update new partition count
    void on_update_child_group_partition_count(update_child_group_partition_count_rpc rpc);

    // TODO: (Tangyanzhao) add some comments
    void on_detect_hotkey(detect_hotkey_rpc rpc);

    void on_query_disk_info(query_disk_info_rpc rpc);
    void on_disk_migrate(replica_disk_migrate_rpc rpc);

    // query partitions compact status by app_id
    void query_app_manual_compact_status(
        int32_t app_id, /*out*/ std::unordered_map<gpid, manual_compaction_status::type> &status);

    void on_add_new_disk(add_new_disk_rpc rpc);

    // query last checkpoint info for follower in duplication process
    void on_query_last_checkpoint(query_last_checkpoint_info_rpc rpc);

    void update_config(const std::string &name);

    fs_manager *get_fs_manager() { return &_fs_manager; }

    template <typename TReqType, typename TRespType>
    bool check_status_and_authz_with_reply(const TReqType &request,
                                           ::dsn::rpc_replier<TRespType> &reply,
                                           const ::dsn::ranger::access_type &ac_type) const
    {
        if (!_access_controller->is_enable_ranger_acl()) {
            return true;
        }
        const auto &pid = request.pid;
        replica_ptr rep = get_replica(pid);

        if (!rep) {
            TRespType resp;
            resp.error = ERR_OBJECT_NOT_FOUND;
            reply(resp);
            return false;
        }
        dsn::message_ex *msg = reply.response_message();
        if (!rep->access_controller_allowed(msg, ac_type)) {
            TRespType resp;
            resp.error = ERR_ACL_DENY;
            reply(resp);
            return false;
        }
        return true;
    }

    void on_nfs_copy(const ::dsn::service::copy_request &request,
                     ::dsn::rpc_replier<::dsn::service::copy_response> &reply);

    void on_nfs_get_file_size(const ::dsn::service::get_file_size_request &request,
                              ::dsn::rpc_replier<::dsn::service::get_file_size_response> &reply);

private:
    enum replica_node_state
    {
        NS_Disconnected,
        NS_Connecting,
        NS_Connected
    };
    friend USER_DEFINED_ENUM_FORMATTER(replica_stub::replica_node_state);

    enum replica_life_cycle
    {
        RL_invalid,
        RL_creating,
        RL_serving,
        RL_closing,
        RL_closed
    };

    void initialize_start();
    void query_configuration_by_node();
    void on_meta_server_disconnected_scatter(replica_stub_ptr this_, gpid id);
    void on_node_query_reply(error_code err, dsn::message_ex *request, dsn::message_ex *response);
    void on_node_query_reply_scatter(replica_stub_ptr this_,
                                     const configuration_update_request &config);
    void on_node_query_reply_scatter2(replica_stub_ptr this_, gpid id);
    void remove_replica_on_meta_server(const app_info &info, const partition_configuration &config);
    task_ptr begin_open_replica(const app_info &app,
                                gpid id,
                                const std::shared_ptr<group_check_request> &req,
                                const std::shared_ptr<configuration_update_request> &req2);
    void open_replica(const app_info &app,
                      gpid id,
                      const std::shared_ptr<group_check_request> &req,
                      const std::shared_ptr<configuration_update_request> &req2);
    // Create a new replica according to the parameters.
    // 'parent_dir' is used in partition split for create_child_replica_dir().
    replica *new_replica(gpid gpid,
                         const app_info &app,
                         bool restore_if_necessary,
                         bool is_duplication_follower,
                         const std::string &parent_dir = "");
    // Load an existing replica which is located in 'dn' with 'dir' directory.
    replica *load_replica(dir_node *dn, const char *dir);
    // Clean up the memory state and on disk data if creating replica failed.
    void clear_on_failure(replica *rep);
    task_ptr begin_close_replica(replica_ptr r);
    void close_replica(replica_ptr r);
    void notify_replica_state_update(const replica_configuration &config, bool is_closing);
    void trigger_checkpoint(replica_ptr r, bool is_emergency);
    void handle_log_failure(error_code err);

    dsn::error_code on_kill_replica(gpid id);

    void get_replica_info(/*out*/ replica_info &info, /*in*/ replica_ptr r);
    void get_local_replicas(/*out*/ std::vector<replica_info> &replicas);
    replica_life_cycle get_replica_life_cycle(gpid id);
    void on_gc_replica(replica_stub_ptr this_, gpid id);

    struct replica_gc_info
    {
        replica_ptr rep;
        partition_status::type status;
        mutation_log_ptr plog;
        decree last_durable_decree;
        int64_t init_offset_in_shared_log;
    };
    using replica_gc_info_map = std::unordered_map<gpid, replica_gc_info>;

    // Try to remove obsolete files of shared log for garbage collection according to the provided
    // states of all replicas. The purpose is to remove all of the files of shared log, since it
    // has been deprecated, and would not be appended any more.
    void gc_slog(const replica_gc_info_map &replica_gc_map);

    // The number of flushed replicas for the garbage collection of shared log at a time should be
    // limited.
    void limit_flush_replicas_for_slog_gc(size_t prevent_gc_replica_count);

    // Flush rocksdb data to sst files for replicas to facilitate garbage collection of more files
    // of shared log.
    void flush_replicas_for_slog_gc(const replica_gc_info_map &replica_gc_map,
                                    const std::set<gpid> &prevent_gc_replicas);

    void response_client(gpid id,
                         bool is_read,
                         dsn::message_ex *request,
                         partition_status::type status,
                         error_code error);
    void update_disk_holding_replicas();

    void register_ctrl_command();

    int get_app_id_from_replicas(std::string app_name)
    {
        for (const auto &replica : _replicas) {
            const app_info &info = *(replica.second)->get_app_info();
            if (info.app_name == app_name) {
                return info.app_id;
            }
        }
        return 0;
    }

    void query_app_data_version(
        int32_t app_id,
        /*pidx => data_version*/ std::unordered_map<int32_t, uint32_t> &version_map);

#ifdef DSN_ENABLE_GPERF
    // Try to release tcmalloc memory back to operating system
    // If release_all = true, it will release all reserved-not-used memory
    uint64_t gc_tcmalloc_memory(bool release_all);
#elif defined(DSN_USE_JEMALLOC)
    void register_jemalloc_ctrl_command();
#endif

    // Wait all replicas in closing state to be finished.
    void wait_closing_replicas_finished();

private:
    friend class ::dsn::replication::test::test_checker;
    friend class ::dsn::replication::replica;
    friend class ::dsn::replication::potential_secondary_context;
    friend class ::dsn::replication::cold_backup_context;

    friend class replica_duplicator;
    friend class replica_http_service;
    friend class replica_bulk_loader;
    friend class replica_split_manager;
    friend class replica_disk_migrator;
    friend class mock_replica_stub;
    friend class duplication_sync_timer;
    friend class duplication_sync_timer_test;
    friend class replica_duplicator_manager_test;
    friend class duplication_test_base;
    friend class replica_test;
    friend class replica_disk_test_base;
    friend class replica_disk_migrate_test;
    friend class replica_stub_test_base;
    friend class open_replica_test;
    friend class replica_follower;
    friend class replica_follower_test;
    friend class replica_http_service_test;
    FRIEND_TEST(open_replica_test, open_replica_add_decree_and_ballot_check);
    FRIEND_TEST(replica_test, test_auto_trash_of_corruption);
    FRIEND_TEST(replica_test, test_clear_on_failure);
    FRIEND_TEST(GcSlogFlushFeplicasTest, FlushReplicas);

    typedef std::unordered_map<gpid, ::dsn::task_ptr> opening_replicas;
    typedef std::unordered_map<gpid, std::tuple<task_ptr, replica_ptr, app_info, replica_info>>
        closing_replicas; // <gpid, <close_task, replica, app_info, replica_info> >
    typedef std::map<gpid, std::pair<app_info, replica_info>>
        closed_replicas; // <gpid, <app_info, replica_info> >

    mutable zrwlock_nr _replicas_lock;
    replicas _replicas;
    opening_replicas _opening_replicas;
    closing_replicas _closing_replicas;
    closed_replicas _closed_replicas;

    // The number of replicas that prevent slog files from being removed for gc at the last round.
    size_t _last_prevent_gc_replica_count;

    // The real limit of flushed replicas for the garbage collection of shared log.
    size_t _real_log_shared_gc_flush_replicas_limit;

    // The number of flushed replicas, mocked only for test.
    size_t _mock_flush_replicas_for_test;

    mutation_log_ptr _log;
    ::dsn::rpc_address _primary_address;
    char _primary_address_str[64];

    std::shared_ptr<dsn::dist::slave_failure_detector_with_multimaster> _failure_detector;
    mutable zlock _state_lock;
    volatile replica_node_state _state;

    // constants
    replication_options _options;
    replica_state_subscriber _replica_state_subscriber;
    bool _is_long_subscriber;

    // temproal states
    ::dsn::task_ptr _config_query_task;
    ::dsn::timer_task_ptr _config_sync_timer_task;
    ::dsn::task_ptr _gc_timer_task;
    ::dsn::task_ptr _disk_stat_timer_task;
    ::dsn::task_ptr _mem_release_timer_task;

    std::unique_ptr<duplication_sync_timer> _duplication_sync_timer;
    std::unique_ptr<replica_backup_server> _backup_server;

    // command_handlers
    std::vector<std::unique_ptr<command_deregister>> _cmds;

    bool _deny_client;
    bool _verbose_client_log;
    bool _verbose_commit_log;
    bool _release_tcmalloc_memory;
    int32_t _mem_release_max_reserved_mem_percentage;
    int32_t _max_concurrent_bulk_load_downloading_count;

    // we limit LT_APP max concurrent count, because nfs service implementation is
    // too simple, it do not support priority.
    std::atomic_int _learn_app_concurrent_count;

    // handle all the data dirs
    fs_manager _fs_manager;

    // handle all the block filesystems for current replica stub
    // (in other words, current service node)
    dist::block_service::block_service_manager _block_service_manager;

    // nfs_node
    std::unique_ptr<dsn::nfs_node> _nfs;

    // replica count executing bulk load downloading concurrently
    std::atomic_int _bulk_load_downloading_count;

    // replica count executing emergency checkpoint concurrently
    std::atomic_int _manual_emergency_checkpointing_count;

    bool _is_running;

    std::unique_ptr<dsn::security::access_controller> _access_controller;

#ifdef DSN_ENABLE_GPERF
    std::atomic_bool _is_releasing_memory{false};
#endif

    METRIC_VAR_DECLARE_gauge_int64(total_replicas);
    METRIC_VAR_DECLARE_gauge_int64(opening_replicas);
    METRIC_VAR_DECLARE_gauge_int64(closing_replicas);

    METRIC_VAR_DECLARE_gauge_int64(learning_replicas);
    METRIC_VAR_DECLARE_gauge_int64(learning_replicas_max_duration_ms);
    METRIC_VAR_DECLARE_gauge_int64(learning_replicas_max_copy_file_bytes);

    METRIC_VAR_DECLARE_counter(moved_error_replicas);
    METRIC_VAR_DECLARE_counter(moved_garbage_replicas);
    METRIC_VAR_DECLARE_counter(replica_removed_dirs);
    METRIC_VAR_DECLARE_gauge_int64(replica_error_dirs);
    METRIC_VAR_DECLARE_gauge_int64(replica_garbage_dirs);
    METRIC_VAR_DECLARE_gauge_int64(replica_tmp_dirs);
    METRIC_VAR_DECLARE_gauge_int64(replica_origin_dirs);

#ifdef DSN_ENABLE_GPERF
    METRIC_VAR_DECLARE_counter(tcmalloc_released_bytes);
#endif

    METRIC_VAR_DECLARE_counter(read_failed_requests);
    METRIC_VAR_DECLARE_counter(write_failed_requests);
    METRIC_VAR_DECLARE_counter(read_busy_requests);
    METRIC_VAR_DECLARE_counter(write_busy_requests);

    METRIC_VAR_DECLARE_gauge_int64(bulk_load_running_count);
    METRIC_VAR_DECLARE_gauge_int64(bulk_load_ingestion_max_duration_ms);
    METRIC_VAR_DECLARE_gauge_int64(bulk_load_max_duration_ms);

    METRIC_VAR_DECLARE_gauge_int64(splitting_replicas);
    METRIC_VAR_DECLARE_gauge_int64(splitting_replicas_max_duration_ms);
    METRIC_VAR_DECLARE_gauge_int64(splitting_replicas_async_learn_max_duration_ms);
    METRIC_VAR_DECLARE_gauge_int64(splitting_replicas_max_copy_file_bytes);

    dsn::task_tracker _tracker;
};
} // namespace replication
} // namespace dsn
