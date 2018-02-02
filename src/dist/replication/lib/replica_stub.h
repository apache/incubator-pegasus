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

#pragma once

//
// the replica_stub is the *singleton* entry to
// access all replica managed in the same process
//   replica_stub(singleton) --> replica --> replication_app
//

#include "../client_lib/replication_common.h"
#include "../client_lib/fs_manager.h"
#include "../client_lib/block_service_manager.h"

#include <dsn/cpp/perf_counter_.h>
#include <dsn/dist/failure_detector_multimaster.h>

namespace dsn {
namespace replication {

class mutation_log;
class replication_checker;
namespace test {
class test_checker;
}
class cold_backup_context;

typedef std::unordered_map<gpid, replica_ptr> replicas;
typedef std::function<void(
    ::dsn::rpc_address /*from*/, const replica_configuration & /*new_config*/, bool /*is_closing*/)>
    replica_state_subscriber;

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
    void on_client_write(gpid gpid, dsn_message_t request);
    void on_client_read(gpid gpid, dsn_message_t request);

    //
    //    messages from meta server
    //
    void on_config_proposal(const configuration_update_request &proposal);
    void on_query_decree(const query_replica_decree_request &req,
                         /*out*/ query_replica_decree_response &resp);
    void on_query_replica_info(const query_replica_info_request &req,
                               /*out*/ query_replica_info_response &resp);
    void on_query_app_info(const query_app_info_request &req,
                           /*out*/ query_app_info_response &resp);
    void on_cold_backup(const backup_request &request, /*out*/ backup_response &response);

    //
    //    messages from peers (primary or secondary)
    //        - prepare
    //        - commit
    //        - learn
    //
    void on_prepare(dsn_message_t request);
    void on_learn(dsn_message_t msg);
    void on_learn_completion_notification(const group_check_response &report,
                                          /*out*/ learn_notify_response &response);
    void on_add_learner(const group_check_request &request);
    void on_remove(const replica_configuration &request);
    void on_group_check(const group_check_request &request, /*out*/ group_check_response &response);
    void on_copy_checkpoint(const replica_configuration &request, /*out*/ learn_response &response);

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
    replica_ptr
    get_replica(gpid gpid, bool new_when_possible = false, const app_info *app = nullptr);
    replica_ptr get_replica(int32_t app_id, int32_t partition_index);
    replication_options &options() { return _options; }
    bool is_connected() const { return NS_Connected == _state; }

    // void json_state(std::stringstream& out) const;

    // static void static_replica_stub_json_state(void* context, int argc, const char** argv,
    // dsn_cli_reply* reply);
    static void static_replica_stub_json_state_freer(dsn_cli_reply reply);

    std::string get_replica_dir(const char *app_type, gpid gpid, bool create_new = true);

private:
    enum replica_node_state
    {
        NS_Disconnected,
        NS_Connecting,
        NS_Connected
    };

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
    void on_meta_server_disconnected_scatter(replica_stub_ptr this_, gpid gpid);
    void on_node_query_reply(error_code err, dsn_message_t request, dsn_message_t response);
    void on_node_query_reply_scatter(replica_stub_ptr this_,
                                     const configuration_update_request &config);
    void on_node_query_reply_scatter2(replica_stub_ptr this_, gpid gpid);
    void remove_replica_on_meta_server(const app_info &info, const partition_configuration &config);
    ::dsn::task_ptr begin_open_replica(const app_info &app,
                                       gpid gpid,
                                       std::shared_ptr<group_check_request> req,
                                       std::shared_ptr<configuration_update_request> req2);
    void open_replica(const app_info &app,
                      gpid gpid,
                      std::shared_ptr<group_check_request> req,
                      std::shared_ptr<configuration_update_request> req2);
    ::dsn::task_ptr begin_close_replica(replica_ptr r);
    void close_replica(replica_ptr r);
    void add_replica(replica_ptr r);
    bool remove_replica(replica_ptr r);
    void notify_replica_state_update(const replica_configuration &config, bool is_closing);
    void trigger_checkpoint(replica_ptr r, bool is_emergency);
    void handle_log_failure(error_code err);

    void install_perf_counters();
    void on_kill_app_cli(void *context, int argc, const char **argv, dsn_cli_reply *reply);

    void get_replica_info(/*out*/ replica_info &info, /*in*/ replica_ptr r);
    void get_local_replicas(/*out*/ std::vector<replica_info> &replicas, bool lock_protected);
    replica_life_cycle get_replica_life_cycle(const dsn::gpid &pid, bool lock_protected);
    void on_gc_replica(replica_stub_ptr this_, gpid pid);

private:
    friend class ::dsn::replication::replication_checker;
    friend class ::dsn::replication::test::test_checker;
    friend class ::dsn::replication::replica;
    friend class ::dsn::replication::cold_backup_context;
    typedef std::unordered_map<gpid, ::dsn::task_ptr> opening_replicas;
    typedef std::unordered_map<gpid, std::pair<::dsn::task_ptr, replica_ptr>>
        closing_replicas; // <gpid, <close_task, replica> >
    typedef std::map<gpid, std::pair<app_info, replica_info>>
        closed_replicas; // <gpid, <app_info, replica_info> >

    mutable zlock _replicas_lock;
    replicas _replicas;
    opening_replicas _opening_replicas;
    closing_replicas _closing_replicas;
    closed_replicas _closed_replicas;

    mutation_log_ptr _log;
    ::dsn::rpc_address _primary_address;

    ::dsn::dist::slave_failure_detector_with_multimaster *_failure_detector;
    volatile replica_node_state _state;

    // constants
    replication_options _options;
    replica_state_subscriber _replica_state_subscriber;
    bool _is_long_subscriber;

    // temproal states
    ::dsn::task_ptr _config_query_task;
    ::dsn::task_ptr _config_sync_timer_task;
    ::dsn::task_ptr _gc_timer_task;
    ::dsn::task_ptr _disk_stat_timer_task;

    // cli handle, for deregister cli command
    // dsn_handle_t    _cli_replica_stub_json_state_handle;
    dsn_handle_t _cli_kill_partition;
    bool _deny_client;
    bool _verbose_client_log;
    bool _verbose_commit_log;

    // we limit LT_APP max concurrent count, because nfs service implementation is
    // too simple, it do not support priority.
    std::atomic_int _learn_app_concurrent_count;

    // handle all the data dirs
    fs_manager _fs_manager;

    // handle all the block filesystems for current replica stub
    // (in other words, current service node)
    block_service_manager _block_service_manager;

    // performance counters
    perf_counter_ _counter_replicas_count;
    perf_counter_ _counter_replicas_opening_count;
    perf_counter_ _counter_replicas_closing_count;
    perf_counter_ _counter_replicas_total_commit_throught;

    perf_counter_ _counter_replicas_learning_count;
    perf_counter_ _counter_replicas_learning_max_duration_time_ms;
    perf_counter_ _counter_replicas_learning_max_copy_file_size;
    perf_counter_ _counter_replicas_learning_recent_start_count;
    perf_counter_ _counter_replicas_learning_recent_round_start_count;
    perf_counter_ _counter_replicas_learning_recent_copy_file_count;
    perf_counter_ _counter_replicas_learning_recent_copy_file_size;
    perf_counter_ _counter_replicas_learning_recent_copy_buffer_size;
    perf_counter_ _counter_replicas_learning_recent_learn_cache_count;
    perf_counter_ _counter_replicas_learning_recent_learn_app_count;
    perf_counter_ _counter_replicas_learning_recent_learn_log_count;
    perf_counter_ _counter_replicas_learning_recent_learn_reset_count;
    perf_counter_ _counter_replicas_learning_recent_learn_fail_count;
    perf_counter_ _counter_replicas_learning_recent_learn_succ_count;

    perf_counter_ _counter_replicas_recent_prepare_fail_count;
    perf_counter_ _counter_replicas_recent_replica_move_error_count;
    perf_counter_ _counter_replicas_recent_replica_move_garbage_count;
    perf_counter_ _counter_replicas_recent_replica_remove_dir_count;
    perf_counter_ _counter_replicas_error_replica_dir_count;
    perf_counter_ _counter_replicas_garbage_replica_dir_count;

    perf_counter_ _counter_shared_log_size;

    perf_counter_ _counter_cold_backup_running_count;
    perf_counter_ _counter_cold_backup_recent_start_count;
    perf_counter_ _counter_cold_backup_recent_succ_count;
    perf_counter_ _counter_cold_backup_recent_fail_count;
    perf_counter_ _counter_cold_backup_recent_cancel_count;
    perf_counter_ _counter_cold_backup_recent_pause_count;
    perf_counter_ _counter_cold_backup_recent_upload_file_succ_count;
    perf_counter_ _counter_cold_backup_recent_upload_file_fail_count;
    perf_counter_ _counter_cold_backup_recent_upload_file_size;
    perf_counter_ _counter_cold_backup_max_duration_time_ms;
    perf_counter_ _counter_cold_backup_max_upload_file_size;

private:
    void response_client_error(gpid gpid, bool is_read, dsn_message_t request, error_code error);
};
//------------ inline impl ----------------------
}
} // namespace
