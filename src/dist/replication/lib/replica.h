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
 *     replica interface, the base object which rdsn replicates
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

//
// a replica is a replication partition of a serivce,
// which handles all replication related issues
// and on_request the app messages to replication_app_base
// which is binded to this replication partition
//

#include <dsn/tool-api/uniq_timestamp_us.h>
#include <dsn/tool-api/thread_access_checker.h>
#include <dsn/cpp/serverlet.h>

#include <dsn/perf_counter/perf_counter_wrapper.h>
#include <dsn/dist/replication/replica_base.h>

#include "dist/replication/common/replication_common.h"
#include "mutation.h"
#include "mutation_log.h"
#include "prepare_list.h"
#include "replica_context.h"
#include "throttling_controller.h"

namespace dsn {
namespace replication {

class replication_app_base;
class replica_stub;
class replication_checker;
namespace test {
class test_checker;
}

class replica : public serverlet<replica>, public ref_counter, public replica_base
{
public:
    ~replica(void);

    //
    //    routines for replica stub
    //
    static replica *load(replica_stub *stub, const char *dir);
    static replica *
    newr(replica_stub *stub, gpid gpid, const app_info &app, bool restore_if_necessary);

    // return true when the mutation is valid for the current replica
    bool replay_mutation(mutation_ptr &mu, bool is_private);
    void reset_prepare_list_after_replay();

    // return false when update fails or replica is going to be closed
    bool update_local_configuration_with_no_ballot_change(partition_status::type status);
    void set_inactive_state_transient(bool t);
    void check_state_completeness();
    // error_code check_and_fix_private_log_completeness();

    // close() will wait all traced tasks to finish
    void close();

    //
    //    requests from clients
    //
    void on_client_write(task_code code, dsn::message_ex *request, bool ignore_throttling = false);
    void on_client_read(task_code code, dsn::message_ex *request);

    //
    //    messages and tools from/for meta server
    //
    void on_config_proposal(configuration_update_request &proposal);
    void on_config_sync(const app_info &info, const partition_configuration &config);
    void on_cold_backup(const backup_request &request, /*out*/ backup_response &response);

    //
    //    messages from peers (primary or secondary)
    //
    void on_prepare(dsn::message_ex *request);
    void on_learn(dsn::message_ex *msg, const learn_request &request);
    void on_learn_completion_notification(const group_check_response &report,
                                          /*out*/ learn_notify_response &response);
    void on_learn_completion_notification_reply(error_code err,
                                                group_check_response &&report,
                                                learn_notify_response &&resp);
    void on_add_learner(const group_check_request &request);
    void on_remove(const replica_configuration &request);
    void on_group_check(const group_check_request &request, /*out*/ group_check_response &response);
    void on_copy_checkpoint(const replica_configuration &request, /*out*/ learn_response &response);

    //
    //    messsages from liveness monitor
    //
    void on_meta_server_disconnected();

    //
    //  routine for testing purpose only
    //
    void inject_error(error_code err);

    //
    //  local information query
    //
    ballot get_ballot() const { return _config.ballot; }
    partition_status::type status() const { return _config.status; }
    replication_app_base *get_app() { return _app.get(); }
    const app_info *get_app_info() const { return &_app_info; }
    decree max_prepared_decree() const { return _prepare_list->max_decree(); }
    decree last_committed_decree() const { return _prepare_list->last_committed_decree(); }
    decree last_prepared_decree() const;
    decree last_durable_decree() const;
    decree last_flushed_decree() const;
    const std::string &dir() const { return _dir; }
    uint64_t create_time_milliseconds() const { return _create_time_ms; }
    const char *name() const { return replica_name(); }
    mutation_log_ptr private_log() const { return _private_log; }
    const replication_options *options() const { return _options; }
    replica_stub *get_replica_stub() { return _stub; }
    bool verbose_commit_log() const;
    dsn::task_tracker *tracker() { return &_tracker; }

    // void json_state(std::stringstream& out) const;
    void update_last_checkpoint_generate_time();
    void update_commit_statistics(int count);

    // routine for get extra envs from replica
    const std::map<std::string, std::string> &get_replica_extra_envs() const { return _extra_envs; }

private:
    // common helpers
    void init_state();
    void response_client_read(dsn::message_ex *request, error_code error);
    void response_client_write(dsn::message_ex *request, error_code error);
    void execute_mutation(mutation_ptr &mu);
    mutation_ptr new_mutation(decree decree);

    // initialization
    replica(replica_stub *stub, gpid gpid, const app_info &app, const char *dir, bool need_restore);
    error_code initialize_on_new();
    error_code initialize_on_load();
    error_code init_app_and_prepare_list(bool create_new);

    /////////////////////////////////////////////////////////////////
    // 2pc
    void init_prepare(mutation_ptr &mu, bool reconciliation);
    void send_prepare_message(::dsn::rpc_address addr,
                              partition_status::type status,
                              const mutation_ptr &mu,
                              int timeout_milliseconds,
                              int64_t learn_signature = invalid_signature);
    void on_append_log_completed(mutation_ptr &mu, error_code err, size_t size);
    void on_prepare_reply(std::pair<mutation_ptr, partition_status::type> pr,
                          error_code err,
                          dsn::message_ex *request,
                          dsn::message_ex *reply);
    void do_possible_commit_on_primary(mutation_ptr &mu);
    void ack_prepare_message(error_code err, mutation_ptr &mu);
    void cleanup_preparing_mutations(bool wait);

    /////////////////////////////////////////////////////////////////
    // learning
    void init_learn(uint64_t signature);
    void on_learn_reply(error_code err, learn_request &&req, learn_response &&resp);
    void on_copy_remote_state_completed(error_code err,
                                        size_t size,
                                        uint64_t copy_start_time,
                                        learn_request &&req,
                                        learn_response &&resp);
    void on_learn_remote_state_completed(error_code err);
    void handle_learning_error(error_code err, bool is_local_error);
    error_code handle_learning_succeeded_on_primary(::dsn::rpc_address node,
                                                    uint64_t learn_signature);
    void notify_learn_completion();
    error_code apply_learned_state_from_private_log(learn_state &state);

    /////////////////////////////////////////////////////////////////
    // failure handling
    void handle_local_failure(error_code error);
    void handle_remote_failure(partition_status::type status,
                               ::dsn::rpc_address node,
                               error_code error,
                               const std::string &caused_by);

    /////////////////////////////////////////////////////////////////
    // reconfiguration
    void assign_primary(configuration_update_request &proposal);
    void add_potential_secondary(configuration_update_request &proposal);
    void upgrade_to_secondary_on_primary(::dsn::rpc_address node);
    void downgrade_to_secondary_on_primary(configuration_update_request &proposal);
    void downgrade_to_inactive_on_primary(configuration_update_request &proposal);
    void remove(configuration_update_request &proposal);
    void update_configuration_on_meta_server(config_type::type type,
                                             ::dsn::rpc_address node,
                                             partition_configuration &newConfig);
    void
    on_update_configuration_on_meta_server_reply(error_code err,
                                                 dsn::message_ex *request,
                                                 dsn::message_ex *response,
                                                 std::shared_ptr<configuration_update_request> req);
    void replay_prepare_list();
    bool is_same_ballot_status_change_allowed(partition_status::type olds,
                                              partition_status::type news);

    // return false when update fails or replica is going to be closed
    bool update_app_envs(const std::map<std::string, std::string> &envs);
    void update_app_envs_internal(const std::map<std::string, std::string> &envs);
    bool query_app_envs(/*out*/ std::map<std::string, std::string> &envs);
    bool update_configuration(const partition_configuration &config);
    bool update_local_configuration(const replica_configuration &config, bool same_ballot = false);

    /////////////////////////////////////////////////////////////////
    // group check
    void init_group_check();
    void broadcast_group_check();
    void on_group_check_reply(error_code err,
                              const std::shared_ptr<group_check_request> &req,
                              const std::shared_ptr<group_check_response> &resp);

    /////////////////////////////////////////////////////////////////
    // check timer for gc, checkpointing etc.
    void on_checkpoint_timer();
    void init_checkpoint(bool is_emergency);
    error_code background_async_checkpoint(bool is_emergency);
    error_code background_sync_checkpoint();
    void catch_up_with_private_logs(partition_status::type s);
    void on_checkpoint_completed(error_code err);
    void on_copy_checkpoint_ack(error_code err,
                                const std::shared_ptr<replica_configuration> &req,
                                const std::shared_ptr<learn_response> &resp);
    void on_copy_checkpoint_file_completed(error_code err,
                                           size_t sz,
                                           std::shared_ptr<learn_response> resp,
                                           const std::string &chk_dir);

    /////////////////////////////////////////////////////////////////
    // cold backup
    void clear_backup_checkpoint(const std::string &policy_name);
    void generate_backup_checkpoint(cold_backup_context_ptr backup_context);
    void trigger_async_checkpoint_for_backup(cold_backup_context_ptr backup_context);
    void wait_async_checkpoint_for_backup(cold_backup_context_ptr backup_context);
    void local_create_backup_checkpoint(cold_backup_context_ptr backup_context);
    void send_backup_request_to_secondary(const backup_request &request);
    // set all cold_backup_state cancel/pause
    void set_backup_context_cancel();
    void set_backup_context_pause();
    void clear_cold_backup_state();

    void collect_backup_info();

    /////////////////////////////////////////////////////////////////
    // replica restore from backup
    bool read_cold_backup_metadata(const std::string &file, cold_backup_metadata &backup_metadata);
    bool verify_checkpoint(const cold_backup_metadata &backup_metadata,
                           const std::string &chkpt_dir);
    // checkpoint on cold backup media maybe contain useless file,
    // we should abandon these file base cold_backup_metadata
    bool remove_useless_file_under_chkpt(const std::string &chkpt_dir,
                                         const cold_backup_metadata &metadata);
    dsn::error_code download_checkpoint(const configuration_restore_request &req,
                                        const std::string &remote_chkpt_dir,
                                        const std::string &local_chkpt_dir);
    dsn::error_code find_valid_checkpoint(const configuration_restore_request &req,
                                          /*out*/ std::string &remote_chkpt_dir);
    dsn::error_code restore_checkpoint();

    dsn::error_code skip_restore_partition(const std::string &restore_dir);
    void tell_meta_to_restore_rollback();

    void report_restore_status_to_meta();

    void update_restore_progress();

    std::string query_compact_state() const;

private:
    friend class ::dsn::replication::replication_checker;
    friend class ::dsn::replication::test::test_checker;
    friend class ::dsn::replication::mutation_queue;
    friend class ::dsn::replication::replica_stub;
    friend class mock_replica;

    // replica configuration, updated by update_local_configuration ONLY
    replica_configuration _config;
    uint64_t _create_time_ms;
    uint64_t _last_config_change_time_ms;
    uint64_t _last_checkpoint_generate_time_ms;
    uint64_t _next_checkpoint_interval_trigger_time_ms;

    // prepare list
    prepare_list *_prepare_list;

    // private prepare log (may be empty, depending on config)
    mutation_log_ptr _private_log;

    // local checkpoint timer for gc, checkpoint, etc.
    dsn::task_ptr _checkpoint_timer;

    // application
    std::unique_ptr<replication_app_base> _app;

    // constants
    replica_stub *_stub;
    std::string _dir;
    replication_options *_options;
    const app_info _app_info;
    std::map<std::string, std::string> _extra_envs;

    // uniq timestamp generator for this replica.
    //
    // we use it to generate an increasing timestamp for current replica
    // and replicate it to secondary in preparing mutations, and secodaries'
    // timestamp value will also updated if value from primary is larger
    //
    // as the timestamp is recorded in mutation log with mutations, we also update the value
    // when do replaying
    //
    // in addition, as a replica can only be accessed by one thread,
    // so the "thread-unsafe" generator works fine
    uniq_timestamp_us _uniq_timestamp_us;

    // replica status specific states
    primary_context _primary_states;
    secondary_context _secondary_states;
    potential_secondary_context _potential_secondary_states;
    // policy_name --> cold_backup_context
    std::map<std::string, cold_backup_context_ptr> _cold_backup_contexts;

    // timer task that running in replication-thread
    dsn::task_ptr _collect_info_timer;
    std::atomic<uint64_t> _cold_backup_running_count;
    std::atomic<uint64_t> _cold_backup_max_duration_time_ms;
    std::atomic<uint64_t> _cold_backup_max_upload_file_size;

    // record the progress of restore
    int64_t _chkpt_total_size;
    std::atomic<int64_t> _cur_download_size;
    std::atomic<int32_t> _restore_progress;
    // _restore_status:
    //      ERR_OK: restore haven't encounter some error
    //      ERR_CORRUPTION : data on backup media is damaged and we can not skip the damage data,
    //                       so should restore rollback
    //      ERR_IGNORE_DAMAGED_DATA : data on backup media is damaged but we can skip the damage
    //                                data, so skip the damaged partition
    dsn::error_code _restore_status;

    bool _inactive_is_transient; // upgrade to P/S is allowed only iff true
    bool _is_initializing;       // when initializing, switching to primary need to update ballot
    bool _deny_client_write;     // if deny all write requests
    throttling_controller _write_throttling_controller;

    // perf counters
    perf_counter_wrapper _counter_private_log_size;
    perf_counter_wrapper _counter_recent_write_throttling_delay_count;
    perf_counter_wrapper _counter_recent_write_throttling_reject_count;

    dsn::task_tracker _tracker;
    // the thread access checker
    dsn::thread_access_checker _checker;
};
typedef dsn::ref_ptr<replica> replica_ptr;
}
} // namespace
