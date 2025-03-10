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
#include <string>
#include <utility>

#include "common/json_helper.h"
#include "common/replication_other_types.h"
#include "dsn.layer2_types.h"
#include "duplication/replica_duplicator_manager.h" // IWYU pragma: keep
#include "meta_admin_types.h"
#include "metadata_types.h"
#include "mutation.h"
#include "mutation_log.h"
#include "prepare_list.h"
#include "ranger/access_type.h"
#include "replica/backup/cold_backup_context.h"
#include "replica/replica_base.h"
#include "replica_context.h"
#include "rpc/rpc_message.h"
#include "runtime/api_layer1.h"
#include "runtime/serverlet.h"
#include "task/task.h"
#include "task/task_tracker.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/metrics.h"
#include "utils/ports.h"
#include "utils/thread_access_checker.h"
#include "utils/throttling_controller.h"
#include "utils/uniq_timestamp_us.h"

namespace pegasus {
namespace server {
class pegasus_server_test_base;
class rocksdb_wrapper_test;
} // namespace server
} // namespace pegasus

namespace dsn {
class gpid;
class host_port;
class task_spec;

namespace dist::block_service {
class block_filesystem;
} // namespace dist::block_service

namespace security {
class access_controller;
} // namespace security

namespace replication {

class backup_request;
class backup_response;
class configuration_restore_request;
class detect_hotkey_request;
class detect_hotkey_response;
class group_check_request;
class group_check_response;
class learn_notify_response;
class learn_request;
class learn_response;
class learn_state;
class mutation_log_tool;
class replica;
class replica_backup_manager;
class replica_bulk_loader;
class replica_disk_migrator;
class replica_follower;
class replica_split_manager;
class replica_stub;
class replication_app_base;
class replication_options;
struct dir_node;

using cold_backup_context_ptr = dsn::ref_ptr<cold_backup_context>;

namespace test {
class test_checker;
} // namespace test

#define CHECK_REQUEST_IF_SPLITTING(op_type)                                                        \
    do {                                                                                           \
        if (!_validate_partition_hash) {                                                           \
            break;                                                                                 \
        }                                                                                          \
        if (_split_mgr->should_reject_request()) {                                                 \
            response_client_##op_type(request, ERR_SPLITTING);                                     \
            METRIC_VAR_INCREMENT(splitting_rejected_##op_type##_requests);                         \
            return;                                                                                \
        }                                                                                          \
        if (!_split_mgr->check_partition_hash(                                                     \
                ((dsn::message_ex *)request)->header->client.partition_hash, #op_type)) {          \
            response_client_##op_type(request, ERR_PARENT_PARTITION_MISUSED);                      \
            return;                                                                                \
        }                                                                                          \
    } while (0)

// get bool envs[name], return false if value is not bool
bool get_bool_envs(const std::map<std::string, std::string> &envs,
                   const std::string &name,
                   /*out*/ bool &value);

struct deny_client
{
    bool read{false};
    bool write{false};
    // deny client and trigger client update partition config by response `ERR_INVALID_STATE`
    bool reconfig{false};

    void reset()
    {
        read = false;
        write = false;
        reconfig = false;
    }

    bool operator==(const deny_client &rhs) const
    {
        return (write == rhs.write && read == rhs.read && reconfig == rhs.reconfig);
    }
};

// The replica interface, the base object which rdsn replicates.
//
// A replica is a replication partition of a serivce, which handles all replication related
// issues and on_request the app messages to replication_app_base which is binded to this
// replication partition.
class replica : public serverlet<replica>, public ref_counter, public replica_base
{
public:
    ~replica() override;

    DISALLOW_COPY_AND_ASSIGN(replica);
    DISALLOW_MOVE_AND_ASSIGN(replica);

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
    void on_client_write(message_ex *request, bool ignore_throttling = false);
    void on_client_read(message_ex *request, bool ignore_throttling = false);

    //
    //    messages and tools from/for meta server
    //
    void on_config_proposal(configuration_update_request &proposal);
    void on_config_sync(const app_info &info,
                        const partition_configuration &pc,
                        split_status::type meta_split_status);
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
    const ballot &get_ballot() const { return _config.ballot; }
    partition_status::type status() const { return _config.status; }
    replication_app_base *get_app() { return _app.get(); }
    const app_info *get_app_info() const { return &_app_info; }
    decree max_prepared_decree() const { return _prepare_list->max_decree(); }
    decree last_committed_decree() const { return _prepare_list->last_committed_decree(); }

    // The last decree that has been applied into rocksdb memtable.
    decree last_applied_decree() const;

    // The last decree that has been flushed into rocksdb sst.
    decree last_flushed_decree() const;

    decree last_prepared_decree() const;
    decree last_durable_decree() const;

    // Encode current progress of decrees into json, including both local writes and duplications
    // of this replica.
    template <typename TWriter>
    void encode_progress(TWriter &writer) const
    {
        writer.StartObject();

        JSON_ENCODE_OBJ(writer, max_prepared_decree, max_prepared_decree());
        JSON_ENCODE_OBJ(writer, max_plog_decree, _private_log->max_decree(get_gpid()));
        JSON_ENCODE_OBJ(writer, max_plog_decree_on_disk, _private_log->max_decree_on_disk());
        JSON_ENCODE_OBJ(writer, max_plog_commit_on_disk, _private_log->max_commit_on_disk());
        JSON_ENCODE_OBJ(writer, last_committed_decree, last_committed_decree());
        JSON_ENCODE_OBJ(writer, last_applied_decree, last_applied_decree());
        JSON_ENCODE_OBJ(writer, last_flushed_decree, last_flushed_decree());
        JSON_ENCODE_OBJ(writer, last_durable_decree, last_durable_decree());
        JSON_ENCODE_OBJ(writer, max_gc_decree, _private_log->max_gced_decree(get_gpid()));

        _duplication_mgr->encode_progress(writer);

        writer.EndObject();
    }

    const std::string &dir() const { return _dir; }
    uint64_t create_time_milliseconds() const { return _create_time_ms; }
    const char *name() const { return replica_name(); }
    mutation_log_ptr private_log() const { return _private_log; }
    const replication_options *options() const { return _options; }
    replica_stub *get_replica_stub() { return _stub; }
    bool verbose_commit_log() const;
    dsn::task_tracker *tracker() { return &_tracker; }

    //
    // Duplication
    //

    using trigger_checkpoint_callback = std::function<void(error_code)>;

    // Choose a fixed thread from pool to trigger an emergency checkpoint asynchronously.
    // A new checkpoint would still be created even if the replica is empty (hasn't received
    // any write operation).
    //
    // Parameters:
    // - `min_checkpoint_decree`: the min decree that should be covered by the triggered
    // checkpoint. Should be a number greater than 0 which means a new checkpoint must be
    // created.
    // - `delay_ms`: the delayed time in milliseconds that the triggering task is put into
    // the thread pool.
    // - `callback`: the callback processor handling the error code of triggering checkpoint.
    void async_trigger_manual_emergency_checkpoint(decree min_checkpoint_decree,
                                                   uint32_t delay_ms,
                                                   trigger_checkpoint_callback callback = {});

    void on_query_last_checkpoint(learn_response &response);
    std::shared_ptr<replica_duplicator_manager> get_duplication_manager() const
    {
        return _duplication_mgr;
    }
    bool is_duplication_master() const { return _is_duplication_master; }
    bool is_duplication_follower() const { return _is_duplication_follower; }
    bool is_duplication_plog_checking() const { return _is_duplication_plog_checking.load(); }
    void set_duplication_plog_checking(bool checking)
    {
        _is_duplication_plog_checking.store(checking);
    }

    void update_app_duplication_status(bool duplicating);

    //
    // Backup
    //
    replica_backup_manager *get_backup_manager() const { return _backup_mgr.get(); }

    void update_last_checkpoint_generate_time();

    //
    // Bulk load
    //
    replica_bulk_loader *get_bulk_loader() const { return _bulk_loader.get(); }
    inline uint64_t ingestion_duration_ms() const
    {
        return _bulk_load_ingestion_start_time_ms > 0
                   ? (dsn_now_ms() - _bulk_load_ingestion_start_time_ms)
                   : 0;
    }

    //
    // Partition Split
    //
    replica_split_manager *get_split_manager() const { return _split_mgr.get(); }

    //
    // Disk migrator
    //
    replica_disk_migrator *disk_migrator() const { return _disk_migrator.get(); }

    replica_follower *get_replica_follower() const { return _replica_follower.get(); };

    // routine for get extra envs from replica
    const std::map<std::string, std::string> &get_replica_extra_envs() const { return _extra_envs; }
    const dir_node *get_dir_node() const { return _dir_node; }

    METRIC_DEFINE_VALUE(write_size_exceed_threshold_requests, int64_t)
    void METRIC_FUNC_NAME_SET(dup_pending_mutations)();
    METRIC_DEFINE_INCREMENT(backup_failed_count)
    METRIC_DEFINE_INCREMENT(backup_successful_count)
    METRIC_DEFINE_INCREMENT(backup_cancelled_count)
    METRIC_DEFINE_INCREMENT(backup_file_upload_failed_count)
    METRIC_DEFINE_INCREMENT(backup_file_upload_successful_count)
    METRIC_DEFINE_INCREMENT_BY(backup_file_upload_total_bytes)

protected:
    // this method is marked protected to enable us to mock it in unit tests.
    virtual decree max_gced_decree_no_lock() const;

private:
    // common helpers
    void init_state();
    void response_client_read(dsn::message_ex *request, error_code error);
    void response_client_write(dsn::message_ex *request, error_code error);
    void execute_mutation(mutation_ptr &mu);

    // Create a new mutation with specified decree and the original atomic write request,
    // which is used to build the response to the client.
    //
    // Parameters:
    // - decree: invalid_decree, or the real decree assigned to this mutation.
    // - original_request: the original request of the atomic write.
    //
    // Return the newly created mutation.
    mutation_ptr new_mutation(decree decree, dsn::message_ex *original_request);

    // Create a new mutation with specified decree and a flag marking whether this is a
    // blocking mutation (for a detailed explanation of blocking mutations, refer to the
    // comments for the field `is_blocking` of class `mutation`).
    //
    // Parameters:
    // - decree: invalid_decree, or the real decree assigned to this mutation.
    // - is_blocking: true means creating a blocking mutation.
    //
    // Return the newly created mutation.
    mutation_ptr new_mutation(decree decree, bool is_blocking);

    // Create a new mutation with specified decree.
    //
    // Parameters:
    // - decree: invalid_decree, or the real decree assigned to this mutation.
    //
    // Return the newly created mutation.
    mutation_ptr new_mutation(decree decree);

    // initialization
    replica(replica_stub *stub,
            gpid gpid,
            const app_info &app,
            dir_node *dn,
            bool need_restore,
            bool is_duplication_follower = false);
    error_code initialize_on_new();
    error_code initialize_on_load();
    error_code init_app_and_prepare_list(bool create_new);
    decree get_replay_start_decree();

    /////////////////////////////////////////////////////////////////
    // 2PC

    // Given the specification for a client request, decide whether to reject it as it is a
    // non-idempotent request.
    //
    // Parameters:
    // - spec: the specification for a client request, should not be null (otherwise the
    // behaviour is undefined).
    //
    // Return true if deciding to reject this client request.
    bool need_reject_non_idempotent(task_spec *spec) const;

    // Given the specification for a client request, decide whether to make it idempotent.
    //
    // Parameters:
    // - spec: the specification for a client request, should not be null (otherwise the
    // behaviour is undefined).
    //
    // Return true if deciding to make this client request idempotent.
    bool need_make_idempotent(task_spec *spec) const;

    // Given a client request, decide whether to make it idempotent.
    //
    // Parameters:
    // - request: the client request, could be null.
    //
    // Return true if deciding to make this client request idempotent.
    bool need_make_idempotent(message_ex *request) const;

    // Make the atomic write request (if any) in a mutation idempotent.
    //
    // Parameters:
    // - mu: the mutation where the atomic write request will be translated into idempotent
    // one. Should contain at least one client request. Once succeed in translating, `mu`
    // will be reassigned with the new idempotent mutation as the output. Thus it is both an
    // input and an output parameter.
    //
    // Return rocksdb::Status::kOk, or other code (rocksdb::Status::Code) if some error
    // occurred while making write idempotent.
    int make_idempotent(mutation_ptr &mu);

    // Launch 2PC for the specified mutation: it will be broadcast to secondary replicas,
    // appended to plog, and finally applied into storage engine.
    //
    // Parameters:
    // - mu: the mutation pushed into the write pipeline.
    // - reconciliation: true means the primary replica will be force to launch 2PC for each
    // uncommitted request in its prepared list to make them committed regardless of whether
    // there is a quorum to receive the prepare requests.
    // - pop_all_committed_mutations: true means popping all committed mutations while preparing
    // locally, used for ingestion in bulk loader with empty write. See `replica_bulk_loader.cpp`
    // for details.
    void init_prepare(mutation_ptr &mu, bool reconciliation, bool pop_all_committed_mutations);

    // The same as the above except that `pop_all_committed_mutations` is set false.
    void init_prepare(mutation_ptr &mu, bool reconciliation)
    {
        init_prepare(mu, reconciliation, false);
    }

    // Reply to the client with the error if 2PC failed.
    //
    // Parameters:
    // - mu: the mutation for which 2PC failed.
    // - err: the error that caused the 2PC failure.
    void reply_with_error(const mutation_ptr &mu, const error_code &err);

    void send_prepare_message(const host_port &hp,
                              partition_status::type status,
                              const mutation_ptr &mu,
                              int timeout_milliseconds,
                              bool pop_all_committed_mutations,
                              int64_t learn_signature);
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
    error_code handle_learning_succeeded_on_primary(const host_port &node,
                                                    uint64_t learn_signature);
    void notify_learn_completion();
    error_code apply_learned_state_from_private_log(learn_state &state);

    // Prepares in-memory mutations for the replica's learning.
    // Returns false if there's no delta data in cache (aka prepare-list).
    bool prepare_cached_learn_state(const learn_request &request,
                                    decree learn_start_decree,
                                    decree local_committed_decree,
                                    /*out*/ remote_learner_state &learner_state,
                                    /*out*/ learn_response &response,
                                    /*out*/ bool &delayed_replay_prepare_list);

    // Gets the position where this round of the learning process should begin.
    // This method is called on primary-side.
    // TODO(wutao1): mark it const
    decree get_learn_start_decree(const learn_request &req);

    // This method differs with `_private_log->max_gced_decree()` in that
    // it also takes `learn/` dir into account, since the learned logs are
    // a part of plog as well.
    // This method is called on learner-side.
    decree get_max_gced_decree_for_learn() const;

    /////////////////////////////////////////////////////////////////
    // failure handling
    void handle_local_failure(error_code error);
    void handle_remote_failure(partition_status::type status,
                               const host_port &node,
                               error_code error,
                               const std::string &caused_by);

    /////////////////////////////////////////////////////////////////
    // reconfiguration
    void assign_primary(configuration_update_request &proposal);
    void add_potential_secondary(const configuration_update_request &proposal);
    void upgrade_to_secondary_on_primary(const host_port &node);
    void downgrade_to_secondary_on_primary(configuration_update_request &proposal);
    void downgrade_to_inactive_on_primary(configuration_update_request &proposal);
    void remove(configuration_update_request &proposal);
    void update_configuration_on_meta_server(config_type::type type,
                                             const host_port &node,
                                             partition_configuration &new_pc);
    void
    on_update_configuration_on_meta_server_reply(error_code err,
                                                 dsn::message_ex *request,
                                                 dsn::message_ex *response,
                                                 std::shared_ptr<configuration_update_request> req);
    void replay_prepare_list();
    bool is_same_ballot_status_change_allowed(partition_status::type olds,
                                              partition_status::type news);

    void update_app_envs(const std::map<std::string, std::string> &envs);
    void update_app_envs_internal(const std::map<std::string, std::string> &envs);
    void query_app_envs(/*out*/ std::map<std::string, std::string> &envs);

    bool update_configuration(const partition_configuration &pc);
    bool update_local_configuration(const replica_configuration &config, bool same_ballot = false);
    error_code update_init_info_ballot_and_decree();

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

    // Enable/Disable plog garbage collection to be executed. For example, to duplicate data
    // to target cluster, we could firstly disable plog garbage collection, then do copy_data.
    // After copy_data is finished, a duplication with DS_LOG status could be added to continue
    // to duplicate data in plog to target cluster; at the same time, plog garbage collection
    // certainly should be enabled again.
    void init_plog_gc_enabled();
    void update_plog_gc_enabled(bool enabled);
    bool is_plog_gc_enabled() const;
    std::string get_plog_gc_enabled_message() const;

    // Trigger an emergency checkpoint for duplication. Once the replica is empty (hasn't
    // received any write operation), there would be no checkpoint created.
    error_code trigger_manual_emergency_checkpoint(decree min_checkpoint_decree);

    /////////////////////////////////////////////////////////////////
    // cold backup
    virtual void generate_backup_checkpoint(cold_backup_context_ptr backup_context);
    void trigger_async_checkpoint_for_backup(cold_backup_context_ptr backup_context);
    void wait_async_checkpoint_for_backup(cold_backup_context_ptr backup_context);
    void local_create_backup_checkpoint(cold_backup_context_ptr backup_context);
    void send_backup_request_to_secondary(const backup_request &request);
    // set all cold_backup_state cancel/pause
    void set_backup_context_cancel();
    void clear_cold_backup_state();

    /////////////////////////////////////////////////////////////////
    // replica restore from backup
    bool read_cold_backup_metadata(const std::string &fname, cold_backup_metadata &backup_metadata);
    // checkpoint on cold backup media maybe contain useless file,
    // we should abandon these file base cold_backup_metadata
    bool remove_useless_file_under_chkpt(const std::string &chkpt_dir,
                                         const cold_backup_metadata &metadata);
    void clear_restore_useless_files(const std::string &local_chkpt_dir,
                                     const cold_backup_metadata &metadata);
    error_code get_backup_metadata(dist::block_service::block_filesystem *fs,
                                   const std::string &remote_chkpt_dir,
                                   const std::string &local_chkpt_dir,
                                   cold_backup_metadata &backup_metadata);
    error_code download_checkpoint(const configuration_restore_request &req,
                                   const std::string &remote_chkpt_dir,
                                   const std::string &local_chkpt_dir);
    dsn::error_code find_valid_checkpoint(const configuration_restore_request &req,
                                          /*out*/ std::string &remote_chkpt_dir);
    dsn::error_code restore_checkpoint();

    dsn::error_code skip_restore_partition(const std::string &restore_dir);
    void tell_meta_to_restore_rollback();

    void report_restore_status_to_meta();

    void update_restore_progress(uint64_t f_size);

    // Used for remote command
    // TODO(clang-tidy): remove this interface and only expose the http interface
    // now this remote commend will be used by `admin_tools/pegasus_manual_compact.sh`
    std::string query_manual_compact_state() const;

    manual_compaction_status::type get_manual_compact_status() const;

    void on_detect_hotkey(const detect_hotkey_request &req, /*out*/ detect_hotkey_response &resp);

    uint32_t query_data_version() const;

    //
    //    Throttling
    //

    /// return true if request is throttled.
    bool throttle_write_request(message_ex *request);
    bool throttle_read_request(message_ex *request);
    bool throttle_backup_request(message_ex *request);
    /// update throttling controllers
    /// \see replica::update_app_envs
    void update_throttle_envs(const std::map<std::string, std::string> &envs);
    void update_throttle_env_internal(const std::map<std::string, std::string> &envs,
                                      const std::string &key,
                                      utils::throttling_controller &cntl);

    // update allowed users for access controller
    void update_ac_allowed_users(const std::map<std::string, std::string> &envs);

    // update replica access controller Ranger policies
    void update_ac_ranger_policies(const std::map<std::string, std::string> &envs);

    // update bool app envs
    void update_bool_envs(const std::map<std::string, std::string> &envs,
                          const std::string &name,
                          /*out*/ bool &value);

    // update envs allow_ingest_behind and store new app_info into file
    void update_allow_ingest_behind(const std::map<std::string, std::string> &envs);

    // update envs to deny client request
    void update_deny_client(const std::map<std::string, std::string> &envs);

    // store `info` into a file under `path` directory
    // path = "" means using the default directory (`_dir`/.app_info)
    error_code store_app_info(app_info &info, const std::string &path = "");

    void update_app_max_replica_count(int32_t max_replica_count);
    void update_app_name(const std::string &app_name);

    bool is_data_corrupted() const { return _data_corrupted; }

    // use Apache Ranger for replica access control
    bool access_controller_allowed(message_ex *msg, const ranger::access_type &ac_type) const;

    // Currently only used for unit test to get the count of backup requests.
    int64_t get_backup_request_count() const;

private:
    friend class ::dsn::replication::test::test_checker;
    friend class ::dsn::replication::mutation_log_tool;
    friend class ::dsn::replication::mutation_queue;
    friend class ::dsn::replication::replica_stub;
    friend class mock_replica;
    friend class throttling_controller_test;
    friend class replica_learn_test;
    friend class replica_duplicator_manager;
    friend class load_mutation;
    friend class replica_split_test;
    friend class replica_test_base;
    friend class replica_test;
    friend class replica_backup_manager;
    friend class replica_bulk_loader;
    friend class replica_split_manager;
    friend class replica_disk_migrator;
    friend class replica_disk_test;
    friend class replica_disk_migrate_test;
    friend class open_replica_test;
    friend class mock_load_replica;
    friend class replica_follower;
    friend class ::pegasus::server::pegasus_server_test_base;
    friend class ::pegasus::server::rocksdb_wrapper_test;
    FRIEND_TEST(replica_disk_test, disk_io_error_test);
    FRIEND_TEST(replica_test, test_auto_trash_of_corruption);

    // replica configuration, updated by update_local_configuration ONLY
    replica_configuration _config;
    uint64_t _create_time_ms;
    uint64_t _last_config_change_time_ms;
    uint64_t _last_checkpoint_generate_time_ms;
    uint64_t _next_checkpoint_interval_trigger_time_ms;

    std::unique_ptr<prepare_list> _prepare_list;

    // private prepare log (may be empty, depending on config)
    mutation_log_ptr _private_log;

    // local checkpoint timer for gc, checkpoint, etc.
    dsn::task_ptr _checkpoint_timer;

    std::atomic<bool> _plog_gc_enabled{true};

    // application
    std::unique_ptr<replication_app_base> _app;

    // constants
    replica_stub *_stub;
    std::string _dir;
    replication_options *_options;
    app_info _app_info;
    std::map<std::string, std::string> _extra_envs;

    // TODO(wangdan): temporarily used to mark whether we make all atomic writes idempotent
    // for this replica. Would make this configurable soon.
    bool _make_write_idempotent;

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
    partition_split_context _split_states;

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
    deny_client _deny_client;    // if deny requests
    utils::throttling_controller
        _write_qps_throttling_controller; // throttling by requests-per-second
    utils::throttling_controller
        _write_size_throttling_controller; // throttling by bytes-per-second
    utils::throttling_controller _read_qps_throttling_controller;
    utils::throttling_controller _backup_request_qps_throttling_controller;

    // duplication
    std::shared_ptr<replica_duplicator_manager> _duplication_mgr;
    bool _is_manual_emergency_checkpointing{false};
    bool _is_duplication_master{false};
    bool _is_duplication_follower{false};
    // Indicate whether the replica is during finding out some private logs to
    // load for duplication. It useful to prevent plog GCed unexpectedly.
    std::atomic<bool> _is_duplication_plog_checking{false};

    // backup
    std::unique_ptr<replica_backup_manager> _backup_mgr;

    // bulk load
    std::unique_ptr<replica_bulk_loader> _bulk_loader;
    // if replica in bulk load ingestion 2pc, will reject other write requests
    bool _is_bulk_load_ingestion{false};
    uint64_t _bulk_load_ingestion_start_time_ms{0};

    // partition split
    std::unique_ptr<replica_split_manager> _split_mgr;
    bool _validate_partition_hash{false};

    // disk migrator
    std::unique_ptr<replica_disk_migrator> _disk_migrator;

    std::unique_ptr<replica_follower> _replica_follower;

    METRIC_VAR_DECLARE_gauge_int64(private_log_size_mb);
    METRIC_VAR_DECLARE_counter(throttling_delayed_write_requests);
    METRIC_VAR_DECLARE_counter(throttling_rejected_write_requests);
    METRIC_VAR_DECLARE_counter(throttling_delayed_read_requests);
    METRIC_VAR_DECLARE_counter(throttling_rejected_read_requests);
    METRIC_VAR_DECLARE_counter(backup_requests);
    METRIC_VAR_DECLARE_counter(throttling_delayed_backup_requests);
    METRIC_VAR_DECLARE_counter(throttling_rejected_backup_requests);
    METRIC_VAR_DECLARE_counter(splitting_rejected_write_requests);
    METRIC_VAR_DECLARE_counter(splitting_rejected_read_requests);
    METRIC_VAR_DECLARE_counter(bulk_load_ingestion_rejected_write_requests);
    METRIC_VAR_DECLARE_counter(dup_rejected_non_idempotent_write_requests);

    METRIC_VAR_DECLARE_counter(learn_count);
    METRIC_VAR_DECLARE_counter(learn_rounds);
    METRIC_VAR_DECLARE_counter(learn_copy_files);
    METRIC_VAR_DECLARE_counter(learn_copy_file_bytes);
    METRIC_VAR_DECLARE_counter(learn_copy_buffer_bytes);
    METRIC_VAR_DECLARE_counter(learn_lt_cache_responses);
    METRIC_VAR_DECLARE_counter(learn_lt_app_responses);
    METRIC_VAR_DECLARE_counter(learn_lt_log_responses);
    METRIC_VAR_DECLARE_counter(learn_resets);
    METRIC_VAR_DECLARE_counter(learn_failed_count);
    METRIC_VAR_DECLARE_counter(learn_successful_count);

    METRIC_VAR_DECLARE_counter(prepare_failed_requests);

    METRIC_VAR_DECLARE_counter(group_check_failed_requests);

    METRIC_VAR_DECLARE_counter(emergency_checkpoints);

    METRIC_VAR_DECLARE_counter(write_size_exceed_threshold_requests);

    METRIC_VAR_DECLARE_counter(backup_started_count);
    METRIC_VAR_DECLARE_counter(backup_failed_count);
    METRIC_VAR_DECLARE_counter(backup_successful_count);
    METRIC_VAR_DECLARE_counter(backup_cancelled_count);
    METRIC_VAR_DECLARE_counter(backup_file_upload_failed_count);
    METRIC_VAR_DECLARE_counter(backup_file_upload_successful_count);
    METRIC_VAR_DECLARE_counter(backup_file_upload_total_bytes);

    dsn::task_tracker _tracker;
    // the thread access checker
    dsn::thread_access_checker _checker;

    std::unique_ptr<security::access_controller> _access_controller;

    // The dir_node where the replica data is placed.
    dir_node *_dir_node{nullptr};

    bool _allow_ingest_behind{false};
    // Indicate where the storage engine data is corrupted and unrecoverable.
    bool _data_corrupted{false};
};

using replica_ptr = dsn::ref_ptr<replica>;

} // namespace replication
} // namespace dsn
