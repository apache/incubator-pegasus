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

#include "replica.h"

#include <string_view>
#include <fmt/core.h>
#include <rocksdb/status.h>
#include <functional>
#include <vector>

#include "backup/replica_backup_manager.h"
#include "bulk_load/replica_bulk_loader.h"
#include "common/backup_common.h"
#include "common/fs_manager.h"
#include "common/gpid.h"
#include "common/replica_envs.h"
#include "common/replication_common.h"
#include "common/replication_enums.h"
#include "consensus_types.h"
#include "duplication/replica_follower.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica/duplication/replica_duplicator_manager.h"
#include "replica/prepare_list.h"
#include "replica/replica_context.h"
#include "replica/replication_app_base.h"
#include "replica_admin_types.h"
#include "replica_disk_migrator.h"
#include "replica_stub.h"
#include "rpc/rpc_message.h"
#include "security/access_controller.h"
#include "split/replica_split_manager.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/latency_tracer.h"
#include "utils/rand.h"

DSN_DEFINE_bool(replication,
                batch_write_disabled,
                false,
                "Whether to disable auto-batch of replicated write requests");
DSN_DEFINE_int32(replication,
                 staleness_for_commit,
                 20,
                 "The maximum number of two-phase commit rounds are allowed");
DSN_DEFINE_int32(replication,
                 max_mutation_count_in_prepare_list,
                 110,
                 "The maximum number of mutations allowed in prepare list");
DSN_DEFINE_group_validator(max_mutation_count_in_prepare_list, [](std::string &message) -> bool {
    if (FLAGS_max_mutation_count_in_prepare_list < FLAGS_staleness_for_commit) {
        message = fmt::format("replication.max_mutation_count_in_prepare_list({}) should be >= "
                              "replication.staleness_for_commit({})",
                              FLAGS_max_mutation_count_in_prepare_list,
                              FLAGS_staleness_for_commit);
        return false;
    }
    return true;
});

DSN_DECLARE_int32(checkpoint_max_interval_hours);

METRIC_DEFINE_gauge_int64(replica,
                          private_log_size_mb,
                          dsn::metric_unit::kMegaBytes,
                          "The size of private log in MB");

METRIC_DEFINE_counter(replica,
                      throttling_delayed_write_requests,
                      dsn::metric_unit::kRequests,
                      "The number of delayed write requests by throttling");

METRIC_DEFINE_counter(replica,
                      throttling_rejected_write_requests,
                      dsn::metric_unit::kRequests,
                      "The number of rejected write requests by throttling");

METRIC_DEFINE_counter(replica,
                      throttling_delayed_read_requests,
                      dsn::metric_unit::kRequests,
                      "The number of delayed read requests by throttling");

METRIC_DEFINE_counter(replica,
                      throttling_rejected_read_requests,
                      dsn::metric_unit::kRequests,
                      "The number of rejected read requests by throttling");

METRIC_DEFINE_counter(replica,
                      backup_requests,
                      dsn::metric_unit::kRequests,
                      "The number of backup requests");

METRIC_DEFINE_counter(replica,
                      throttling_delayed_backup_requests,
                      dsn::metric_unit::kRequests,
                      "The number of delayed backup requests by throttling");

METRIC_DEFINE_counter(replica,
                      throttling_rejected_backup_requests,
                      dsn::metric_unit::kRequests,
                      "The number of rejected backup requests by throttling");

METRIC_DEFINE_counter(replica,
                      splitting_rejected_write_requests,
                      dsn::metric_unit::kRequests,
                      "The number of rejected write requests by splitting");

METRIC_DEFINE_counter(replica,
                      splitting_rejected_read_requests,
                      dsn::metric_unit::kRequests,
                      "The number of rejected read requests by splitting");

METRIC_DEFINE_counter(replica,
                      bulk_load_ingestion_rejected_write_requests,
                      dsn::metric_unit::kRequests,
                      "The number of rejected write requests by bulk load ingestion");

METRIC_DEFINE_counter(replica,
                      dup_rejected_non_idempotent_write_requests,
                      dsn::metric_unit::kRequests,
                      "The number of rejected non-idempotent write requests by duplication");

METRIC_DEFINE_counter(
    replica,
    learn_count,
    dsn::metric_unit::kLearns,
    "The number of learns launched by learner (i.e. potential secondary replica)");

METRIC_DEFINE_counter(replica,
                      learn_rounds,
                      dsn::metric_unit::kRounds,
                      "The number of learn rounds launched by learner (during a learn there might"
                      "be multiple rounds)");

METRIC_DEFINE_counter(replica,
                      learn_copy_files,
                      dsn::metric_unit::kFiles,
                      "The number of files that are copied from learnee (i.e. primary replica)");

METRIC_DEFINE_counter(replica,
                      learn_copy_file_bytes,
                      dsn::metric_unit::kBytes,
                      "The size of file that are copied from learnee");

METRIC_DEFINE_counter(replica,
                      learn_copy_buffer_bytes,
                      dsn::metric_unit::kBytes,
                      "The size of data that are copied from learnee's buffer");

METRIC_DEFINE_counter(replica,
                      learn_lt_cache_responses,
                      dsn::metric_unit::kResponses,
                      "The number of learn responses of LT_CACHE type decided by learner, with "
                      "each learn response related to an RPC_LEARN request");

METRIC_DEFINE_counter(replica,
                      learn_lt_app_responses,
                      dsn::metric_unit::kResponses,
                      "The number of learn responses of LT_APP type decided by learner, with each "
                      "learn response related to an RPC_LEARN request");

METRIC_DEFINE_counter(replica,
                      learn_lt_log_responses,
                      dsn::metric_unit::kResponses,
                      "The number of learn responses of LT_LOG type decided by learner, with each "
                      "learn response related to an RPC_LEARN request");

METRIC_DEFINE_counter(replica,
                      learn_resets,
                      dsn::metric_unit::kResets,
                      "The number of times learner resets its local state (since its local state "
                      "is newer than learnee's), with each reset related to an learn response of "
                      "an RPC_LEARN request");

METRIC_DEFINE_counter(replica,
                      learn_failed_count,
                      dsn::metric_unit::kLearns,
                      "The number of failed learns launched by learner");

METRIC_DEFINE_counter(replica,
                      learn_successful_count,
                      dsn::metric_unit::kLearns,
                      "The number of successful learns launched by learner");

METRIC_DEFINE_counter(replica,
                      prepare_failed_requests,
                      dsn::metric_unit::kRequests,
                      "The number of failed RPC_PREPARE requests");

METRIC_DEFINE_counter(replica,
                      group_check_failed_requests,
                      dsn::metric_unit::kRequests,
                      "The number of failed RPC_GROUP_CHECK requests launched by primary replicas");

METRIC_DEFINE_counter(replica,
                      emergency_checkpoints,
                      dsn::metric_unit::kCheckpoints,
                      "The number of triggered emergency checkpoints");

METRIC_DEFINE_counter(replica,
                      write_size_exceed_threshold_requests,
                      dsn::metric_unit::kRequests,
                      "The number of write requests whose size exceeds threshold");

METRIC_DEFINE_counter(replica,
                      backup_started_count,
                      dsn::metric_unit::kBackups,
                      "The number of started backups");

METRIC_DEFINE_counter(replica,
                      backup_failed_count,
                      dsn::metric_unit::kBackups,
                      "The number of failed backups");

METRIC_DEFINE_counter(replica,
                      backup_successful_count,
                      dsn::metric_unit::kBackups,
                      "The number of successful backups");

METRIC_DEFINE_counter(replica,
                      backup_cancelled_count,
                      dsn::metric_unit::kBackups,
                      "The number of cancelled backups");

METRIC_DEFINE_counter(replica,
                      backup_file_upload_failed_count,
                      dsn::metric_unit::kFileUploads,
                      "The number of failed file uploads for backups");

METRIC_DEFINE_counter(replica,
                      backup_file_upload_successful_count,
                      dsn::metric_unit::kFileUploads,
                      "The number of successful file uploads for backups");

METRIC_DEFINE_counter(replica,
                      backup_file_upload_total_bytes,
                      dsn::metric_unit::kBytes,
                      "The total size of uploaded files for backups");

namespace dsn {
namespace replication {

replica::replica(replica_stub *stub,
                 gpid gpid,
                 const app_info &app,
                 dir_node *dn,
                 bool need_restore,
                 bool is_duplication_follower)
    : serverlet<replica>(replication_options::kReplicaAppType.c_str()),
      replica_base(gpid, fmt::format("{}@{}", gpid, stub->_primary_host_port_cache), app.app_name),
      _app_info(app),
      _make_write_idempotent(false),
      _primary_states(this, gpid, FLAGS_staleness_for_commit, FLAGS_batch_write_disabled),
      _potential_secondary_states(this),
      _chkpt_total_size(0),
      _cur_download_size(0),
      _restore_progress(0),
      _restore_status(ERR_OK),
      _duplication_mgr(std::make_shared<replica_duplicator_manager>(this)),
      // todo(jiashuo1): app.duplicating need rename
      _is_duplication_master(app.duplicating),
      _is_duplication_follower(is_duplication_follower),
      _backup_mgr(new replica_backup_manager(this)),
      METRIC_VAR_INIT_replica(private_log_size_mb),
      METRIC_VAR_INIT_replica(throttling_delayed_write_requests),
      METRIC_VAR_INIT_replica(throttling_rejected_write_requests),
      METRIC_VAR_INIT_replica(throttling_delayed_read_requests),
      METRIC_VAR_INIT_replica(throttling_rejected_read_requests),
      METRIC_VAR_INIT_replica(backup_requests),
      METRIC_VAR_INIT_replica(throttling_delayed_backup_requests),
      METRIC_VAR_INIT_replica(throttling_rejected_backup_requests),
      METRIC_VAR_INIT_replica(splitting_rejected_write_requests),
      METRIC_VAR_INIT_replica(splitting_rejected_read_requests),
      METRIC_VAR_INIT_replica(bulk_load_ingestion_rejected_write_requests),
      METRIC_VAR_INIT_replica(dup_rejected_non_idempotent_write_requests),
      METRIC_VAR_INIT_replica(learn_count),
      METRIC_VAR_INIT_replica(learn_rounds),
      METRIC_VAR_INIT_replica(learn_copy_files),
      METRIC_VAR_INIT_replica(learn_copy_file_bytes),
      METRIC_VAR_INIT_replica(learn_copy_buffer_bytes),
      METRIC_VAR_INIT_replica(learn_lt_cache_responses),
      METRIC_VAR_INIT_replica(learn_lt_app_responses),
      METRIC_VAR_INIT_replica(learn_lt_log_responses),
      METRIC_VAR_INIT_replica(learn_resets),
      METRIC_VAR_INIT_replica(learn_failed_count),
      METRIC_VAR_INIT_replica(learn_successful_count),
      METRIC_VAR_INIT_replica(prepare_failed_requests),
      METRIC_VAR_INIT_replica(group_check_failed_requests),
      METRIC_VAR_INIT_replica(emergency_checkpoints),
      METRIC_VAR_INIT_replica(write_size_exceed_threshold_requests),
      METRIC_VAR_INIT_replica(backup_started_count),
      METRIC_VAR_INIT_replica(backup_failed_count),
      METRIC_VAR_INIT_replica(backup_successful_count),
      METRIC_VAR_INIT_replica(backup_cancelled_count),
      METRIC_VAR_INIT_replica(backup_file_upload_failed_count),
      METRIC_VAR_INIT_replica(backup_file_upload_successful_count),
      METRIC_VAR_INIT_replica(backup_file_upload_total_bytes)
{
    init_plog_gc_enabled();

    CHECK(!_app_info.app_type.empty(), "");
    CHECK_NOTNULL(stub, "");
    _stub = stub;
    CHECK_NOTNULL(dn, "");
    _dir_node = dn;
    _dir = dn->replica_dir(_app_info.app_type, gpid);
    _options = &stub->options();
    init_state();
    _config.pid = gpid;
    _bulk_loader = std::make_unique<replica_bulk_loader>(this);
    _split_mgr = std::make_unique<replica_split_manager>(this);
    _disk_migrator = std::make_unique<replica_disk_migrator>(this);
    _replica_follower = std::make_unique<replica_follower>(this);

    if (need_restore) {
        // add an extra env for restore
        _extra_envs.insert(
            std::make_pair(backup_restore_constant::FORCE_RESTORE, std::string("true")));
    }

    _access_controller = security::create_replica_access_controller(name());
}

void replica::update_last_checkpoint_generate_time()
{
    _last_checkpoint_generate_time_ms = dsn_now_ms();
    uint64_t max_interval_ms = FLAGS_checkpoint_max_interval_hours * 3600000UL;
    // use random trigger time to avoid flush peek
    _next_checkpoint_interval_trigger_time_ms =
        _last_checkpoint_generate_time_ms + rand::next_u64(max_interval_ms / 2, max_interval_ms);
}

void replica::init_state()
{
    _inactive_is_transient = false;
    _is_initializing = false;
    _prepare_list = std::make_unique<prepare_list>(
        this,
        0,
        FLAGS_max_mutation_count_in_prepare_list,
        std::bind(&replica::execute_mutation, this, std::placeholders::_1));

    _config.ballot = 0;
    _config.pid.set_app_id(0);
    _config.pid.set_partition_index(0);
    _config.status = partition_status::PS_INACTIVE;
    _primary_states.pc.ballot = 0;
    _create_time_ms = dsn_now_ms();
    _last_config_change_time_ms = _create_time_ms;
    update_last_checkpoint_generate_time();
    _private_log = nullptr;
    get_bool_envs(_app_info.envs, replica_envs::ROCKSDB_ALLOW_INGEST_BEHIND, _allow_ingest_behind);
}

replica::~replica()
{
    close();
    _prepare_list = nullptr;
    LOG_DEBUG_PREFIX("replica destroyed");
}

void replica::on_client_read(dsn::message_ex *request, bool ignore_throttling)
{
    if (!_access_controller->allowed(request, ranger::access_type::kRead)) {
        response_client_read(request, ERR_ACL_DENY);
        return;
    }

    if (_deny_client.read) {
        if (_deny_client.reconfig) {
            // return ERR_INVALID_STATE will trigger client update config immediately
            response_client_read(request, ERR_INVALID_STATE);
            return;
        }
        // Do not reply any message to the peer client to let it timeout, it's OK coz some users
        // may retry immediately when they got a not success code which will make the server side
        // pressure more and more heavy.
        return;
    }

    CHECK_REQUEST_IF_SPLITTING(read);

    if (status() == partition_status::PS_INACTIVE ||
        status() == partition_status::PS_POTENTIAL_SECONDARY) {
        response_client_read(request, ERR_INVALID_STATE);
        return;
    }

    if (!request->is_backup_request()) {
        // only backup request is allowed to read from a stale replica

        if (!ignore_throttling && throttle_read_request(request)) {
            return;
        }

        if (status() != partition_status::PS_PRIMARY) {
            response_client_read(request, ERR_INVALID_STATE);
            return;
        }

        // a small window where the state is not the latest yet
        if (last_committed_decree() < _primary_states.last_prepare_decree_on_new_primary) {
            LOG_ERROR_PREFIX("last_committed_decree({}) < last_prepare_decree_on_new_primary({})",
                             last_committed_decree(),
                             _primary_states.last_prepare_decree_on_new_primary);
            response_client_read(request, ERR_INVALID_STATE);
            return;
        }
    } else {
        if (!ignore_throttling && throttle_backup_request(request)) {
            return;
        }
        METRIC_VAR_INCREMENT(backup_requests);
    }

    CHECK(_app, "");
    auto storage_error = _app->on_request(request);
    // kNotFound is normal, it indicates that the key is not found (including expired)
    // in the storage engine, so just ignore it.
    if (dsn_unlikely(storage_error != rocksdb::Status::kOk &&
                     storage_error != rocksdb::Status::kNotFound)) {
        switch (storage_error) {
        // TODO(yingchun): Now only kCorruption and kIOError are dealt, consider to deal with
        //  more storage engine errors.
        case rocksdb::Status::kCorruption:
            handle_local_failure(ERR_RDB_CORRUPTION);
            break;
        case rocksdb::Status::kIOError:
            handle_local_failure(ERR_DISK_IO_ERROR);
            break;
        default:
            LOG_ERROR_PREFIX("client read encountered an unhandled error: {}", storage_error);
        }
        return;
    }
}

void replica::response_client_read(dsn::message_ex *request, error_code error)
{
    _stub->response_client(get_gpid(), true, request, status(), error);
}

void replica::response_client_write(dsn::message_ex *request, error_code error)
{
    _stub->response_client(get_gpid(), false, request, status(), error);
}

void replica::check_state_completeness()
{
    /* prepare commit durable */
    CHECK_GE(max_prepared_decree(), last_committed_decree());
    CHECK_GE(last_committed_decree(), last_durable_decree());
}

void replica::execute_mutation(mutation_ptr &mu)
{
    LOG_DEBUG_PREFIX(
        "execute mutation {}: request_count = {}", mu->name(), mu->client_requests.size());

    error_code err = ERR_OK;
    decree d = mu->data.header.decree;

    switch (status()) {
    case partition_status::PS_INACTIVE:
        if (_app->last_committed_decree() + 1 == d) {
            err = _app->apply_mutation(mu);
        } else {
            LOG_DEBUG_PREFIX("mutation {} commit to {} skipped, app.last_committed_decree = {}",
                             mu->name(),
                             enum_to_string(status()),
                             _app->last_committed_decree());
        }
        break;
    case partition_status::PS_PRIMARY: {
        ADD_POINT(mu->_tracer);
        check_state_completeness();
        CHECK_EQ(_app->last_committed_decree() + 1, d);
        err = _app->apply_mutation(mu);
    } break;

    case partition_status::PS_SECONDARY:
        if (!_secondary_states.checkpoint_is_running) {
            check_state_completeness();
            CHECK_EQ(_app->last_committed_decree() + 1, d);
            err = _app->apply_mutation(mu);
        } else {
            LOG_DEBUG_PREFIX("mutation {} commit to {} skipped, app.last_committed_decree = {}",
                             mu->name(),
                             enum_to_string(status()),
                             _app->last_committed_decree());

            // make sure private log saves the state
            // catch-up will be done later after checkpoint task is fininished
            CHECK_NOTNULL(_private_log, "");
        }
        break;
    case partition_status::PS_POTENTIAL_SECONDARY:
        if (_potential_secondary_states.learning_status == learner_status::LearningSucceeded ||
            _potential_secondary_states.learning_status ==
                learner_status::LearningWithPrepareTransient) {
            CHECK_EQ(_app->last_committed_decree() + 1, d);
            err = _app->apply_mutation(mu);
        } else {
            LOG_DEBUG_PREFIX("mutation {} commit to {} skipped, app.last_committed_decree = {}",
                             mu->name(),
                             enum_to_string(status()),
                             _app->last_committed_decree());

            // prepare also happens with learner_status::LearningWithPrepare, in this case
            // make sure private log saves the state,
            // catch-up will be done later after the checkpoint task is finished
            CHECK_NOTNULL(_private_log, "");
        }
        break;
    case partition_status::PS_PARTITION_SPLIT:
        if (_split_states.is_caught_up) {
            CHECK_EQ(_app->last_committed_decree() + 1, d);
            err = _app->apply_mutation(mu);
        }
        break;
    case partition_status::PS_ERROR:
        break;
    default:
        CHECK(false, "invalid partition_status, status = {}", enum_to_string(status()));
    }

    LOG_DEBUG_PREFIX("TwoPhaseCommit, mutation {} committed, err = {}", mu->name(), err);

    if (err != ERR_OK) {
        handle_local_failure(err);
    }

    if (status() != partition_status::PS_PRIMARY) {
        return;
    }

    ADD_CUSTOM_POINT(mu->_tracer, "completed");
    auto next = _primary_states.write_queue.next_work(static_cast<int>(max_prepared_decree() - d));

    if (next != nullptr) {
        init_prepare(next, false);
    }
}

mutation_ptr replica::new_mutation(decree decree, dsn::message_ex *original_request)
{
    auto mu = new_mutation(decree);
    mu->original_request = original_request;
    return mu;
}

mutation_ptr replica::new_mutation(decree decree, bool is_blocking)
{
    auto mu = new_mutation(decree);
    mu->is_blocking = is_blocking;
    return mu;
}

mutation_ptr replica::new_mutation(decree decree)
{
    mutation_ptr mu(new mutation());
    mu->data.header.pid = get_gpid();
    mu->data.header.ballot = get_ballot();
    mu->data.header.decree = decree;
    mu->data.header.log_offset = invalid_offset;
    return mu;
}

decree replica::last_applied_decree() const { return _app->last_committed_decree(); }

decree replica::last_flushed_decree() const { return _app->last_flushed_decree(); }

decree replica::last_durable_decree() const { return _app->last_durable_decree(); }

decree replica::last_prepared_decree() const
{
    ballot lastBallot = 0;
    decree start = last_committed_decree();
    while (true) {
        auto mu = _prepare_list->get_mutation_by_decree(start + 1);
        if (mu == nullptr || mu->data.header.ballot < lastBallot || !mu->is_logged())
            break;

        start++;
        lastBallot = mu->data.header.ballot;
    }
    return start;
}

bool replica::verbose_commit_log() const { return _stub->_verbose_commit_log; }

void replica::close()
{
    CHECK_PREFIX_MSG(status() == partition_status::PS_ERROR ||
                         status() == partition_status::PS_INACTIVE ||
                         _disk_migrator->status() == disk_migration_status::IDLE ||
                         _disk_migrator->status() >= disk_migration_status::MOVED,
                     "invalid state(partition_status={}, migration_status={}) when calling "
                     "replica close",
                     enum_to_string(status()),
                     enum_to_string(_disk_migrator->status()));

    uint64_t start_time = dsn_now_ms();

    if (_checkpoint_timer != nullptr) {
        _checkpoint_timer->cancel(true);
        _checkpoint_timer = nullptr;
    }

    _tracker.cancel_outstanding_tasks();

    cleanup_preparing_mutations(true);
    CHECK(_primary_states.is_cleaned(), "primary context is not cleared");

    if (partition_status::PS_INACTIVE == status()) {
        CHECK(_secondary_states.is_cleaned(), "secondary context is not cleared");
        CHECK(_potential_secondary_states.is_cleaned(),
              "potential secondary context is not cleared");
        CHECK(_split_states.is_cleaned(), "partition split context is not cleared");
    }

    // for partition_status::PS_ERROR, context cleanup is done here as they may block
    else {
        CHECK_PREFIX_MSG(_secondary_states.cleanup(true), "secondary context is not cleared");
        CHECK_PREFIX_MSG(_potential_secondary_states.cleanup(true),
                         "potential secondary context is not cleared");
        CHECK_PREFIX_MSG(_split_states.cleanup(true), "partition split context is not cleared");
    }

    if (_private_log != nullptr) {
        _private_log->close();
        _private_log = nullptr;
    }

    if (_app != nullptr) {
        std::unique_ptr<replication_app_base> tmp_app = std::move(_app);
        error_code err = tmp_app->close(false);
        if (err != dsn::ERR_OK) {
            LOG_WARNING_PREFIX("close app failed, err = {}", err);
        }
    }

    if (_disk_migrator->status() == disk_migration_status::MOVED) {
        // this will update disk_migration_status::MOVED->disk_migration_status::CLOSED
        _disk_migrator->update_replica_dir();
    } else if (_disk_migrator->status() == disk_migration_status::CLOSED) {
        _disk_migrator.reset();
    }

    // duplication_impl may have ongoing tasks.
    // release it before release replica.
    _duplication_mgr.reset();

    _backup_mgr.reset();

    _bulk_loader.reset();

    _split_mgr.reset();

    LOG_INFO_PREFIX("replica closed, time_used = {} ms", dsn_now_ms() - start_time);
}

std::string replica::query_manual_compact_state() const
{
    CHECK_PREFIX(_app);
    return _app->query_compact_state();
}

manual_compaction_status::type replica::get_manual_compact_status() const
{
    CHECK_PREFIX(_app);
    return _app->query_compact_status();
}

void replica::on_detect_hotkey(const detect_hotkey_request &req, detect_hotkey_response &resp)
{
    _app->on_detect_hotkey(req, resp);
}

uint32_t replica::query_data_version() const
{
    CHECK_PREFIX(_app);
    return _app->query_data_version();
}

error_code replica::store_app_info(app_info &info, const std::string &path)
{
    replica_app_info new_info((app_info *)&info);
    const auto &info_path =
        path.empty() ? utils::filesystem::path_combine(_dir, replica_app_info::kAppInfo) : path;
    auto err = new_info.store(info_path);
    if (dsn_unlikely(err != ERR_OK)) {
        LOG_ERROR_PREFIX("failed to save app_info to {}, error = {}", info_path, err);
    }
    return err;
}

bool replica::access_controller_allowed(message_ex *msg, const ranger::access_type &ac_type) const
{
    return !_access_controller->is_enable_ranger_acl() || _access_controller->allowed(msg, ac_type);
}

int64_t replica::get_backup_request_count() const { return METRIC_VAR_VALUE(backup_requests); }

void replica::METRIC_FUNC_NAME_SET(dup_pending_mutations)()
{
    METRIC_SET(*_duplication_mgr, dup_pending_mutations);
}

} // namespace replication
} // namespace dsn
