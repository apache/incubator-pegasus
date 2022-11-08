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
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include "duplication/replica_duplicator_manager.h"
#include "duplication/replica_follower.h"
#include "backup/replica_backup_manager.h"
#include "backup/cold_backup_context.h"
#include "bulk_load/replica_bulk_loader.h"
#include "split/replica_split_manager.h"
#include "replica_disk_migrator.h"
#include "runtime/security/access_controller.h"

#include "utils/latency_tracer.h"
#include "common/json_helper.h"
#include "replica/replication_app_base.h"
#include "common/replica_envs.h"
#include "utils/fmt_logging.h"
#include "utils/filesystem.h"
#include "utils/rand.h"
#include "utils/string_conv.h"
#include "utils/strings.h"
#include "runtime/rpc/rpc_message.h"

namespace dsn {
namespace replication {

const std::string replica::kAppInfo = ".app-info";

replica::replica(replica_stub *stub,
                 gpid gpid,
                 const app_info &app,
                 const char *dir,
                 bool need_restore,
                 bool is_duplication_follower)
    : serverlet<replica>("replica"),
      replica_base(gpid, fmt::format("{}@{}", gpid, stub->_primary_address_str), app.app_name),
      _app_info(app),
      _primary_states(
          gpid, stub->options().staleness_for_commit, stub->options().batch_write_disabled),
      _potential_secondary_states(this),
      _cold_backup_running_count(0),
      _cold_backup_max_duration_time_ms(0),
      _cold_backup_max_upload_file_size(0),
      _chkpt_total_size(0),
      _cur_download_size(0),
      _restore_progress(0),
      _restore_status(ERR_OK),
      _duplication_mgr(new replica_duplicator_manager(this)),
      // todo(jiashuo1): app.duplicating need rename
      _is_duplication_master(app.duplicating),
      _is_duplication_follower(is_duplication_follower),
      _backup_mgr(new replica_backup_manager(this))
{
    CHECK(!_app_info.app_type.empty(), "");
    CHECK_NOTNULL(stub, "");
    _stub = stub;
    _dir = dir;
    _options = &stub->options();
    init_state();
    _config.pid = gpid;
    _bulk_loader = make_unique<replica_bulk_loader>(this);
    _split_mgr = make_unique<replica_split_manager>(this);
    _disk_migrator = make_unique<replica_disk_migrator>(this);
    _replica_follower = make_unique<replica_follower>(this);

    std::string counter_str = fmt::format("private.log.size(MB)@{}", gpid);
    _counter_private_log_size.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_NUMBER, counter_str.c_str());

    counter_str = fmt::format("recent.write.throttling.delay.count@{}", gpid);
    _counter_recent_write_throttling_delay_count.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_VOLATILE_NUMBER, counter_str.c_str());

    counter_str = fmt::format("recent.write.throttling.reject.count@{}", gpid);
    _counter_recent_write_throttling_reject_count.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_VOLATILE_NUMBER, counter_str.c_str());

    counter_str = fmt::format("recent.read.throttling.delay.count@{}", gpid);
    _counter_recent_read_throttling_delay_count.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_VOLATILE_NUMBER, counter_str.c_str());

    counter_str = fmt::format("recent.read.throttling.reject.count@{}", gpid);
    _counter_recent_read_throttling_reject_count.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_VOLATILE_NUMBER, counter_str.c_str());

    counter_str =
        fmt::format("recent.backup.request.throttling.delay.count@{}", _app_info.app_name);
    _counter_recent_backup_request_throttling_delay_count.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_VOLATILE_NUMBER, counter_str.c_str());

    counter_str =
        fmt::format("recent.backup.request.throttling.reject.count@{}", _app_info.app_name);
    _counter_recent_backup_request_throttling_reject_count.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_VOLATILE_NUMBER, counter_str.c_str());

    counter_str = fmt::format("dup.disabled_non_idempotent_write_count@{}", _app_info.app_name);
    _counter_dup_disabled_non_idempotent_write_count.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_VOLATILE_NUMBER, counter_str.c_str());

    counter_str = fmt::format("recent.read.splitting.reject.count@{}", gpid);
    _counter_recent_read_splitting_reject_count.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_VOLATILE_NUMBER, counter_str.c_str());

    counter_str = fmt::format("recent.write.splitting.reject.count@{}", gpid);
    _counter_recent_write_splitting_reject_count.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_VOLATILE_NUMBER, counter_str.c_str());

    counter_str = fmt::format("recent.write.bulk.load.ingestion.reject.count@{}", gpid);
    _counter_recent_write_bulk_load_ingestion_reject_count.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_VOLATILE_NUMBER, counter_str.c_str());

    // init table level latency perf counters
    init_table_level_latency_counters();

    counter_str = fmt::format("backup_request_qps@{}", _app_info.app_name);
    _counter_backup_request_qps.init_app_counter(
        "eon.replica", counter_str.c_str(), COUNTER_TYPE_RATE, counter_str.c_str());

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
    uint64_t max_interval_ms = _options->checkpoint_max_interval_hours * 3600000UL;
    // use random trigger time to avoid flush peek
    _next_checkpoint_interval_trigger_time_ms =
        _last_checkpoint_generate_time_ms + rand::next_u64(max_interval_ms / 2, max_interval_ms);
}

//            //
// Statistics //
//            //

void replica::update_commit_qps(int count)
{
    _stub->_counter_replicas_commit_qps->add((uint64_t)count);
}

void replica::init_state()
{
    _inactive_is_transient = false;
    _is_initializing = false;
    _prepare_list = dsn::make_unique<prepare_list>(
        this,
        0,
        _options->max_mutation_count_in_prepare_list,
        std::bind(&replica::execute_mutation, this, std::placeholders::_1));

    _config.ballot = 0;
    _config.pid.set_app_id(0);
    _config.pid.set_partition_index(0);
    _config.status = partition_status::PS_INACTIVE;
    _primary_states.membership.ballot = 0;
    _create_time_ms = dsn_now_ms();
    _last_config_change_time_ms = _create_time_ms;
    update_last_checkpoint_generate_time();
    _private_log = nullptr;
    init_disk_tag();
    get_bool_envs(_app_info.envs, replica_envs::ROCKSDB_ALLOW_INGEST_BEHIND, _allow_ingest_behind);
}

replica::~replica(void)
{
    close();
    _prepare_list = nullptr;
    LOG_DEBUG("%s: replica destroyed", name());
}

void replica::on_client_read(dsn::message_ex *request, bool ignore_throttling)
{
    if (!_access_controller->allowed(request)) {
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

    CHECK_REQUEST_IF_SPLITTING(read)

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
            LOG_ERROR_PREFIX("last_committed_decree(%" PRId64
                             ") < last_prepare_decree_on_new_primary(%" PRId64 ")",
                             last_committed_decree(),
                             _primary_states.last_prepare_decree_on_new_primary);
            response_client_read(request, ERR_INVALID_STATE);
            return;
        }
    } else {
        if (!ignore_throttling && throttle_backup_request(request)) {
            return;
        }
        _counter_backup_request_qps->increment();
    }

    uint64_t start_time_ns = dsn_now_ns();
    CHECK(_app, "");
    _app->on_request(request);

    // If the corresponding perf counter exist, count the duration of this operation.
    // rpc code of request is already checked in message_ex::rpc_code, so it will always be legal
    if (_counters_table_level_latency[request->rpc_code()] != nullptr) {
        _counters_table_level_latency[request->rpc_code()]->set(dsn_now_ns() - start_time_ns);
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
    LOG_DEBUG("%s: execute mutation %s: request_count = %u",
              name(),
              mu->name(),
              static_cast<int>(mu->client_requests.size()));

    error_code err = ERR_OK;
    decree d = mu->data.header.decree;

    switch (status()) {
    case partition_status::PS_INACTIVE:
        if (_app->last_committed_decree() + 1 == d) {
            err = _app->apply_mutation(mu);
        } else {
            LOG_DEBUG("%s: mutation %s commit to %s skipped, app.last_committed_decree = %" PRId64,
                      name(),
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
            LOG_DEBUG("%s: mutation %s commit to %s skipped, app.last_committed_decree = %" PRId64,
                      name(),
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
            LOG_DEBUG("%s: mutation %s commit to %s skipped, app.last_committed_decree = %" PRId64,
                      name(),
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

    LOG_DEBUG(
        "TwoPhaseCommit, %s: mutation %s committed, err = %s", name(), mu->name(), err.to_string());

    if (err != ERR_OK) {
        handle_local_failure(err);
    }

    if (status() == partition_status::PS_PRIMARY) {
        ADD_CUSTOM_POINT(mu->_tracer, "completed");
        mutation_ptr next = _primary_states.write_queue.check_possible_work(
            static_cast<int>(_prepare_list->max_decree() - d));

        if (next) {
            init_prepare(next, false);
        }
    }

    // update table level latency perf-counters for primary partition
    if (partition_status::PS_PRIMARY == status()) {
        uint64_t now_ns = dsn_now_ns();
        for (auto update : mu->data.updates) {
            // If the corresponding perf counter exist, count the duration of this operation.
            // code in update will always be legal
            if (_counters_table_level_latency[update.code] != nullptr) {
                _counters_table_level_latency[update.code]->set(now_ns - update.start_time_ns);
            }
        }
    }
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

decree replica::last_durable_decree() const { return _app->last_durable_decree(); }

decree replica::last_flushed_decree() const { return _app->last_flushed_decree(); }

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
            LOG_WARNING("%s: close app failed, err = %s", name(), err.to_string());
        }
    }

    if (_disk_migrator->status() == disk_migration_status::MOVED) {
        // this will update disk_migration_status::MOVED->disk_migration_status::CLOSED
        _disk_migrator->update_replica_dir();
    } else if (_disk_migrator->status() == disk_migration_status::CLOSED) {
        _disk_migrator.reset();
    }

    _counter_private_log_size.clear();

    // duplication_impl may have ongoing tasks.
    // release it before release replica.
    _duplication_mgr.reset();

    _backup_mgr.reset();

    _bulk_loader.reset();

    _split_mgr.reset();

    LOG_INFO("%s: replica closed, time_used = %" PRIu64 "ms", name(), dsn_now_ms() - start_time);
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

// Replicas on the server which serves for the same table will share the same perf-counter.
// For example counter `table.level.RPC_RRDB_RRDB_MULTI_PUT.latency(ns)@test_table` is shared by
// all the replicas for `test_table`.
void replica::init_table_level_latency_counters()
{
    int max_task_code = task_code::max();
    _counters_table_level_latency.resize(max_task_code + 1);

    for (int code = 0; code <= max_task_code; code++) {
        _counters_table_level_latency[code] = nullptr;
        if (get_storage_rpc_req_codes().find(task_code(code)) !=
            get_storage_rpc_req_codes().end()) {
            std::string counter_str = fmt::format(
                "table.level.{}.latency(ns)@{}", task_code(code).to_string(), _app_info.app_name);
            _counters_table_level_latency[code] =
                dsn::perf_counters::instance()
                    .get_app_counter("eon.replica",
                                     counter_str.c_str(),
                                     COUNTER_TYPE_NUMBER_PERCENTILES,
                                     counter_str.c_str(),
                                     true)
                    .get();
        }
    }
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

void replica::init_disk_tag()
{
    dsn::error_code err = _stub->_fs_manager.get_disk_tag(dir(), _disk_tag);
    if (dsn::ERR_OK != err) {
        LOG_ERROR_PREFIX("get disk tag of {} failed: {}, init it to empty ", dir(), err);
    }
}

error_code replica::store_app_info(app_info &info, const std::string &path)
{
    replica_app_info new_info((app_info *)&info);
    const auto &info_path = path.empty() ? utils::filesystem::path_combine(_dir, kAppInfo) : path;
    auto err = new_info.store(info_path);
    if (dsn_unlikely(err != ERR_OK)) {
        LOG_ERROR_PREFIX("failed to save app_info to {}, error = {}", info_path, err);
    }
    return err;
}

} // namespace replication
} // namespace dsn
