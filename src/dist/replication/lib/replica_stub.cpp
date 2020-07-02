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
 *     replica container - replica stub
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "replica.h"
#include "replica_stub.h"
#include "mutation_log.h"
#include "mutation.h"
#include "bulk_load/replica_bulk_loader.h"
#include "duplication/duplication_sync_timer.h"
#include "dist/replication/lib/backup/replica_backup_manager.h"

#include <dsn/cpp/json_helper.h>
#include <dsn/utility/filesystem.h>
#include <dsn/utility/rand.h>
#include <dsn/utility/string_conv.h>
#include <dsn/tool-api/command_manager.h>
#include <dsn/dist/replication/replication_app_base.h>
#include <vector>
#include <deque>
#include <dsn/dist/fmt_logging.h>
#ifdef DSN_ENABLE_GPERF
#include <gperftools/malloc_extension.h>
#endif
#include <dsn/utility/fail_point.h>
#include <dsn/dist/remote_command.h>

namespace dsn {
namespace replication {

bool replica_stub::s_not_exit_on_log_failure = false;

replica_stub::replica_stub(replica_state_subscriber subscriber /*= nullptr*/,
                           bool is_long_subscriber /* = true*/)
    : serverlet("replica_stub"),
      _kill_partition_command(nullptr),
      _deny_client_command(nullptr),
      _verbose_client_log_command(nullptr),
      _verbose_commit_log_command(nullptr),
      _trigger_chkpt_command(nullptr),
      _query_compact_command(nullptr),
      _query_app_envs_command(nullptr),
      _useless_dir_reserve_seconds_command(nullptr),
      _max_concurrent_bulk_load_downloading_count_command(nullptr),
      _deny_client(false),
      _verbose_client_log(false),
      _verbose_commit_log(false),
      _gc_disk_error_replica_interval_seconds(3600),
      _gc_disk_garbage_replica_interval_seconds(3600),
      _release_tcmalloc_memory(false),
      _mem_release_max_reserved_mem_percentage(10),
      _max_concurrent_bulk_load_downloading_count(5),
      _learn_app_concurrent_count(0),
      _fs_manager(false),
      _bulk_load_downloading_count(0)
{
#ifdef DSN_ENABLE_GPERF
    _release_tcmalloc_memory_command = nullptr;
    _max_reserved_memory_percentage_command = nullptr;
#endif
    _replica_state_subscriber = subscriber;
    _is_long_subscriber = is_long_subscriber;
    _failure_detector = nullptr;
    _state = NS_Disconnected;
    _log = nullptr;
    _primary_address_str[0] = '\0';
    install_perf_counters();

    _max_allowed_write_size = dsn_config_get_value_uint64("replication",
                                                          "max_allowed_write_size",
                                                          1 << 20,
                                                          "write operation exceed this "
                                                          "threshold will be logged and reject, "
                                                          "default is 1MB, 0 means no check");
}

replica_stub::~replica_stub(void) { close(); }

void replica_stub::install_perf_counters()
{
    _counter_replicas_count.init_app_counter(
        "eon.replica_stub", "replica(Count)", COUNTER_TYPE_NUMBER, "# in replica_stub._replicas");
    _counter_replicas_opening_count.init_app_counter("eon.replica_stub",
                                                     "opening.replica(Count)",
                                                     COUNTER_TYPE_NUMBER,
                                                     "# in replica_stub._opening_replicas");
    _counter_replicas_closing_count.init_app_counter("eon.replica_stub",
                                                     "closing.replica(Count)",
                                                     COUNTER_TYPE_NUMBER,
                                                     "# in replica_stub._closing_replicas");
    _counter_replicas_commit_qps.init_app_counter("eon.replica_stub",
                                                  "replicas.commit.qps",
                                                  COUNTER_TYPE_RATE,
                                                  "server-level commit throughput");
    _counter_replicas_learning_count.init_app_counter("eon.replica_stub",
                                                      "replicas.learning.count",
                                                      COUNTER_TYPE_NUMBER,
                                                      "current learning count");
    _counter_replicas_learning_max_duration_time_ms.init_app_counter(
        "eon.replica_stub",
        "replicas.learning.max.duration.time(ms)",
        COUNTER_TYPE_NUMBER,
        "current learning max duration time(ms)");
    _counter_replicas_learning_max_copy_file_size.init_app_counter(
        "eon.replica_stub",
        "replicas.learning.max.copy.file.size",
        COUNTER_TYPE_NUMBER,
        "current learning max copy file size");
    _counter_replicas_learning_recent_start_count.init_app_counter(
        "eon.replica_stub",
        "replicas.learning.recent.start.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "current learning start count in the recent period");
    _counter_replicas_learning_recent_round_start_count.init_app_counter(
        "eon.replica_stub",
        "replicas.learning.recent.round.start.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "learning round start count in the recent period");
    _counter_replicas_learning_recent_copy_file_count.init_app_counter(
        "eon.replica_stub",
        "replicas.learning.recent.copy.file.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "learning copy file count in the recent period");
    _counter_replicas_learning_recent_copy_file_size.init_app_counter(
        "eon.replica_stub",
        "replicas.learning.recent.copy.file.size",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "learning copy file size in the recent period");
    _counter_replicas_learning_recent_copy_buffer_size.init_app_counter(
        "eon.replica_stub",
        "replicas.learning.recent.copy.buffer.size",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "learning copy buffer size in the recent period");
    _counter_replicas_learning_recent_learn_cache_count.init_app_counter(
        "eon.replica_stub",
        "replicas.learning.recent.learn.cache.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "learning LT_CACHE count in the recent period");
    _counter_replicas_learning_recent_learn_app_count.init_app_counter(
        "eon.replica_stub",
        "replicas.learning.recent.learn.app.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "learning LT_APP count in the recent period");
    _counter_replicas_learning_recent_learn_log_count.init_app_counter(
        "eon.replica_stub",
        "replicas.learning.recent.learn.log.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "learning LT_LOG count in the recent period");
    _counter_replicas_learning_recent_learn_reset_count.init_app_counter(
        "eon.replica_stub",
        "replicas.learning.recent.learn.reset.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "learning reset count in the recent period"
        "for the reason of resp.last_committed_decree < _app->last_committed_decree()");
    _counter_replicas_learning_recent_learn_fail_count.init_app_counter(
        "eon.replica_stub",
        "replicas.learning.recent.learn.fail.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "learning fail count in the recent period");
    _counter_replicas_learning_recent_learn_succ_count.init_app_counter(
        "eon.replica_stub",
        "replicas.learning.recent.learn.succ.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "learning succeed count in the recent period");

    _counter_replicas_recent_prepare_fail_count.init_app_counter(
        "eon.replica_stub",
        "replicas.recent.prepare.fail.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "prepare fail count in the recent period");
    _counter_replicas_recent_replica_move_error_count.init_app_counter(
        "eon.replica_stub",
        "replicas.recent.replica.move.error.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "replica move to error count in the recent period");
    _counter_replicas_recent_replica_move_garbage_count.init_app_counter(
        "eon.replica_stub",
        "replicas.recent.replica.move.garbage.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "replica move to garbage count in the recent period");
    _counter_replicas_recent_replica_remove_dir_count.init_app_counter(
        "eon.replica_stub",
        "replicas.recent.replica.remove.dir.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "replica directory remove count in the recent period");
    _counter_replicas_error_replica_dir_count.init_app_counter("eon.replica_stub",
                                                               "replicas.error.replica.dir.count",
                                                               COUNTER_TYPE_NUMBER,
                                                               "error replica directory count");
    _counter_replicas_garbage_replica_dir_count.init_app_counter(
        "eon.replica_stub",
        "replicas.garbage.replica.dir.count",
        COUNTER_TYPE_NUMBER,
        "garbage replica directory count");

    _counter_replicas_recent_group_check_fail_count.init_app_counter(
        "eon.replica_stub",
        "replicas.recent.group.check.fail.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "group check fail count in the recent period");

    _counter_shared_log_size.init_app_counter(
        "eon.replica_stub", "shared.log.size(MB)", COUNTER_TYPE_NUMBER, "shared log size(MB)");
    _counter_shared_log_recent_write_size.init_app_counter(
        "eon.replica_stub",
        "shared.log.recent.write.size",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "shared log write size in the recent period");
    _counter_recent_trigger_emergency_checkpoint_count.init_app_counter(
        "eon.replica_stub",
        "recent.trigger.emergency.checkpoint.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "trigger emergency checkpoint count in the recent period");

    // <- Duplication Metrics ->

    _counter_dup_confirmed_rate.init_app_counter("eon.replica_stub",
                                                 "dup.confirmed_rate",
                                                 COUNTER_TYPE_RATE,
                                                 "increasing rate of confirmed mutations");
    _counter_dup_pending_mutations_count.init_app_counter(
        "eon.replica_stub",
        "dup.pending_mutations_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "number of mutations pending for duplication");

    // <- Cold Backup Metrics ->

    _counter_cold_backup_running_count.init_app_counter("eon.replica_stub",
                                                        "cold.backup.running.count",
                                                        COUNTER_TYPE_NUMBER,
                                                        "current cold backup count");
    _counter_cold_backup_recent_start_count.init_app_counter(
        "eon.replica_stub",
        "cold.backup.recent.start.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "current cold backup start count in the recent period");
    _counter_cold_backup_recent_succ_count.init_app_counter(
        "eon.replica_stub",
        "cold.backup.recent.succ.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "current cold backup succeed count in the recent period");
    _counter_cold_backup_recent_fail_count.init_app_counter(
        "eon.replica_stub",
        "cold.backup.recent.fail.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "current cold backup fail count in the recent period");
    _counter_cold_backup_recent_cancel_count.init_app_counter(
        "eon.replica_stub",
        "cold.backup.recent.cancel.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "current cold backup cancel count in the recent period");
    _counter_cold_backup_recent_pause_count.init_app_counter(
        "eon.replica_stub",
        "cold.backup.recent.pause.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "current cold backup pause count in the recent period");
    _counter_cold_backup_recent_upload_file_succ_count.init_app_counter(
        "eon.replica_stub",
        "cold.backup.recent.upload.file.succ.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "current cold backup upload file succeed count in the recent period");
    _counter_cold_backup_recent_upload_file_fail_count.init_app_counter(
        "eon.replica_stub",
        "cold.backup.recent.upload.file.fail.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "current cold backup upload file failed count in the recent period");
    _counter_cold_backup_recent_upload_file_size.init_app_counter(
        "eon.replica_stub",
        "cold.backup.recent.upload.file.size",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "current cold backup upload file size in the recent perriod");
    _counter_cold_backup_max_duration_time_ms.init_app_counter(
        "eon.replica_stub",
        "cold.backup.max.duration.time.ms",
        COUNTER_TYPE_NUMBER,
        "current cold backup max duration time");
    _counter_cold_backup_max_upload_file_size.init_app_counter(
        "eon.replica_stub",
        "cold.backup.max.upload.file.size",
        COUNTER_TYPE_NUMBER,
        "current cold backup max upload file size");

    _counter_recent_read_fail_count.init_app_counter("eon.replica_stub",
                                                     "recent.read.fail.count",
                                                     COUNTER_TYPE_VOLATILE_NUMBER,
                                                     "read fail count in the recent period");
    _counter_recent_write_fail_count.init_app_counter("eon.replica_stub",
                                                      "recent.write.fail.count",
                                                      COUNTER_TYPE_VOLATILE_NUMBER,
                                                      "write fail count in the recent period");
    _counter_recent_read_busy_count.init_app_counter("eon.replica_stub",
                                                     "recent.read.busy.count",
                                                     COUNTER_TYPE_VOLATILE_NUMBER,
                                                     "read busy count in the recent period");
    _counter_recent_write_busy_count.init_app_counter("eon.replica_stub",
                                                      "recent.write.busy.count",
                                                      COUNTER_TYPE_VOLATILE_NUMBER,
                                                      "write busy count in the recent period");

    _counter_recent_write_size_exceed_threshold_count.init_app_counter(
        "eon.replica_stub",
        "recent_write_size_exceed_threshold_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "write size exceed threshold count in the recent period");

#ifdef DSN_ENABLE_GPERF
    _counter_tcmalloc_release_memory_size.init_app_counter("eon.replica_stub",
                                                           "tcmalloc.release.memory.size",
                                                           COUNTER_TYPE_NUMBER,
                                                           "current tcmalloc release memory size");
#endif
}

void replica_stub::initialize(bool clear /* = false*/)
{
    replication_options opts;
    opts.initialize();
    initialize(opts, clear);
}

void replica_stub::initialize(const replication_options &opts, bool clear /* = false*/)
{
    _primary_address = dsn_primary_address();
    strcpy(_primary_address_str, _primary_address.to_string());
    ddebug("primary_address = %s", _primary_address_str);

    set_options(opts);
    std::ostringstream oss;
    for (int i = 0; i < _options.meta_servers.size(); ++i) {
        if (i != 0)
            oss << ",";
        oss << _options.meta_servers[i].to_string();
    }
    ddebug("meta_servers = %s", oss.str().c_str());

    _deny_client = _options.deny_client_on_start;
    _verbose_client_log = _options.verbose_client_log_on_start;
    _verbose_commit_log = _options.verbose_commit_log_on_start;
    _gc_disk_error_replica_interval_seconds = _options.gc_disk_error_replica_interval_seconds;
    _gc_disk_garbage_replica_interval_seconds = _options.gc_disk_garbage_replica_interval_seconds;
    _release_tcmalloc_memory = _options.mem_release_enabled;
    _mem_release_max_reserved_mem_percentage = _options.mem_release_max_reserved_mem_percentage;
    _max_concurrent_bulk_load_downloading_count =
        _options.max_concurrent_bulk_load_downloading_count;

    // clear dirs if need
    if (clear) {
        if (!dsn::utils::filesystem::remove_path(_options.slog_dir)) {
            dassert(false, "Fail to remove %s.", _options.slog_dir.c_str());
        }
        for (auto &dir : _options.data_dirs) {
            if (!dsn::utils::filesystem::remove_path(dir)) {
                dassert(false, "Fail to remove %s.", dir.c_str());
            }
        }
    }

    // init dirs
    if (!dsn::utils::filesystem::create_directory(_options.slog_dir)) {
        dassert(false, "Fail to create directory %s.", _options.slog_dir.c_str());
    }
    std::string cdir;
    if (!dsn::utils::filesystem::get_absolute_path(_options.slog_dir, cdir)) {
        dassert(false, "Fail to get absolute path from %s.", _options.slog_dir.c_str());
    }
    _options.slog_dir = cdir;
    int count = 0;
    for (auto &dir : _options.data_dirs) {
        if (!dsn::utils::filesystem::create_directory(dir)) {
            dassert(false, "Fail to create directory %s.", dir.c_str());
        }
        std::string cdir;
        if (!dsn::utils::filesystem::get_absolute_path(dir, cdir)) {
            dassert(false, "Fail to get absolute path from %s.", dir.c_str());
        }
        dir = cdir;
        ddebug("data_dirs[%d] = %s", count, dir.c_str());
        count++;
    }

    {
        dsn::error_code err;
        err = _fs_manager.initialize(_options.data_dirs, _options.data_dir_tags, false);
        dassert(err == dsn::ERR_OK, "initialize fs manager failed, err(%s)", err.to_string());
    }

    _log = new mutation_log_shared(_options.slog_dir,
                                   _options.log_shared_file_size_mb,
                                   _options.log_shared_force_flush,
                                   &_counter_shared_log_recent_write_size);
    ddebug("slog_dir = %s", _options.slog_dir.c_str());

    // init rps
    ddebug("start to load replicas");

    std::vector<std::string> dir_list;
    for (auto &dir : _options.data_dirs) {
        std::vector<std::string> tmp_list;
        if (!dsn::utils::filesystem::get_subdirectories(dir, tmp_list, false)) {
            dassert(false, "Fail to get subdirectories in %s.", dir.c_str());
        }
        dir_list.insert(dir_list.end(), tmp_list.begin(), tmp_list.end());
    }

    replicas rps;
    utils::ex_lock rps_lock;
    std::deque<task_ptr> load_tasks;
    uint64_t start_time = dsn_now_ms();
    for (auto &dir : dir_list) {
        if (dir.length() >= 4 &&
            (dir.substr(dir.length() - 4) == ".err" || dir.substr(dir.length() - 4) == ".gar" ||
             dir.substr(dir.length() - 4) == ".bak")) {
            ddebug("ignore dir %s", dir.c_str());
            continue;
        }

        load_tasks.push_back(tasking::create_task(
            LPC_REPLICATION_INIT_LOAD,
            &_tracker,
            [this, dir, &rps, &rps_lock] {
                ddebug("process dir %s", dir.c_str());

                auto r = replica::load(this, dir.c_str());
                if (r != nullptr) {
                    ddebug("%s@%s: load replica '%s' success, <durable, commit> = <%" PRId64
                           ", %" PRId64 ">, last_prepared_decree = %" PRId64,
                           r->get_gpid().to_string(),
                           dsn_primary_address().to_string(),
                           dir.c_str(),
                           r->last_durable_decree(),
                           r->last_committed_decree(),
                           r->last_prepared_decree());

                    utils::auto_lock<utils::ex_lock> l(rps_lock);

                    if (rps.find(r->get_gpid()) != rps.end()) {
                        dassert(false,
                                "conflict replica dir: %s <--> %s",
                                r->dir().c_str(),
                                rps[r->get_gpid()]->dir().c_str());
                    }

                    rps[r->get_gpid()] = r;
                }
            },
            load_tasks.size()));
        load_tasks.back()->enqueue();
    }
    for (auto &tsk : load_tasks) {
        tsk->wait();
    }
    uint64_t finish_time = dsn_now_ms();

    dir_list.clear();
    load_tasks.clear();
    ddebug("load replicas succeed, replica_count = %d, time_used = %" PRIu64 " ms",
           static_cast<int>(rps.size()),
           finish_time - start_time);

    // init shared prepare log
    ddebug("start to replay shared log");

    std::map<gpid, decree> replay_condition;
    for (auto it = rps.begin(); it != rps.end(); ++it) {
        replay_condition[it->first] = it->second->last_committed_decree();
    }

    start_time = dsn_now_ms();
    error_code err = _log->open(
        [&rps](int log_length, mutation_ptr &mu) {
            auto it = rps.find(mu->data.header.pid);
            if (it != rps.end()) {
                return it->second->replay_mutation(mu, false);
            } else {
                return false;
            }
        },
        [this](error_code err) { this->handle_log_failure(err); },
        replay_condition);
    finish_time = dsn_now_ms();

    if (err == ERR_OK) {
        ddebug("replay shared log succeed, time_used = %" PRIu64 " ms", finish_time - start_time);
    } else {
        derror("replay shared log failed, err = %s, time_used = %" PRIu64 " ms, clear all logs ...",
               err.to_string(),
               finish_time - start_time);

        // we must delete or update meta server the error for all replicas
        // before we fix the logs
        // otherwise, the next process restart may consider the replicas'
        // state complete

        // delete all replicas
        // TODO: checkpoint latest state and update on meta server so learning is cheaper
        for (auto it = rps.begin(); it != rps.end(); ++it) {
            it->second->close();
            // move to '.err' directory
            const char *dir = it->second->dir().c_str();
            char rename_dir[1024];
            sprintf(rename_dir, "%s.%" PRIu64 ".err", dir, dsn_now_us());
            bool ret = dsn::utils::filesystem::rename_path(dir, rename_dir);
            dassert(ret, "init_replica: failed to move directory '%s' to '%s'", dir, rename_dir);
            dwarn("init_replica: {replica_dir_op} succeed to move directory '%s' to '%s'",
                  dir,
                  rename_dir);
            _counter_replicas_recent_replica_move_error_count->increment();
        }
        rps.clear();

        // restart log service
        _log->close();
        _log = nullptr;
        if (!utils::filesystem::remove_path(_options.slog_dir)) {
            dassert(false, "remove directory %s failed", _options.slog_dir.c_str());
        }
        _log = new mutation_log_shared(_options.slog_dir,
                                       _options.log_shared_file_size_mb,
                                       _options.log_shared_force_flush,
                                       &_counter_shared_log_recent_write_size);
        auto lerr = _log->open(nullptr, [this](error_code err) { this->handle_log_failure(err); });
        dassert(lerr == ERR_OK, "restart log service must succeed");
    }

    bool is_log_complete = true;
    for (auto it = rps.begin(); it != rps.end(); ++it) {
        auto err = it->second->background_sync_checkpoint();
        dassert(err == ERR_OK, "sync checkpoint failed, err = %s", err.to_string());

        it->second->reset_prepare_list_after_replay();

        decree smax = _log->max_decree(it->first);
        decree pmax = invalid_decree;
        decree pmax_commit = invalid_decree;
        if (it->second->private_log()) {
            pmax = it->second->private_log()->max_decree(it->first);
            pmax_commit = it->second->private_log()->max_commit_on_disk();

            // possible when shared log is restarted
            if (smax == 0) {
                _log->update_max_decree(it->first, pmax);
                smax = pmax;
            }

            else if (err == ERR_OK && pmax < smax) {
                it->second->private_log()->flush();
                pmax = it->second->private_log()->max_decree(it->first);
            }
        }

        ddebug("%s: load replica done, err = %s, durable = %" PRId64 ", committed = %" PRId64 ", "
               "prepared = %" PRId64 ", ballot = %" PRId64 ", "
               "valid_offset_in_plog = %" PRId64 ", max_decree_in_plog = %" PRId64
               ", max_commit_on_disk_in_plog = %" PRId64 ", "
               "valid_offset_in_slog = %" PRId64 ", max_decree_in_slog = %" PRId64 "",
               it->second->name(),
               err.to_string(),
               it->second->last_durable_decree(),
               it->second->last_committed_decree(),
               it->second->max_prepared_decree(),
               it->second->get_ballot(),
               it->second->get_app()->init_info().init_offset_in_private_log,
               pmax,
               pmax_commit,
               it->second->get_app()->init_info().init_offset_in_shared_log,
               smax);

        if (err == ERR_OK) {
            if (smax != pmax) {
                derror("%s: some shared log state must be lost, smax(%" PRId64 ") vs pmax(%" PRId64
                       ")",
                       it->second->name(),
                       smax,
                       pmax);
                is_log_complete = false;
            } else {
                // just leave inactive_state_transient as its old value
            }
        } else {
            it->second->set_inactive_state_transient(false);
        }
    }

    // we will mark all replicas inactive not transient unless all logs are complete
    if (!is_log_complete) {
        derror("logs are not complete for some replicas, which means that shared log is truncated, "
               "mark all replicas as inactive");
        for (auto it = rps.begin(); it != rps.end(); ++it) {
            it->second->set_inactive_state_transient(false);
        }
    }

    // gc
    if (false == _options.gc_disabled) {
        _gc_timer_task = tasking::enqueue_timer(
            LPC_GARBAGE_COLLECT_LOGS_AND_REPLICAS,
            &_tracker,
            [this] { on_gc(); },
            std::chrono::milliseconds(_options.gc_interval_ms),
            0,
            std::chrono::milliseconds(rand::next_u32(0, _options.gc_interval_ms)));
    }

    // disk stat
    if (false == _options.disk_stat_disabled) {
        _disk_stat_timer_task = ::dsn::tasking::enqueue_timer(
            LPC_DISK_STAT,
            &_tracker,
            [this]() { on_disk_stat(); },
            std::chrono::seconds(_options.disk_stat_interval_seconds),
            0,
            std::chrono::seconds(_options.disk_stat_interval_seconds));
    }

    // attach rps
    _replicas = std::move(rps);
    _counter_replicas_count->add((uint64_t)_replicas.size());
    for (const auto &kv : _replicas) {
        _fs_manager.add_replica(kv.first, kv.second->dir());
    }

    _nfs = std::move(dsn::nfs_node::create());
    _nfs->start();

    dist::cmd::register_remote_command_rpc();

    if (_options.delay_for_fd_timeout_on_start) {
        uint64_t now_time_ms = dsn_now_ms();
        uint64_t delay_time_ms =
            (_options.fd_grace_seconds + 3) * 1000; // for more 3 seconds than grace seconds
        if (now_time_ms < dsn::utils::process_start_millis() + delay_time_ms) {
            uint64_t delay = dsn::utils::process_start_millis() + delay_time_ms - now_time_ms;
            ddebug("delay for %" PRIu64 "ms to make failure detector timeout", delay);
            tasking::enqueue(LPC_REPLICA_SERVER_DELAY_START,
                             &_tracker,
                             [this]() { this->initialize_start(); },
                             0,
                             std::chrono::milliseconds(delay));
        } else {
            initialize_start();
        }
    } else {
        initialize_start();
    }
}

void replica_stub::initialize_start()
{
    // start timer for configuration sync
    if (!_options.config_sync_disabled) {
        _config_sync_timer_task =
            tasking::enqueue_timer(LPC_QUERY_CONFIGURATION_ALL,
                                   &_tracker,
                                   [this]() {
                                       zauto_lock l(_state_lock);
                                       this->query_configuration_by_node();
                                   },
                                   std::chrono::milliseconds(_options.config_sync_interval_ms),
                                   0,
                                   std::chrono::milliseconds(_options.config_sync_interval_ms));
    }

#ifdef DSN_ENABLE_GPERF
    _mem_release_timer_task =
        tasking::enqueue_timer(LPC_MEM_RELEASE,
                               &_tracker,
                               std::bind(&replica_stub::gc_tcmalloc_memory, this),
                               std::chrono::milliseconds(_options.mem_release_check_interval_ms),
                               0,
                               std::chrono::milliseconds(_options.mem_release_check_interval_ms));
#endif

    if (_options.duplication_enabled) {
        _duplication_sync_timer = dsn::make_unique<duplication_sync_timer>(this);
        _duplication_sync_timer->start();
    }

    // init liveness monitor
    dassert(NS_Disconnected == _state, "");
    if (_options.fd_disabled == false) {
        _failure_detector = new ::dsn::dist::slave_failure_detector_with_multimaster(
            _options.meta_servers,
            [this]() { this->on_meta_server_disconnected(); },
            [this]() { this->on_meta_server_connected(); });

        auto err = _failure_detector->start(_options.fd_check_interval_seconds,
                                            _options.fd_beacon_interval_seconds,
                                            _options.fd_lease_seconds,
                                            _options.fd_grace_seconds);
        dassert(err == ERR_OK, "FD start failed, err = %s", err.to_string());

        _failure_detector->register_master(_failure_detector->current_server_contact());
    } else {
        _state = NS_Connected;
    }
}

dsn::error_code replica_stub::on_kill_replica(gpid id)
{
    ddebug("kill replica: gpid = %s", id.to_string());
    if (id.get_app_id() == -1 || id.get_partition_index() == -1) {
        replicas rs;
        {
            zauto_read_lock l(_replicas_lock);
            rs = _replicas;
        }
        for (auto it = rs.begin(); it != rs.end(); ++it) {
            replica_ptr &r = it->second;
            if (id.get_app_id() == -1 || id.get_app_id() == r->get_gpid().get_app_id())
                r->inject_error(ERR_INJECTED);
        }
        return ERR_OK;
    } else {
        error_code err = ERR_INVALID_PARAMETERS;
        replica_ptr r = get_replica(id);
        if (r == nullptr) {
            err = ERR_OBJECT_NOT_FOUND;
        } else {
            r->inject_error(ERR_INJECTED);
            err = ERR_OK;
        }
        return err;
    }
}

replica_ptr replica_stub::get_replica(gpid id)
{
    zauto_read_lock l(_replicas_lock);
    auto it = _replicas.find(id);
    if (it != _replicas.end())
        return it->second;
    else
        return nullptr;
}

replica_stub::replica_life_cycle replica_stub::get_replica_life_cycle(gpid id)
{
    zauto_read_lock l(_replicas_lock);
    if (_opening_replicas.find(id) != _opening_replicas.end())
        return replica_stub::RL_creating;
    if (_replicas.find(id) != _replicas.end())
        return replica_stub::RL_serving;
    if (_closing_replicas.find(id) != _closing_replicas.end())
        return replica_stub::RL_closing;
    if (_closed_replicas.find(id) != _closed_replicas.end())
        return replica_stub::RL_closed;
    return replica_stub::RL_invalid;
}

void replica_stub::on_client_write(gpid id, dsn::message_ex *request)
{
    if (_deny_client) {
        // ignore and do not reply
        return;
    }
    if (_verbose_client_log && request) {
        ddebug("%s@%s: client = %s, code = %s, timeout = %d",
               id.to_string(),
               _primary_address_str,
               request->header->from_address.to_string(),
               request->header->rpc_name,
               request->header->client.timeout_ms);
    }
    replica_ptr rep = get_replica(id);
    if (rep != nullptr) {
        rep->on_client_write(request);
    } else {
        response_client(id, false, request, partition_status::PS_INVALID, ERR_OBJECT_NOT_FOUND);
    }
}

void replica_stub::on_client_read(gpid id, dsn::message_ex *request)
{
    if (_deny_client) {
        // ignore and do not reply
        return;
    }
    if (_verbose_client_log && request) {
        ddebug("%s@%s: client = %s, code = %s, timeout = %d",
               id.to_string(),
               _primary_address_str,
               request->header->from_address.to_string(),
               request->header->rpc_name,
               request->header->client.timeout_ms);
    }
    replica_ptr rep = get_replica(id);
    if (rep != nullptr) {
        rep->on_client_read(request);
    } else {
        response_client(id, true, request, partition_status::PS_INVALID, ERR_OBJECT_NOT_FOUND);
    }
}

void replica_stub::on_config_proposal(const configuration_update_request &proposal)
{
    if (!is_connected()) {
        dwarn("%s@%s: received config proposal %s for %s: not connected, ignore",
              proposal.config.pid.to_string(),
              _primary_address_str,
              enum_to_string(proposal.type),
              proposal.node.to_string());
        return;
    }

    ddebug("%s@%s: received config proposal %s for %s",
           proposal.config.pid.to_string(),
           _primary_address_str,
           enum_to_string(proposal.type),
           proposal.node.to_string());

    replica_ptr rep = get_replica(proposal.config.pid);
    if (rep == nullptr) {
        if (proposal.type == config_type::CT_ASSIGN_PRIMARY) {
            std::shared_ptr<configuration_update_request> req2(new configuration_update_request);
            *req2 = proposal;
            begin_open_replica(proposal.info, proposal.config.pid, nullptr, req2);
        } else if (proposal.type == config_type::CT_UPGRADE_TO_PRIMARY) {
            remove_replica_on_meta_server(proposal.info, proposal.config);
        }
    }

    if (rep != nullptr) {
        rep->on_config_proposal((configuration_update_request &)proposal);
    }
}

void replica_stub::on_query_decree(query_replica_decree_rpc rpc)
{
    const query_replica_decree_request &req = rpc.request();
    query_replica_decree_response &resp = rpc.response();

    replica_ptr rep = get_replica(req.pid);
    if (rep != nullptr) {
        resp.err = ERR_OK;
        if (partition_status::PS_POTENTIAL_SECONDARY == rep->status()) {
            resp.last_decree = 0;
        } else {
            resp.last_decree = rep->last_committed_decree();
            // TODO: use the following to alleviate data lost
            // resp.last_decree = rep->last_prepared_decree();
        }
    } else {
        resp.err = ERR_OBJECT_NOT_FOUND;
        resp.last_decree = 0;
    }
}

void replica_stub::on_query_replica_info(query_replica_info_rpc rpc)
{
    query_replica_info_response &resp = rpc.response();
    std::set<gpid> visited_replicas;
    {
        zauto_read_lock l(_replicas_lock);
        for (auto it = _replicas.begin(); it != _replicas.end(); ++it) {
            replica_ptr &r = it->second;
            replica_info info;
            get_replica_info(info, r);
            if (visited_replicas.find(info.pid) == visited_replicas.end()) {
                visited_replicas.insert(info.pid);
                resp.replicas.push_back(std::move(info));
            }
        }
        for (auto it = _closing_replicas.begin(); it != _closing_replicas.end(); ++it) {
            const replica_info &info = std::get<3>(it->second);
            if (visited_replicas.find(info.pid) == visited_replicas.end()) {
                visited_replicas.insert(info.pid);
                resp.replicas.push_back(info);
            }
        }
        for (auto it = _closed_replicas.begin(); it != _closed_replicas.end(); ++it) {
            const replica_info &info = it->second.second;
            if (visited_replicas.find(info.pid) == visited_replicas.end()) {
                visited_replicas.insert(info.pid);
                resp.replicas.push_back(info);
            }
        }
    }
    resp.err = ERR_OK;
}

// ThreadPool: THREAD_POOL_DEFAULT
void replica_stub::on_query_disk_info(query_disk_info_rpc rpc)
{
    const query_disk_info_request &req = rpc.request();
    query_disk_info_response &resp = rpc.response();
    int app_id = 0;
    if (!req.app_name.empty()) {
        zauto_read_lock l(_replicas_lock);
        if (!(app_id = get_app_id_from_replicas(req.app_name))) {
            resp.err = ERR_OBJECT_NOT_FOUND;
            return;
        }
    }

    for (const auto &dir_node : _fs_manager._dir_nodes) {
        disk_info info;
        // app_name empty means query all app replica_count
        if (req.app_name.empty()) {
            for (const auto &holding_primary_replicas : dir_node->holding_primary_replicas) {
                info.holding_primary_replica_counts[holding_primary_replicas.first] =
                    static_cast<int>(holding_primary_replicas.second.size());
            }

            for (const auto &holding_secondary_replicas : dir_node->holding_secondary_replicas) {
                info.holding_secondary_replica_counts[holding_secondary_replicas.first] =
                    static_cast<int>(holding_secondary_replicas.second.size());
            }
        } else {
            const auto &primary_iter = dir_node->holding_primary_replicas.find(app_id);
            if (primary_iter != dir_node->holding_primary_replicas.end()) {
                info.holding_primary_replica_counts[app_id] =
                    static_cast<int>(primary_iter->second.size());
            }

            const auto &secondary_iter = dir_node->holding_secondary_replicas.find(app_id);
            if (secondary_iter != dir_node->holding_secondary_replicas.end()) {
                info.holding_secondary_replica_counts[app_id] =
                    static_cast<int>(secondary_iter->second.size());
            }
        }
        info.tag = dir_node->tag;
        info.full_dir = dir_node->full_dir;
        info.disk_capacity_mb = dir_node->disk_capacity_mb;
        info.disk_available_mb = dir_node->disk_available_mb;

        resp.disk_infos.emplace_back(info);
    }

    resp.total_capacity_mb = _fs_manager._total_capacity_mb;
    resp.total_available_mb = _fs_manager._total_available_mb;

    resp.err = ERR_OK;
}

void replica_stub::on_query_app_info(query_app_info_rpc rpc)
{
    const query_app_info_request &req = rpc.request();
    query_app_info_response &resp = rpc.response();

    ddebug("got query app info request from (%s)", req.meta_server.to_string());
    resp.err = dsn::ERR_OK;
    std::set<app_id> visited_apps;
    {
        zauto_read_lock l(_replicas_lock);
        for (auto it = _replicas.begin(); it != _replicas.end(); ++it) {
            replica_ptr &r = it->second;
            const app_info &info = *r->get_app_info();
            if (visited_apps.find(info.app_id) == visited_apps.end()) {
                resp.apps.push_back(info);
                visited_apps.insert(info.app_id);
            }
        }
        for (auto it = _closing_replicas.begin(); it != _closing_replicas.end(); ++it) {
            const app_info &info = std::get<2>(it->second);
            if (visited_apps.find(info.app_id) == visited_apps.end()) {
                resp.apps.push_back(info);
                visited_apps.insert(info.app_id);
            }
        }
        for (auto it = _closed_replicas.begin(); it != _closed_replicas.end(); ++it) {
            const app_info &info = it->second.first;
            if (visited_apps.find(info.app_id) == visited_apps.end()) {
                resp.apps.push_back(info);
                visited_apps.insert(info.app_id);
            }
        }
    }
}

void replica_stub::on_cold_backup(backup_rpc rpc)
{
    const backup_request &request = rpc.request();
    backup_response &response = rpc.response();

    ddebug("received cold backup request: backup{%s.%s.%" PRId64 "}",
           request.pid.to_string(),
           request.policy.policy_name.c_str(),
           request.backup_id);
    response.pid = request.pid;
    response.policy_name = request.policy.policy_name;
    response.backup_id = request.backup_id;

    if (_options.cold_backup_root.empty()) {
        derror("backup{%s.%s.%" PRId64
               "}: cold_backup_root is empty, response ERR_OPERATION_DISABLED",
               request.pid.to_string(),
               request.policy.policy_name.c_str(),
               request.backup_id);
        response.err = ERR_OPERATION_DISABLED;
        return;
    }

    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->on_cold_backup(request, response);
    } else {
        derror("backup{%s.%s.%" PRId64 "}: replica not found, response ERR_OBJECT_NOT_FOUND",
               request.pid.to_string(),
               request.policy.policy_name.c_str(),
               request.backup_id);
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::on_clear_cold_backup(const backup_clear_request &request)
{
    ddebug_f("receive clear cold backup request: backup({}.{})",
             request.pid.to_string(),
             request.policy_name.c_str());

    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->get_backup_manager()->on_clear_cold_backup(request);
    }
}

void replica_stub::on_prepare(dsn::message_ex *request)
{
    gpid id;
    dsn::unmarshall(request, id);
    replica_ptr rep = get_replica(id);
    if (rep != nullptr) {
        rep->on_prepare(request);
    } else {
        prepare_ack resp;
        resp.pid = id;
        resp.err = ERR_OBJECT_NOT_FOUND;
        reply(request, resp);
    }
}

void replica_stub::on_group_check(group_check_rpc rpc)
{
    const group_check_request &request = rpc.request();
    group_check_response &response = rpc.response();
    if (!is_connected()) {
        dwarn("%s@%s: received group check: not connected, ignore",
              request.config.pid.to_string(),
              _primary_address_str);
        return;
    }

    ddebug("%s@%s: received group check, primary = %s, ballot = %" PRId64
           ", status = %s, last_committed_decree = %" PRId64,
           request.config.pid.to_string(),
           _primary_address_str,
           request.config.primary.to_string(),
           request.config.ballot,
           enum_to_string(request.config.status),
           request.last_committed_decree);

    replica_ptr rep = get_replica(request.config.pid);
    if (rep != nullptr) {
        rep->on_group_check(request, response);
    } else {
        if (request.config.status == partition_status::PS_POTENTIAL_SECONDARY) {
            std::shared_ptr<group_check_request> req(new group_check_request);
            *req = request;

            begin_open_replica(request.app, request.config.pid, req, nullptr);
            response.err = ERR_OK;
            response.learner_signature = invalid_signature;
        } else {
            response.err = ERR_OBJECT_NOT_FOUND;
        }
    }
}

void replica_stub::on_learn(dsn::message_ex *msg)
{
    learn_request request;
    ::dsn::unmarshall(msg, request);

    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->on_learn(msg, request);
    } else {
        learn_response response;
        response.err = ERR_OBJECT_NOT_FOUND;
        reply(msg, response);
    }
}

void replica_stub::on_copy_checkpoint(copy_checkpoint_rpc rpc)
{
    const replica_configuration &request = rpc.request();
    learn_response &response = rpc.response();

    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->on_copy_checkpoint(request, response);
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::on_learn_completion_notification(learn_completion_notification_rpc rpc)
{
    const group_check_response &report = rpc.request();
    learn_notify_response &response = rpc.response();
    response.pid = report.pid;
    response.signature = report.learner_signature;
    replica_ptr rep = get_replica(report.pid);
    if (rep != nullptr) {
        rep->on_learn_completion_notification(report, response);
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::on_add_learner(const group_check_request &request)
{
    if (!is_connected()) {
        dwarn("%s@%s: received add learner: not connected, ignore",
              request.config.pid.to_string(),
              _primary_address_str,
              request.config.primary.to_string());
        return;
    }

    ddebug("%s@%s: received add learner, primary = %s, ballot = %" PRId64
           ", status = %s, last_committed_decree = %" PRId64,
           request.config.pid.to_string(),
           _primary_address_str,
           request.config.primary.to_string(),
           request.config.ballot,
           enum_to_string(request.config.status),
           request.last_committed_decree);

    replica_ptr rep = get_replica(request.config.pid);
    if (rep != nullptr) {
        rep->on_add_learner(request);
    } else {
        std::shared_ptr<group_check_request> req(new group_check_request);
        *req = request;
        begin_open_replica(request.app, request.config.pid, req, nullptr);
    }
}

void replica_stub::on_remove(const replica_configuration &request)
{
    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->on_remove(request);
    }
}

void replica_stub::get_replica_info(replica_info &info, replica_ptr r)
{
    info.pid = r->get_gpid();
    info.ballot = r->get_ballot();
    info.status = r->status();
    info.app_type = r->get_app_info()->app_type;
    info.last_committed_decree = r->last_committed_decree();
    info.last_prepared_decree = r->last_prepared_decree();
    info.last_durable_decree = r->last_durable_decree();

    dsn::error_code err = _fs_manager.get_disk_tag(r->dir(), info.disk_tag);
    if (dsn::ERR_OK != err) {
        dwarn("get disk tag of %s failed: %s", r->dir().c_str(), err.to_string());
    }
}

void replica_stub::get_local_replicas(std::vector<replica_info> &replicas)
{
    zauto_read_lock l(_replicas_lock);
    // local_replicas = replicas + closing_replicas + closed_replicas
    int total_replicas = _replicas.size() + _closing_replicas.size() + _closed_replicas.size();
    replicas.reserve(total_replicas);

    for (auto &pairs : _replicas) {
        replica_ptr &rep = pairs.second;
        replica_info info;
        get_replica_info(info, rep);
        replicas.push_back(std::move(info));
    }

    for (auto &pairs : _closing_replicas) {
        replicas.push_back(std::get<3>(pairs.second));
    }

    for (auto &pairs : _closed_replicas) {
        replicas.push_back(pairs.second.second);
    }
}

// run in THREAD_POOL_META_SERVER
// assert(_state_lock.locked())
void replica_stub::query_configuration_by_node()
{
    if (_state == NS_Disconnected) {
        return;
    }

    if (_config_query_task != nullptr) {
        return;
    }

    dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_CONFIG_SYNC);

    configuration_query_by_node_request req;
    req.node = _primary_address;

    // TODO: send stored replicas may cost network, we shouldn't config the frequency
    get_local_replicas(req.stored_replicas);
    req.__isset.stored_replicas = true;

    ::dsn::marshall(msg, req);

    ddebug("send query node partitions request to meta server, stored_replicas_count = %d",
           (int)req.stored_replicas.size());

    rpc_address target(_failure_detector->get_servers());
    _config_query_task =
        rpc::call(target,
                  msg,
                  &_tracker,
                  [this](error_code err, dsn::message_ex *request, dsn::message_ex *resp) {
                      on_node_query_reply(err, request, resp);
                  });
}

void replica_stub::on_meta_server_connected()
{
    ddebug("meta server connected");

    zauto_lock l(_state_lock);
    if (_state == NS_Disconnected) {
        _state = NS_Connecting;
        tasking::enqueue(LPC_QUERY_CONFIGURATION_ALL, &_tracker, [this]() {
            zauto_lock l(_state_lock);
            this->query_configuration_by_node();
        });
    }
}

// run in THREAD_POOL_META_SERVER
void replica_stub::on_node_query_reply(error_code err,
                                       dsn::message_ex *request,
                                       dsn::message_ex *response)
{
    ddebug("query node partitions replied, err = %s", err.to_string());

    zauto_lock l(_state_lock);
    _config_query_task = nullptr;
    if (err != ERR_OK) {
        if (_state == NS_Connecting) {
            query_configuration_by_node();
        }
    } else {
        if (_state == NS_Connecting) {
            _state = NS_Connected;
        }

        // DO NOT UPDATE STATE WHEN DISCONNECTED
        if (_state != NS_Connected)
            return;

        configuration_query_by_node_response resp;
        ::dsn::unmarshall(response, resp);

        if (resp.err == ERR_BUSY) {
            int delay_ms = 500;
            ddebug("resend query node partitions request after %d ms for resp.err = ERR_BUSY",
                   delay_ms);
            _config_query_task = tasking::enqueue(LPC_QUERY_CONFIGURATION_ALL,
                                                  &_tracker,
                                                  [this]() {
                                                      zauto_lock l(_state_lock);
                                                      _config_query_task = nullptr;
                                                      this->query_configuration_by_node();
                                                  },
                                                  0,
                                                  std::chrono::milliseconds(delay_ms));
            return;
        }
        if (resp.err != ERR_OK) {
            ddebug("ignore query node partitions response for resp.err = %s", resp.err.to_string());
            return;
        }

        ddebug("process query node partitions response for resp.err = ERR_OK, "
               "partitions_count(%d), gc_replicas_count(%d)",
               (int)resp.partitions.size(),
               (int)resp.gc_replicas.size());

        replicas rs;
        {
            zauto_read_lock l(_replicas_lock);
            rs = _replicas;
        }

        for (auto it = resp.partitions.begin(); it != resp.partitions.end(); ++it) {
            rs.erase(it->config.pid);
            tasking::enqueue(LPC_QUERY_NODE_CONFIGURATION_SCATTER,
                             &_tracker,
                             std::bind(&replica_stub::on_node_query_reply_scatter, this, this, *it),
                             it->config.pid.thread_hash());
        }

        // for rps not exist on meta_servers
        for (auto it = rs.begin(); it != rs.end(); ++it) {
            tasking::enqueue(
                LPC_QUERY_NODE_CONFIGURATION_SCATTER2,
                &_tracker,
                std::bind(&replica_stub::on_node_query_reply_scatter2, this, this, it->first),
                it->first.thread_hash());
        }

        // handle the replicas which need to be gc
        if (resp.__isset.gc_replicas) {
            for (replica_info &rep : resp.gc_replicas) {
                replica_stub::replica_life_cycle lc = get_replica_life_cycle(rep.pid);
                if (lc == replica_stub::RL_closed) {
                    tasking::enqueue(LPC_GARBAGE_COLLECT_LOGS_AND_REPLICAS,
                                     &_tracker,
                                     std::bind(&replica_stub::on_gc_replica, this, this, rep.pid),
                                     0);
                }
            }
        }
    }
}

void replica_stub::set_meta_server_connected_for_test(
    const configuration_query_by_node_response &resp)
{
    zauto_lock l(_state_lock);
    dassert(_state != NS_Connected, "");
    _state = NS_Connected;

    for (auto it = resp.partitions.begin(); it != resp.partitions.end(); ++it) {
        tasking::enqueue(LPC_QUERY_NODE_CONFIGURATION_SCATTER,
                         &_tracker,
                         std::bind(&replica_stub::on_node_query_reply_scatter, this, this, *it),
                         it->config.pid.thread_hash());
    }
}

void replica_stub::set_replica_state_subscriber_for_test(replica_state_subscriber subscriber,
                                                         bool is_long_subscriber)
{
    _replica_state_subscriber = subscriber;
    _is_long_subscriber = is_long_subscriber;
}

// this_ is used to hold a ref to replica_stub so we don't need to cancel the task on
// replica_stub::close
// ThreadPool: THREAD_POOL_REPLICATION
void replica_stub::on_node_query_reply_scatter(replica_stub_ptr this_,
                                               const configuration_update_request &req)
{
    replica_ptr replica = get_replica(req.config.pid);
    if (replica != nullptr) {
        replica->on_config_sync(req.info, req.config);
    } else {
        if (req.config.primary == _primary_address) {
            ddebug("%s@%s: replica not exists on replica server, which is primary, remove it "
                   "from meta server",
                   req.config.pid.to_string(),
                   _primary_address_str);
            remove_replica_on_meta_server(req.info, req.config);
        } else {
            ddebug("%s@%s: replica not exists on replica server, which is not primary, just ignore",
                   req.config.pid.to_string(),
                   _primary_address_str);
        }
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_stub::on_node_query_reply_scatter2(replica_stub_ptr this_, gpid id)
{
    replica_ptr replica = get_replica(id);
    if (replica != nullptr && replica->status() != partition_status::PS_POTENTIAL_SECONDARY) {
        if (replica->status() == partition_status::PS_INACTIVE &&
            dsn_now_ms() - replica->create_time_milliseconds() <
                _options.gc_memory_replica_interval_ms) {
            ddebug("%s: replica not exists on meta server, wait to close", replica->name());
            return;
        }

        ddebug("%s: replica not exists on meta server, remove", replica->name());

        // TODO: set PS_INACTIVE instead for further state reuse
        replica->update_local_configuration_with_no_ballot_change(partition_status::PS_ERROR);
    }
}

void replica_stub::remove_replica_on_meta_server(const app_info &info,
                                                 const partition_configuration &config)
{
    dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_UPDATE_PARTITION_CONFIGURATION);

    std::shared_ptr<configuration_update_request> request(new configuration_update_request);
    request->info = info;
    request->config = config;
    request->config.ballot++;
    request->node = _primary_address;
    request->type = config_type::CT_DOWNGRADE_TO_INACTIVE;

    if (_primary_address == config.primary) {
        request->config.primary.set_invalid();
    } else if (replica_helper::remove_node(_primary_address, request->config.secondaries)) {
    } else {
        return;
    }

    ::dsn::marshall(msg, *request);

    rpc_address target(_failure_detector->get_servers());
    rpc::call(_failure_detector->get_servers(),
              msg,
              nullptr,
              [](error_code err, dsn::message_ex *, dsn::message_ex *) {});
}

void replica_stub::on_meta_server_disconnected()
{
    ddebug("meta server disconnected");

    zauto_lock l(_state_lock);
    if (NS_Disconnected == _state)
        return;

    _state = NS_Disconnected;

    replicas rs;
    {
        zauto_read_lock l(_replicas_lock);
        rs = _replicas;
    }

    for (auto it = rs.begin(); it != rs.end(); ++it) {
        tasking::enqueue(
            LPC_CM_DISCONNECTED_SCATTER,
            &_tracker,
            std::bind(&replica_stub::on_meta_server_disconnected_scatter, this, this, it->first),
            it->first.thread_hash());
    }
}

// this_ is used to hold a ref to replica_stub so we don't need to cancel the task on
// replica_stub::close
void replica_stub::on_meta_server_disconnected_scatter(replica_stub_ptr this_, gpid id)
{
    {
        zauto_lock l(_state_lock);
        if (_state != NS_Disconnected)
            return;
    }

    replica_ptr replica = get_replica(id);
    if (replica != nullptr) {
        replica->on_meta_server_disconnected();
    }
}

void replica_stub::response_client(gpid id,
                                   bool is_read,
                                   dsn::message_ex *request,
                                   partition_status::type status,
                                   error_code error)
{
    if (error == ERR_BUSY) {
        if (is_read)
            _counter_recent_read_busy_count->increment();
        else
            _counter_recent_write_busy_count->increment();
    } else if (error != ERR_OK) {
        if (is_read)
            _counter_recent_read_fail_count->increment();
        else
            _counter_recent_write_fail_count->increment();
        derror("%s@%s: %s fail: client = %s, code = %s, timeout = %d, status = %s, error = %s",
               id.to_string(),
               _primary_address_str,
               is_read ? "read" : "write",
               request == nullptr ? "null" : request->header->from_address.to_string(),
               request == nullptr ? "null" : request->header->rpc_name,
               request == nullptr ? 0 : request->header->client.timeout_ms,
               enum_to_string(status),
               error.to_string());
    }

    if (request != nullptr) {
        dsn_rpc_reply(request->create_response(), error);
    }
}

void replica_stub::init_gc_for_test()
{
    dassert(_options.gc_disabled, "");

    _gc_timer_task = tasking::enqueue(LPC_GARBAGE_COLLECT_LOGS_AND_REPLICAS,
                                      &_tracker,
                                      [this] { on_gc(); },
                                      0,
                                      std::chrono::milliseconds(_options.gc_interval_ms));
}

void replica_stub::on_gc_replica(replica_stub_ptr this_, gpid id)
{
    std::string replica_path;
    std::pair<app_info, replica_info> closed_info;

    {
        zauto_write_lock l(_replicas_lock);
        auto iter = _closed_replicas.find(id);
        if (iter == _closed_replicas.end())
            return;
        closed_info = iter->second;
        _closed_replicas.erase(iter);
        _fs_manager.remove_replica(id);
    }

    replica_path = get_replica_dir(closed_info.first.app_type.c_str(), id, false);
    if (replica_path.empty()) {
        dwarn("gc closed replica(%s.%s) failed, no exist data",
              id.to_string(),
              closed_info.first.app_type.c_str());
        return;
    }

    ddebug("start to move replica(%s) as garbage, path: %s", id.to_string(), replica_path.c_str());
    char rename_path[1024];
    sprintf(rename_path, "%s.%" PRIu64 ".gar", replica_path.c_str(), dsn_now_us());
    if (!dsn::utils::filesystem::rename_path(replica_path, rename_path)) {
        dwarn(
            "gc_replica: failed to move directory '%s' to '%s'", replica_path.c_str(), rename_path);

        // if gc the replica failed, add it back
        zauto_write_lock l(_replicas_lock);
        _fs_manager.add_replica(id, replica_path);
        _closed_replicas.emplace(id, closed_info);
    } else {
        dwarn("gc_replica: {replica_dir_op} succeed to move directory '%s' to '%s'",
              replica_path.c_str(),
              rename_path);
        _counter_replicas_recent_replica_move_garbage_count->increment();
    }
}

void replica_stub::on_gc()
{
    uint64_t start = dsn_now_ns();

    struct gc_info
    {
        replica_ptr rep;
        partition_status::type status;
        mutation_log_ptr plog;
        decree last_durable_decree;
        int64_t init_offset_in_shared_log;
    };

    std::unordered_map<gpid, gc_info> rs;
    {
        zauto_read_lock l(_replicas_lock);
        // collect info in lock to prevent the case that the replica is closed in replica::close()
        for (auto &kv : _replicas) {
            const replica_ptr &rep = kv.second;
            gc_info &info = rs[kv.first];
            info.rep = rep;
            info.status = rep->status();
            info.plog = rep->private_log();
            info.last_durable_decree = rep->last_durable_decree();
            info.init_offset_in_shared_log = rep->get_app()->init_info().init_offset_in_shared_log;
        }
    }

    ddebug("start to garbage collection, replica_count = %d", (int)rs.size());

    // gc shared prepare log
    //
    // Now that checkpoint is very important for gc, we must be able to trigger checkpoint when
    // necessary.
    // that is, we should be able to trigger memtable flush when necessary.
    //
    // How to trigger memtable flush?
    //   we add a parameter `is_emergency' in dsn_app_async_checkpoint() function, when set true,
    //   the undering
    //   storage system should flush memtable as soon as possiable.
    //
    // When to trigger memtable flush?
    //   1. Using `[replication].checkpoint_max_interval_hours' option, we can set max interval time
    //   of two
    //      adjacent checkpoints; If the time interval is arrived, then emergency checkpoint will be
    //      triggered.
    //   2. Using `[replication].log_shared_file_count_limit' option, we can set max file count of
    //   shared log;
    //      If the limit is exceeded, then emergency checkpoint will be triggered; Instead of
    //      triggering all
    //      replicas to do checkpoint, we will only trigger a few of necessary replicas which block
    //      garbage
    //      collection of the oldest log file.
    //
    if (_log != nullptr) {
        replica_log_info_map gc_condition;
        for (auto &kv : rs) {
            replica_log_info ri;
            replica_ptr &rep = kv.second.rep;
            mutation_log_ptr &plog = kv.second.plog;
            if (plog) {
                // flush private log to update plog_max_commit_on_disk,
                // and just flush once to avoid flushing infinitely
                plog->flush_once();

                decree plog_max_commit_on_disk = plog->max_commit_on_disk();
                ri.max_decree = std::min(kv.second.last_durable_decree, plog_max_commit_on_disk);
                ddebug("gc_shared: gc condition for %s, status = %s, garbage_max_decree = %" PRId64
                       ", last_durable_decree= %" PRId64 ", plog_max_commit_on_disk = %" PRId64 "",
                       rep->name(),
                       enum_to_string(kv.second.status),
                       ri.max_decree,
                       kv.second.last_durable_decree,
                       plog_max_commit_on_disk);
            } else {
                ri.max_decree = kv.second.last_durable_decree;
                ddebug("gc_shared: gc condition for %s, status = %s, garbage_max_decree = %" PRId64
                       ", last_durable_decree = %" PRId64 "",
                       rep->name(),
                       enum_to_string(kv.second.status),
                       ri.max_decree,
                       kv.second.last_durable_decree);
            }
            ri.valid_start_offset = kv.second.init_offset_in_shared_log;
            gc_condition[kv.first] = ri;
        }

        std::set<gpid> prevent_gc_replicas;
        int reserved_log_count = _log->garbage_collection(
            gc_condition, _options.log_shared_file_count_limit, prevent_gc_replicas);
        if (reserved_log_count > _options.log_shared_file_count_limit * 2) {
            ddebug("gc_shared: trigger emergency checkpoint by log_shared_file_count_limit, "
                   "file_count_limit = %d, reserved_log_count = %d, trigger all replicas to do "
                   "checkpoint",
                   _options.log_shared_file_count_limit,
                   reserved_log_count);
            for (auto &kv : rs) {
                tasking::enqueue(
                    LPC_PER_REPLICA_CHECKPOINT_TIMER,
                    kv.second.rep->tracker(),
                    std::bind(&replica_stub::trigger_checkpoint, this, kv.second.rep, true),
                    kv.first.thread_hash(),
                    std::chrono::milliseconds(rand::next_u32(0, _options.gc_interval_ms / 2)));
            }
        } else if (reserved_log_count > _options.log_shared_file_count_limit) {
            std::ostringstream oss;
            int c = 0;
            for (auto &i : prevent_gc_replicas) {
                if (c != 0)
                    oss << ", ";
                oss << i.to_string();
                c++;
            }
            ddebug("gc_shared: trigger emergency checkpoint by log_shared_file_count_limit, "
                   "file_count_limit = %d, reserved_log_count = %d, prevent_gc_replica_count = %d, "
                   "trigger them to do checkpoint: { %s }",
                   _options.log_shared_file_count_limit,
                   reserved_log_count,
                   (int)prevent_gc_replicas.size(),
                   oss.str().c_str());
            for (auto &id : prevent_gc_replicas) {
                auto find = rs.find(id);
                if (find != rs.end()) {
                    tasking::enqueue(
                        LPC_PER_REPLICA_CHECKPOINT_TIMER,
                        find->second.rep->tracker(),
                        std::bind(&replica_stub::trigger_checkpoint, this, find->second.rep, true),
                        id.thread_hash(),
                        std::chrono::milliseconds(rand::next_u32(0, _options.gc_interval_ms / 2)));
                }
            }
        }

        _counter_shared_log_size->set(_log->total_size() / (1024 * 1024));
    }

    // statistic learning info
    uint64_t learning_count = 0;
    uint64_t learning_max_duration_time_ms = 0;
    uint64_t learning_max_copy_file_size = 0;
    uint64_t cold_backup_running_count = 0;
    uint64_t cold_backup_max_duration_time_ms = 0;
    uint64_t cold_backup_max_upload_file_size = 0;
    for (auto &kv : rs) {
        replica_ptr &rep = kv.second.rep;
        if (rep->status() == partition_status::PS_POTENTIAL_SECONDARY) {
            learning_count++;
            learning_max_duration_time_ms = std::max(
                learning_max_duration_time_ms, rep->_potential_secondary_states.duration_ms());
            learning_max_copy_file_size =
                std::max(learning_max_copy_file_size,
                         rep->_potential_secondary_states.learning_copy_file_size);
        }
        if (rep->status() == partition_status::PS_PRIMARY ||
            rep->status() == partition_status::PS_SECONDARY) {
            cold_backup_running_count += rep->_cold_backup_running_count.load();
            cold_backup_max_duration_time_ms = std::max(
                cold_backup_max_duration_time_ms, rep->_cold_backup_max_duration_time_ms.load());
            cold_backup_max_upload_file_size = std::max(
                cold_backup_max_upload_file_size, rep->_cold_backup_max_upload_file_size.load());
        }
    }

    _counter_replicas_learning_count->set(learning_count);
    _counter_replicas_learning_max_duration_time_ms->set(learning_max_duration_time_ms);
    _counter_replicas_learning_max_copy_file_size->set(learning_max_copy_file_size);
    _counter_cold_backup_running_count->set(cold_backup_running_count);
    _counter_cold_backup_max_duration_time_ms->set(cold_backup_max_duration_time_ms);
    _counter_cold_backup_max_upload_file_size->set(cold_backup_max_upload_file_size);

    ddebug("finish to garbage collection, time_used_ns = %" PRIu64, dsn_now_ns() - start);
}

void replica_stub::on_disk_stat()
{
    ddebug("start to update disk stat");
    uint64_t start = dsn_now_ns();

    // gc on-disk rps
    std::vector<std::string> sub_list;
    for (auto &dir : _options.data_dirs) {
        std::vector<std::string> tmp_list;
        if (!dsn::utils::filesystem::get_subdirectories(dir, tmp_list, false)) {
            dwarn("gc_disk: failed to get subdirectories in %s", dir.c_str());
            return;
        }
        sub_list.insert(sub_list.end(), tmp_list.begin(), tmp_list.end());
    }
    int error_replica_dir_count = 0;
    int garbage_replica_dir_count = 0;
    for (auto &fpath : sub_list) {
        auto name = dsn::utils::filesystem::get_file_name(fpath);
        // don't delete ".bak" directory because it is backed by administrator.
        if (name.length() >= 4 && (name.substr(name.length() - 4) == ".err" ||
                                   name.substr(name.length() - 4) == ".gar")) {
            if (name.substr(name.length() - 4) == ".err") {
                error_replica_dir_count++;
            } else {
                garbage_replica_dir_count++;
            }

            time_t mt;
            if (!dsn::utils::filesystem::last_write_time(fpath, mt)) {
                dwarn("gc_disk: failed to get last write time of %s", fpath.c_str());
                continue;
            }

            uint64_t last_write_time = (uint64_t)mt;
            uint64_t current_time_ms = dsn_now_ms();
            uint64_t interval_seconds = (name.substr(name.length() - 4) == ".err"
                                             ? _gc_disk_error_replica_interval_seconds
                                             : _gc_disk_garbage_replica_interval_seconds);
            if (last_write_time + interval_seconds <= current_time_ms / 1000) {
                if (!dsn::utils::filesystem::remove_path(fpath)) {
                    dwarn("gc_disk: failed to delete directory '%s', time_used_ms = %" PRIu64,
                          fpath.c_str(),
                          dsn_now_ms() - current_time_ms);
                } else {
                    dwarn("gc_disk: {replica_dir_op} succeed to delete directory '%s'"
                          ", time_used_ms = %" PRIu64,
                          fpath.c_str(),
                          dsn_now_ms() - current_time_ms);
                    _counter_replicas_recent_replica_remove_dir_count->increment();
                }
            } else {
                ddebug("gc_disk: reserve directory '%s', wait_seconds = %" PRIu64,
                       fpath.c_str(),
                       last_write_time + interval_seconds - current_time_ms / 1000);
            }
        }
    }
    _counter_replicas_error_replica_dir_count->set(error_replica_dir_count);
    _counter_replicas_garbage_replica_dir_count->set(garbage_replica_dir_count);

    _fs_manager.update_disk_stat();
    update_disk_holding_replicas();

    ddebug("finish to update disk stat, time_used_ns = %" PRIu64, dsn_now_ns() - start);
}

::dsn::task_ptr replica_stub::begin_open_replica(const app_info &app,
                                                 gpid id,
                                                 std::shared_ptr<group_check_request> req,
                                                 std::shared_ptr<configuration_update_request> req2)
{
    _replicas_lock.lock_write();

    if (_replicas.find(id) != _replicas.end()) {
        _replicas_lock.unlock_write();
        ddebug("open replica '%s.%s' failed coz replica is already opened",
               app.app_type.c_str(),
               id.to_string());
        return nullptr;
    }

    if (_opening_replicas.find(id) != _opening_replicas.end()) {
        _replicas_lock.unlock_write();
        ddebug("open replica '%s.%s' failed coz replica is under opening",
               app.app_type.c_str(),
               id.to_string());
        return nullptr;
    }

    auto it = _closing_replicas.find(id);
    if (it != _closing_replicas.end()) {
        task_ptr tsk = std::get<0>(it->second);
        replica_ptr rep = std::get<1>(it->second);
        if (rep->status() == partition_status::PS_INACTIVE && tsk->cancel(false)) {
            // reopen it
            _closing_replicas.erase(it);
            _counter_replicas_closing_count->decrement();

            _replicas.emplace(id, rep);
            _counter_replicas_count->increment();

            _closed_replicas.erase(id);

            // unlock here to avoid dead lock
            _replicas_lock.unlock_write();

            ddebug("open replica '%s.%s' which is to be closed, reopen it",
                   app.app_type.c_str(),
                   id.to_string());

            // open by add learner
            if (req != nullptr) {
                on_add_learner(*req);
            }
        } else {
            _replicas_lock.unlock_write();
            ddebug("open replica '%s.%s' failed coz replica is under closing",
                   app.app_type.c_str(),
                   id.to_string());
        }
        return nullptr;
    }

    task_ptr task =
        tasking::enqueue(LPC_OPEN_REPLICA,
                         &_tracker,
                         std::bind(&replica_stub::open_replica, this, app, id, req, req2));

    _opening_replicas[id] = task;
    _counter_replicas_opening_count->increment();
    _closed_replicas.erase(id);

    _replicas_lock.unlock_write();
    return task;
}

void replica_stub::open_replica(const app_info &app,
                                gpid id,
                                std::shared_ptr<group_check_request> req,
                                std::shared_ptr<configuration_update_request> req2)
{
    std::string dir = get_replica_dir(app.app_type.c_str(), id, false);
    replica_ptr rep = nullptr;
    if (!dir.empty()) {
        // NOTICE: if partition is DDD, and meta select one replica as primary, it will execute the
        // load-process because of a.b.pegasus is exist, so it will never execute the restore
        // process below
        ddebug("%s@%s: start to load replica %s group check, dir = %s",
               id.to_string(),
               _primary_address_str,
               req ? "with" : "without",
               dir.c_str());
        rep = replica::load(this, dir.c_str());
    }

    if (rep == nullptr) {
        // NOTICE: only new_replica_group's assign_primary will execute this; if server restart when
        // download restore-data from cold backup media, the a.b.pegasus will move to
        // a.b.pegasus.timestamp.err when replica-server load all the replicas, so restore-flow will
        // do it again

        bool restore_if_necessary =
            ((req2 != nullptr) && (req2->type == config_type::CT_ASSIGN_PRIMARY) &&
             (app.envs.find(backup_restore_constant::POLICY_NAME) != app.envs.end()));

        // NOTICE: when we don't need execute restore-process, we should remove a.b.pegasus
        // directory because it don't contain the valid data dir and also we need create a new
        // replica(if contain valid data, it will execute load-process)

        if (!restore_if_necessary && ::dsn::utils::filesystem::directory_exists(dir)) {
            if (!::dsn::utils::filesystem::remove_path(dir)) {
                dassert(false, "remove useless directory(%s) failed", dir.c_str());
                return;
            }
        }
        rep = replica::newr(this, id, app, restore_if_necessary);
    }

    if (rep == nullptr) {
        ddebug("%s@%s: open replica failed, erase from opening replicas",
               id.to_string(),
               _primary_address_str);
        zauto_write_lock l(_replicas_lock);
        auto ret = _opening_replicas.erase(id);
        dassert(ret > 0, "replica %s is not in _opening_replicas", id.to_string());
        _counter_replicas_opening_count->decrement();
        return;
    }

    {
        zauto_write_lock l(_replicas_lock);
        auto ret = _opening_replicas.erase(id);
        dassert(ret > 0, "replica %s is not in _opening_replicas", id.to_string());
        _counter_replicas_opening_count->decrement();

        auto it = _replicas.find(id);
        dassert(it == _replicas.end(), "replica %s is already in _replicas", id.to_string());
        _replicas.insert(replicas::value_type(rep->get_gpid(), rep));
        _counter_replicas_count->increment();

        _closed_replicas.erase(id);
    }

    if (nullptr != req) {
        rpc::call_one_way_typed(
            _primary_address, RPC_LEARN_ADD_LEARNER, *req, req->config.pid.thread_hash());
    } else if (nullptr != req2) {
        rpc::call_one_way_typed(
            _primary_address, RPC_CONFIG_PROPOSAL, *req2, req2->config.pid.thread_hash());
    }
}

::dsn::task_ptr replica_stub::begin_close_replica(replica_ptr r)
{
    dassert(r->status() == partition_status::PS_ERROR ||
                r->status() == partition_status::PS_INACTIVE,
            "%s: invalid state %s when calling begin_close_replica",
            r->name(),
            enum_to_string(r->status()));

    gpid id = r->get_gpid();

    zauto_write_lock l(_replicas_lock);

    if (_replicas.erase(id) > 0) {
        _counter_replicas_count->decrement();

        int delay_ms = 0;
        if (r->status() == partition_status::PS_INACTIVE) {
            delay_ms = _options.gc_memory_replica_interval_ms;
            ddebug("%s: delay %d milliseconds to close replica, status = PS_INACTIVE",
                   r->name(),
                   delay_ms);
        }

        app_info a_info = *(r->get_app_info());
        replica_info r_info;
        get_replica_info(r_info, r);
        task_ptr task = tasking::enqueue(LPC_CLOSE_REPLICA,
                                         &_tracker,
                                         [=]() { close_replica(r); },
                                         0,
                                         std::chrono::milliseconds(delay_ms));
        _closing_replicas[id] = std::make_tuple(task, r, std::move(a_info), std::move(r_info));
        _counter_replicas_closing_count->increment();
        return task;
    } else {
        return nullptr;
    }
}

void replica_stub::close_replica(replica_ptr r)
{
    ddebug("%s: start to close replica", r->name());

    gpid id = r->get_gpid();
    std::string name = r->name();

    r->close();

    {
        zauto_write_lock l(_replicas_lock);
        auto find = _closing_replicas.find(id);
        dassert(find != _closing_replicas.end(),
                "replica %s is not in _closing_replicas",
                name.c_str());
        _closed_replicas.emplace(
            id, std::make_pair(std::get<2>(find->second), std::get<3>(find->second)));
        _closing_replicas.erase(find);
        _counter_replicas_closing_count->decrement();
    }

    ddebug("%s: finish to close replica", name.c_str());
}

void replica_stub::notify_replica_state_update(const replica_configuration &config, bool is_closing)
{
    if (nullptr != _replica_state_subscriber) {
        if (_is_long_subscriber) {
            tasking::enqueue(
                LPC_REPLICA_STATE_CHANGE_NOTIFICATION,
                &_tracker,
                std::bind(_replica_state_subscriber, _primary_address, config, is_closing));
        } else {
            _replica_state_subscriber(_primary_address, config, is_closing);
        }
    }
}

void replica_stub::trigger_checkpoint(replica_ptr r, bool is_emergency)
{
    r->init_checkpoint(is_emergency);
}

void replica_stub::handle_log_failure(error_code err)
{
    derror("handle log failure: %s", err.to_string());
    if (!s_not_exit_on_log_failure) {
        dassert(false, "TODO: better log failure handling ...");
    }
}

void replica_stub::open_service()
{
    register_rpc_handler(RPC_CONFIG_PROPOSAL, "ProposeConfig", &replica_stub::on_config_proposal);
    register_rpc_handler(RPC_PREPARE, "prepare", &replica_stub::on_prepare);
    register_rpc_handler(RPC_LEARN, "Learn", &replica_stub::on_learn);
    register_rpc_handler_with_rpc_holder(RPC_LEARN_COMPLETION_NOTIFY,
                                         "LearnNotify",
                                         &replica_stub::on_learn_completion_notification);
    register_rpc_handler(RPC_LEARN_ADD_LEARNER, "LearnAdd", &replica_stub::on_add_learner);
    register_rpc_handler(RPC_REMOVE_REPLICA, "remove", &replica_stub::on_remove);
    register_rpc_handler_with_rpc_holder(
        RPC_GROUP_CHECK, "GroupCheck", &replica_stub::on_group_check);
    register_rpc_handler_with_rpc_holder(
        RPC_QUERY_PN_DECREE, "query_decree", &replica_stub::on_query_decree);
    register_rpc_handler_with_rpc_holder(
        RPC_QUERY_REPLICA_INFO, "query_replica_info", &replica_stub::on_query_replica_info);
    register_rpc_handler_with_rpc_holder(
        RPC_REPLICA_COPY_LAST_CHECKPOINT, "copy_checkpoint", &replica_stub::on_copy_checkpoint);
    register_rpc_handler_with_rpc_holder(
        RPC_QUERY_DISK_INFO, "query_disk_info", &replica_stub::on_query_disk_info);
    register_rpc_handler_with_rpc_holder(
        RPC_QUERY_APP_INFO, "query_app_info", &replica_stub::on_query_app_info);
    register_rpc_handler_with_rpc_holder(
        RPC_COLD_BACKUP, "cold_backup", &replica_stub::on_cold_backup);
    register_rpc_handler(
        RPC_CLEAR_COLD_BACKUP, "clear_cold_backup", &replica_stub::on_clear_cold_backup);
    register_rpc_handler_with_rpc_holder(RPC_SPLIT_NOTIFY_CATCH_UP,
                                         "child_notify_catch_up",
                                         &replica_stub::on_notify_primary_split_catch_up);
    register_rpc_handler_with_rpc_holder(RPC_BULK_LOAD, "bulk_load", &replica_stub::on_bulk_load);
    register_rpc_handler_with_rpc_holder(
        RPC_GROUP_BULK_LOAD, "group_bulk_load", &replica_stub::on_group_bulk_load);

    register_ctrl_command();
}

void replica_stub::register_ctrl_command()
{
    /// In simple_kv test, three replica apps are created, which means that three replica_stubs are
    /// initialized in simple_kv test. If we don't use std::call_once, these command are registered
    /// for three times. And in command_manager, one same command is not allowed to be registered
    /// more than twice times. That is why we use std::call_once here. Same situation in
    /// failure_detector::register_ctrl_commands and nfs_client_impl::register_cli_commands
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        _kill_partition_command = ::dsn::command_manager::instance().register_command(
            {"replica.kill_partition"},
            "kill_partition [app_id [partition_index]]",
            "kill_partition: kill partitions by (all, one app, one partition)",
            [this](const std::vector<std::string> &args) {
                dsn::gpid pid;
                if (args.size() == 0) {
                    pid.set_app_id(-1);
                    pid.set_partition_index(-1);
                } else if (args.size() == 1) {
                    pid.set_app_id(atoi(args[0].c_str()));
                    pid.set_partition_index(-1);
                } else if (args.size() == 2) {
                    pid.set_app_id(atoi(args[0].c_str()));
                    pid.set_partition_index(atoi(args[1].c_str()));
                } else {
                    return std::string(ERR_INVALID_PARAMETERS.to_string());
                }
                dsn::error_code e = this->on_kill_replica(pid);
                return std::string(e.to_string());
            });

        _deny_client_command = ::dsn::command_manager::instance().register_command(
            {"replica.deny-client"},
            "deny-client <true|false>",
            "deny-client - control if deny client read & write request",
            [this](const std::vector<std::string> &args) {
                return remote_command_set_bool_flag(_deny_client, "deny-client", args);
            });

        _verbose_client_log_command = ::dsn::command_manager::instance().register_command(
            {"replica.verbose-client-log"},
            "verbose-client-log <true|false>",
            "verbose-client-log - control if print verbose error log when reply read & write "
            "request",
            [this](const std::vector<std::string> &args) {
                return remote_command_set_bool_flag(
                    _verbose_client_log, "verbose-client-log", args);
            });

        _verbose_commit_log_command = ::dsn::command_manager::instance().register_command(
            {"replica.verbose-commit-log"},
            "verbose-commit-log <true|false>",
            "verbose-commit-log - control if print verbose log when commit mutation",
            [this](const std::vector<std::string> &args) {
                return remote_command_set_bool_flag(
                    _verbose_commit_log, "verbose-commit-log", args);
            });

        _trigger_chkpt_command = ::dsn::command_manager::instance().register_command(
            {"replica.trigger-checkpoint"},
            "trigger-checkpoint [id1,id2,...] (where id is 'app_id' or 'app_id.partition_id')",
            "trigger-checkpoint - trigger replicas to do checkpoint",
            [this](const std::vector<std::string> &args) {
                return exec_command_on_replica(args, true, [this](const replica_ptr &rep) {
                    tasking::enqueue(LPC_PER_REPLICA_CHECKPOINT_TIMER,
                                     rep->tracker(),
                                     std::bind(&replica_stub::trigger_checkpoint, this, rep, true),
                                     rep->get_gpid().thread_hash());
                    return std::string("triggered");
                });
            });

        _query_compact_command = ::dsn::command_manager::instance().register_command(
            {"replica.query-compact"},
            "query-compact [id1,id2,...] (where id is 'app_id' or 'app_id.partition_id')",
            "query-compact - query full compact status on the underlying storage engine",
            [this](const std::vector<std::string> &args) {
                return exec_command_on_replica(
                    args, true, [](const replica_ptr &rep) { return rep->query_compact_state(); });
            });

        _query_app_envs_command = ::dsn::command_manager::instance().register_command(
            {"replica.query-app-envs"},
            "query-app-envs [id1,id2,...] (where id is 'app_id' or 'app_id.partition_id')",
            "query-app-envs - query app envs on the underlying storage engine",
            [this](const std::vector<std::string> &args) {
                return exec_command_on_replica(args, true, [](const replica_ptr &rep) {
                    std::map<std::string, std::string> kv_map;
                    rep->query_app_envs(kv_map);
                    return dsn::utils::kv_map_to_string(kv_map, ',', '=');
                });
            });

        _useless_dir_reserve_seconds_command = dsn::command_manager::instance().register_command(
            {"replica.useless-dir-reserve-seconds"},
            "useless-dir-reserve-seconds [num | DEFAULT]",
            "control gc_disk_error_replica_interval_seconds and "
            "gc_disk_garbage_replica_interval_seconds",
            [this](const std::vector<std::string> &args) {
                std::string result("OK");
                if (args.empty()) {
                    result = "error_dir_reserve_seconds=" +
                             std::to_string(_gc_disk_error_replica_interval_seconds) +
                             ",garbage_dir_reserve_seconds=" +
                             std::to_string(_gc_disk_garbage_replica_interval_seconds);
                } else {
                    if (args[0] == "DEFAULT") {
                        _gc_disk_error_replica_interval_seconds =
                            _options.gc_disk_error_replica_interval_seconds;
                        _gc_disk_garbage_replica_interval_seconds =
                            _options.gc_disk_garbage_replica_interval_seconds;
                    } else {
                        int32_t seconds = 0;
                        if (!dsn::buf2int32(args[0], seconds) || seconds < 0) {
                            result = std::string("ERR: invalid arguments");
                        } else {
                            _gc_disk_error_replica_interval_seconds = seconds;
                            _gc_disk_garbage_replica_interval_seconds = seconds;
                        }
                    }
                }
                return result;
            });

#ifdef DSN_ENABLE_GPERF
        _release_tcmalloc_memory_command = ::dsn::command_manager::instance().register_command(
            {"replica.release-tcmalloc-memory"},
            "release-tcmalloc-memory <true|false>",
            "release-tcmalloc-memory - control if try to release tcmalloc memory",
            [this](const std::vector<std::string> &args) {
                return remote_command_set_bool_flag(
                    _release_tcmalloc_memory, "release-tcmalloc-memory", args);
            });

        _max_reserved_memory_percentage_command = dsn::command_manager::instance().register_command(
            {"replica.mem-release-max-reserved-percentage"},
            "mem-release-max-reserved-percentage [num | DEFAULT]",
            "control tcmalloc max reserved but not-used memory percentage",
            [this](const std::vector<std::string> &args) {
                std::string result("OK");
                if (args.empty()) {
                    // show current value
                    result = "mem-release-max-reserved-percentage = " +
                             std::to_string(_mem_release_max_reserved_mem_percentage);
                    return result;
                }
                if (args[0] == "DEFAULT") {
                    // set to default value
                    _mem_release_max_reserved_mem_percentage =
                        _options.mem_release_max_reserved_mem_percentage;
                    return result;
                }
                int32_t percentage = 0;
                if (!dsn::buf2int32(args[0], percentage) || percentage <= 0 || percentage > 100) {
                    result = std::string("ERR: invalid arguments");
                } else {
                    _mem_release_max_reserved_mem_percentage = percentage;
                }
                return result;
            });
#endif
        _max_concurrent_bulk_load_downloading_count_command =
            dsn::command_manager::instance().register_command(
                {"replica.max-concurrent-bulk-load-downloading-count"},
                "max-concurrent-bulk-load-downloading-count [num | DEFAULT]",
                "control stub max_concurrent_bulk_load_downloading_count",
                [this](const std::vector<std::string> &args) {
                    std::string result("OK");
                    if (args.empty()) {
                        result = "max_concurrent_bulk_load_downloading_count=" +
                                 std::to_string(_max_concurrent_bulk_load_downloading_count);
                        return result;
                    }

                    if (args[0] == "DEFAULT") {
                        _max_concurrent_bulk_load_downloading_count =
                            _options.max_concurrent_bulk_load_downloading_count;
                        return result;
                    }

                    int32_t count = 0;
                    if (!dsn::buf2int32(args[0], count) || count <= 0) {
                        result = std::string("ERR: invalid arguments");
                    } else {
                        _max_concurrent_bulk_load_downloading_count = count;
                    }
                    return result;
                });
    });
}

std::string
replica_stub::exec_command_on_replica(const std::vector<std::string> &args,
                                      bool allow_empty_args,
                                      std::function<std::string(const replica_ptr &rep)> func)
{
    if (!allow_empty_args && args.empty()) {
        return std::string("invalid arguments");
    }

    replicas rs;
    {
        zauto_read_lock l(_replicas_lock);
        rs = _replicas;
    }

    std::set<gpid> required_ids;
    replicas choosed_rs;
    if (!args.empty()) {
        for (int i = 0; i < args.size(); i++) {
            std::vector<std::string> arg_strs;
            utils::split_args(args[i].c_str(), arg_strs, ',');
            if (arg_strs.empty()) {
                return std::string("invalid arguments");
            }

            for (const std::string &arg : arg_strs) {
                if (arg.empty())
                    continue;
                gpid id;
                int pid;
                if (id.parse_from(arg.c_str())) {
                    // app_id.partition_index
                    required_ids.insert(id);
                    auto find = rs.find(id);
                    if (find != rs.end()) {
                        choosed_rs[id] = find->second;
                    }
                } else if (sscanf(arg.c_str(), "%d", &pid) == 1) {
                    // app_id
                    for (auto kv : rs) {
                        id = kv.second->get_gpid();
                        if (id.get_app_id() == pid) {
                            choosed_rs[id] = kv.second;
                        }
                    }
                } else {
                    return std::string("invalid arguments");
                }
            }
        }
    } else {
        // all replicas
        choosed_rs = rs;
    }

    std::vector<task_ptr> tasks;
    ::dsn::zlock results_lock;
    std::map<gpid, std::pair<partition_status::type, std::string>> results; // id => status,result
    for (auto &kv : choosed_rs) {
        replica_ptr rep = kv.second;
        task_ptr tsk = tasking::enqueue(LPC_EXEC_COMMAND_ON_REPLICA,
                                        rep->tracker(),
                                        [rep, &func, &results_lock, &results]() {
                                            partition_status::type status = rep->status();
                                            if (status != partition_status::PS_PRIMARY &&
                                                status != partition_status::PS_SECONDARY)
                                                return;
                                            std::string result = func(rep);
                                            ::dsn::zauto_lock l(results_lock);
                                            auto &value = results[rep->get_gpid()];
                                            value.first = status;
                                            value.second = result;
                                        },
                                        rep->get_gpid().thread_hash());
        tasks.emplace_back(std::move(tsk));
    }

    for (auto &tsk : tasks) {
        tsk->wait();
    }

    int processed = results.size();
    int not_found = 0;
    for (auto &id : required_ids) {
        if (results.find(id) == results.end()) {
            auto &value = results[id];
            value.first = partition_status::PS_INVALID;
            value.second = "not found";
            not_found++;
        }
    }

    std::stringstream query_state;
    query_state << processed << " processed, " << not_found << " not found";
    for (auto &kv : results) {
        query_state << "\n    " << kv.first.to_string() << "@" << _primary_address_str;
        if (kv.second.first != partition_status::PS_INVALID)
            query_state << "@" << (kv.second.first == partition_status::PS_PRIMARY ? "P" : "S");
        query_state << " : " << kv.second.second;
    }

    return query_state.str();
}

void replica_stub::close()
{
    _tracker.cancel_outstanding_tasks();

    // this replica may not be opened
    // or is already closed by calling tool_app::stop_all_apps()
    // in this case, just return
    if (_kill_partition_command == nullptr) {
        return;
    }

    dsn::command_manager::instance().deregister_command(_kill_partition_command);
    dsn::command_manager::instance().deregister_command(_deny_client_command);
    dsn::command_manager::instance().deregister_command(_verbose_client_log_command);
    dsn::command_manager::instance().deregister_command(_verbose_commit_log_command);
    dsn::command_manager::instance().deregister_command(_trigger_chkpt_command);
    dsn::command_manager::instance().deregister_command(_query_compact_command);
    dsn::command_manager::instance().deregister_command(_query_app_envs_command);
    dsn::command_manager::instance().deregister_command(_useless_dir_reserve_seconds_command);
#ifdef DSN_ENABLE_GPERF
    dsn::command_manager::instance().deregister_command(_release_tcmalloc_memory_command);
    dsn::command_manager::instance().deregister_command(_max_reserved_memory_percentage_command);
#endif
    dsn::command_manager::instance().deregister_command(
        _max_concurrent_bulk_load_downloading_count_command);

    _kill_partition_command = nullptr;
    _deny_client_command = nullptr;
    _verbose_client_log_command = nullptr;
    _verbose_commit_log_command = nullptr;
    _trigger_chkpt_command = nullptr;
    _query_compact_command = nullptr;
    _query_app_envs_command = nullptr;
    _useless_dir_reserve_seconds_command = nullptr;
#ifdef DSN_ENABLE_GPERF
    _release_tcmalloc_memory_command = nullptr;
    _max_reserved_memory_percentage_command = nullptr;
#endif
    _max_concurrent_bulk_load_downloading_count_command = nullptr;

    if (_config_sync_timer_task != nullptr) {
        _config_sync_timer_task->cancel(true);
        _config_sync_timer_task = nullptr;
    }

    if (_duplication_sync_timer != nullptr) {
        _duplication_sync_timer->close();
        _duplication_sync_timer = nullptr;
    }

    if (_config_query_task != nullptr) {
        _config_query_task->cancel(true);
        _config_query_task = nullptr;
    }
    _state = NS_Disconnected;

    if (_disk_stat_timer_task != nullptr) {
        _disk_stat_timer_task->cancel(true);
        _disk_stat_timer_task = nullptr;
    }

    if (_gc_timer_task != nullptr) {
        _gc_timer_task->cancel(true);
        _gc_timer_task = nullptr;
    }

    if (_mem_release_timer_task != nullptr) {
        _mem_release_timer_task->cancel(true);
        _mem_release_timer_task = nullptr;
    }

    {
        zauto_write_lock l(_replicas_lock);
        while (!_closing_replicas.empty()) {
            task_ptr task = std::get<0>(_closing_replicas.begin()->second);
            gpid tmp_gpid = _closing_replicas.begin()->first;
            _replicas_lock.unlock_write();

            task->wait();

            _replicas_lock.lock_write();
            // task will automatically remove this replica from _closing_replicas
            if (!_closing_replicas.empty()) {
                dassert(tmp_gpid != _closing_replicas.begin()->first,
                        "this replica '%s' should have been removed from _closing_replicas",
                        tmp_gpid.to_string());
            }
        }

        while (!_opening_replicas.empty()) {
            task_ptr task = _opening_replicas.begin()->second;
            _replicas_lock.unlock_write();

            task->cancel(true);

            _counter_replicas_opening_count->decrement();
            _replicas_lock.lock_write();
            _opening_replicas.erase(_opening_replicas.begin());
        }

        while (!_replicas.empty()) {
            _replicas.begin()->second->close();

            _counter_replicas_count->decrement();
            _replicas.erase(_replicas.begin());
        }
    }

    if (_failure_detector != nullptr) {
        _failure_detector->stop();
        delete _failure_detector;
        _failure_detector = nullptr;
    }

    if (_log != nullptr) {
        _log->close();
        _log = nullptr;
    }
}

std::string replica_stub::get_replica_dir(const char *app_type, gpid id, bool create_new)
{
    std::string gpid_str = fmt::format("{}.{}", id, app_type);
    std::string replica_dir;
    bool is_dir_exist = false;
    for (const std::string &data_dir : _options.data_dirs) {
        std::string dir = utils::filesystem::path_combine(data_dir, gpid_str);
        if (utils::filesystem::directory_exists(dir)) {
            if (is_dir_exist) {
                dassert(
                    false, "replica dir conflict: %s <--> %s", dir.c_str(), replica_dir.c_str());
            }
            replica_dir = dir;
            is_dir_exist = true;
        }
    }
    if (replica_dir.empty() && create_new) {
        _fs_manager.allocate_dir(id, app_type, replica_dir);
    }
    return replica_dir;
}

std::string
replica_stub::get_child_dir(const char *app_type, gpid child_pid, const std::string &parent_dir)
{
    std::string gpid_str = fmt::format("{}.{}", child_pid.to_string(), app_type);
    std::string child_dir;
    for (const std::string &data_dir : _options.data_dirs) {
        std::string dir = utils::filesystem::path_combine(data_dir, gpid_str);
        // <parent_dir> = <prefix>/<gpid>.<app_type>
        // check if <parent_dir>'s <prefix> is equal to <data_dir>
        if (parent_dir.substr(0, data_dir.size() + 1) == data_dir + "/") {
            child_dir = dir;
            _fs_manager.add_replica(child_pid, child_dir);
            break;
        }
    }
    dassert_f(!child_dir.empty(), "can not find parent_dir {} in data_dirs", parent_dir);
    return child_dir;
}

#ifdef DSN_ENABLE_GPERF
// Get tcmalloc numeric property (name is "prop") value.
// Return -1 if get property failed (property we used will be greater than zero)
// Properties can be found in 'gperftools/malloc_extension.h'
static int64_t get_tcmalloc_numeric_property(const char *prop)
{
    size_t value;
    if (!::MallocExtension::instance()->GetNumericProperty(prop, &value)) {
        derror_f("Failed to get tcmalloc property {}", prop);
        return -1;
    }
    return value;
}

void replica_stub::gc_tcmalloc_memory()
{
    int64_t tcmalloc_released_bytes = 0;
    if (!_release_tcmalloc_memory) {
        _counter_tcmalloc_release_memory_size->set(tcmalloc_released_bytes);
        return;
    }

    int64_t total_allocated_bytes =
        get_tcmalloc_numeric_property("generic.current_allocated_bytes");
    int64_t reserved_bytes = get_tcmalloc_numeric_property("tcmalloc.pageheap_free_bytes");
    if (total_allocated_bytes == -1 || reserved_bytes == -1) {
        return;
    }

    int64_t max_reserved_bytes =
        total_allocated_bytes * _mem_release_max_reserved_mem_percentage / 100.0;
    if (reserved_bytes > max_reserved_bytes) {
        int64_t release_bytes = reserved_bytes - max_reserved_bytes;
        tcmalloc_released_bytes = release_bytes;
        ddebug_f("Memory release started, almost {} bytes will be released", release_bytes);
        while (release_bytes > 0) {
            // tcmalloc releasing memory will lock page heap, release 1MB at a time to avoid locking
            // page heap for long time
            ::MallocExtension::instance()->ReleaseToSystem(1024 * 1024);
            release_bytes -= 1024 * 1024;
        }
    }
    _counter_tcmalloc_release_memory_size->set(tcmalloc_released_bytes);
}
#endif

//
// partition split
//
void replica_stub::create_child_replica(rpc_address primary_address,
                                        app_info app,
                                        ballot init_ballot,
                                        gpid child_gpid,
                                        gpid parent_gpid,
                                        const std::string &parent_dir)
{
    replica_ptr child_replica = create_child_replica_if_not_found(child_gpid, &app, parent_dir);
    if (child_replica != nullptr) {
        ddebug_f("create child replica ({}) succeed", child_gpid);
        tasking::enqueue(LPC_PARTITION_SPLIT,
                         child_replica->tracker(),
                         std::bind(&replica::child_init_replica,
                                   child_replica,
                                   parent_gpid,
                                   primary_address,
                                   init_ballot),
                         child_gpid.thread_hash());
    } else {
        dwarn_f("failed to create child replica ({}), ignore it and wait next run", child_gpid);
        split_replica_error_handler(parent_gpid,
                                    [](replica_ptr r) { r->_child_gpid.set_app_id(0); });
    }
}

replica_ptr replica_stub::create_child_replica_if_not_found(gpid child_pid,
                                                            app_info *app,
                                                            const std::string &parent_dir)
{
    FAIL_POINT_INJECT_F("replica_stub_create_child_replica_if_not_found",
                        [=](dsn::string_view) -> replica_ptr {
                            replica *rep = new replica(this, child_pid, *app, "./", false);
                            rep->_config.status = partition_status::PS_INACTIVE;
                            _replicas.insert(replicas::value_type(child_pid, rep));
                            ddebug_f("mock create_child_replica_if_not_found succeed");
                            return rep;
                        });

    zauto_write_lock l(_replicas_lock);
    auto it = _replicas.find(child_pid);
    if (it != _replicas.end()) {
        return it->second;
    } else {
        if (_opening_replicas.find(child_pid) != _opening_replicas.end()) {
            dwarn_f("failed create child replica({}) because it is under open", child_pid);
            return nullptr;
        } else if (_closing_replicas.find(child_pid) != _closing_replicas.end()) {
            dwarn_f("failed create child replica({}) because it is under close", child_pid);
            return nullptr;
        } else {
            replica *rep = replica::newr(this, child_pid, *app, false, parent_dir);
            if (rep != nullptr) {
                auto pr = _replicas.insert(replicas::value_type(child_pid, rep));
                dassert_f(pr.second, "child replica {} has been existed", rep->name());
                _counter_replicas_count->increment();
                _closed_replicas.erase(child_pid);
            }
            return rep;
        }
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_stub::split_replica_error_handler(gpid pid, local_execution handler)
{
    split_replica_exec(LPC_PARTITION_SPLIT_ERROR, pid, handler);
}

// ThreadPool: THREAD_POOL_REPLICATION
dsn::error_code
replica_stub::split_replica_exec(dsn::task_code code, gpid pid, local_execution handler)
{
    FAIL_POINT_INJECT_F("replica_stub_split_replica_exec", [](dsn::string_view) { return ERR_OK; });
    replica_ptr replica = pid.get_app_id() == 0 ? nullptr : get_replica(pid);
    if (replica && handler) {
        tasking::enqueue(code,
                         replica.get()->tracker(),
                         [this, handler, replica]() { handler(replica); },
                         pid.thread_hash());
        return ERR_OK;
    }
    dwarn_f("replica({}) is invalid", pid);
    return ERR_OBJECT_NOT_FOUND;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_stub::on_notify_primary_split_catch_up(notify_catch_up_rpc rpc)
{
    const notify_catch_up_request &request = rpc.request();
    notify_cacth_up_response &response = rpc.response();
    replica_ptr replica = get_replica(request.parent_gpid);
    if (replica != nullptr) {
        replica->parent_handle_child_catch_up(request, response);
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::update_disk_holding_replicas()
{
    for (const auto &dir_node : _fs_manager._dir_nodes) {
        // clear the holding_primary_replicas/holding_secondary_replicas and re-calculate it from
        // holding_replicas
        dir_node->holding_primary_replicas.clear();
        dir_node->holding_secondary_replicas.clear();
        for (const auto &holding_replicas : dir_node->holding_replicas) {
            const std::set<dsn::gpid> &pids = holding_replicas.second;
            for (const auto &pid : pids) {
                replica_ptr replica = get_replica(pid);
                if (replica == nullptr) {
                    continue;
                }
                if (replica->status() == partition_status::PS_PRIMARY) {
                    dir_node->holding_primary_replicas[holding_replicas.first].emplace(pid);
                } else if (replica->status() == partition_status::PS_SECONDARY) {
                    dir_node->holding_secondary_replicas[holding_replicas.first].emplace(pid);
                }
            }
        }
    }
}

void replica_stub::on_bulk_load(bulk_load_rpc rpc)
{
    const bulk_load_request &request = rpc.request();
    bulk_load_response &response = rpc.response();

    ddebug_f("[{}@{}]: receive bulk load request", request.pid, _primary_address_str);
    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->get_bulk_loader()->on_bulk_load(request, response);
    } else {
        derror_f("replica({}) is not existed", request.pid);
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::on_group_bulk_load(group_bulk_load_rpc rpc)
{
    const group_bulk_load_request &request = rpc.request();
    group_bulk_load_response &response = rpc.response();

    ddebug_f("[{}@{}]: received group bulk load request, primary = {}, ballot = {}, "
             "meta_bulk_load_status = {}",
             request.config.pid,
             _primary_address_str,
             request.config.primary.to_string(),
             request.config.ballot,
             enum_to_string(request.meta_bulk_load_status));

    replica_ptr rep = get_replica(request.config.pid);
    if (rep != nullptr) {
        rep->get_bulk_loader()->on_group_bulk_load(request, response);
    } else {
        derror_f("replica({}) is not existed", request.config.pid);
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

} // namespace replication
} // namespace dsn
