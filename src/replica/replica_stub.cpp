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
#include "backup/replica_backup_server.h"
#include "split/replica_split_manager.h"
#include "replica_disk_migrator.h"
#include "disk_cleaner.h"

#include <boost/algorithm/string/replace.hpp>
#include "common/json_helper.h"
#include "utils/filesystem.h"
#include "utils/rand.h"
#include "utils/string_conv.h"
#include "utils/command_manager.h"
#include "replica/replication_app_base.h"
#include "utils/enum_helper.h"
#include <vector>
#include <deque>
#include "utils/fmt_logging.h"
#ifdef DSN_ENABLE_GPERF
#include <gperftools/malloc_extension.h>
#elif defined(DSN_USE_JEMALLOC)
#include "utils/je_ctl.h"
#endif
#include "utils/fail_point.h"
#include "remote_cmd/remote_command.h"

namespace dsn {
namespace replication {

DSN_DEFINE_bool("replication",
                ignore_broken_disk,
                true,
                "true means ignore broken data disk when initialize");

DSN_DEFINE_uint32("replication",
                  max_concurrent_manual_emergency_checkpointing_count,
                  10,
                  "max concurrent manual emergency checkpoint running count");
DSN_TAG_VARIABLE(max_concurrent_manual_emergency_checkpointing_count, FT_MUTABLE);

bool replica_stub::s_not_exit_on_log_failure = false;

replica_stub::replica_stub(replica_state_subscriber subscriber /*= nullptr*/,
                           bool is_long_subscriber /* = true*/)
    : serverlet("replica_stub"),
      _deny_client(false),
      _verbose_client_log(false),
      _verbose_commit_log(false),
      _release_tcmalloc_memory(false),
      _mem_release_max_reserved_mem_percentage(10),
      _max_concurrent_bulk_load_downloading_count(5),
      _learn_app_concurrent_count(0),
      _fs_manager(false),
      _bulk_load_downloading_count(0),
      _manual_emergency_checkpointing_count(0),
      _is_running(false)
{
#ifdef DSN_ENABLE_GPERF
    _is_releasing_memory = false;
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
    _counter_replicas_error_replica_dir_count.init_app_counter(
        "eon.replica_stub",
        "replicas.error.replica.dir.count",
        COUNTER_TYPE_NUMBER,
        "error replica directory(*.err) count");
    _counter_replicas_garbage_replica_dir_count.init_app_counter(
        "eon.replica_stub",
        "replicas.garbage.replica.dir.count",
        COUNTER_TYPE_NUMBER,
        "garbage replica directory(*.gar) count");
    _counter_replicas_tmp_replica_dir_count.init_app_counter(
        "eon.replica_stub",
        "replicas.tmp.replica.dir.count",
        COUNTER_TYPE_NUMBER,
        "disk migration tmp replica directory(*.tmp) count");
    _counter_replicas_origin_replica_dir_count.init_app_counter(
        "eon.replica_stub",
        "replicas.origin.replica.dir.count",
        COUNTER_TYPE_NUMBER,
        "disk migration origin replica directory(.ori) count");

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

    // <- Bulk Load Metrics ->

    _counter_bulk_load_running_count.init_app_counter("eon.replica_stub",
                                                      "bulk.load.running.count",
                                                      COUNTER_TYPE_VOLATILE_NUMBER,
                                                      "current bulk load running count");
    _counter_bulk_load_downloading_count.init_app_counter("eon.replica_stub",
                                                          "bulk.load.downloading.count",
                                                          COUNTER_TYPE_VOLATILE_NUMBER,
                                                          "current bulk load downloading count");
    _counter_bulk_load_ingestion_count.init_app_counter("eon.replica_stub",
                                                        "bulk.load.ingestion.count",
                                                        COUNTER_TYPE_VOLATILE_NUMBER,
                                                        "current bulk load ingestion count");
    _counter_bulk_load_succeed_count.init_app_counter("eon.replica_stub",
                                                      "bulk.load.succeed.count",
                                                      COUNTER_TYPE_VOLATILE_NUMBER,
                                                      "current bulk load succeed count");
    _counter_bulk_load_failed_count.init_app_counter("eon.replica_stub",
                                                     "bulk.load.failed.count",
                                                     COUNTER_TYPE_VOLATILE_NUMBER,
                                                     "current bulk load failed count");
    _counter_bulk_load_download_file_succ_count.init_app_counter(
        "eon.replica_stub",
        "bulk.load.download.file.success.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "bulk load recent download file success count");
    _counter_bulk_load_download_file_fail_count.init_app_counter(
        "eon.replica_stub",
        "bulk.load.download.file.fail.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "bulk load recent download file failed count");
    _counter_bulk_load_download_file_size.init_app_counter("eon.replica_stub",
                                                           "bulk.load.download.file.size",
                                                           COUNTER_TYPE_VOLATILE_NUMBER,
                                                           "bulk load recent download file size");
    _counter_bulk_load_max_ingestion_time_ms.init_app_counter(
        "eon.replica_stub",
        "bulk.load.max.ingestion.duration.time.ms",
        COUNTER_TYPE_NUMBER,
        "bulk load max ingestion duration time(ms)");
    _counter_bulk_load_max_duration_time_ms.init_app_counter("eon.replica_stub",
                                                             "bulk.load.max.duration.time.ms",
                                                             COUNTER_TYPE_NUMBER,
                                                             "bulk load max duration time(ms)");

#ifdef DSN_ENABLE_GPERF
    _counter_tcmalloc_release_memory_size.init_app_counter("eon.replica_stub",
                                                           "tcmalloc.release.memory.size",
                                                           COUNTER_TYPE_NUMBER,
                                                           "current tcmalloc release memory size");
#endif

    // <- Partition split Metrics ->

    _counter_replicas_splitting_count.init_app_counter("eon.replica_stub",
                                                       "replicas.splitting.count",
                                                       COUNTER_TYPE_NUMBER,
                                                       "current partition splitting count");

    _counter_replicas_splitting_max_duration_time_ms.init_app_counter(
        "eon.replica_stub",
        "replicas.splitting.max.duration.time(ms)",
        COUNTER_TYPE_NUMBER,
        "current partition splitting max duration time(ms)");
    _counter_replicas_splitting_max_async_learn_time_ms.init_app_counter(
        "eon.replica_stub",
        "replicas.splitting.max.async.learn.time(ms)",
        COUNTER_TYPE_NUMBER,
        "current partition splitting max async learn time(ms)");
    _counter_replicas_splitting_max_copy_file_size.init_app_counter(
        "eon.replica_stub",
        "replicas.splitting.max.copy.file.size",
        COUNTER_TYPE_NUMBER,
        "current splitting max copy file size");
    _counter_replicas_splitting_recent_start_count.init_app_counter(
        "eon.replica_stub",
        "replicas.splitting.recent.start.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "current splitting start count in the recent period");
    _counter_replicas_splitting_recent_copy_file_count.init_app_counter(
        "eon.replica_stub",
        "replicas.splitting.recent.copy.file.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "splitting copy file count in the recent period");
    _counter_replicas_splitting_recent_copy_file_size.init_app_counter(
        "eon.replica_stub",
        "replicas.splitting.recent.copy.file.size",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "splitting copy file size in the recent period");
    _counter_replicas_splitting_recent_copy_mutation_count.init_app_counter(
        "eon.replica_stub",
        "replicas.splitting.recent.copy.mutation.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "splitting copy mutation count in the recent period");
    _counter_replicas_splitting_recent_split_succ_count.init_app_counter(
        "eon.replica_stub",
        "replicas.splitting.recent.split.succ.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "splitting succeed count in the recent period");
    _counter_replicas_splitting_recent_split_fail_count.init_app_counter(
        "eon.replica_stub",
        "replicas.splitting.recent.split.fail.count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "splitting fail count in the recent period");
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
    LOG_INFO("primary_address = %s", _primary_address_str);

    set_options(opts);
    std::ostringstream oss;
    for (int i = 0; i < _options.meta_servers.size(); ++i) {
        if (i != 0)
            oss << ",";
        oss << _options.meta_servers[i].to_string();
    }
    LOG_INFO("meta_servers = %s", oss.str().c_str());

    _deny_client = _options.deny_client_on_start;
    _verbose_client_log = _options.verbose_client_log_on_start;
    _verbose_commit_log = _options.verbose_commit_log_on_start;
    _release_tcmalloc_memory = _options.mem_release_enabled;
    _mem_release_max_reserved_mem_percentage = _options.mem_release_max_reserved_mem_percentage;
    _max_concurrent_bulk_load_downloading_count =
        _options.max_concurrent_bulk_load_downloading_count;

    // clear dirs if need
    if (clear) {
        CHECK(dsn::utils::filesystem::remove_path(_options.slog_dir),
              "Fail to remove {}.",
              _options.slog_dir);
        for (auto &dir : _options.data_dirs) {
            CHECK(dsn::utils::filesystem::remove_path(dir), "Fail to remove {}.", dir);
        }
    }

    // init dirs
    std::string cdir;
    std::string err_msg;
    CHECK(
        dsn::utils::filesystem::create_directory(_options.slog_dir, cdir, err_msg), "{}", err_msg);
    _options.slog_dir = cdir;
    initialize_fs_manager(_options.data_dirs, _options.data_dir_tags);

    _log = new mutation_log_shared(_options.slog_dir,
                                   _options.log_shared_file_size_mb,
                                   _options.log_shared_force_flush,
                                   &_counter_shared_log_recent_write_size);
    LOG_INFO("slog_dir = %s", _options.slog_dir.c_str());

    // init rps
    LOG_INFO("start to load replicas");

    std::vector<std::string> dir_list;
    for (auto &dir : _fs_manager.get_available_data_dirs()) {
        std::vector<std::string> tmp_list;
        CHECK(dsn::utils::filesystem::get_subdirectories(dir, tmp_list, false),
              "Fail to get subdirectories in {}.",
              dir);
        dir_list.insert(dir_list.end(), tmp_list.begin(), tmp_list.end());
    }

    replicas rps;
    utils::ex_lock rps_lock;
    std::deque<task_ptr> load_tasks;
    uint64_t start_time = dsn_now_ms();
    for (auto &dir : dir_list) {
        if (dsn::replication::is_data_dir_invalid(dir)) {
            LOG_INFO_F("ignore dir {}", dir);
            continue;
        }

        load_tasks.push_back(tasking::create_task(
            LPC_REPLICATION_INIT_LOAD,
            &_tracker,
            [this, dir, &rps, &rps_lock] {
                LOG_INFO("process dir %s", dir.c_str());

                auto r = replica::load(this, dir.c_str());
                if (r != nullptr) {
                    LOG_INFO("%s@%s: load replica '%s' success, <durable, commit> = <%" PRId64
                             ", %" PRId64 ">, last_prepared_decree = %" PRId64,
                             r->get_gpid().to_string(),
                             dsn_primary_address().to_string(),
                             dir.c_str(),
                             r->last_durable_decree(),
                             r->last_committed_decree(),
                             r->last_prepared_decree());

                    utils::auto_lock<utils::ex_lock> l(rps_lock);
                    CHECK(rps.find(r->get_gpid()) == rps.end(),
                          "conflict replica dir: {} <--> {}",
                          r->dir(),
                          rps[r->get_gpid()]->dir());

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
    LOG_INFO("load replicas succeed, replica_count = %d, time_used = %" PRIu64 " ms",
             static_cast<int>(rps.size()),
             finish_time - start_time);

    // init shared prepare log
    LOG_INFO("start to replay shared log");

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
        LOG_INFO("replay shared log succeed, time_used = %" PRIu64 " ms", finish_time - start_time);
    } else {
        LOG_ERROR("replay shared log failed, err = %s, time_used = %" PRIu64
                  " ms, clear all logs ...",
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
            CHECK(dsn::utils::filesystem::rename_path(dir, rename_dir),
                  "init_replica: failed to move directory '{}' to '{}'",
                  dir,
                  rename_dir);
            LOG_WARNING("init_replica: {replica_dir_op} succeed to move directory '%s' to '%s'",
                        dir,
                        rename_dir);
            _counter_replicas_recent_replica_move_error_count->increment();
        }
        rps.clear();

        // restart log service
        _log->close();
        _log = nullptr;
        CHECK(utils::filesystem::remove_path(_options.slog_dir),
              "remove directory {} failed",
              _options.slog_dir);
        _log = new mutation_log_shared(_options.slog_dir,
                                       _options.log_shared_file_size_mb,
                                       _options.log_shared_force_flush,
                                       &_counter_shared_log_recent_write_size);
        CHECK_EQ_MSG(_log->open(nullptr, [this](error_code err) { this->handle_log_failure(err); }),
                     ERR_OK,
                     "restart log service failed");
    }

    bool is_log_complete = true;
    for (auto it = rps.begin(); it != rps.end(); ++it) {
        CHECK_EQ_MSG(it->second->background_sync_checkpoint(), ERR_OK, "sync checkpoint failed");

        it->second->reset_prepare_list_after_replay();

        decree pmax = invalid_decree;
        decree pmax_commit = invalid_decree;
        if (it->second->private_log()) {
            pmax = it->second->private_log()->max_decree(it->first);
            pmax_commit = it->second->private_log()->max_commit_on_disk();
        }

        LOG_INFO_F(
            "{}: load replica done, err = {}, durable = {}, committed = {}, "
            "prepared = {}, ballot = {}, "
            "valid_offset_in_plog = {}, max_decree_in_plog = {}, max_commit_on_disk_in_plog = {}, "
            "valid_offset_in_slog = {}",
            it->second->name(),
            err.to_string(),
            it->second->last_durable_decree(),
            it->second->last_committed_decree(),
            it->second->max_prepared_decree(),
            it->second->get_ballot(),
            it->second->get_app()->init_info().init_offset_in_private_log,
            pmax,
            pmax_commit,
            it->second->get_app()->init_info().init_offset_in_shared_log);
    }

    // we will mark all replicas inactive not transient unless all logs are complete
    if (!is_log_complete) {
        LOG_ERROR(
            "logs are not complete for some replicas, which means that shared log is truncated, "
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

    _nfs = dsn::nfs_node::create();
    _nfs->start();

    dist::cmd::register_remote_command_rpc();

    if (_options.delay_for_fd_timeout_on_start) {
        uint64_t now_time_ms = dsn_now_ms();
        uint64_t delay_time_ms =
            (_options.fd_grace_seconds + 3) * 1000; // for more 3 seconds than grace seconds
        if (now_time_ms < dsn::utils::process_start_millis() + delay_time_ms) {
            uint64_t delay = dsn::utils::process_start_millis() + delay_time_ms - now_time_ms;
            LOG_INFO("delay for %" PRIu64 "ms to make failure detector timeout", delay);
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

void replica_stub::initialize_fs_manager(std::vector<std::string> &data_dirs,
                                         std::vector<std::string> &data_dir_tags)
{
    std::string cdir;
    std::string err_msg;
    int count = 0;
    std::vector<std::string> available_dirs;
    std::vector<std::string> available_dir_tags;
    for (auto i = 0; i < data_dir_tags.size(); ++i) {
        std::string &dir = data_dirs[i];
        if (dsn_unlikely(!utils::filesystem::create_directory(dir, cdir, err_msg) ||
                         !utils::filesystem::check_dir_rw(dir, err_msg))) {
            if (FLAGS_ignore_broken_disk) {
                LOG_WARNING_F("data dir[{}] is broken, ignore it, error:{}", dir, err_msg);
            } else {
                CHECK(false, "{}", err_msg);
            }
            continue;
        }
        LOG_INFO_F("data_dirs[{}] = {}", count, cdir);
        available_dirs.emplace_back(cdir);
        available_dir_tags.emplace_back(data_dir_tags[i]);
        count++;
    }

    CHECK_GT_MSG(
        available_dirs.size(), 0, "initialize fs manager failed, no available data directory");
    CHECK_EQ_MSG(_fs_manager.initialize(available_dirs, available_dir_tags, false),
                 dsn::ERR_OK,
                 "initialize fs manager failed");
}

void replica_stub::initialize_start()
{
    if (_is_running) {
        return;
    }

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
                               std::bind(&replica_stub::gc_tcmalloc_memory, this, false),
                               std::chrono::milliseconds(_options.mem_release_check_interval_ms),
                               0,
                               std::chrono::milliseconds(_options.mem_release_check_interval_ms));
#endif

    if (_options.duplication_enabled) {
        _duplication_sync_timer = dsn::make_unique<duplication_sync_timer>(this);
        _duplication_sync_timer->start();
    }

    _backup_server = dsn::make_unique<replica_backup_server>(this);

    // init liveness monitor
    CHECK_EQ(NS_Disconnected, _state);
    if (_options.fd_disabled == false) {
        _failure_detector = std::make_shared<dsn::dist::slave_failure_detector_with_multimaster>(
            _options.meta_servers,
            [this]() { this->on_meta_server_disconnected(); },
            [this]() { this->on_meta_server_connected(); });

        CHECK_EQ_MSG(_failure_detector->start(_options.fd_check_interval_seconds,
                                              _options.fd_beacon_interval_seconds,
                                              _options.fd_lease_seconds,
                                              _options.fd_grace_seconds),
                     ERR_OK,
                     "FD start failed");

        _failure_detector->register_master(_failure_detector->current_server_contact());
    } else {
        _state = NS_Connected;
    }

    _is_running = true;
}

dsn::error_code replica_stub::on_kill_replica(gpid id)
{
    LOG_INFO("kill replica: gpid = %s", id.to_string());
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

replica_ptr replica_stub::get_replica(gpid id) const
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
        LOG_INFO("%s@%s: client = %s, code = %s, timeout = %d",
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
        LOG_INFO("%s@%s: client = %s, code = %s, timeout = %d",
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
        LOG_WARNING("%s@%s: received config proposal %s for %s: not connected, ignore",
                    proposal.config.pid.to_string(),
                    _primary_address_str,
                    enum_to_string(proposal.type),
                    proposal.node.to_string());
        return;
    }

    LOG_INFO("%s@%s: received config proposal %s for %s",
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

void replica_stub::on_query_last_checkpoint(query_last_checkpoint_info_rpc rpc)
{
    const learn_request &request = rpc.request();
    learn_response &response = rpc.response();

    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->on_query_last_checkpoint(response);
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
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
            info.holding_primary_replicas = dir_node->holding_primary_replicas;
            info.holding_secondary_replicas = dir_node->holding_secondary_replicas;
        } else {
            const auto &primary_iter = dir_node->holding_primary_replicas.find(app_id);
            if (primary_iter != dir_node->holding_primary_replicas.end()) {
                info.holding_primary_replicas[app_id] = primary_iter->second;
            }

            const auto &secondary_iter = dir_node->holding_secondary_replicas.find(app_id);
            if (secondary_iter != dir_node->holding_secondary_replicas.end()) {
                info.holding_secondary_replicas[app_id] = secondary_iter->second;
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

void replica_stub::on_disk_migrate(replica_disk_migrate_rpc rpc)
{
    const replica_disk_migrate_request &request = rpc.request();
    replica_disk_migrate_response &response = rpc.response();

    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->disk_migrator()->on_migrate_replica(rpc); // THREAD_POOL_DEFAULT
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::on_query_app_info(query_app_info_rpc rpc)
{
    const query_app_info_request &req = rpc.request();
    query_app_info_response &resp = rpc.response();

    LOG_INFO("got query app info request from (%s)", req.meta_server.to_string());
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

// ThreadPool: THREAD_POOL_DEFAULT
void replica_stub::on_add_new_disk(add_new_disk_rpc rpc)
{
    const auto &disk_str = rpc.request().disk_str;
    auto &resp = rpc.response();
    resp.err = ERR_OK;

    std::vector<std::string> data_dirs;
    std::vector<std::string> data_dir_tags;
    std::string err_msg = "";
    if (disk_str.empty() ||
        !replication_options::get_data_dir_and_tag(
            disk_str, "", "replica", data_dirs, data_dir_tags, err_msg)) {
        resp.err = ERR_INVALID_PARAMETERS;
        resp.__set_err_hint(fmt::format("invalid str({}), err_msg: {}", disk_str, err_msg));
        return;
    }

    for (auto i = 0; i < data_dir_tags.size(); ++i) {
        auto dir = data_dirs[i];
        if (_fs_manager.is_dir_node_available(dir, data_dir_tags[i])) {
            resp.err = ERR_NODE_ALREADY_EXIST;
            resp.__set_err_hint(
                fmt::format("data_dir({}) tag({}) already available", dir, data_dir_tags[i]));
            return;
        }

        if (dsn_unlikely(utils::filesystem::directory_exists(dir) &&
                         !utils::filesystem::is_directory_empty(dir).second)) {
            resp.err = ERR_DIR_NOT_EMPTY;
            resp.__set_err_hint(fmt::format("Disk({}) directory is not empty", dir));
            return;
        }

        std::string cdir;
        if (dsn_unlikely(!utils::filesystem::create_directory(dir, cdir, err_msg) ||
                         !utils::filesystem::check_dir_rw(dir, err_msg))) {
            resp.err = ERR_FILE_OPERATION_FAILED;
            resp.__set_err_hint(err_msg);
            return;
        }

        LOG_INFO_F("Add a new disk in fs_manager, data_dir={}, tag={}", cdir, data_dir_tags[i]);
        _fs_manager.add_new_dir_node(cdir, data_dir_tags[i]);
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
        LOG_WARNING("%s@%s: received group check: not connected, ignore",
                    request.config.pid.to_string(),
                    _primary_address_str);
        return;
    }

    LOG_INFO("%s@%s: received group check, primary = %s, ballot = %" PRId64
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
        LOG_WARNING("%s@%s: received add learner: not connected, ignore",
                    request.config.pid.to_string(),
                    _primary_address_str,
                    request.config.primary.to_string());
        return;
    }

    LOG_INFO("%s@%s: received add learner, primary = %s, ballot = %" PRId64
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
        LOG_WARNING("get disk tag of %s failed: %s", r->dir().c_str(), err.to_string());
    }

    info.__set_manual_compact_status(r->get_manual_compact_status());
}

void replica_stub::get_local_replicas(std::vector<replica_info> &replicas)
{
    zauto_read_lock l(_replicas_lock);
    // local_replicas = replicas + closing_replicas + closed_replicas
    int total_replicas = _replicas.size() + _closing_replicas.size() + _closed_replicas.size();
    replicas.reserve(total_replicas);

    for (auto &pairs : _replicas) {
        replica_ptr &rep = pairs.second;
        // child partition should not sync config from meta server
        // because it is not ready in meta view
        if (rep->status() == partition_status::PS_PARTITION_SPLIT) {
            continue;
        }
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

    LOG_INFO("send query node partitions request to meta server, stored_replicas_count = %d",
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
    LOG_INFO("meta server connected");

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
    LOG_INFO("query node partitions replied, err = %s", err.to_string());

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
            LOG_INFO("resend query node partitions request after %d ms for resp.err = ERR_BUSY",
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
            LOG_INFO("ignore query node partitions response for resp.err = %s",
                     resp.err.to_string());
            return;
        }

        LOG_INFO_F("process query node partitions response for resp.err = ERR_OK, "
                   "partitions_count({}), gc_replicas_count({})",
                   resp.partitions.size(),
                   resp.gc_replicas.size());

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
    CHECK_NE(_state, NS_Connected);
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
        replica->on_config_sync(req.info,
                                req.config,
                                req.__isset.meta_split_status ? req.meta_split_status
                                                              : split_status::NOT_SPLIT);
    } else {
        if (req.config.primary == _primary_address) {
            LOG_INFO("%s@%s: replica not exists on replica server, which is primary, remove it "
                     "from meta server",
                     req.config.pid.to_string(),
                     _primary_address_str);
            remove_replica_on_meta_server(req.info, req.config);
        } else {
            LOG_INFO(
                "%s@%s: replica not exists on replica server, which is not primary, just ignore",
                req.config.pid.to_string(),
                _primary_address_str);
        }
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_stub::on_node_query_reply_scatter2(replica_stub_ptr this_, gpid id)
{
    replica_ptr replica = get_replica(id);
    if (replica != nullptr && replica->status() != partition_status::PS_POTENTIAL_SECONDARY &&
        replica->status() != partition_status::PS_PARTITION_SPLIT) {
        if (replica->status() == partition_status::PS_INACTIVE &&
            dsn_now_ms() - replica->create_time_milliseconds() <
                _options.gc_memory_replica_interval_ms) {
            LOG_INFO("%s: replica not exists on meta server, wait to close", replica->name());
            return;
        }

        LOG_INFO("%s: replica not exists on meta server, remove", replica->name());

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
    LOG_INFO("meta server disconnected");

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
        LOG_ERROR("%s@%s: %s fail: client = %s, code = %s, timeout = %d, status = %s, error = %s",
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
    CHECK(_options.gc_disabled, "");

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
        LOG_WARNING("gc closed replica(%s.%s) failed, no exist data",
                    id.to_string(),
                    closed_info.first.app_type.c_str());
        return;
    }

    LOG_INFO(
        "start to move replica(%s) as garbage, path: %s", id.to_string(), replica_path.c_str());
    char rename_path[1024];
    sprintf(rename_path, "%s.%" PRIu64 ".gar", replica_path.c_str(), dsn_now_us());
    if (!dsn::utils::filesystem::rename_path(replica_path, rename_path)) {
        LOG_WARNING(
            "gc_replica: failed to move directory '%s' to '%s'", replica_path.c_str(), rename_path);

        // if gc the replica failed, add it back
        zauto_write_lock l(_replicas_lock);
        _fs_manager.add_replica(id, replica_path);
        _closed_replicas.emplace(id, closed_info);
    } else {
        LOG_WARNING("gc_replica: {replica_dir_op} succeed to move directory '%s' to '%s'",
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

    LOG_INFO("start to garbage collection, replica_count = %d", (int)rs.size());

    // gc shared prepare log
    //
    // Now that checkpoint is very important for gc, we must be able to trigger checkpoint when
    // necessary.
    // that is, we should be able to trigger memtable flush when necessary.
    //
    // How to trigger memtable flush?
    //   we add a parameter `is_emergency' in dsn_app_async_checkpoint() function, when set true,
    //   the undering storage system should flush memtable as soon as possiable.
    //
    // When to trigger memtable flush?
    //   1. Using `[replication].checkpoint_max_interval_hours' option, we can set max interval time
    //   of two adjacent checkpoints; If the time interval is arrived, then emergency checkpoint
    //   will be triggered.
    //   2. Using `[replication].log_shared_file_count_limit' option, we can set max file count of
    //   shared log; If the limit is exceeded, then emergency checkpoint will be triggered; Instead
    //   of triggering all replicas to do checkpoint, we will only trigger a few of necessary
    //   replicas which block garbage collection of the oldest log file.
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
                LOG_INFO(
                    "gc_shared: gc condition for %s, status = %s, garbage_max_decree = %" PRId64
                    ", last_durable_decree= %" PRId64 ", plog_max_commit_on_disk = %" PRId64 "",
                    rep->name(),
                    enum_to_string(kv.second.status),
                    ri.max_decree,
                    kv.second.last_durable_decree,
                    plog_max_commit_on_disk);
            } else {
                ri.max_decree = kv.second.last_durable_decree;
                LOG_INFO(
                    "gc_shared: gc condition for %s, status = %s, garbage_max_decree = %" PRId64
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
            LOG_INFO("gc_shared: trigger emergency checkpoint by log_shared_file_count_limit, "
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
            LOG_INFO(
                "gc_shared: trigger emergency checkpoint by log_shared_file_count_limit, "
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
    uint64_t bulk_load_running_count = 0;
    uint64_t bulk_load_max_ingestion_time_ms = 0;
    uint64_t bulk_load_max_duration_time_ms = 0;
    uint64_t splitting_count = 0;
    uint64_t splitting_max_duration_time_ms = 0;
    uint64_t splitting_max_async_learn_time_ms = 0;
    uint64_t splitting_max_copy_file_size = 0;
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

            if (rep->get_bulk_loader()->get_bulk_load_status() != bulk_load_status::BLS_INVALID) {
                bulk_load_running_count++;
                bulk_load_max_ingestion_time_ms =
                    std::max(bulk_load_max_ingestion_time_ms, rep->ingestion_duration_ms());
                bulk_load_max_duration_time_ms =
                    std::max(bulk_load_max_duration_time_ms, rep->get_bulk_loader()->duration_ms());
            }
        }
        // splitting_max_copy_file_size, rep->_split_states.copy_file_size
        if (rep->status() == partition_status::PS_PARTITION_SPLIT) {
            splitting_count++;
            splitting_max_duration_time_ms =
                std::max(splitting_max_duration_time_ms, rep->_split_states.total_ms());
            splitting_max_async_learn_time_ms =
                std::max(splitting_max_async_learn_time_ms, rep->_split_states.async_learn_ms());
            splitting_max_copy_file_size =
                std::max(splitting_max_copy_file_size, rep->_split_states.splitting_copy_file_size);
        }
    }

    _counter_replicas_learning_count->set(learning_count);
    _counter_replicas_learning_max_duration_time_ms->set(learning_max_duration_time_ms);
    _counter_replicas_learning_max_copy_file_size->set(learning_max_copy_file_size);
    _counter_cold_backup_running_count->set(cold_backup_running_count);
    _counter_cold_backup_max_duration_time_ms->set(cold_backup_max_duration_time_ms);
    _counter_cold_backup_max_upload_file_size->set(cold_backup_max_upload_file_size);
    _counter_bulk_load_running_count->set(bulk_load_running_count);
    _counter_bulk_load_max_ingestion_time_ms->set(bulk_load_max_ingestion_time_ms);
    _counter_bulk_load_max_duration_time_ms->set(bulk_load_max_duration_time_ms);
    _counter_replicas_splitting_count->set(splitting_count);
    _counter_replicas_splitting_max_duration_time_ms->set(splitting_max_duration_time_ms);
    _counter_replicas_splitting_max_async_learn_time_ms->set(splitting_max_async_learn_time_ms);
    _counter_replicas_splitting_max_copy_file_size->set(splitting_max_copy_file_size);

    LOG_INFO("finish to garbage collection, time_used_ns = %" PRIu64, dsn_now_ns() - start);
}

void replica_stub::on_disk_stat()
{
    LOG_INFO("start to update disk stat");
    uint64_t start = dsn_now_ns();
    disk_cleaning_report report{};

    dsn::replication::disk_remove_useless_dirs(_fs_manager.get_available_data_dirs(), report);
    _fs_manager.update_disk_stat();
    update_disk_holding_replicas();
    update_disks_status();

    _counter_replicas_error_replica_dir_count->set(report.error_replica_count);
    _counter_replicas_garbage_replica_dir_count->set(report.garbage_replica_count);
    _counter_replicas_tmp_replica_dir_count->set(report.disk_migrate_tmp_count);
    _counter_replicas_origin_replica_dir_count->set(report.disk_migrate_origin_count);
    _counter_replicas_recent_replica_remove_dir_count->add(report.remove_dir_count);

    LOG_INFO("finish to update disk stat, time_used_ns = %" PRIu64, dsn_now_ns() - start);
}

task_ptr replica_stub::begin_open_replica(
    const app_info &app,
    gpid id,
    const std::shared_ptr<group_check_request> &group_check,
    const std::shared_ptr<configuration_update_request> &configuration_update)
{
    _replicas_lock.lock_write();

    if (_replicas.find(id) != _replicas.end()) {
        _replicas_lock.unlock_write();
        LOG_INFO("open replica '%s.%s' failed coz replica is already opened",
                 app.app_type.c_str(),
                 id.to_string());
        return nullptr;
    }

    if (_opening_replicas.find(id) != _opening_replicas.end()) {
        _replicas_lock.unlock_write();
        LOG_INFO("open replica '%s.%s' failed coz replica is under opening",
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

            LOG_INFO("open replica '%s.%s' which is to be closed, reopen it",
                     app.app_type.c_str(),
                     id.to_string());

            // open by add learner
            if (group_check != nullptr) {
                on_add_learner(*group_check);
            }
        } else {
            _replicas_lock.unlock_write();
            LOG_INFO("open replica '%s.%s' failed coz replica is under closing",
                     app.app_type.c_str(),
                     id.to_string());
        }
        return nullptr;
    }

    task_ptr task = tasking::enqueue(
        LPC_OPEN_REPLICA,
        &_tracker,
        std::bind(&replica_stub::open_replica, this, app, id, group_check, configuration_update));

    _opening_replicas[id] = task;
    _counter_replicas_opening_count->increment();
    _closed_replicas.erase(id);

    _replicas_lock.unlock_write();
    return task;
}

void replica_stub::open_replica(
    const app_info &app,
    gpid id,
    const std::shared_ptr<group_check_request> &group_check,
    const std::shared_ptr<configuration_update_request> &configuration_update)
{
    std::string dir = get_replica_dir(app.app_type.c_str(), id, false);
    replica_ptr rep = nullptr;
    if (!dir.empty()) {
        // NOTICE: if partition is DDD, and meta select one replica as primary, it will execute the
        // load-process because of a.b.pegasus is exist, so it will never execute the restore
        // process below
        LOG_INFO("%s@%s: start to load replica %s group check, dir = %s",
                 id.to_string(),
                 _primary_address_str,
                 group_check ? "with" : "without",
                 dir.c_str());
        rep = replica::load(this, dir.c_str());

        // if load data failed, re-open the `*.ori` folder which is the origin replica dir of disk
        // migration
        if (rep == nullptr) {
            std::string origin_tmp_dir = get_replica_dir(
                fmt::format("{}{}", app.app_type, replica_disk_migrator::kReplicaDirOriginSuffix)
                    .c_str(),
                id,
                false);
            if (!origin_tmp_dir.empty()) {
                LOG_INFO_F(
                    "mark the dir {} is garbage, start revert and load disk migration origin "
                    "replica data({})",
                    dir,
                    origin_tmp_dir);
                dsn::utils::filesystem::rename_path(dir,
                                                    fmt::format("{}{}", dir, kFolderSuffixGar));

                std::string origin_dir = origin_tmp_dir;
                // revert the origin replica dir
                boost::replace_first(
                    origin_dir, replica_disk_migrator::kReplicaDirOriginSuffix, "");
                dsn::utils::filesystem::rename_path(origin_tmp_dir, origin_dir);
                rep = replica::load(this, origin_dir.c_str());

                FAIL_POINT_INJECT_F("mock_replica_load", [&](string_view) -> void {});
            }
        }
    }

    if (rep == nullptr) {
        // NOTICE: if dir a.b.pegasus does not exist, or .app-info does not exist, but the ballot >
        // 0, or the last_committed_decree > 0, start replica will fail
        if ((configuration_update != nullptr) && (configuration_update->info.is_stateful)) {
            CHECK(configuration_update->config.ballot == 0 &&
                      configuration_update->config.last_committed_decree == 0,
                  "{}@{}: cannot load replica({}.{}), ballot = {}, "
                  "last_committed_decree = {}, but it does not existed!",
                  id.to_string(),
                  _primary_address_str,
                  id.to_string(),
                  app.app_type.c_str(),
                  configuration_update->config.ballot,
                  configuration_update->config.last_committed_decree);
        }

        // NOTICE: only new_replica_group's assign_primary will execute this; if server restart when
        // download restore-data from cold backup media, the a.b.pegasus will move to
        // a.b.pegasus.timestamp.err when replica-server load all the replicas, so restore-flow will
        // do it again

        bool restore_if_necessary =
            ((configuration_update != nullptr) &&
             (configuration_update->type == config_type::CT_ASSIGN_PRIMARY) &&
             (app.envs.find(backup_restore_constant::POLICY_NAME) != app.envs.end()));

        bool is_duplication_follower =
            ((configuration_update != nullptr) &&
             (configuration_update->type == config_type::CT_ASSIGN_PRIMARY) &&
             (app.envs.find(duplication_constants::kDuplicationEnvMasterClusterKey) !=
              app.envs.end()) &&
             (app.envs.find(duplication_constants::kDuplicationEnvMasterMetasKey) !=
              app.envs.end()));

        // NOTICE: when we don't need execute restore-process, we should remove a.b.pegasus
        // directory because it don't contain the valid data dir and also we need create a new
        // replica(if contain valid data, it will execute load-process)

        if (!restore_if_necessary && ::dsn::utils::filesystem::directory_exists(dir)) {
            CHECK(::dsn::utils::filesystem::remove_path(dir),
                  "remove useless directory({}) failed",
                  dir);
        }
        rep = replica::newr(this, id, app, restore_if_necessary, is_duplication_follower);
    }

    if (rep == nullptr) {
        LOG_INFO("%s@%s: open replica failed, erase from opening replicas",
                 id.to_string(),
                 _primary_address_str);
        zauto_write_lock l(_replicas_lock);
        CHECK_GT_MSG(_opening_replicas.erase(id),
                     0,
                     "replica {} is not in _opening_replicas",
                     id.to_string());
        _counter_replicas_opening_count->decrement();
        return;
    }

    {
        zauto_write_lock l(_replicas_lock);
        CHECK_GT_MSG(_opening_replicas.erase(id),
                     0,
                     "replica {} is not in _opening_replicas",
                     id.to_string());
        _counter_replicas_opening_count->decrement();

        CHECK(_replicas.find(id) == _replicas.end(),
              "replica {} is already in _replicas",
              id.to_string());
        _replicas.insert(replicas::value_type(rep->get_gpid(), rep));
        _counter_replicas_count->increment();

        _closed_replicas.erase(id);
    }

    if (nullptr != group_check) {
        rpc::call_one_way_typed(_primary_address,
                                RPC_LEARN_ADD_LEARNER,
                                *group_check,
                                group_check->config.pid.thread_hash());
    } else if (nullptr != configuration_update) {
        rpc::call_one_way_typed(_primary_address,
                                RPC_CONFIG_PROPOSAL,
                                *configuration_update,
                                configuration_update->config.pid.thread_hash());
    }
}

task_ptr replica_stub::begin_close_replica(replica_ptr r)
{
    CHECK(r->status() == partition_status::PS_ERROR ||
              r->status() == partition_status::PS_INACTIVE ||
              r->disk_migrator()->status() >= disk_migration_status::MOVED,
          "invalid state(partition_status={}, migration_status={}) when calling "
          "replica({}) close",
          enum_to_string(r->status()),
          enum_to_string(r->disk_migrator()->status()),
          r->name());

    gpid id = r->get_gpid();

    zauto_write_lock l(_replicas_lock);

    if (_replicas.erase(id) > 0) {
        _counter_replicas_count->decrement();

        int delay_ms = 0;
        if (r->status() == partition_status::PS_INACTIVE) {
            delay_ms = _options.gc_memory_replica_interval_ms;
            LOG_INFO("%s: delay %d milliseconds to close replica, status = PS_INACTIVE",
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
    LOG_INFO("%s: start to close replica", r->name());

    gpid id = r->get_gpid();
    std::string name = r->name();

    r->close();

    {
        zauto_write_lock l(_replicas_lock);
        auto find = _closing_replicas.find(id);
        CHECK(find != _closing_replicas.end(), "replica {} is not in _closing_replicas", name);
        _closed_replicas.emplace(
            id, std::make_pair(std::get<2>(find->second), std::get<3>(find->second)));
        _closing_replicas.erase(find);
        _counter_replicas_closing_count->decrement();
    }

    LOG_INFO("%s: finish to close replica", name.c_str());
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
    LOG_ERROR("handle log failure: %s", err.to_string());
    CHECK(s_not_exit_on_log_failure, "");
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
    register_rpc_handler_with_rpc_holder(RPC_QUERY_LAST_CHECKPOINT_INFO,
                                         "query_last_checkpoint_info",
                                         &replica_stub::on_query_last_checkpoint);
    register_rpc_handler_with_rpc_holder(
        RPC_QUERY_DISK_INFO, "query_disk_info", &replica_stub::on_query_disk_info);
    register_rpc_handler_with_rpc_holder(
        RPC_REPLICA_DISK_MIGRATE, "disk_migrate_replica", &replica_stub::on_disk_migrate);
    register_rpc_handler_with_rpc_holder(
        RPC_QUERY_APP_INFO, "query_app_info", &replica_stub::on_query_app_info);
    register_rpc_handler_with_rpc_holder(RPC_SPLIT_UPDATE_CHILD_PARTITION_COUNT,
                                         "update_child_group_partition_count",
                                         &replica_stub::on_update_child_group_partition_count);
    register_rpc_handler_with_rpc_holder(RPC_SPLIT_NOTIFY_CATCH_UP,
                                         "child_notify_catch_up",
                                         &replica_stub::on_notify_primary_split_catch_up);
    register_rpc_handler_with_rpc_holder(RPC_BULK_LOAD, "bulk_load", &replica_stub::on_bulk_load);
    register_rpc_handler_with_rpc_holder(
        RPC_GROUP_BULK_LOAD, "group_bulk_load", &replica_stub::on_group_bulk_load);
    register_rpc_handler_with_rpc_holder(
        RPC_DETECT_HOTKEY, "detect_hotkey", &replica_stub::on_detect_hotkey);
    register_rpc_handler_with_rpc_holder(
        RPC_ADD_NEW_DISK, "add_new_disk", &replica_stub::on_add_new_disk);

    register_ctrl_command();
}

#if !defined(DSN_ENABLE_GPERF) && defined(DSN_USE_JEMALLOC)
void replica_stub::register_jemalloc_ctrl_command()
{
    _cmds.emplace_back(::dsn::command_manager::instance().register_command(
        {"replica.dump-jemalloc-stats"},
        fmt::format("replica.dump-jemalloc-stats <{}> [buffer size]", kAllJeStatsTypesStr),
        "dump stats of jemalloc",
        [](const std::vector<std::string> &args) {
            if (args.empty()) {
                return std::string("invalid arguments");
            }

            auto type = enum_from_string(args[0].c_str(), je_stats_type::INVALID);
            if (type == je_stats_type::INVALID) {
                return std::string("invalid stats type");
            }

            std::string stats("\n");

            if (args.size() == 1) {
                dsn::je_dump_stats(type, stats);
                return stats;
            }

            uint64_t buf_sz;
            if (!dsn::buf2uint64(args[1], buf_sz)) {
                return std::string("invalid buffer size");
            }

            dsn::je_dump_stats(type, static_cast<size_t>(buf_sz), stats);
            return stats;
        }));
}
#endif

void replica_stub::register_ctrl_command()
{
    /// In simple_kv test, three replica apps are created, which means that three replica_stubs are
    /// initialized in simple_kv test. If we don't use std::call_once, these command are registered
    /// for three times. And in command_manager, one same command is not allowed to be registered
    /// more than twice times. That is why we use std::call_once here. Same situation in
    /// failure_detector::register_ctrl_commands and nfs_client_impl::register_cli_commands
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        _cmds.emplace_back(::dsn::command_manager::instance().register_command(
            {"replica.kill_partition"},
            "replica.kill_partition [app_id [partition_index]]",
            "replica.kill_partition: kill partitions by (all, one app, one partition)",
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
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_command(
            {"replica.deny-client"},
            "replica.deny-client <true|false>",
            "replica.deny-client - control if deny client read & write request",
            [this](const std::vector<std::string> &args) {
                return remote_command_set_bool_flag(_deny_client, "deny-client", args);
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_command(
            {"replica.verbose-client-log"},
            "replica.verbose-client-log <true|false>",
            "replica.verbose-client-log - control if print verbose error log when reply read & "
            "write request",
            [this](const std::vector<std::string> &args) {
                return remote_command_set_bool_flag(
                    _verbose_client_log, "verbose-client-log", args);
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_command(
            {"replica.verbose-commit-log"},
            "replica.verbose-commit-log <true|false>",
            "replica.verbose-commit-log - control if print verbose log when commit mutation",
            [this](const std::vector<std::string> &args) {
                return remote_command_set_bool_flag(
                    _verbose_commit_log, "verbose-commit-log", args);
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_command(
            {"replica.trigger-checkpoint"},
            "replica.trigger-checkpoint [id1,id2,...] (where id is 'app_id' or "
            "'app_id.partition_id')",
            "replica.trigger-checkpoint - trigger replicas to do checkpoint",
            [this](const std::vector<std::string> &args) {
                return exec_command_on_replica(args, true, [this](const replica_ptr &rep) {
                    tasking::enqueue(LPC_PER_REPLICA_CHECKPOINT_TIMER,
                                     rep->tracker(),
                                     std::bind(&replica_stub::trigger_checkpoint, this, rep, true),
                                     rep->get_gpid().thread_hash());
                    return std::string("triggered");
                });
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_command(
            {"replica.query-compact"},
            "replica.query-compact [id1,id2,...] (where id is 'app_id' or 'app_id.partition_id')",
            "replica.query-compact - query full compact status on the underlying storage engine",
            [this](const std::vector<std::string> &args) {
                return exec_command_on_replica(args, true, [](const replica_ptr &rep) {
                    return rep->query_manual_compact_state();
                });
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_command(
            {"replica.query-app-envs"},
            "replica.query-app-envs [id1,id2,...] (where id is 'app_id' or 'app_id.partition_id')",
            "replica.query-app-envs - query app envs on the underlying storage engine",
            [this](const std::vector<std::string> &args) {
                return exec_command_on_replica(args, true, [](const replica_ptr &rep) {
                    std::map<std::string, std::string> kv_map;
                    rep->query_app_envs(kv_map);
                    return dsn::utils::kv_map_to_string(kv_map, ',', '=');
                });
            }));

#ifdef DSN_ENABLE_GPERF
        _cmds.emplace_back(::dsn::command_manager::instance().register_command(
            {"replica.release-tcmalloc-memory"},
            "replica.release-tcmalloc-memory <true|false>",
            "replica.release-tcmalloc-memory - control if try to release tcmalloc memory",
            [this](const std::vector<std::string> &args) {
                return remote_command_set_bool_flag(
                    _release_tcmalloc_memory, "release-tcmalloc-memory", args);
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_command(
            {"replica.get-tcmalloc-status"},
            "replica.get-tcmalloc-status - get status of tcmalloc",
            "get status of tcmalloc",
            [](const std::vector<std::string> &args) {
                char buf[4096];
                MallocExtension::instance()->GetStats(buf, 4096);
                return std::string(buf);
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_command(
            {"replica.mem-release-max-reserved-percentage"},
            "replica.mem-release-max-reserved-percentage [num | DEFAULT]",
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
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_command(
            {"replica.release-all-reserved-memory"},
            "replica.release-all-reserved-memory - release tcmalloc all reserved-not-used memory",
            "release tcmalloc all reserverd not-used memory back to operating system",
            [this](const std::vector<std::string> &args) {
                auto release_bytes = gc_tcmalloc_memory(true);
                return "OK, release_bytes=" + std::to_string(release_bytes);
            }));
#elif defined(DSN_USE_JEMALLOC)
        register_jemalloc_ctrl_command();
#endif
        _cmds.emplace_back(::dsn::command_manager::instance().register_command(
            {"replica.max-concurrent-bulk-load-downloading-count"},
            "replica.max-concurrent-bulk-load-downloading-count [num | DEFAULT]",
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
            }));
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
    if (!_is_running) {
        return;
    }

    _tracker.cancel_outstanding_tasks();

    // this replica may not be opened
    // or is already closed by calling tool_app::stop_all_apps()
    // in this case, just return
    if (_cmds.empty()) {
        return;
    }
    _cmds.clear();

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
                CHECK_NE_MSG(tmp_gpid,
                             _closing_replicas.begin()->first,
                             "this replica '{}' should has been removed",
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
    _is_running = false;
}

std::string replica_stub::get_replica_dir(const char *app_type, gpid id, bool create_new)
{
    std::string gpid_str = fmt::format("{}.{}", id, app_type);
    std::string replica_dir;
    bool is_dir_exist = false;
    for (const std::string &data_dir : _fs_manager.get_available_data_dirs()) {
        std::string dir = utils::filesystem::path_combine(data_dir, gpid_str);
        if (utils::filesystem::directory_exists(dir)) {
            CHECK(!is_dir_exist, "replica dir conflict: {} <--> {}", dir, replica_dir);
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
    for (const std::string &data_dir : _fs_manager.get_available_data_dirs()) {
        std::string dir = utils::filesystem::path_combine(data_dir, gpid_str);
        // <parent_dir> = <prefix>/<gpid>.<app_type>
        // check if <parent_dir>'s <prefix> is equal to <data_dir>
        if (parent_dir.substr(0, data_dir.size() + 1) == data_dir + "/") {
            child_dir = dir;
            _fs_manager.add_replica(child_pid, child_dir);
            break;
        }
    }
    CHECK(!child_dir.empty(), "can not find parent_dir {} in data_dirs", parent_dir);
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
        LOG_ERROR_F("Failed to get tcmalloc property {}", prop);
        return -1;
    }
    return value;
}

uint64_t replica_stub::gc_tcmalloc_memory(bool release_all)
{
    auto tcmalloc_released_bytes = 0;
    if (!_release_tcmalloc_memory) {
        _is_releasing_memory.store(false);
        _counter_tcmalloc_release_memory_size->set(tcmalloc_released_bytes);
        return tcmalloc_released_bytes;
    }

    if (_is_releasing_memory.load()) {
        LOG_WARNING_F("This node is releasing memory...");
        return tcmalloc_released_bytes;
    }

    _is_releasing_memory.store(true);
    int64_t total_allocated_bytes =
        get_tcmalloc_numeric_property("generic.current_allocated_bytes");
    int64_t reserved_bytes = get_tcmalloc_numeric_property("tcmalloc.pageheap_free_bytes");
    if (total_allocated_bytes == -1 || reserved_bytes == -1) {
        return tcmalloc_released_bytes;
    }

    int64_t max_reserved_bytes =
        release_all ? 0
                    : (total_allocated_bytes * _mem_release_max_reserved_mem_percentage / 100.0);
    if (reserved_bytes > max_reserved_bytes) {
        int64_t release_bytes = reserved_bytes - max_reserved_bytes;
        tcmalloc_released_bytes = release_bytes;
        LOG_INFO_F("Memory release started, almost {} bytes will be released", release_bytes);
        while (release_bytes > 0) {
            // tcmalloc releasing memory will lock page heap, release 1MB at a time to avoid locking
            // page heap for long time
            ::MallocExtension::instance()->ReleaseToSystem(1024 * 1024);
            release_bytes -= 1024 * 1024;
        }
    }
    _counter_tcmalloc_release_memory_size->set(tcmalloc_released_bytes);
    _is_releasing_memory.store(false);
    return tcmalloc_released_bytes;
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
        LOG_INFO_F("app({}), create child replica ({}) succeed", app.app_name, child_gpid);
        tasking::enqueue(LPC_PARTITION_SPLIT,
                         child_replica->tracker(),
                         std::bind(&replica_split_manager::child_init_replica,
                                   child_replica->get_split_manager(),
                                   parent_gpid,
                                   primary_address,
                                   init_ballot),
                         child_gpid.thread_hash());
    } else {
        LOG_WARNING_F("failed to create child replica ({}), ignore it and wait next run",
                      child_gpid);
        split_replica_error_handler(
            parent_gpid,
            std::bind(&replica_split_manager::parent_cleanup_split_context, std::placeholders::_1));
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
                            LOG_INFO_F("mock create_child_replica_if_not_found succeed");
                            return rep;
                        });

    zauto_write_lock l(_replicas_lock);
    auto it = _replicas.find(child_pid);
    if (it != _replicas.end()) {
        return it->second;
    } else {
        if (_opening_replicas.find(child_pid) != _opening_replicas.end()) {
            LOG_WARNING_F("failed create child replica({}) because it is under open", child_pid);
            return nullptr;
        } else if (_closing_replicas.find(child_pid) != _closing_replicas.end()) {
            LOG_WARNING_F("failed create child replica({}) because it is under close", child_pid);
            return nullptr;
        } else {
            replica *rep = replica::newr(this, child_pid, *app, false, false, parent_dir);
            if (rep != nullptr) {
                auto pr = _replicas.insert(replicas::value_type(child_pid, rep));
                CHECK(pr.second, "child replica {} has been existed", rep->name());
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
                         [handler, replica]() { handler(replica->get_split_manager()); },
                         pid.thread_hash());
        return ERR_OK;
    }
    LOG_WARNING_F("replica({}) is invalid", pid);
    return ERR_OBJECT_NOT_FOUND;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_stub::on_notify_primary_split_catch_up(notify_catch_up_rpc rpc)
{
    const notify_catch_up_request &request = rpc.request();
    notify_cacth_up_response &response = rpc.response();
    replica_ptr replica = get_replica(request.parent_gpid);
    if (replica != nullptr) {
        replica->get_split_manager()->parent_handle_child_catch_up(request, response);
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_stub::on_update_child_group_partition_count(update_child_group_partition_count_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    replica_ptr replica = get_replica(request.child_pid);
    if (replica != nullptr) {
        replica->get_split_manager()->on_update_child_group_partition_count(request, response);
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

    LOG_INFO_F("[{}@{}]: receive bulk load request", request.pid, _primary_address_str);
    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->get_bulk_loader()->on_bulk_load(request, response);
    } else {
        LOG_ERROR_F("replica({}) is not existed", request.pid);
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::on_group_bulk_load(group_bulk_load_rpc rpc)
{
    const group_bulk_load_request &request = rpc.request();
    group_bulk_load_response &response = rpc.response();

    LOG_INFO_F("[{}@{}]: received group bulk load request, primary = {}, ballot = {}, "
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
        LOG_ERROR_F("replica({}) is not existed", request.config.pid);
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::on_detect_hotkey(detect_hotkey_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    LOG_INFO_F("[{}@{}]: received detect hotkey request, hotkey_type = {}, detect_action = {}",
               request.pid,
               _primary_address_str,
               enum_to_string(request.type),
               enum_to_string(request.action));

    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->on_detect_hotkey(request, response);
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
        response.err_hint = fmt::format("not find the replica {} \n", request.pid);
    }
}

void replica_stub::query_app_data_version(
    int32_t app_id, /*pidx => data_version*/ std::unordered_map<int32_t, uint32_t> &version_map)
{
    zauto_read_lock l(_replicas_lock);
    for (const auto &kv : _replicas) {
        if (kv.first.get_app_id() == app_id) {
            replica_ptr rep = kv.second;
            if (rep != nullptr) {
                uint32_t data_version = rep->query_data_version();
                version_map[kv.first.get_partition_index()] = data_version;
            }
        }
    }
}

void replica_stub::query_app_manual_compact_status(
    int32_t app_id, std::unordered_map<gpid, manual_compaction_status::type> &status)
{
    zauto_read_lock l(_replicas_lock);
    for (auto it = _replicas.begin(); it != _replicas.end(); ++it) {
        if (it->first.get_app_id() == app_id) {
            status[it->first] = it->second->get_manual_compact_status();
        }
    }
}

void replica_stub::update_disks_status()
{
    for (const auto &dir_node : _fs_manager._status_updated_dir_nodes) {
        for (const auto &holding_replicas : dir_node->holding_replicas) {
            const std::set<gpid> &pids = holding_replicas.second;
            for (const auto &pid : pids) {
                replica_ptr replica = get_replica(pid);
                if (replica == nullptr) {
                    continue;
                }
                replica->set_disk_status(dir_node->status);
                LOG_INFO_F("{} update disk_status to {}",
                           replica->name(),
                           enum_to_string(replica->get_disk_status()));
            }
        }
    }
}

} // namespace replication
} // namespace dsn
