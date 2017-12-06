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
#include <dsn/cpp/json_helper.h>
#include <dsn/utility/filesystem.h>
#include <dsn/tool-api/command_manager.h>
#include <dsn/dist/replication/replication_app_base.h>
#include <vector>
#include <deque>

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "replica.stub"

namespace dsn {
namespace replication {

using namespace dsn::service;

bool replica_stub::s_not_exit_on_log_failure = false;

replica_stub::replica_stub(replica_state_subscriber subscriber /*= nullptr*/,
                           bool is_long_subscriber /* = true*/)
    : serverlet("replica_stub"),
      _replicas_lock(true),
      _kill_partition_command(nullptr),
      _deny_client_command(nullptr),
      _verbose_client_log_command(nullptr),
      _verbose_commit_log_command(nullptr),
      _trigger_chkpt_command(nullptr),
      _deny_client(false),
      _verbose_client_log(false),
      _verbose_commit_log(false),
      _learn_app_concurrent_count(0),
      _fs_manager(false)
{
    _replica_state_subscriber = subscriber;
    _is_long_subscriber = is_long_subscriber;
    _failure_detector = nullptr;
    _state = NS_Disconnected;
    _log = nullptr;
    install_perf_counters();
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
    _counter_replicas_total_commit_throught.init_app_counter(
        "eon.replica_stub",
        "replicas.commit.qps",
        COUNTER_TYPE_RATE,
        "app commit throughput for all replicas");

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

    _counter_shared_log_size.init_app_counter(
        "eon.replica_stub", "shared.log.size(MB)", COUNTER_TYPE_NUMBER, "shared log size(MB)");

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
}

void replica_stub::initialize(bool clear /* = false*/)
{
    replication_options opts;
    opts.initialize();
    initialize(opts, clear);
}

void replica_stub::initialize(const replication_options &opts, bool clear /* = false*/)
{
    _primary_address = primary_address();
    ddebug("primary_address = %s", _primary_address.to_string());

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

    _log = new mutation_log_shared(
        _options.slog_dir, _options.log_shared_file_size_mb, _options.log_shared_force_flush);
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
            this,
            [this, dir, &rps, &rps_lock] {
                ddebug("process dir %s", dir.c_str());

                auto r = replica::load(this, dir.c_str());
                if (r != nullptr) {
                    ddebug("%d.%d@%s: load replica '%s' success, <durable, commit> = <%" PRId64
                           ", %" PRId64 ">, last_prepared_decree = %" PRId64,
                           r->get_gpid().get_app_id(),
                           r->get_gpid().get_partition_index(),
                           primary_address().to_string(),
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
        _log = new mutation_log_shared(
            _options.slog_dir, _options.log_shared_file_size_mb, _options.log_shared_force_flush);
        auto lerr = _log->open(nullptr, [this](error_code err) { this->handle_log_failure(err); });
        dassert(lerr == ERR_OK, "restart log service must succeed");
    }

    bool is_log_complete = true;
    for (auto it = rps.begin(); it != rps.end(); ++it) {
        it->second->sync_checkpoint();
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
        _gc_timer_task =
            tasking::enqueue_timer(LPC_GARBAGE_COLLECT_LOGS_AND_REPLICAS,
                                   this,
                                   [this] { on_gc(); },
                                   std::chrono::milliseconds(_options.gc_interval_ms),
                                   0,
                                   std::chrono::milliseconds(random32(0, _options.gc_interval_ms)));
    }

    // disk stat
    if (false == _options.disk_stat_disabled) {
        _disk_stat_timer_task = ::dsn::tasking::enqueue_timer(
            LPC_DISK_STAT,
            this,
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

    if (_options.delay_for_fd_timeout_on_start) {
        uint64_t now_time_ms = now_ms();
        uint64_t delay_time_ms =
            (_options.fd_grace_seconds + 3) * 1000; // for more 3 seconds than grace seconds
        if (now_time_ms < dsn_runtime_init_time_ms() + delay_time_ms) {
            uint64_t delay = dsn_runtime_init_time_ms() + delay_time_ms - now_time_ms;
            ddebug("delay for %" PRIu64 "ms to make failure detector timeout", delay);
            tasking::enqueue(LPC_REPLICA_SERVER_DELAY_START,
                             this,
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
                                   this,
                                   [this]() {
                                       zauto_lock l(_replicas_lock);
                                       this->query_configuration_by_node();
                                   },
                                   std::chrono::milliseconds(_options.config_sync_interval_ms),
                                   0,
                                   std::chrono::milliseconds(_options.config_sync_interval_ms));
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

dsn::error_code replica_stub::on_kill_replica(gpid pid)
{
    error_code err = ERR_INVALID_PARAMETERS;
    replica_ptr r = get_replica(pid);
    if (r == nullptr) {
        err = ERR_OBJECT_NOT_FOUND;
    } else {
        r->inject_error(ERR_INJECTED);
        err = ERR_OK;
    }
    return err;
}

replica_ptr replica_stub::get_replica(gpid gpid, bool new_when_possible, const app_info *app)
{
    zauto_lock l(_replicas_lock);
    auto it = _replicas.find(gpid);
    if (it != _replicas.end())
        return it->second;
    else {
        if (!new_when_possible)
            return nullptr;
        else if (_opening_replicas.find(gpid) != _opening_replicas.end()) {
            ddebug("cannot create new replica coz it is under open");
            return nullptr;
        } else if (_closing_replicas.find(gpid) != _closing_replicas.end()) {
            ddebug("cannot create new replica coz it is under close");
            return nullptr;
        } else {
            dassert(app, "");
            replica *rep = replica::newr(this, gpid, *app, false);
            if (rep != nullptr) {
                add_replica(rep);
                _closed_replicas.erase(gpid);
            }
            return rep;
        }
    }
}

replica_ptr replica_stub::get_replica(int32_t app_id, int32_t partition_index)
{
    gpid gpid;
    gpid.set_app_id(app_id);
    gpid.set_partition_index(partition_index);
    return get_replica(gpid);
}

replica_stub::replica_life_cycle replica_stub::get_replica_life_cycle(const gpid &pid,
                                                                      bool lock_protected)
{
    dassert(lock_protected, "this can only be visited in the _replicas_lock");
    if (_opening_replicas.find(pid) != _opening_replicas.end())
        return replica_stub::RL_creating;
    if (_replicas.find(pid) != _replicas.end())
        return replica_stub::RL_serving;
    if (_closing_replicas.find(pid) != _closing_replicas.end())
        return replica_stub::RL_closing;
    if (_closed_replicas.find(pid) != _closed_replicas.end())
        return replica_stub::RL_closed;
    return replica_stub::RL_invalid;
}

void replica_stub::on_client_write(gpid gpid, dsn_message_t request)
{
    if (_deny_client) {
        // ignore and do not reply
        return;
    }
    replica_ptr rep = get_replica(gpid);
    if (rep != nullptr) {
        rep->on_client_write(task_code(dsn_msg_task_code(request)), request);
    } else {
        response_client_error(gpid, false, request, ERR_OBJECT_NOT_FOUND);
    }
}

void replica_stub::on_client_read(gpid gpid, dsn_message_t request)
{
    if (_deny_client) {
        // ignore and do not reply
        return;
    }
    replica_ptr rep = get_replica(gpid);
    if (rep != nullptr) {
        rep->on_client_read(task_code(dsn_msg_task_code(request)), request);
    } else {
        response_client_error(gpid, true, request, ERR_OBJECT_NOT_FOUND);
    }
}

void replica_stub::on_config_proposal(const configuration_update_request &proposal)
{
    if (!is_connected()) {
        dwarn("%d.%d@%s: received config proposal %s for %s: not connected, ignore",
              proposal.config.pid.get_app_id(),
              proposal.config.pid.get_partition_index(),
              _primary_address.to_string(),
              enum_to_string(proposal.type),
              proposal.node.to_string());
        return;
    }

    ddebug("%d.%d@%s: received config proposal %s for %s",
           proposal.config.pid.get_app_id(),
           proposal.config.pid.get_partition_index(),
           _primary_address.to_string(),
           enum_to_string(proposal.type),
           proposal.node.to_string());

    replica_ptr rep = get_replica(proposal.config.pid, false, &proposal.info);
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

void replica_stub::on_query_decree(const query_replica_decree_request &req,
                                   /*out*/ query_replica_decree_response &resp)
{
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

void replica_stub::on_query_replica_info(const query_replica_info_request &req,
                                         /*out*/ query_replica_info_response &resp)
{
    std::set<gpid> visited_replicas;
    {
        zauto_lock l(_replicas_lock);
        for (auto it = _replicas.begin(); it != _replicas.end(); ++it) {
            replica_ptr r = it->second;
            replica_info info;
            get_replica_info(info, r);
            if (visited_replicas.find(info.pid) == visited_replicas.end()) {
                visited_replicas.insert(info.pid);
                resp.replicas.push_back(std::move(info));
            }
        }
        for (auto it = _closing_replicas.begin(); it != _closing_replicas.end(); ++it) {
            replica_ptr r = it->second.second;
            replica_info info;
            get_replica_info(info, r);
            if (visited_replicas.find(info.pid) == visited_replicas.end()) {
                visited_replicas.insert(info.pid);
                resp.replicas.push_back(std::move(info));
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

void replica_stub::on_query_app_info(const query_app_info_request &req,
                                     query_app_info_response &resp)
{
    ddebug("got query app info request from (%s)", req.meta_server.to_string());
    resp.err = dsn::ERR_OK;
    std::set<app_id> visited_apps;
    {
        zauto_lock l(_replicas_lock);
        for (auto it = _replicas.begin(); it != _replicas.end(); ++it) {
            replica_ptr r = it->second;
            const app_info &info = *r->get_app_info();
            if (visited_apps.find(info.app_id) == visited_apps.end()) {
                resp.apps.push_back(info);
                visited_apps.insert(info.app_id);
            }
        }
        for (auto it = _closing_replicas.begin(); it != _closing_replicas.end(); ++it) {
            replica_ptr r = it->second.second;
            const app_info &info = *r->get_app_info();
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

void replica_stub::on_cold_backup(const backup_request &request, /*out*/ backup_response &response)
{
    ddebug("received cold backup request: backup{%d.%d.%s.%" PRId64 "}",
           request.pid.get_app_id(),
           request.pid.get_partition_index(),
           request.policy.policy_name.c_str(),
           request.backup_id);
    response.pid = request.pid;
    response.policy_name = request.policy.policy_name;
    response.backup_id = request.backup_id;

    if (_options.cold_backup_root.empty()) {
        derror("backup{%d.%d.%s.%" PRId64
               "}: cold_backup_root is empty, response ERR_OPERATION_DISABLED",
               request.pid.get_app_id(),
               request.pid.get_partition_index(),
               request.policy.policy_name.c_str(),
               request.backup_id);
        response.err = ERR_OPERATION_DISABLED;
        return;
    }

    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->on_cold_backup(request, response);
    } else {
        derror("backup{%d.%d.%s.%" PRId64 "}: replica not found, response ERR_OBJECT_NOT_FOUND",
               request.pid.get_app_id(),
               request.pid.get_partition_index(),
               request.policy.policy_name.c_str(),
               request.backup_id);
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::on_prepare(dsn_message_t request)
{
    gpid gpid;
    dsn::unmarshall(request, gpid);
    replica_ptr rep = get_replica(gpid);
    if (rep != nullptr) {
        rep->on_prepare(request);
    } else {
        prepare_ack resp;
        resp.pid = gpid;
        resp.err = ERR_OBJECT_NOT_FOUND;
        reply(request, resp);
    }
}

void replica_stub::on_group_check(const group_check_request &request,
                                  /*out*/ group_check_response &response)
{
    if (!is_connected()) {
        dwarn("%d.%d@%s: received group check: not connected, ignore",
              request.config.pid.get_app_id(),
              request.config.pid.get_partition_index(),
              _primary_address.to_string());
        return;
    }

    ddebug("%d.%d@%s: received group check, primary = %s, ballot = %" PRId64
           ", status = %s, last_committed_decree = %" PRId64,
           request.config.pid.get_app_id(),
           request.config.pid.get_partition_index(),
           _primary_address.to_string(),
           request.config.primary.to_string(),
           request.config.ballot,
           enum_to_string(request.config.status),
           request.last_committed_decree);

    replica_ptr rep = get_replica(request.config.pid, false, &request.app);
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

void replica_stub::on_learn(dsn_message_t msg)
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

void replica_stub::on_copy_checkpoint(const replica_configuration &request,
                                      /*out*/ learn_response &response)
{
    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->on_copy_checkpoint(request, response);
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::on_learn_completion_notification(const group_check_response &report,
                                                    /*out*/ learn_notify_response &response)
{
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
        dwarn("%d.%d@%s: received add learner: not connected, ignore",
              request.config.pid.get_app_id(),
              request.config.pid.get_partition_index(),
              _primary_address.to_string(),
              request.config.primary.to_string());
        return;
    }

    ddebug("%d.%d@%s: received add learner, primary = %s, ballot = %" PRId64
           ", status = %s, last_committed_decree = %" PRId64,
           request.config.pid.get_app_id(),
           request.config.pid.get_partition_index(),
           _primary_address.to_string(),
           request.config.primary.to_string(),
           request.config.ballot,
           enum_to_string(request.config.status),
           request.last_committed_decree);

    replica_ptr rep = get_replica(request.config.pid, false, &request.app);
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

void replica_stub::get_local_replicas(std::vector<replica_info> &replicas, bool lock_protected)
{
    dassert(lock_protected, "this should be visited in the protection of replica_lock");

    // local_replicas = replicas + closing_replicas + closed_replicas
    int total_replicas = _replicas.size() + _closing_replicas.size() + _closed_replicas.size();
    replicas.reserve(total_replicas);
    replica_info info;

    for (auto &pairs : _replicas) {
        replica_ptr &rep = pairs.second;
        get_replica_info(info, rep);
        replicas.push_back(info);
    }

    for (auto &pairs : _closing_replicas) {
        replica_ptr &rep = pairs.second.second;
        get_replica_info(info, rep);
        replicas.push_back(info);
    }

    for (auto &pairs : _closed_replicas) {
        replicas.push_back(pairs.second.second);
    }
}

void replica_stub::query_configuration_by_node()
{
    if (_state == NS_Disconnected) {
        return;
    }

    if (_config_query_task != nullptr) {
        return;
    }

    dsn_message_t msg = dsn_msg_create_request(RPC_CM_CONFIG_SYNC);

    configuration_query_by_node_request req;
    req.node = _primary_address;

    // TODO: send stored replicas may cost network, we shouldn't config the frequency
    get_local_replicas(req.stored_replicas, true);
    req.__isset.stored_replicas = true;

    ::dsn::marshall(msg, req);

    ddebug("send query node partitions request to meta server, stored_replicas_count = %d",
           (int)req.stored_replicas.size());

    rpc_address target(_failure_detector->get_servers());
    _config_query_task = rpc::call(
        target, msg, this, [this](error_code err, dsn_message_t request, dsn_message_t resp) {
            on_node_query_reply(err, request, resp);
        });
}

void replica_stub::on_meta_server_connected()
{
    ddebug("meta server connected");

    zauto_lock l(_replicas_lock);
    if (_state == NS_Disconnected) {
        _state = NS_Connecting;
        query_configuration_by_node();
    }
}

void replica_stub::on_node_query_reply(error_code err,
                                       dsn_message_t request,
                                       dsn_message_t response)
{
    ddebug("query node partitions replied, err = %s", err.to_string());

    zauto_lock l(_replicas_lock);
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
                                                  this,
                                                  [this]() {
                                                      zauto_lock l(_replicas_lock);
                                                      _config_query_task = nullptr;
                                                      query_configuration_by_node();
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

        replicas rs = _replicas;
        for (auto it = resp.partitions.begin(); it != resp.partitions.end(); ++it) {
            rs.erase(it->config.pid);
            tasking::enqueue(LPC_QUERY_NODE_CONFIGURATION_SCATTER,
                             this,
                             std::bind(&replica_stub::on_node_query_reply_scatter, this, this, *it),
                             gpid_to_thread_hash(it->config.pid));
        }

        // for rps not exist on meta_servers
        for (auto it = rs.begin(); it != rs.end(); ++it) {
            tasking::enqueue(
                LPC_QUERY_NODE_CONFIGURATION_SCATTER,
                this,
                std::bind(&replica_stub::on_node_query_reply_scatter2, this, this, it->first),
                gpid_to_thread_hash(it->first));
        }

        // handle the replicas which need to be gc
        if (resp.__isset.gc_replicas) {
            for (replica_info &rep : resp.gc_replicas) {
                replica_stub::replica_life_cycle lc = get_replica_life_cycle(rep.pid, true);
                if (lc == replica_stub::RL_closed) {
                    tasking::enqueue(LPC_GARBAGE_COLLECT_LOGS_AND_REPLICAS,
                                     this,
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
    zauto_lock l(_replicas_lock);
    dassert(_state != NS_Connected, "");
    _state = NS_Connected;

    for (auto it = resp.partitions.begin(); it != resp.partitions.end(); ++it) {
        tasking::enqueue(LPC_QUERY_NODE_CONFIGURATION_SCATTER,
                         this,
                         std::bind(&replica_stub::on_node_query_reply_scatter, this, this, *it),
                         gpid_to_thread_hash(it->config.pid));
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
void replica_stub::on_node_query_reply_scatter(replica_stub_ptr this_,
                                               const configuration_update_request &req)
{
    replica_ptr replica = get_replica(req.config.pid);
    if (replica != nullptr) {
        replica->on_config_sync(req.config);
    } else {
        if (req.config.primary == _primary_address) {
            ddebug("%d.%d@%s: replica not exists on replica server, which is primary, remove it "
                   "from meta server",
                   req.config.pid.get_app_id(),
                   req.config.pid.get_partition_index(),
                   _primary_address.to_string());
            remove_replica_on_meta_server(req.info, req.config);
        } else {
            ddebug(
                "%d.%d@%s: replica not exists on replica server, which is not primary, just ignore",
                req.config.pid.get_app_id(),
                req.config.pid.get_partition_index(),
                _primary_address.to_string());
        }
    }
}

void replica_stub::on_node_query_reply_scatter2(replica_stub_ptr this_, gpid gpid)
{
    replica_ptr replica = get_replica(gpid);
    if (replica != nullptr && replica->status() != partition_status::PS_POTENTIAL_SECONDARY) {
        if (replica->status() == partition_status::PS_INACTIVE &&
            now_ms() - replica->create_time_milliseconds() <
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
    dsn_message_t msg = dsn_msg_create_request(RPC_CM_UPDATE_PARTITION_CONFIGURATION);

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
              [](error_code err, dsn_message_t, dsn_message_t) {});
}

void replica_stub::on_meta_server_disconnected()
{
    ddebug("meta server disconnected");

    zauto_lock l(_replicas_lock);
    if (NS_Disconnected == _state)
        return;

    _state = NS_Disconnected;

    for (auto it = _replicas.begin(); it != _replicas.end(); ++it) {
        tasking::enqueue(
            LPC_CM_DISCONNECTED_SCATTER,
            this,
            std::bind(&replica_stub::on_meta_server_disconnected_scatter, this, this, it->first),
            gpid_to_thread_hash(it->first));
    }
}

// this_ is used to hold a ref to replica_stub so we don't need to cancel the task on
// replica_stub::close
void replica_stub::on_meta_server_disconnected_scatter(replica_stub_ptr this_, gpid gpid)
{
    {
        zauto_lock l(_replicas_lock);
        if (_state != NS_Disconnected)
            return;
    }

    replica_ptr replica = get_replica(gpid);
    if (replica != nullptr) {
        replica->on_meta_server_disconnected();
    }
}

void replica_stub::response_client_error(gpid gpid,
                                         bool is_read,
                                         dsn_message_t request,
                                         error_code error)
{
    if (nullptr == request) {
        return;
    }

    if (error == ERR_OK) {
        dinfo("%d.%d@%s: reply client %s to %s, err = %s",
              gpid.get_app_id(),
              gpid.get_partition_index(),
              _primary_address.to_string(),
              is_read ? "read" : "write",
              dsn_address_to_string(dsn_msg_from_address(request)),
              error.to_string());
    } else {
        derror("%d.%d@%s: reply client %s to %s, err = %s",
               gpid.get_app_id(),
               gpid.get_partition_index(),
               _primary_address.to_string(),
               is_read ? "read" : "write",
               dsn_address_to_string(dsn_msg_from_address(request)),
               error.to_string());
    }
    dsn_rpc_reply(dsn_msg_create_response(request), error);
}

void replica_stub::init_gc_for_test()
{
    dassert(_options.gc_disabled, "");

    _gc_timer_task = tasking::enqueue(LPC_GARBAGE_COLLECT_LOGS_AND_REPLICAS,
                                      this,
                                      [this] { on_gc(); },
                                      0,
                                      std::chrono::milliseconds(_options.gc_interval_ms));
}

void replica_stub::on_gc_replica(replica_stub_ptr this_, gpid pid)
{
    std::string replica_path;
    std::pair<app_info, replica_info> closed_info;

    {
        zauto_lock l(_replicas_lock);
        auto iter = _closed_replicas.find(pid);
        if (iter == _closed_replicas.end())
            return;
        closed_info = iter->second;
        _closed_replicas.erase(iter);
        _fs_manager.remove_replica(pid);
    }

    replica_path = get_replica_dir(closed_info.first.app_type.c_str(), pid, false);
    if (replica_path.empty()) {
        dwarn("gc closed replica(%d.%d.%s) failed, no exist data",
              pid.get_app_id(),
              pid.get_partition_index(),
              closed_info.first.app_type.c_str());
        return;
    }

    ddebug("start to move replica(%d.%d) as garbage, path: %s",
           pid.get_app_id(),
           pid.get_partition_index(),
           replica_path.c_str());
    char rename_path[1024];
    sprintf(rename_path, "%s.%" PRIu64 ".gar", replica_path.c_str(), dsn_now_us());
    if (!dsn::utils::filesystem::rename_path(replica_path, rename_path)) {
        dwarn(
            "gc_replica: failed to move directory '%s' to '%s'", replica_path.c_str(), rename_path);

        // if gc the replica failed, add it back
        zauto_lock l(_replicas_lock);
        _fs_manager.add_replica(pid, replica_path);
        _closed_replicas.emplace(pid, closed_info);
    } else {
        dwarn("gc_replica: {replica_dir_op} succeed to move directory '%s' to '%s'",
              replica_path.c_str(),
              rename_path);
        _counter_replicas_recent_replica_move_garbage_count->increment();
    }
}

void replica_stub::on_gc()
{
    ddebug("start to garbage collection");
    uint64_t start = dsn_now_ns();

    replicas rs;
    {
        zauto_lock l(_replicas_lock);
        rs = _replicas;
    }

    // statistic learning info
    uint64_t learning_count = 0;
    uint64_t learning_max_duration_time_ms = 0;
    uint64_t learning_max_copy_file_size = 0;
    uint64_t cold_backup_running_count = 0;
    uint64_t cold_backup_max_duration_time_ms = 0;
    uint64_t cold_backup_max_upload_file_size = 0;
    for (auto it = rs.begin(); it != rs.end(); ++it) {
        replica_ptr &r = it->second;
        if (r->status() == partition_status::PS_POTENTIAL_SECONDARY) {
            learning_count++;
            learning_max_duration_time_ms = std::max(learning_max_duration_time_ms,
                                                     r->_potential_secondary_states.duration_ms());
            learning_max_copy_file_size =
                std::max(learning_max_copy_file_size,
                         r->_potential_secondary_states.learning_copy_file_size);
        }
        if (r->status() == partition_status::PS_PRIMARY ||
            r->status() == partition_status::PS_SECONDARY) {
            cold_backup_running_count += r->_cold_backup_running_count.load();
            cold_backup_max_duration_time_ms = std::max(
                cold_backup_max_duration_time_ms, r->_cold_backup_max_duration_time_ms.load());
            cold_backup_max_upload_file_size = std::max(
                cold_backup_max_upload_file_size, r->_cold_backup_max_upload_file_size.load());
        }
    }

    _counter_replicas_learning_count->set(learning_count);
    _counter_replicas_learning_max_duration_time_ms->set(learning_max_duration_time_ms);
    _counter_replicas_learning_max_copy_file_size->set(learning_max_copy_file_size);
    _counter_cold_backup_running_count->set(cold_backup_running_count);
    _counter_cold_backup_max_duration_time_ms->set(cold_backup_max_duration_time_ms);
    _counter_cold_backup_max_upload_file_size->set(cold_backup_max_upload_file_size);
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
        for (auto it = rs.begin(); it != rs.end(); ++it) {
            replica_log_info ri;
            replica_ptr &r = it->second;
            mutation_log_ptr plog = r->private_log();
            if (plog) {
                // flush private log to update plog_max_commit_on_disk,
                // and just flush once to avoid flushing infinitely
                plog->flush_once();

                ri.max_decree = std::min(r->last_durable_decree(), plog->max_commit_on_disk());
                ddebug("gc_shared: gc condition for %s, status = %s, garbage_max_decree = %" PRId64
                       ", "
                       "last_durable_decree= %" PRId64 ", plog_max_commit_on_disk = %" PRId64 "",
                       r->name(),
                       enum_to_string(r->status()),
                       ri.max_decree,
                       r->last_durable_decree(),
                       plog->max_commit_on_disk());
            } else {
                ri.max_decree = r->last_durable_decree();
                ddebug("gc_shared: gc condition for %s, status = %s, garbage_max_decree = %" PRId64
                       ", "
                       "last_durable_decree = %" PRId64 "",
                       r->name(),
                       enum_to_string(r->status()),
                       ri.max_decree,
                       r->last_durable_decree());
            }
            ri.valid_start_offset = r->get_app()->init_info().init_offset_in_shared_log;
            gc_condition[it->first] = ri;
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
            for (auto it = rs.begin(); it != rs.end(); ++it) {
                tasking::enqueue(
                    LPC_PER_REPLICA_CHECKPOINT_TIMER,
                    this,
                    std::bind(&replica_stub::trigger_checkpoint, this, it->second, true),
                    gpid_to_thread_hash(it->first),
                    std::chrono::milliseconds(
                        dsn_random32(0, 3000)) // delay random to avoid write compete
                    );
            }
        } else if (reserved_log_count > _options.log_shared_file_count_limit) {
            std::ostringstream oss;
            int c = 0;
            for (auto &i : prevent_gc_replicas) {
                if (c != 0)
                    oss << ", ";
                oss << i.get_app_id() << "." << i.get_partition_index();
                c++;
            }
            ddebug("gc_shared: trigger emergency checkpoint by log_shared_file_count_limit, "
                   "file_count_limit = %d, reserved_log_count = %d, prevent_gc_replica_count = %d, "
                   "trigger them to do checkpoint: { %s }",
                   _options.log_shared_file_count_limit,
                   reserved_log_count,
                   (int)prevent_gc_replicas.size(),
                   oss.str().c_str());
            zauto_lock l(_replicas_lock);
            for (auto &id : prevent_gc_replicas) {
                auto find = _replicas.find(id);
                if (find != _replicas.end()) {
                    tasking::enqueue(
                        LPC_PER_REPLICA_CHECKPOINT_TIMER,
                        this,
                        std::bind(&replica_stub::trigger_checkpoint, this, find->second, true),
                        gpid_to_thread_hash(id),
                        std::chrono::milliseconds(
                            dsn_random32(0, 3000)) // delay random to avoid write compete
                        );
                }
            }
        }

        _counter_shared_log_size->set(_log->size() / (1024 * 1024));
    }

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
        auto &&name = dsn::utils::filesystem::get_file_name(fpath);
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
                                             ? _options.gc_disk_error_replica_interval_seconds
                                             : _options.gc_disk_garbage_replica_interval_seconds);
            if (last_write_time + interval_seconds < current_time_ms / 1000) {
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
            }
        }
    }
    _counter_replicas_error_replica_dir_count->set(error_replica_dir_count);
    _counter_replicas_garbage_replica_dir_count->set(garbage_replica_dir_count);

    _fs_manager.update_disk_stat();

    ddebug("finish to update disk stat, time_used_ns = %" PRIu64, dsn_now_ns() - start);
}

::dsn::task_ptr replica_stub::begin_open_replica(const app_info &app,
                                                 gpid gpid,
                                                 std::shared_ptr<group_check_request> req,
                                                 std::shared_ptr<configuration_update_request> req2)
{
    _replicas_lock.lock();
    if (_replicas.find(gpid) != _replicas.end()) {
        _replicas_lock.unlock();
        return nullptr;
    }

    auto it = _opening_replicas.find(gpid);
    if (it != _opening_replicas.end()) {
        _replicas_lock.unlock();
        return nullptr;
    } else {
        auto it2 = _closing_replicas.find(gpid);
        if (it2 != _closing_replicas.end()) {
            if (it2->second.second->status() == partition_status::PS_INACTIVE &&
                it2->second.first->cancel(false)) {
                replica_ptr r = it2->second.second;
                _closing_replicas.erase(it2);
                _counter_replicas_closing_count->decrement();

                add_replica(r);

                // unlock here to avoid dead lock
                _replicas_lock.unlock();

                ddebug("open replica which is to be closed '%s.%d.%d'",
                       app.app_type.c_str(),
                       gpid.get_app_id(),
                       gpid.get_partition_index());

                if (req != nullptr) {
                    on_add_learner(*req);
                }
                return nullptr;
            } else {
                _replicas_lock.unlock();
                dwarn("open replica '%s.%d.%d' failed coz replica is under closing",
                      app.app_type.c_str(),
                      gpid.get_app_id(),
                      gpid.get_partition_index());
                return nullptr;
            }
        } else {
            task_ptr task = tasking::enqueue(
                LPC_OPEN_REPLICA,
                this,
                std::bind(&replica_stub::open_replica, this, app, gpid, req, req2));

            _counter_replicas_opening_count->increment();
            _opening_replicas[gpid] = task;
            _closed_replicas.erase(gpid);
            _replicas_lock.unlock();
            return task;
        }
    }
}

void replica_stub::open_replica(const app_info &app,
                                gpid gpid,
                                std::shared_ptr<group_check_request> req,
                                std::shared_ptr<configuration_update_request> req2)
{
    std::string dir = get_replica_dir(app.app_type.c_str(), gpid, false);
    replica_ptr rep = nullptr;
    if (!dir.empty()) {
        // NOTICE: if partition is DDD, and meta select one replica as primary, it will execute the
        // load-process because of a.b.pegasus is exist, so it will never execute the restore
        // process below
        ddebug("%d.%d@%s: start to load replica %s group check, dir = %s",
               gpid.get_app_id(),
               gpid.get_partition_index(),
               _primary_address.to_string(),
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
             (app.envs.find(cold_backup_constant::POLICY_NAME) != app.envs.end()));

        // NOTICE: when we don't need execute restore-process, we should remove a.b.pegasus
        // directory because it don't contain the valid data dir and also we need create a new
        // replica(if contain valid data, it will execute load-process)

        if (!restore_if_necessary && ::dsn::utils::filesystem::directory_exists(dir)) {
            if (!::dsn::utils::filesystem::remove_path(dir)) {
                dassert(false, "remove use directory(%s) failed", dir.c_str());
                return;
            }
        }
        rep = replica::newr(this, gpid, app, restore_if_necessary);
    }

    if (rep == nullptr) {
        _counter_replicas_opening_count->decrement();
        zauto_lock l(_replicas_lock);
        _opening_replicas.erase(gpid);
        return;
    }

    {
        _counter_replicas_opening_count->decrement();
        zauto_lock l(_replicas_lock);
        auto it = _replicas.find(gpid);
        dassert(it == _replicas.end(), "");
        add_replica(rep);
        _opening_replicas.erase(gpid);
    }

    if (nullptr != req) {
        rpc::call_one_way_typed(
            _primary_address, RPC_LEARN_ADD_LEARNER, *req, gpid_to_thread_hash(req->config.pid));
    } else if (nullptr != req2) {
        rpc::call_one_way_typed(
            _primary_address, RPC_CONFIG_PROPOSAL, *req2, gpid_to_thread_hash(req2->config.pid));
    }
}

::dsn::task_ptr replica_stub::begin_close_replica(replica_ptr r)
{
    dassert(r->status() == partition_status::PS_ERROR ||
                r->status() == partition_status::PS_INACTIVE,
            "%s: invalid state %s when calling begin_close_replica",
            r->name(),
            enum_to_string(r->status()));

    zauto_lock l(_replicas_lock);

    if (remove_replica(r)) {
        int delay_ms = 0;
        if (r->status() == partition_status::PS_INACTIVE) {
            delay_ms = _options.gc_memory_replica_interval_ms;
            ddebug("%s: delay %d milliseconds to close replica, status = PS_INACTIVE",
                   r->name(),
                   delay_ms);
        }

        task_ptr task = tasking::enqueue(LPC_CLOSE_REPLICA,
                                         this,
                                         [=]() { close_replica(r); },
                                         0,
                                         std::chrono::milliseconds(delay_ms));
        _closing_replicas[r->get_gpid()] = std::make_pair(task, r);
        _counter_replicas_closing_count->increment();
        return task;
    } else {
        return nullptr;
    }
}

void replica_stub::close_replica(replica_ptr r)
{
    ddebug("%s: start to close replica", r->name());

    replica_info r_info;
    get_replica_info(r_info, r);
    app_info a_info = *(r->get_app_info());
    r->close();

    {
        _counter_replicas_closing_count->decrement();
        zauto_lock l(_replicas_lock);
        _closing_replicas.erase(r->get_gpid());
        _closed_replicas.emplace(r_info.pid, std::make_pair(std::move(a_info), std::move(r_info)));
    }

    ddebug("%s: replica closed", r->name());
}

void replica_stub::add_replica(replica_ptr r)
{
    _counter_replicas_count->increment();
    zauto_lock l(_replicas_lock);
    auto pr = _replicas.insert(replicas::value_type(r->get_gpid(), r));
    dassert(pr.second, "replica %s is already in the collection", r->name());
    _closed_replicas.erase(r->get_gpid());
}

bool replica_stub::remove_replica(replica_ptr r)
{
    zauto_lock l(_replicas_lock);
    if (_replicas.erase(r->get_gpid()) > 0) {
        _counter_replicas_count->decrement();
        return true;
    } else {
        return false;
    }
}

void replica_stub::notify_replica_state_update(const replica_configuration &config, bool is_closing)
{
    if (nullptr != _replica_state_subscriber) {
        if (_is_long_subscriber) {
            tasking::enqueue(
                LPC_REPLICA_STATE_CHANGE_NOTIFICATION,
                this,
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
    register_rpc_handler(RPC_LEARN_COMPLETION_NOTIFY,
                         "LearnNotify",
                         &replica_stub::on_learn_completion_notification);
    register_rpc_handler(RPC_LEARN_ADD_LEARNER, "LearnAdd", &replica_stub::on_add_learner);
    register_rpc_handler(RPC_REMOVE_REPLICA, "remove", &replica_stub::on_remove);
    register_rpc_handler(RPC_GROUP_CHECK, "GroupCheck", &replica_stub::on_group_check);
    register_rpc_handler(RPC_QUERY_PN_DECREE, "query_decree", &replica_stub::on_query_decree);
    register_rpc_handler(
        RPC_QUERY_REPLICA_INFO, "query_replica_info", &replica_stub::on_query_replica_info);
    register_rpc_handler(
        RPC_REPLICA_COPY_LAST_CHECKPOINT, "copy_checkpoint", &replica_stub::on_copy_checkpoint);

    register_rpc_handler(RPC_QUERY_APP_INFO, "query_app_info", &replica_stub::on_query_app_info);
    register_rpc_handler(RPC_COLD_BACKUP, "ColdBackup", &replica_stub::on_cold_backup);

    _kill_partition_command = ::dsn::command_manager::instance().register_app_command(
        {"kill_partition"},
        "kill_partition <app_id> <partition_index>",
        "kill_partition: kill partition with its global partition id",
        [this](const std::vector<std::string> &args) {
            if (args.size() != 2)
                return std::string(ERR_INVALID_PARAMETERS.to_string());
            dsn::gpid pid;
            pid.set_app_id(atoi(args[0].c_str()));
            pid.set_partition_index(atoi(args[1].c_str()));
            if (pid.get_app_id() <= 0 || pid.get_partition_index() < 0)
                return std::string(ERR_INVALID_PARAMETERS.to_string());
            dsn::error_code e = this->on_kill_replica(pid);
            return std::string(e.to_string());
        });

    _deny_client_command = ::dsn::command_manager::instance().register_app_command(
        {"deny-client"},
        "deny-client <true|false>",
        "deny-client - control if deny client read & write request",
        [this](const std::vector<std::string> &args) { HANDLE_CLI_FLAGS(_deny_client, args); });

    _verbose_client_log_command = ::dsn::command_manager::instance().register_app_command(
        {"verbose-client-log"},
        "verbose-client-log <true|false>",
        "verbose-client-log - control if print verbose error log when reply read & write request",
        [this](const std::vector<std::string> &args) {
            HANDLE_CLI_FLAGS(_verbose_client_log, args);
        });

    _verbose_commit_log_command = ::dsn::command_manager::instance().register_app_command(
        {"verbose-commit-log"},
        "verbose-commit-log <true|false>",
        "verbose-commit-log - control if print verbose log when commit mutation",
        [this](const std::vector<std::string> &args) {
            HANDLE_CLI_FLAGS(_verbose_commit_log, args);
        });

    _trigger_chkpt_command = ::dsn::command_manager::instance().register_app_command(
        {"trigger-checkpoint"},
        "trigger-checkpoint",
        "trigger-checkpoint - trigger all replicas to do checkpoints",
        [this](const std::vector<std::string> &args) {
            ddebug("start to trigger checkpoint by remote command");
            replicas rs;
            {
                zauto_lock l(_replicas_lock);
                rs = _replicas;
            }
            for (auto it = rs.begin(); it != rs.end(); ++it) {
                tasking::enqueue(
                    LPC_PER_REPLICA_CHECKPOINT_TIMER,
                    this,
                    std::bind(&replica_stub::trigger_checkpoint, this, it->second, true),
                    gpid_to_thread_hash(it->first),
                    std::chrono::milliseconds(
                        dsn_random32(0, 3000)) // delay random to avoid write compete
                    );
            }
            return "OK";
        });
}

void replica_stub::close()
{
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

    _kill_partition_command = nullptr;
    _deny_client_command = nullptr;
    _verbose_client_log_command = nullptr;
    _verbose_commit_log_command = nullptr;
    _trigger_chkpt_command = nullptr;

    if (_config_sync_timer_task != nullptr) {
        _config_sync_timer_task->cancel(true);
        _config_sync_timer_task = nullptr;
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

    {
        zauto_lock l(_replicas_lock);
        while (_closing_replicas.empty() == false) {
            task_ptr task = _closing_replicas.begin()->second.first;
            gpid tmp_gpid = _closing_replicas.begin()->first;
            _replicas_lock.unlock();

            task->wait();

            _replicas_lock.lock();
            // task will automatically remove this replica from _closing_replicas
            if (false == _closing_replicas.empty()) {
                dassert((tmp_gpid == _closing_replicas.begin()->first) == false,
                        "this replica '%d.%d' should be removed from _closing_replicas, gpid",
                        tmp_gpid.get_app_id(),
                        tmp_gpid.get_partition_index());
            }
        }

        while (_opening_replicas.empty() == false) {
            task_ptr task = _opening_replicas.begin()->second;
            _replicas_lock.unlock();

            task->cancel(true);

            _counter_replicas_opening_count->decrement();
            _replicas_lock.lock();
            _opening_replicas.erase(_opening_replicas.begin());
        }

        while (_replicas.empty() == false) {
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

std::string replica_stub::get_replica_dir(const char *app_type, gpid gpid, bool create_new)
{
    char buffer[256];
    sprintf(buffer, "%d.%d.%s", gpid.get_app_id(), gpid.get_partition_index(), app_type);
    std::string ret_dir;
    for (auto &dir : _options.data_dirs) {
        std::string cur_dir = utils::filesystem::path_combine(dir, buffer);
        if (utils::filesystem::directory_exists(cur_dir)) {
            if (!ret_dir.empty()) {
                dassert(
                    false, "replica dir conflict: %s <--> %s", cur_dir.c_str(), ret_dir.c_str());
            }
            ret_dir = cur_dir;
        }
    }
    if (ret_dir.empty() && create_new) {
        _fs_manager.allocate_dir(gpid, app_type, ret_dir);
    }
    return ret_dir;
}
}
} // namespace
