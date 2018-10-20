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
 *     initialization for replica object
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "replica.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include <dsn/utility/factory_store.h>
#include <dsn/utility/filesystem.h>
#include <dsn/dist/replication/replication_app_base.h>

namespace dsn {
namespace replication {

error_code replica::initialize_on_new()
{
    // if (dsn::utils::filesystem::directory_exists(_dir) &&
    //    !dsn::utils::filesystem::remove_path(_dir))
    //{
    //    derror("cannot allocate new replica @ %s, as the dir is already exists", _dir.c_str());
    //    return ERR_PATH_ALREADY_EXIST;
    //}
    //
    // TODO: check if _dir contain other file or directory except for
    // "restore.policy_name.backup_id"
    // which is applied to restore from cold backup
    if (!dsn::utils::filesystem::directory_exists(_dir) &&
        !dsn::utils::filesystem::create_directory(_dir)) {
        derror("cannot allocate new replica @ %s, because create dir failed", _dir.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    replica_app_info info((app_info *)&_app_info);
    std::string path = utils::filesystem::path_combine(_dir, ".app-info");
    auto err = info.store(path.c_str());
    if (err != ERR_OK) {
        derror("save app-info to %s failed, err = %s", path.c_str(), err.to_string());
        dsn::utils::filesystem::remove_path(_dir);
        return err;
    }

    return init_app_and_prepare_list(true);
}

/*static*/ replica *
replica::newr(replica_stub *stub, gpid gpid, const app_info &app, bool restore_if_necessary)
{
    std::string dir = stub->get_replica_dir(app.app_type.c_str(), gpid);
    replica *rep = new replica(stub, gpid, app, dir.c_str(), restore_if_necessary);
    error_code err;
    if (restore_if_necessary && (err = rep->restore_checkpoint()) != dsn::ERR_OK) {
        derror("try to restore replica %s failed, error(%s)", rep->name(), err.to_string());
        rep->close();
        delete rep;
        rep = nullptr;

        // clear work on failure
        utils::filesystem::remove_path(dir);
        stub->_fs_manager.remove_replica(gpid);
        return nullptr;
    }

    err = rep->initialize_on_new();
    if (err == ERR_OK) {
        dinfo("%s: new replica succeed", rep->name());
        return rep;
    } else {
        derror("%s: new replica failed, err = %s", rep->name(), err.to_string());
        rep->close();
        delete rep;
        rep = nullptr;

        // clear work on failure
        utils::filesystem::remove_path(dir);
        stub->_fs_manager.remove_replica(gpid);
        return nullptr;
    }
}

error_code replica::initialize_on_load()
{
    ddebug("%s: initialize replica on load, dir = %s", name(), _dir.c_str());

    if (!dsn::utils::filesystem::directory_exists(_dir)) {
        derror("%s: cannot load replica, because dir %s is not exist", name(), _dir.c_str());
        return ERR_PATH_NOT_FOUND;
    }

    return init_app_and_prepare_list(false);
}

/*static*/ replica *replica::load(replica_stub *stub, const char *dir)
{
    char splitters[] = {'\\', '/', 0};
    std::string name = utils::get_last_component(std::string(dir), splitters);
    if (name == "") {
        derror("invalid replica dir %s", dir);
        return nullptr;
    }

    char app_type[128];
    int32_t app_id, pidx;
    if (3 != sscanf(name.c_str(), "%d.%d.%s", &app_id, &pidx, app_type)) {
        derror("invalid replica dir %s", dir);
        return nullptr;
    }

    gpid pid(app_id, pidx);
    if (!utils::filesystem::directory_exists(dir)) {
        derror("replica dir %s not exist", dir);
        return nullptr;
    }

    dsn::app_info info;
    replica_app_info info2(&info);
    std::string path = utils::filesystem::path_combine(dir, ".app-info");
    auto err = info2.load(path.c_str());
    if (ERR_OK != err) {
        derror("load app-info from %s failed, err = %s", path.c_str(), err.to_string());
        return nullptr;
    }

    if (info.app_type != app_type) {
        derror("unmatched app type %s for %s", info.app_type.c_str(), path.c_str());
        return nullptr;
    }

    replica *rep = new replica(stub, pid, info, dir, false);

    err = rep->initialize_on_load();
    if (err == ERR_OK) {
        ddebug("%s: load replica succeed", rep->name());
        return rep;
    } else {
        derror("%s: load replica failed, err = %s", rep->name(), err.to_string());
        rep->close();
        delete rep;
        rep = nullptr;

        // clear work on failure
        if (dsn::utils::filesystem::directory_exists(dir)) {
            char rename_dir[1024];
            sprintf(rename_dir, "%s.%" PRIu64 ".err", dir, dsn_now_us());
            bool ret = dsn::utils::filesystem::rename_path(dir, rename_dir);
            dassert(ret, "load_replica: failed to move directory '%s' to '%s'", dir, rename_dir);
            dwarn("load_replica: {replica_dir_op} succeed to move directory '%s' to '%s'",
                  dir,
                  rename_dir);
            stub->_counter_replicas_recent_replica_move_error_count->increment();
            stub->_fs_manager.remove_replica(pid);
        }

        return nullptr;
    }
}

error_code replica::init_app_and_prepare_list(bool create_new)
{
    dassert(nullptr == _app, "");
    error_code err;
    std::string log_dir = utils::filesystem::path_combine(dir(), "plog");

    _app.reset(replication_app_base::new_storage_instance(_app_info.app_type, this));
    dassert(nullptr == _private_log, "private log must not be initialized yet");

    if (create_new) {
        err = _app->open_new_internal(this, _stub->_log->on_partition_reset(get_gpid(), 0), 0);
        // two case:
        //      1, just open a new app, in this case, the last_committed_decree and
        //      last_durable_decree
        //         and committed_decree of prepare_list are all equal, and is 0
        //      2, open app with some data, but don't have slog and plog and also don't have
        //      app_info;
        //         in this case, last_committed_decree = last_durable_decree >= 0, but
        //         last_committed_decree
        //         in prepare_list is 0, so should make it equal to last_committed_decree in app
        _prepare_list->reset(_app->last_committed_decree());
    } else {
        err = _app->open_internal(this);
        if (err == ERR_OK) {
            dassert(_app->last_committed_decree() == _app->last_durable_decree(),
                    "invalid app state, %" PRId64 " VS %" PRId64 "",
                    _app->last_committed_decree(),
                    _app->last_durable_decree());
            _config.ballot = _app->init_info().init_ballot;
            _prepare_list->reset(_app->last_committed_decree());

            _private_log =
                new mutation_log_private(log_dir,
                                         _options->log_private_file_size_mb,
                                         get_gpid(),
                                         this,
                                         _options->log_private_batch_buffer_kb * 1024,
                                         _options->log_private_batch_buffer_count,
                                         _options->log_private_batch_buffer_flush_interval_ms);
            ddebug("%s: plog_dir = %s", name(), log_dir.c_str());

            // sync valid_start_offset between app and logs
            _stub->_log->set_valid_start_offset_on_open(
                get_gpid(), _app->init_info().init_offset_in_shared_log);
            _private_log->set_valid_start_offset_on_open(
                get_gpid(), _app->init_info().init_offset_in_private_log);

            // replay the logs
            {
                ddebug("%s: start to replay private log", name());

                std::map<gpid, decree> replay_condition;
                replay_condition[_config.pid] = _app->last_committed_decree();

                uint64_t start_time = dsn_now_ms();
                err = _private_log->open(
                    [this](int log_length, mutation_ptr &mu) { return replay_mutation(mu, true); },
                    [this](error_code err) {
                        tasking::enqueue(LPC_REPLICATION_ERROR,
                                         &_tracker,
                                         [this, err]() { handle_local_failure(err); },
                                         get_gpid().thread_hash());
                    },
                    replay_condition);

                uint64_t finish_time = dsn_now_ms();

                if (err == ERR_OK) {
                    ddebug("%s: replay private log succeed, durable = %" PRId64
                           ", committed = %" PRId64 ", "
                           "max_prepared = %" PRId64 ", ballot = %" PRId64
                           ", valid_offset_in_plog = %" PRId64 ", "
                           "max_decree_in_plog = %" PRId64 ", max_commit_on_disk_in_plog = %" PRId64
                           ", "
                           "time_used = %" PRIu64 " ms",
                           name(),
                           _app->last_durable_decree(),
                           _app->last_committed_decree(),
                           max_prepared_decree(),
                           get_ballot(),
                           _app->init_info().init_offset_in_private_log,
                           _private_log->max_decree(get_gpid()),
                           _private_log->max_commit_on_disk(),
                           finish_time - start_time);

                    _private_log->check_valid_start_offset(
                        get_gpid(), _app->init_info().init_offset_in_private_log);

                    set_inactive_state_transient(true);
                }
                /* in the beginning the prepare_list is reset to the durable_decree */
                else {
                    derror("%s: replay private log failed, err = %s, durable = %" PRId64
                           ", committed = %" PRId64 ", "
                           "maxpd = %" PRId64 ", ballot = %" PRId64
                           ", valid_offset_in_plog = %" PRId64 ", "
                           "time_used = %" PRIu64 " ms",
                           name(),
                           err.to_string(),
                           _app->last_durable_decree(),
                           _app->last_committed_decree(),
                           max_prepared_decree(),
                           get_ballot(),
                           _app->init_info().init_offset_in_private_log,
                           finish_time - start_time);

                    _private_log->close();
                    _private_log = nullptr;

                    _stub->_log->on_partition_removed(get_gpid());
                }
            }
        }
    }

    if (err != ERR_OK) {
        derror("%s: open replica failed, err = %s", name(), err.to_string());
        _app->close(false);
        _app = nullptr;
    } else {
        _is_initializing = true;

        if (nullptr == _private_log) {
            ddebug("%s: clear private log, dir = %s", name(), log_dir.c_str());
            if (!dsn::utils::filesystem::remove_path(log_dir)) {
                dassert(false, "Fail to delete directory %s.", log_dir.c_str());
            }
            if (!::dsn::utils::filesystem::create_directory(log_dir)) {
                dassert(false, "Fail to create directory %s.", log_dir.c_str());
            }

            _private_log =
                new mutation_log_private(log_dir,
                                         _options->log_private_file_size_mb,
                                         get_gpid(),
                                         this,
                                         _options->log_private_batch_buffer_kb * 1024,
                                         _options->log_private_batch_buffer_count,
                                         _options->log_private_batch_buffer_flush_interval_ms);
            ddebug("%s: plog_dir = %s", name(), log_dir.c_str());

            err = _private_log->open(nullptr, [this](error_code err) {
                tasking::enqueue(LPC_REPLICATION_ERROR,
                                 &_tracker,
                                 [this, err]() { handle_local_failure(err); },
                                 get_gpid().thread_hash());
            });
        }

        if (err == ERR_OK) {
            if (_checkpoint_timer == nullptr && !_options->checkpoint_disabled) {
                _checkpoint_timer = tasking::enqueue_timer(
                    LPC_PER_REPLICA_CHECKPOINT_TIMER,
                    &_tracker,
                    [this] { on_checkpoint_timer(); },
                    std::chrono::seconds(_options->checkpoint_interval_seconds),
                    get_gpid().thread_hash());
            }

            if (_collect_info_timer == nullptr) {
                _collect_info_timer =
                    tasking::enqueue_timer(LPC_PER_REPLICA_COLLECT_INFO_TIMER,
                                           &_tracker,
                                           [this]() { collect_backup_info(); },
                                           std::chrono::milliseconds(_options->gc_interval_ms),
                                           get_gpid().thread_hash());
            }
        }
    }

    return err;
}

// return false only when the log is invalid:
// - for private log, return false if offset < init_offset_in_private_log
// - for shared log, return false if offset < init_offset_in_shared_log
bool replica::replay_mutation(mutation_ptr &mu, bool is_private)
{
    auto d = mu->data.header.decree;
    auto offset = mu->data.header.log_offset;

    // it's very import to keep the ballot.
    // for example, the recovery need it to select a proper primary
    if (mu->data.header.ballot > get_ballot()) {
        _config.ballot = mu->data.header.ballot;
        bool ret = update_local_configuration(_config, true);
        dassert(ret, "");
    }

    if (is_private && offset < _app->init_info().init_offset_in_private_log) {
        dinfo("%s: replay mutation skipped1 as offset is invalid in private log, ballot = %" PRId64
              ", decree = %" PRId64 ", last_committed_decree = %" PRId64 ", offset = %" PRId64,
              name(),
              mu->data.header.ballot,
              d,
              mu->data.header.last_committed_decree,
              offset);
        return false;
    }

    if (!is_private && offset < _app->init_info().init_offset_in_shared_log) {
        dinfo("%s: replay mutation skipped2 as offset is invalid in shared log, ballot = %" PRId64
              ", decree = %" PRId64 ", last_committed_decree = %" PRId64 ", offset = %" PRId64,
              name(),
              mu->data.header.ballot,
              d,
              mu->data.header.last_committed_decree,
              offset);
        return false;
    }

    // fix private log completeness when it is from shared
    if (!is_private && d > _private_log->max_commit_on_disk()) {
        _private_log->append(mu, LPC_WRITE_REPLICATION_LOG_COMMON, &_tracker, nullptr);
    }

    if (d <= last_committed_decree()) {
        dinfo("%s: replay mutation skipped3 as decree is outdated, ballot = %" PRId64
              ", decree = %" PRId64 "(vs app %" PRId64 "), last_committed_decree = %" PRId64
              ", offset = %" PRId64,
              name(),
              mu->data.header.ballot,
              d,
              last_committed_decree(),
              mu->data.header.last_committed_decree,
              offset);
        return true;
    }

    auto old = _prepare_list->get_mutation_by_decree(d);
    if (old != nullptr && old->data.header.ballot >= mu->data.header.ballot) {
        dinfo("%s: replay mutation skipped4 as ballot is outdated, ballot = %" PRId64
              " (vs local-ballot=%" PRId64 "), decree = %" PRId64
              ", last_committed_decree = %" PRId64 ", offset = %" PRId64,
              name(),
              mu->data.header.ballot,
              old->data.header.ballot,
              d,
              mu->data.header.last_committed_decree,
              offset);

        return true;
    }

    dinfo("%s: replay mutation ballot = %" PRId64 ", decree = %" PRId64
          ", last_committed_decree = %" PRId64,
          name(),
          mu->data.header.ballot,
          d,
          mu->data.header.last_committed_decree);

    // prepare
    _uniq_timestamp_us.try_update(mu->data.header.timestamp);
    error_code err = _prepare_list->prepare(mu, partition_status::PS_INACTIVE);
    dassert(err == ERR_OK, "prepare failed, err = %s", err.to_string());

    return true;
}

void replica::set_inactive_state_transient(bool t)
{
    if (status() == partition_status::PS_INACTIVE) {
        ddebug("%s: set inactive_is_transient from %s to %s",
               name(),
               _inactive_is_transient ? "true" : "false",
               t ? "true" : "false");
        _inactive_is_transient = t;
    }
}

void replica::reset_prepare_list_after_replay()
{
    // commit prepare list if possible
    _prepare_list->commit(_app->last_committed_decree(), COMMIT_TO_DECREE_SOFT);

    // align the prepare list and the app
    _prepare_list->truncate(_app->last_committed_decree());
}
}
} // namespace
