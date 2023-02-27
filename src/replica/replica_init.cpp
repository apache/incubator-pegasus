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
#include "backup/replica_backup_manager.h"
#include "duplication/replica_follower.h"
#include "utils/factory_store.h"
#include "utils/filesystem.h"
#include "replica/replication_app_base.h"
#include "utils/fmt_logging.h"
#include "utils/fail_point.h"

namespace dsn {
namespace replication {
DSN_DEFINE_bool(replication, checkpoint_disabled, false, "whether checkpoint is disabled");
DSN_DEFINE_int32(replication,
                 checkpoint_interval_seconds,
                 100,
                 "every what period (seconds) we do checkpoints for replicated apps");

DSN_DEFINE_int32(replication,
                 log_private_file_size_mb,
                 32,
                 "private log maximum segment file size (MB)");

error_code replica::initialize_on_new()
{
    // if (dsn::utils::filesystem::directory_exists(_dir) &&
    //    !dsn::utils::filesystem::remove_path(_dir))
    //{
    //    LOG_ERROR("cannot allocate new replica @ {}, as the dir is already exists", _dir);
    //    return ERR_PATH_ALREADY_EXIST;
    //}
    //
    // TODO: check if _dir contain other file or directory except for
    // "restore.policy_name.backup_id"
    // which is applied to restore from cold backup
    if (!dsn::utils::filesystem::directory_exists(_dir) &&
        !dsn::utils::filesystem::create_directory(_dir)) {
        LOG_ERROR("cannot allocate new replica @ {}, because create dir failed", _dir);
        return ERR_FILE_OPERATION_FAILED;
    }

    auto err = store_app_info(_app_info);
    if (err != ERR_OK) {
        dsn::utils::filesystem::remove_path(_dir);
        return err;
    }

    return init_app_and_prepare_list(true);
}

/*static*/ replica *replica::newr(replica_stub *stub,
                                  gpid gpid,
                                  const app_info &app,
                                  bool restore_if_necessary,
                                  bool is_duplication_follower,
                                  const std::string &parent_dir)
{
    std::string dir;
    if (parent_dir.empty()) {
        dir = stub->get_replica_dir(app.app_type.c_str(), gpid);
    } else {
        dir = stub->get_child_dir(app.app_type.c_str(), gpid, parent_dir);
    }
    replica *rep =
        new replica(stub, gpid, app, dir.c_str(), restore_if_necessary, is_duplication_follower);
    error_code err;
    if (restore_if_necessary && (err = rep->restore_checkpoint()) != dsn::ERR_OK) {
        LOG_ERROR("{}: try to restore replica failed, error({})", rep->name(), err.to_string());
        return clear_on_failure(stub, rep, dir, gpid);
    }

    if (is_duplication_follower &&
        (err = rep->get_replica_follower()->duplicate_checkpoint()) != dsn::ERR_OK) {
        LOG_ERROR("{}: try to duplicate replica checkpoint failed, error({}) and please check "
                  "previous detail error log",
                  rep->name(),
                  err.to_string());
        return clear_on_failure(stub, rep, dir, gpid);
    }

    err = rep->initialize_on_new();
    if (err == ERR_OK) {
        LOG_DEBUG("{}: new replica succeed", rep->name());
        return rep;
    } else {
        LOG_ERROR("{}: new replica failed, err = {}", rep->name(), err.to_string());
        return clear_on_failure(stub, rep, dir, gpid);
    }
}

/* static */ replica *replica::clear_on_failure(replica_stub *stub,
                                                replica *rep,
                                                const std::string &path,
                                                const gpid &pid)
{
    rep->close();
    delete rep;
    rep = nullptr;

    // clear work on failure
    utils::filesystem::remove_path(path);
    stub->_fs_manager.remove_replica(pid);
    return nullptr;
}

error_code replica::initialize_on_load()
{
    LOG_INFO_PREFIX("initialize replica on load, dir = {}", _dir);

    if (!dsn::utils::filesystem::directory_exists(_dir)) {
        LOG_ERROR_PREFIX("cannot load replica, because dir {} is not exist", _dir);
        return ERR_PATH_NOT_FOUND;
    }

    return init_app_and_prepare_list(false);
}

/*static*/ replica *replica::load(replica_stub *stub, const char *dir)
{
    FAIL_POINT_INJECT_F("mock_replica_load", [&](string_view) -> replica * { return nullptr; });

    char splitters[] = {'\\', '/', 0};
    std::string name = utils::get_last_component(std::string(dir), splitters);
    if (name == "") {
        LOG_ERROR("invalid replica dir {}", dir);
        return nullptr;
    }

    char app_type[128];
    int32_t app_id, pidx;
    if (3 != sscanf(name.c_str(), "%d.%d.%s", &app_id, &pidx, app_type)) {
        LOG_ERROR("invalid replica dir {}", dir);
        return nullptr;
    }

    gpid pid(app_id, pidx);
    if (!utils::filesystem::directory_exists(dir)) {
        LOG_ERROR("replica dir {} not exist", dir);
        return nullptr;
    }

    dsn::app_info info;
    replica_app_info info2(&info);
    std::string path = utils::filesystem::path_combine(dir, kAppInfo);
    auto err = info2.load(path);
    if (ERR_OK != err) {
        LOG_ERROR("load app-info from {} failed, err = {}", path, err);
        return nullptr;
    }

    if (info.app_type != app_type) {
        LOG_ERROR("unmatched app type {} for {}", info.app_type, path);
        return nullptr;
    }

    if (info.partition_count < pidx) {
        LOG_ERROR("partition[{}], count={}, this replica may be partition split garbage partition, "
                  "ignore it",
                  pid,
                  info.partition_count);
        return nullptr;
    }

    replica *rep = new replica(stub, pid, info, dir, false);

    err = rep->initialize_on_load();
    if (err == ERR_OK) {
        LOG_INFO("{}: load replica succeed", rep->name());
        return rep;
    } else {
        LOG_ERROR("{}: load replica failed, err = {}", rep->name(), err);
        rep->close();
        delete rep;
        rep = nullptr;

        // clear work on failure
        if (dsn::utils::filesystem::directory_exists(dir)) {
            char rename_dir[1024];
            sprintf(rename_dir, "%s.%" PRIu64 ".err", dir, dsn_now_us());
            CHECK(dsn::utils::filesystem::rename_path(dir, rename_dir),
                  "load_replica: failed to move directory '{}' to '{}'",
                  dir,
                  rename_dir);
            LOG_WARNING("load_replica: replica_dir_op succeed to move directory '{}' to '{}'",
                        dir,
                        rename_dir);
            stub->_counter_replicas_recent_replica_move_error_count->increment();
            stub->_fs_manager.remove_replica(pid);
        }

        return nullptr;
    }
}

decree replica::get_replay_start_decree()
{
    decree replay_start_decree = _app->last_committed_decree();
    LOG_INFO_PREFIX("start to replay private log [replay_start_decree: {}]", replay_start_decree);
    return replay_start_decree;
}

error_code replica::init_app_and_prepare_list(bool create_new)
{
    CHECK(nullptr == _app, "");
    error_code err;
    std::string log_dir = utils::filesystem::path_combine(dir(), "plog");

    _app.reset(replication_app_base::new_storage_instance(_app_info.app_type, this));
    CHECK(nullptr == _private_log, "");

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
            CHECK_EQ(_app->last_committed_decree(), _app->last_durable_decree());
            _config.ballot = _app->init_info().init_ballot;
            _prepare_list->reset(_app->last_committed_decree());

            _private_log =
                new mutation_log_private(log_dir, FLAGS_log_private_file_size_mb, get_gpid(), this);
            LOG_INFO_PREFIX("plog_dir = {}", log_dir);

            // sync valid_start_offset between app and logs
            _stub->_log->set_valid_start_offset_on_open(
                get_gpid(), _app->init_info().init_offset_in_shared_log);
            _private_log->set_valid_start_offset_on_open(
                get_gpid(), _app->init_info().init_offset_in_private_log);

            // replay the logs
            {
                std::map<gpid, decree> replay_condition;
                replay_condition[_config.pid] = get_replay_start_decree();

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
                    LOG_INFO_PREFIX("replay private log succeed, durable = {}, committed = {}, "
                                    "max_prepared = {}, ballot = {}, valid_offset_in_plog = {}, "
                                    "max_decree_in_plog = {}, max_commit_on_disk_in_plog = {}, "
                                    "time_used = {} ms",
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
                    LOG_ERROR_PREFIX("replay private log failed, err = {}, durable = {}, committed "
                                     "= {}, maxpd = {}, ballot = {}, valid_offset_in_plog = {}, "
                                     "time_used = {} ms",
                                     err,
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
        LOG_ERROR_PREFIX("open replica failed, err = {}", err);
        _app->close(false);
        _app = nullptr;
    } else {
        _is_initializing = true;
        _app->set_partition_version(_app_info.partition_count - 1);

        if (nullptr == _private_log) {
            LOG_INFO_PREFIX("clear private log, dir = {}", log_dir);
            CHECK(dsn::utils::filesystem::remove_path(log_dir),
                  "Fail to delete directory {}",
                  log_dir);
            CHECK(dsn::utils::filesystem::create_directory(log_dir),
                  "Fail to create directory {}",
                  log_dir);

            _private_log =
                new mutation_log_private(log_dir, FLAGS_log_private_file_size_mb, get_gpid(), this);
            LOG_INFO_PREFIX("plog_dir = {}", log_dir);

            err = _private_log->open(nullptr, [this](error_code err) {
                tasking::enqueue(LPC_REPLICATION_ERROR,
                                 &_tracker,
                                 [this, err]() { handle_local_failure(err); },
                                 get_gpid().thread_hash());
            });
        }

        if (err == ERR_OK) {
            if (_checkpoint_timer == nullptr && !FLAGS_checkpoint_disabled) {
                _checkpoint_timer =
                    tasking::enqueue_timer(LPC_PER_REPLICA_CHECKPOINT_TIMER,
                                           &_tracker,
                                           [this] { on_checkpoint_timer(); },
                                           std::chrono::seconds(FLAGS_checkpoint_interval_seconds),
                                           get_gpid().thread_hash());
            }

            _backup_mgr->start_collect_backup_info();
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
        CHECK(update_local_configuration(_config, true), "");
    }

    if (is_private && offset < _app->init_info().init_offset_in_private_log) {
        LOG_DEBUG_PREFIX("replay mutation skipped1 as offset is invalid in private log, ballot = {}"
                         ", decree = {}, last_committed_decree = {}, offset = {}",
                         mu->data.header.ballot,
                         d,
                         mu->data.header.last_committed_decree,
                         offset);
        return false;
    }

    if (!is_private && offset < _app->init_info().init_offset_in_shared_log) {
        LOG_DEBUG_PREFIX(
            "replay mutation skipped2 as offset is invalid in shared log, ballot = {}, "
            "decree = {}, last_committed_decree = {}, offset = {}",
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
        LOG_DEBUG_PREFIX("replay mutation skipped3 as decree is outdated, ballot = {}, "
                         "decree = {}(vs app {}), last_committed_decree = {}, offset = {}",
                         mu->data.header.ballot,
                         d,
                         last_committed_decree(),
                         mu->data.header.last_committed_decree,
                         offset);
        return true;
    }

    auto old = _prepare_list->get_mutation_by_decree(d);
    if (old != nullptr && old->data.header.ballot >= mu->data.header.ballot) {
        LOG_DEBUG_PREFIX(
            "replay mutation skipped4 as ballot is outdated, ballot = {} (vs local-ballot={}), "
            "decree = {}, last_committed_decree = {}, offset = {}",
            mu->data.header.ballot,
            old->data.header.ballot,
            d,
            mu->data.header.last_committed_decree,
            offset);

        return true;
    }

    LOG_DEBUG_PREFIX("replay mutation ballot = {}, decree = {}, last_committed_decree = {}",
                     mu->data.header.ballot,
                     d,
                     mu->data.header.last_committed_decree);

    // prepare
    _uniq_timestamp_us.try_update(mu->data.header.timestamp);
    error_code err = _prepare_list->prepare(mu, partition_status::PS_INACTIVE);
    CHECK_EQ_PREFIX(err, ERR_OK);

    return true;
}

void replica::set_inactive_state_transient(bool t)
{
    if (status() == partition_status::PS_INACTIVE) {
        LOG_INFO_PREFIX("set inactive_is_transient from {} to {}",
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

} // namespace replication
} // namespace dsn
