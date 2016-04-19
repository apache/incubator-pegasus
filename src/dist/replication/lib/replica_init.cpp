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
#include <dsn/internal/factory_store.h>
#include "replication_app_base.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.init"

namespace dsn { namespace replication {

using namespace dsn::service;

error_code replica::initialize_on_new()
{
    if (dsn::utils::filesystem::directory_exists(_dir) &&
        !dsn::utils::filesystem::remove_path(_dir))
    {
        derror("cannot allocate new replica @ %s, as the dir is already exists", _dir.c_str());
        return ERR_PATH_ALREADY_EXIST;
    }

    if (!dsn::utils::filesystem::create_directory(_dir))
    {
        derror("cannot allocate new replica @ %s, because create dir failed", _dir.c_str());
        return ERR_FILE_OPERATION_FAILED;
    }

    replica_app_info info((app_info*)&_app_info);
    std::string path = utils::filesystem::path_combine(_dir, ".app-info");
    auto err = info.store(path.c_str());
    if (err != ERR_OK)
    {
        derror("save app-info to %s failed, err = %s", path.c_str(), err.to_string());
        dsn::utils::filesystem::remove_path(_dir);
        return err;
    }

    return init_app_and_prepare_list(true);
}

/*static*/ replica* replica::newr(replica_stub* stub, gpid gpid, const app_info& app)
{
    std::string dir = stub->get_replica_dir(app.app_type.c_str(), gpid);
    replica* rep = new replica(stub, gpid, app, dir.c_str());
    error_code err = rep->initialize_on_new();
    if (err == ERR_OK)
    {
        dinfo("%s: new replica succeed", rep->name());
        return rep;
    }
    else
    {
        derror("%s: new replica failed: %s", rep->name(), err.to_string());
        rep->close();
        delete rep;
        rep = nullptr;

        // clear work on failure
        utils::filesystem::remove_path(dir);

        return nullptr;
    }
}

error_code replica::initialize_on_load()
{
    if (!dsn::utils::filesystem::directory_exists(_dir))
    {
        derror("cannot load replica @ %s, because dir not exist", _dir.c_str());
        return ERR_PATH_NOT_FOUND;
    }

    return init_app_and_prepare_list(false);
}

/*static*/ replica* replica::load(replica_stub* stub, const char* dir)
{
    char splitters[] = { '\\', '/', 0 };
    std::string name = utils::get_last_component(std::string(dir), splitters);
    if (name == "")
    {
        derror("invalid replica dir %s", dir);
        return nullptr;
    }

    char app_type[128];
    gpid gpid;
    if (3 != sscanf(name.c_str(), "%u.%u.%s", &gpid.raw().u.app_id, &gpid.raw().u.partition_index, app_type))
    {
        derror("invalid replica dir %s", dir);
        return nullptr;
    }

    dsn::app_info info;
    replica_app_info info2(&info);
    std::string path = utils::filesystem::path_combine(dir, ".app-info");
    auto err = info2.load(path.c_str());
    if (ERR_OK != err)
    {
        derror("load app-info from %s failed, err = %s", path.c_str(), err.to_string());
        return nullptr;
    }

    replica* rep = new replica(stub, gpid, info, dir);
    
    err = rep->initialize_on_load();
    if (err == ERR_OK)
    {
        ddebug("%s: load replica @ %s succeed", dir, rep->name());
        return rep;
    }
    else
    {
        derror("%s: load replica @ %s failed: %s", rep->name(), dir, err.to_string());
        rep->close();
        delete rep;
        rep = nullptr;

        // clear work on failure
        if (dsn::utils::filesystem::directory_exists(dir))
        {
            char rename_dir[256];
            sprintf(rename_dir, "%s.%" PRIu64 ".err", dir, dsn_now_us());
            if (dsn::utils::filesystem::rename_path(dir, rename_dir))
            {
                dwarn("move bad replica from '%s' to '%s'", dir, rename_dir);
            }
            else
            {
                derror("move bad replica from '%s' to '%s' failed", dir, rename_dir);
            }
        }

        return nullptr;
    }
}

error_code replica::init_app_and_prepare_list(bool create_new)
{
    dassert(nullptr == _app, "");

    _app.reset(new replication_app_base(this));
    error_code err = _app->open_internal(
        this,
        create_new
        );

    if (err == ERR_OK)
    {
        dassert(_app->last_committed_decree() == _app->last_durable_decree(), "");
        _prepare_list->reset(_app->last_committed_decree());
        
        if (!_options->log_private_disabled/*
            || !_app->is_delta_state_learning_supported()*/)
        {
            dassert(nullptr == _private_log, "private log must not be initialized yet");

            std::string log_dir = utils::filesystem::path_combine(dir(), "plog");

            _private_log = new mutation_log(
                log_dir,
                _options->log_private_batch_buffer_kb,
                _options->log_private_file_size_mb,
                _options->log_private_force_flush,
                true,
                get_gpid()
                );
        }

        // sync valid_start_offset between app and logs
        if (create_new)
        {
            dassert(_app->last_committed_decree() == 0, "");
            int64_t shared_log_offset = _stub->_log->on_partition_reset(get_gpid(), 0);
            int64_t private_log_offset = _private_log ? _private_log->on_partition_reset(get_gpid(), 0) : 0;
            err = _app->update_init_info(this, shared_log_offset, private_log_offset);
        }
        else
        {
            _stub->_log->set_valid_start_offset_on_open(get_gpid(), _app->init_info().init_offset_in_shared_log);
            if (_private_log)
                _private_log->set_valid_start_offset_on_open(get_gpid(), _app->init_info().init_offset_in_private_log);
        }

        // replay the logs
        if (nullptr != _private_log)
        {
            err = _private_log->open(
                [this](mutation_ptr& mu)
                {
                    return replay_mutation(mu, true);
                }
            );

            if (err == ERR_OK)
            {
                ddebug(
                    "%s: private log initialized, durable = %" PRId64 ", committed = %" PRId64 ", "
                    "max_prepared = %" PRId64 ", ballot = %" PRId64 ", valid_offset_in_plog = %" PRId64 ", "
                    "max_decree_in_plog = %" PRId64 ", max_commit_on_disk_in_plog = %" PRId64,
                    name(),
                    _app->last_durable_decree(),
                    _app->last_committed_decree(),
                    max_prepared_decree(),
                    get_ballot(),
                    _app->init_info().init_offset_in_private_log,
                    _private_log->max_decree(get_gpid()),
                    _private_log->max_commit_on_disk()
                    );
                _private_log->check_valid_start_offset(get_gpid(), _app->init_info().init_offset_in_private_log);
                set_inactive_state_transient(true);
            }
            /* in the beginning the prepare_list is reset to the durable_decree */
            else
            {
                derror(
                    "%s: private log initialized with err = %s, durable = %" PRId64 ", committed = %" PRId64 ", "
                    "maxpd = %" PRId64 ", ballot = %" PRId64 ", valid_offset_in_plog = %" PRId64,
                    name(),
                    err.to_string(),
                    _app->last_durable_decree(),
                    _app->last_committed_decree(),
                    max_prepared_decree(),
                    get_ballot(),
                    _app->init_info().init_offset_in_private_log
                    );

                set_inactive_state_transient(false);

                _private_log->close();
                _private_log = nullptr;

                _stub->_log->on_partition_removed(get_gpid());
            }
        }

        if (err == ERR_OK && !_options->checkpoint_disabled && nullptr == _checkpoint_timer)
        {
            _checkpoint_timer = tasking::enqueue_timer(
                LPC_PER_REPLICA_CHECKPOINT_TIMER,
                this,
                [this] {on_checkpoint_timer();},
                std::chrono::seconds(_options->checkpoint_interval_seconds),
                gpid_to_hash(get_gpid())
                );
        }
    }

    if (err != ERR_OK)
    {
        derror( "%s: open replica failed, error = %s", name(), err.to_string());
        _app->close(false);
        _app = nullptr;
    }

    return err;
}

// return false only when the log is invalid:
// - for private log, return false if offset < init_offset_in_private_log
// - for shared log, return false if offset < init_offset_in_shared_log
bool replica::replay_mutation(mutation_ptr& mu, bool is_private)
{
    auto d = mu->data.header.decree;
    auto offset = mu->data.header.log_offset;
    if (is_private && offset < _app->init_info().init_offset_in_private_log)
    {
        ddebug(
            "%s: replay mutation skipped1 as offset is invalid in private log, ballot = %" PRId64 ", decree = %" PRId64 ", last_committed_decree = %" PRId64 ", offset = %" PRId64,
            name(),
            mu->data.header.ballot,
            d,
            mu->data.header.last_committed_decree,
            offset
            );
        return false;
    }
    
    if (!is_private && offset < _app->init_info().init_offset_in_shared_log)
    {
        ddebug(
            "%s: replay mutation skipped2 as offset is invalid in shared log, ballot = %" PRId64 ", decree = %" PRId64 ", last_committed_decree = %" PRId64 ", offset = %" PRId64,
            name(),
            mu->data.header.ballot,
            d,
            mu->data.header.last_committed_decree,
            offset
            );
        return false;
    }

    // fix private log completeness when it is from shared
    if (!is_private && _private_log && d > _private_log->max_commit_on_disk())
    {
        _private_log->append(mu,
            LPC_WRITE_REPLICATION_LOG,
            this,
            [this, mu](error_code err, size_t size)
        {
            if (err != ERR_OK)
            {
                handle_local_failure(err);
            }
        },
            gpid_to_hash(get_gpid())
            );
    }

    if (d <= last_committed_decree())
    {
        ddebug(
            "%s: replay mutation skipped3 as decree is outdated, ballot = %" PRId64 ", decree = %" PRId64 "(vs app %" PRId64 "), last_committed_decree = %" PRId64 ", offset = %" PRId64,
            name(),
            mu->data.header.ballot,
            d,
            last_committed_decree(),
            mu->data.header.last_committed_decree,
            offset
            );
        return true;
    }   

    auto old = _prepare_list->get_mutation_by_decree(d);
    if (old != nullptr && old->data.header.ballot >= mu->data.header.ballot)
    {
        ddebug(
            "%s: replay mutation skipped4 as ballot is outdated, ballot = %" PRId64 " (vs local-ballot=%" PRId64 "), decree = %" PRId64 ", last_committed_decree = %" PRId64 ", offset = %" PRId64,
            name(),
            mu->data.header.ballot,
            old->data.header.ballot,
            d,
            mu->data.header.last_committed_decree,
            offset
            );

        return true;
    }
    
    if (mu->data.header.ballot > get_ballot())
    {
        _config.ballot = mu->data.header.ballot;
        bool ret = update_local_configuration(_config, true);
        dassert(ret, "");
    }

    ddebug(
        "%s: replay mutation ballot = %" PRId64 ", decree = %" PRId64 ", last_committed_decree = %" PRId64,
        name(),
        mu->data.header.ballot,
        d,
        mu->data.header.last_committed_decree
        );

    // prepare
    error_code err = _prepare_list->prepare(mu, partition_status::PS_INACTIVE);
    dassert (err == ERR_OK, "");

    return true;
}

void replica::set_inactive_state_transient(bool t)
{
    if (status() == partition_status::PS_INACTIVE)
    {
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

}} // namespace
