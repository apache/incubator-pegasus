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
#include <dsn/internal/factory_store.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replica.init"

namespace dsn { namespace replication {

using namespace dsn::service;

error_code replica::initialize_on_new(const char* app_type, global_partition_id gpid)
{
    char buffer[256];
    sprintf(buffer, "%u.%u.%s", gpid.app_id, gpid.pidx, app_type);

    _config.gpid = gpid;
    _dir = _stub->dir() + "/" + buffer;

    if (dsn::utils::filesystem::directory_exists(_dir))
    {
        return ERR_PATH_ALREADY_EXIST;
    }

	if (!dsn::utils::filesystem::create_directory(_dir))
	{
		dassert(false, "Fail to create directory %s.", _dir.c_str());
		return ERR_FILE_OPERATION_FAILED;
	}

    error_code err = init_app_and_prepare_list(app_type, true);
    dassert (err == ERR_OK, "");
    return err;
}

/*static*/ replica* replica::newr(replica_stub* stub, const char* app_type, global_partition_id gpid)
{
    replica* rep = new replica(stub, gpid, app_type);
    if (rep->initialize_on_new(app_type, gpid) == ERR_OK)
        return rep;
    else
    {
        rep->close();
        delete rep;
        return nullptr;
    }
}

error_code replica::initialize_on_load(const char* dir, bool rename_dir_on_failure)
{
    std::string dr(dir);
    char splitters[] = { '\\', '/', 0 };
    std::string name = utils::get_last_component(dr, splitters);

    if (name == "")
    {
        derror("invalid replica dir %s", dir);
        return ERR_PATH_NOT_FOUND;
    }

    char app_type[128];
    global_partition_id gpid;
    if (3 != sscanf(name.c_str(), "%u.%u.%s", &gpid.app_id, &gpid.pidx, app_type))
    {
        derror( "invalid replica dir %s", dir);
        return ERR_PATH_NOT_FOUND;
    }
    
    _config.gpid = gpid;
    _dir = dr;

    error_code err = init_app_and_prepare_list(app_type, false);

    if (err != ERR_OK && rename_dir_on_failure)
    {
        // GCed later
        char newPath[256];
        sprintf(newPath, "%s.%x.err", dir, random32(0, (uint32_t)-1));  
		if (dsn::utils::filesystem::remove_path(newPath)
			&& dsn::utils::filesystem::rename_path(dir, newPath)
			)
		{
			derror("move bad replica from '%s' to '%s'", dir, newPath);
		}
		else
		{
			derror("Fail to move bad replica from '%s' to '%s'", dir, newPath);
			//return ERR_FILE_OPERATION_FAILED;
		}
    }
    return err;
}


/*static*/ replica* replica::load(replica_stub* stub, const char* dir, bool rename_dir_on_failure)
{
    replica* rep = new replica(stub, dir);
    error_code err = rep->initialize_on_load(dir, rename_dir_on_failure);
    if (err != ERR_OK)
    {
        rep->close();
        delete rep;
        return nullptr;
    }
    else
    {
        return rep;
    }
}

error_code replica::init_app_and_prepare_list(const char* app_type, bool create_new)
{
    dassert (nullptr == _app, "");

    sprintf(_name, "%u.%u @ %s", _config.gpid.app_id, _config.gpid.pidx, primary_address().to_string());

    _app = ::dsn::utils::factory_store<replication_app_base>::create(app_type, PROVIDER_TYPE_MAIN, this);
    if (nullptr == _app)
    {
        return ERR_OBJECT_NOT_FOUND;
    }
    dassert (nullptr != _app, "");

    int lerr = _app->open(create_new);
    error_code err = (lerr == 0 ? ERR_OK : ERR_LOCAL_APP_FAILURE);
    if (err == ERR_OK)
    {
        if (_options->log_enable_private_commit 
        || !_app->is_delta_state_learning_supported())
        {
            err = init_commit_log_service();
        }

        if (err == ERR_OK)
        {
            if (_commit_log && _commit_log->max_decree(get_gpid()) < _app->last_committed_decree())
            {
                derror("%s: commit log is incomplete (%lld vs %lld), to be fixed using prepare log replay later (if enabled)",
                    name(),
                    _commit_log->max_decree(get_gpid()),
                    _app->last_committed_decree()
                    );

                _prepare_list->reset(_commit_log->max_decree(get_gpid()));
            }
            else
                _prepare_list->reset(_app->last_committed_decree());
        }
    }

    if (err != ERR_OK)
    {
        derror( "%s: open replica failed, error = %s", name(), err.to_string());
        _app->close(false);
        delete _app;
        _app = nullptr;
    }
    
    if (nullptr != _check_timer && err == ERR_OK)
    {
        _check_timer = tasking::enqueue(
            LPC_PER_REPLICA_CHECK_TIMER,
            this,
            &replica::on_check_timer,
            gpid_to_hash(get_gpid()),
            0,
            5 * 60 * 1000 // check every five mins
            );
    }

    return err;
}

error_code replica::init_commit_log_service()
{
    error_code err = ERR_OK;
    decree last_cd = 0;
    multi_partition_decrees init_previous_log_range_decrees; // for log truncate

    dassert(nullptr == _commit_log, "commit log must not be initialized yet");

    _commit_log = new mutation_log(
        _options->log_buffer_size_mb,
        _options->log_pending_max_ms,
        _options->log_file_size_mb,
        _options->log_batch_write,
        true
        );

    std::string log_dir = dir() + "/commit-log";
    err = _commit_log->initialize(log_dir.c_str());
    if (err != ERR_OK) goto err_out;

    err = _commit_log->garbage_collection_when_as_commit_logs(
        get_gpid(),
        _app->last_durable_decree()
        );
    if (err != ERR_OK) goto err_out;
    
    err = _commit_log->replay(
        [this, &last_cd](mutation_ptr& mu)
        {
            if (last_cd != 0)
            {
                dassert(mu->data.header.decree == last_cd + 1,
                    "decrees in commit logs must be contiguous: %lld vs %lld",
                    last_cd,
                    mu->data.header.decree
                    );
            }
            last_cd = mu->data.header.decree;
                
            if (last_cd == _app->last_committed_decree() + 1)
            {
                _app->write_internal(mu);
            }
            else
            {
                ddebug("%s: mutation %s skipped coz unmached decree %llu vs %llu (last_committed)",
                    name(), mu->name(),
                    last_cd,
                    _app->last_committed_decree()
                    );
            }
        }
        );

    if (err == ERR_OK)
    {
        derror(
            "%s: local log initialized, durable = %lld, committed(log/app) = <%lld,%lld>, maxpd = %llu, ballot = %llu",
            name(),
            _app->last_durable_decree(),
            last_cd,
            _app->last_committed_decree(),
            max_prepared_decree(),
            get_ballot()
            );

        set_inactive_state_transient(true);
    }
    else
    {
        set_inactive_state_transient(false);

        // restart commit log service
        _commit_log->close();
        
        std::string err_log_path = log_dir + ".err";
        if (utils::filesystem::directory_exists(err_log_path))
            utils::filesystem::remove_path(err_log_path);

        utils::filesystem::rename_path(log_dir, err_log_path);
        utils::filesystem::create_directory(log_dir);

        _commit_log = new mutation_log(
            _options->log_buffer_size_mb,
            _options->log_pending_max_ms,
            _options->log_file_size_mb,
            _options->log_batch_write,
            true
            );

        err = _commit_log->initialize(log_dir.c_str());
    }

    if (err != ERR_OK) goto err_out;
        
    init_previous_log_range_decrees[get_gpid()] = last_cd ? last_cd : _app->last_durable_decree();    
    err = _commit_log->start_write_service(init_previous_log_range_decrees, _options->staleness_for_commit);

err_out:
    if (err != ERR_OK)
    {
        derror(
            "%s: local log initialized with log error, durable = %lld, committed(log/app) = <%lld,%lld>, maxpd = %llu, ballot = %llu",
            name(),
            _app->last_durable_decree(),
            last_cd,
            _app->last_committed_decree(),
            max_prepared_decree(),
            get_ballot()
            );

        _commit_log->close();
        _commit_log = nullptr;
    }
    return err;
}

void replica::replay_mutation(mutation_ptr& mu)
{
    auto d = mu->data.header.decree;

    ddebug(
        "%s: replay mutation ballot = %llu, decree = %llu, last_committed_decree = %llu",
        name(),
        mu->data.header.ballot,
        d,
        mu->data.header.last_committed_decree
        );

    if (d <= last_committed_decree()
        || mu->data.header.ballot < get_ballot())
    {
        return;
    }
    
    if (mu->data.header.ballot > get_ballot())
    {
        _config.ballot = mu->data.header.ballot;
        update_local_configuration(_config, true);
    }

    // prepare
    error_code err = _prepare_list->prepare(mu, PS_INACTIVE);
    dassert (err == ERR_OK, "");
}

void replica::set_inactive_state_transient(bool t)
{
    if (status() == PS_INACTIVE)
    {
        _inactive_is_transient = t;
    }
}

void replica::reset_prepare_list_after_replay()
{
    // fix commit log if possible
    _prepare_list->commit(_app->last_committed_decree(), COMMIT_TO_DECREE_SOFT);

    // align the prepare list and the app
    _prepare_list->truncate(_app->last_committed_decree());
}

}} // namespace
