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

    sprintf(_name, "%u.%u@%s:%hu", _config.gpid.app_id, _config.gpid.pidx,
            _primary_address.name(), _primary_address.port());

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

    if (_options->log_private)
        start_private_log_service();

    return err;
}

/*static*/ replica* replica::newr(replica_stub* stub, const char* app_type, global_partition_id gpid)
{
    replica* rep = new replica(stub, gpid, app_type);
    if (rep->initialize_on_new(app_type, gpid) == ERR_OK)
        return rep;
    else
    {
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

    sprintf(_name, "%u.%u@%s:%hu", _config.gpid.app_id, _config.gpid.pidx,
            _primary_address.name(), _primary_address.port());

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

    if (_options->log_private)
        start_private_log_service();

    return err;
}


/*static*/ replica* replica::load(replica_stub* stub, const char* dir, bool rename_dir_on_failure)
{
    replica* rep = new replica(stub, dir);
    error_code err = rep->initialize_on_load(dir, rename_dir_on_failure);
    if (err != ERR_OK)
    {
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
        dassert (_app->last_durable_decree() == _app->last_committed_decree(), "");
        _prepare_list->reset(_app->last_committed_decree());
    }
    else
    {
        derror( "open replica '%s' under '%s' failed, error = %d", app_type, dir().c_str(), lerr);
        delete _app;
        _app = nullptr;
    }

    return err;
}

void replica::start_private_log_service()
{
    dassert(_log != nullptr, "private log must be present");

    multi_partition_decrees init_max_decrees; // for log truncate
    init_max_decrees[get_gpid()] = max_prepared_decree();
    error_code err = _log->start_write_service(init_max_decrees, _options->staleness_for_commit);
    dassert(err == ERR_OK, "");
}

error_code replica::replay_private_log()
{
    replicas rps;
    rps[get_gpid()] = this;

    error_code err = _log->replay(
        std::bind(&replica_stub::replay_mutation, _stub, std::placeholders::_1, &rps)
        );

    if (!_options->log_shared)
        reset_prepare_list_after_replay();
    else
    {
        // let replica stub does this with global log
    }

    if (err == ERR_OK)
    {
        derror(
            "%u.%u @ %s:%hu: local log initialized, durable = %lld, committed = %llu, maxpd = %llu, ballot = %llu",
            _config.gpid.app_id, _config.gpid.pidx,
            _primary_address.name(), _primary_address.port(),
            last_durable_decree(),
            last_committed_decree(),
            max_prepared_decree(),
            get_ballot()
            );

        set_inactive_state_transient(true);
    }
    else
    {
        derror(
            "%u.%u @ %s:%hu: local log initialized with log error, durable = %lld, committed = %llu, maxpd = %llu, ballot = %llu",
            _config.gpid.app_id, _config.gpid.pidx,
            _primary_address.name(), _primary_address.port(),
            last_durable_decree(),
            last_committed_decree(),
            max_prepared_decree(),
            get_ballot()
            );

        set_inactive_state_transient(false);
    }
    return err;
}

void replica::replay_mutation(mutation_ptr& mu)
{
    if (mu->data.header.decree <= last_committed_decree() ||
        mu->data.header.ballot < get_ballot())
    {
        return;
    }
        
    
    if (mu->data.header.ballot > get_ballot())
    {
        _config.ballot = mu->data.header.ballot;
        update_local_configuration(_config, true);
    }

    // prepare
    /*ddebug( 
            "%u.%u @ %s:%hu: replay mutation ballot = %llu, decree = %llu, last_committed_decree = %llu",
            get_gpid().app_id, get_gpid().pidx, 
            address().name(), address().port(),
            mu->data.header.ballot, 
            mu->data.header.decree,
            mu->data.header.last_committed_decree
        );*/

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
    if (_prepare_list->min_decree() > _app->last_committed_decree() + 1)
    {
        _prepare_list->reset(_app->last_committed_decree());
    }
    else
    {
        _prepare_list->truncate(_app->last_committed_decree());
    }
}

}} // namespace
