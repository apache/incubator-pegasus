#include "replica.h"
#include "replication_app_base.h"
#include "mutation.h"
#include "mutation_log.h"
#include "replica_stub.h"
#include "replication_app_factory.h"
#include <boost/filesystem.hpp>

#define __TITLE__ "init"

namespace rdsn { namespace replication {

using namespace rdsn::service;

int replica::initialize_on_new(const char* app_type, global_partition_id gpid)
{
    char buffer[256];
    sprintf(buffer, "%u.%u.%s", gpid.tableId, gpid.pidx, app_type);

    _config.gpid = gpid;
    _dir = _stub->dir() + "/" + buffer;

    if (boost::filesystem::exists(_dir))
    {
        return ERR_PATH_ALREADY_EXIST;
    }

    boost::filesystem::create_directory(_dir);

    int err = init_app_and_prepare_list(app_type, true);
    rdsn_assert (err == ERR_SUCCESS, "");
    return err;
}

/*static*/ replica* replica::newr(replica_stub* stub, const char* app_type, global_partition_id gpid, replication_options& options)
{
    replica* rep = new replica(stub, gpid, options);
    if (ERR_SUCCESS == rep->initialize_on_new(app_type, gpid))
        return rep;
    else
    {
        delete rep;
        return nullptr;
    }
}

int replica::initialize_on_load(const char* dir, bool renameDirOnFailure)
{
    std::string dr(dir);
    auto pos = dr.find_last_of('/');
    if (pos == std::string::npos)
    {
        rdsn_error( "invalid replica dir %s", dir);
        return ERR_PATH_NOT_FOUND;
    }

    char app_type[128];
    global_partition_id gpid;
    std::string name = dr.substr(pos + 1);
    if (4 != sscanf(name.c_str(), "%u.%u.%s", &gpid.tableId, &gpid.pidx, app_type))
    {
        rdsn_error( "invalid replica dir %s", dir);
        return ERR_PATH_NOT_FOUND;
    }
    
    _config.gpid = gpid;
    _dir = dr;

    int err = init_app_and_prepare_list(app_type, false);

    if (ERR_SUCCESS != err && renameDirOnFailure)
    {
        // GCed later
        char newPath[256];
        sprintf(newPath, "%s.%x.err", dir, random32(0, (uint32_t)-1));  
        boost::filesystem::remove_all(newPath);
        boost::filesystem::rename(dir, newPath);
        rdsn_error( "move bad replica from '%s' to '%s'", dir, newPath);
    }

    return err;
}


/*static*/ replica* replica::load(replica_stub* stub, const char* dir, replication_options& options, bool renameDirOnFailure)
{
    replica* rep = new replica(stub, options);
    int err = rep->initialize_on_load(dir, renameDirOnFailure);
    if (err != ERR_SUCCESS)
    {
        delete rep;
        return nullptr;
    }
    else
    {
        return rep;
    }
}

int replica::init_app_and_prepare_list(const char* app_type, bool createNew)
{
    rdsn_assert (nullptr == _app, "");

    _app = replication_app_factory::instance().create(app_type, this, _stub->config());
    if (nullptr == _app)
    {
        return ERR_OBJECT_NOT_FOUND;
    }
    rdsn_assert(nullptr != _app, "");

    int err = _app->open(createNew);    
    if (ERR_SUCCESS == err)
    {
        rdsn_assert (_app->last_durable_decree() == _app->last_committed_decree(), "");
        _prepare_list->reset(_app->last_committed_decree());
    }
    else
    {
        rdsn_error( "open replica '%s' under '%s' failed, err = %x", app_type, dir().c_str(), err);
        delete _app;
        _app = nullptr;
    }

    sprintf(_name, "%u.%u @ %s:%u", _config.gpid.tableId, _config.gpid.pidx, address().name.c_str(), (int)address().port);

    return err;
}

void replica::replay_mutation(mutation_ptr& mu)
{
    if (mu->data.header.decree <= last_committed_decree() ||
        mu->data.header.ballot < get_ballot())
        return;
    
    if (mu->data.header.ballot > get_ballot())
    {
        _config.ballot = mu->data.header.ballot;
        update_local_configuration(_config);
    }

    // prepare
    /*rdsn_debug( 
            "%u.%u @ %s:%u: replay mutation ballot = %llu, decree = %llu, lastCommittedDecree = %llu",
            get_gpid().tableId, get_gpid().pidx, 
            address().name.c_str(), (int)address().port,
            mu->data.header.ballot, 
            mu->data.header.decree,
            mu->data.header.lastCommittedDecree
        );*/

    int err = _prepare_list->prepare(mu, PS_INACTIVE);
    rdsn_assert(err == ERR_SUCCESS, "");
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
