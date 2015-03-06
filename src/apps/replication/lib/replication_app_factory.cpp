#include "replication_app_factory.h"
#include "mutation.h"

#define __TITLE__ "init"

namespace rdsn { namespace replication {

    bool replication_app_factory::register_factory(const char* app_type, replica_factory factory)
{
    auto it = _factories.find(app_type);
    if (it == _factories.end())
    {
        _factories[app_type] = factory;
        return true;
    }
    else
    {
        return false;
    }
}

replication_app_base* replication_app_factory::create(const char* app_type, replica* replica, configuration_ptr config)
{
    replica_factory factory = nullptr;

    auto it = _factories.find(app_type);
    if (it != _factories.end())
    {
        factory = it->second;
    }

    if (factory != nullptr)
    {
        return factory(replica, config);
    }
    else
    {
        rdsn_error( "replica type '%s' not registered", app_type);
        return nullptr;
    }
}

}} // end namespace
