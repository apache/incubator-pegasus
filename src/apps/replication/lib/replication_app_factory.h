#pragma once

#include "replication_app_base.h"


namespace rdsn { namespace replication {

class replication_app_factory : public rdsn::utils::singleton<replication_app_factory>
{
public:
    typedef std::function<replication_app_base* (replica* replica, configuration_ptr config)> replica_factory;
    replication_app_factory(){}

public:
    bool register_factory(const char* app_type, replica_factory factory);

    replication_app_base* create(const char* app_type, replica* replica, configuration_ptr config);

private:
    std::map<std::string, replica_factory> _factories;
};

}} // end namespace
