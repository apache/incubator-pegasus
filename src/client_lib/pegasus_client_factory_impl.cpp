// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_client_factory_impl.h"

namespace pegasus {
namespace client {

std::unordered_map<std::string, pegasus_client_factory_impl::app_to_client_map>
    pegasus_client_factory_impl::_cluster_to_clients;
dsn::zlock *pegasus_client_factory_impl::_map_lock;

bool pegasus_client_factory_impl::initialize(const char *config_file)
{
    bool is_initialized = ::dsn::tools::is_engine_ready();
    if (config_file == nullptr) {
        dassert(is_initialized, "rdsn engine not started, please specify a valid config file");
    } else {
        if (is_initialized) {
            dwarn("rdsn engine already started, ignore the config file '%s'", config_file);
        } else {
            // use config file to run
            char exe[] = "client";
            char config[1024];
            sprintf(config, "%s", config_file);
            char *argv[] = {exe, config};
            dsn_run(2, argv, false);
        }
    }
    pegasus_client_impl::init_error();
    _map_lock = new ::dsn::zlock();
    return true;
}

pegasus_client *pegasus_client_factory_impl::get_client(const char *cluster_name,
                                                        const char *app_name)
{
    if (cluster_name == nullptr || cluster_name[0] == '\0') {
        derror("invalid parameter 'cluster_name'");
        return nullptr;
    }
    if (app_name == nullptr || app_name[0] == '\0') {
        derror("invalid parameter 'app_name'");
        return nullptr;
    }

    ::dsn::zauto_lock l(*_map_lock);
    auto it = _cluster_to_clients.find(cluster_name);
    if (it == _cluster_to_clients.end()) {
        it = _cluster_to_clients
                 .insert(cluster_to_app_map::value_type(cluster_name, app_to_client_map()))
                 .first;
    }

    app_to_client_map &app_to_clients = it->second;
    auto it2 = app_to_clients.find(app_name);
    if (it2 == app_to_clients.end()) {
        pegasus_client_impl *client = new pegasus_client_impl(cluster_name, app_name);
        it2 = app_to_clients.insert(app_to_client_map::value_type(app_name, client)).first;
    }

    return it2->second;
}
}
} // namespace

#if defined(__linux__)
#include <pegasus/version.h>
#include <pegasus/git_commit.h>
#include <dsn/version.h>
#include <dsn/git_commit.h>
#define STR_I(var) #var
#define STR(var) STR_I(var)
static char const rcsid[] =
    "$Version: Pegasus Client " PEGASUS_VERSION " (" PEGASUS_GIT_COMMIT ")"
#if defined(DSN_BUILD_TYPE)
    " " STR(DSN_BUILD_TYPE)
#endif
        ", built with rDSN " DSN_CORE_VERSION " (" DSN_GIT_COMMIT ")"
        ", built by gcc " STR(__GNUC__) "." STR(__GNUC_MINOR__) "." STR(__GNUC_PATCHLEVEL__)
#if defined(DSN_BUILD_HOSTNAME)
            ", built on " STR(DSN_BUILD_HOSTNAME)
#endif
                ", built at " __DATE__ " " __TIME__ " $";
const char *pegasus_client_rcsid() { return rcsid; }
#endif
