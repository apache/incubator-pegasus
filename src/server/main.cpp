// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_server_impl.h"
#include "pegasus_service_app.h"
#include "info_collector_app.h"
#include "brief_stat.h"

#include <dsn/version.h>
#include <dsn/git_commit.h>
#include <pegasus/version.h>
#include <pegasus/git_commit.h>

#include <dsn/tool_api.h>
#include <dsn/tool-api/command_manager.h>

#include <dsn/dist/replication/replication_service_app.h>
#include <dsn/dist/replication/meta_service_app.h>

#include <cstdio>
#include <cstring>
#include <chrono>

#include <sys/types.h>
#include <unistd.h>

#define STR_I(var) #var
#define STR(var) STR_I(var)
#ifndef DSN_BUILD_TYPE
#define PEGASUS_BUILD_TYPE ""
#else
#define PEGASUS_BUILD_TYPE STR(DSN_BUILD_TYPE)
#endif

static char const rcsid[] =
    "$Version: Pegasus Server " PEGASUS_VERSION " (" PEGASUS_GIT_COMMIT ")"
#if defined(DSN_BUILD_TYPE)
    " " STR(DSN_BUILD_TYPE)
#endif
        ", built with rDSN " DSN_CORE_VERSION " (" DSN_GIT_COMMIT ")"
        ", built by gcc " STR(__GNUC__) "." STR(__GNUC_MINOR__) "." STR(__GNUC_PATCHLEVEL__)
#if defined(DSN_BUILD_HOSTNAME)
            ", built on " STR(DSN_BUILD_HOSTNAME)
#endif
                ", built at " __DATE__ " " __TIME__ " $";

const char *pegasus_server_rcsid() { return rcsid; }

using namespace dsn;
using namespace dsn::replication;

void dsn_app_registration_pegasus()
{
    dsn::service::meta_service_app::register_components();
    service_app::register_factory<pegasus::server::pegasus_meta_service_app>("meta");
    service_app::register_factory<pegasus::server::pegasus_replication_service_app>("replica");
    service_app::register_factory<pegasus::server::info_collector_app>("collector");
    pegasus::server::pegasus_server_impl::register_service();

    dsn::command_manager::instance().register_command(
        {"server-info"},
        "server-info - query server information",
        "server-info",
        [](const std::vector<std::string> &args) {
            char str[100];
            ::dsn::utils::time_ms_to_date_time(dsn::utils::process_start_millis(), str, 100);
            std::ostringstream oss;
            oss << "Pegasus Server " << PEGASUS_VERSION << " (" << PEGASUS_GIT_COMMIT << ") "
                << PEGASUS_BUILD_TYPE << ", Started at " << str;
            return oss.str();
        });
    dsn::command_manager::instance().register_command(
        {"server-stat"},
        "server-stat - query selected perf counters",
        "server-stat",
        [](const std::vector<std::string> &args) { return pegasus::get_brief_stat(); });
}

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "-version") == 0 ||
            strcmp(argv[i], "--version") == 0) {
            printf("Pegasus Server %s (%s) %s\n",
                   PEGASUS_VERSION,
                   PEGASUS_GIT_COMMIT,
                   PEGASUS_BUILD_TYPE);
            dsn_exit(0);
        }
    }
    ddebug("pegasus server starting, pid(%d), version(%s)", (int)getpid(), pegasus_server_rcsid());
    dsn_app_registration_pegasus();
    dsn_run(argc, argv, true);

    return 0;
}
