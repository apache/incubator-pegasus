// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_server_impl.h"
#include "pegasus_service_app.h"
#include "info_collector_app.h"
#include "pegasus_perf_counter.h"
#include "pegasus_counter_updater.h"

#include <pegasus/version.h>
#include <pegasus/git_commit.h>
#include <dsn/cpp/replicated_service_app.h>
#include <dsn/tool_api.h>
#include <dsn/tool-api/command.h>

#include <cstdio>
#include <cstring>
#include <chrono>

#if defined(__linux__)
#include <sys/types.h>
#include <unistd.h>
#endif

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "pegasus.server.main"

#define STR_I(var) #var
#define STR(var) STR_I(var)
#ifndef DSN_BUILD_TYPE
#define PEGASUS_BUILD_TYPE ""
#else
#define PEGASUS_BUILD_TYPE STR(DSN_BUILD_TYPE)
#endif

void dsn_app_registration_pegasus()
{
    // register all possible service apps
    dsn_task_code_register(
        "RPC_L2_CLIENT_READ", TASK_TYPE_RPC_REQUEST, TASK_PRIORITY_COMMON, THREAD_POOL_LOCAL_APP);
    dsn_task_code_register(
        "RPC_L2_CLIENT_WRITE", TASK_TYPE_RPC_REQUEST, TASK_PRIORITY_LOW, THREAD_POOL_REPLICATION);
    ::dsn::register_layer2_framework<::pegasus::server::pegasus_replication_service_app>(
        "replica", DSN_APP_MASK_FRAMEWORK);

    ::dsn::register_app<::pegasus::server::pegasus_meta_service_app>("meta");
    ::dsn::register_app_with_type_1_replication_support<::pegasus::server::pegasus_server_impl>(
        "pegasus");
    ::dsn::register_app<::pegasus::server::info_collector_app>("collector");

    ::dsn::tools::internal_use_only::register_component_provider(
        "pegasus::server::pegasus_perf_counter",
        pegasus::server::pegasus_perf_counter_factory,
        ::dsn::PROVIDER_TYPE_MAIN);

    ::dsn::register_command(
        "server-info",
        "server-info - query server information",
        "server-info",
        [](const std::vector<std::string> &args) {
            char str[100];
            ::dsn::utils::time_ms_to_date_time(dsn_runtime_init_time_ms(), str, 100);
            std::ostringstream oss;
            oss << "Pegasus Server " << PEGASUS_VERSION << " (" << PEGASUS_GIT_COMMIT << ") "
                << PEGASUS_BUILD_TYPE << ", Started at " << str;
            return oss.str();
        });

    ::dsn::register_command(
        "server-stat",
        "server-stat - query server statistics",
        "server-stat",
        [](const std::vector<std::string> &args) {
            return ::pegasus::server::pegasus_counter_updater::instance().get_brief_stat();
        });

    ::dsn::register_command(
        "perf-counters",
        "perf-counters - query perf counters, supporting filter by POSIX basic regular expressions",
        "perf-counters [name-filter]...",
        [](const std::vector<std::string> &args) {
            return ::pegasus::server::pegasus_counter_updater::instance().get_perf_counters(args);
        });
}

#if defined(__linux__)
#include <pegasus/version.h>
#include <pegasus/git_commit.h>
#include <dsn/version.h>
#include <dsn/git_commit.h>
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
#endif

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "-v") == 0 || strcmp(argv[i], "-version") == 0 ||
            strcmp(argv[i], "--version") == 0) {
            printf("Pegasus Server %s (%s) %s\n",
                   PEGASUS_VERSION,
                   PEGASUS_GIT_COMMIT,
                   PEGASUS_BUILD_TYPE);
            return 0;
        }
    }
    ddebug("pegasus server starting, pid(%d), version(%s)", (int)getpid(), pegasus_server_rcsid());
    dsn_app_registration_pegasus();
    dsn_run(argc, argv, true);
    return 0;
}
