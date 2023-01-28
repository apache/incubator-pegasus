/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <cstdio>
#include <cstring>
#include <chrono>

#include <sys/types.h>
#include <unistd.h>

#include <pegasus/version.h>
#include <pegasus/git_commit.h>

#include "compaction_operation.h"
#include "info_collector_app.h"
#include "meta/meta_service_app.h"
#include "pegasus_server_impl.h"
#include "pegasus_service_app.h"
#include "replica/replication_service_app.h"
#include "runtime/tool_api.h"
#include "utils/command_manager.h"

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
    pegasus::server::register_compaction_operations();
}

int main(int argc, char **argv)
{
    for (int i = 1; i < argc; ++i) {
        if (utils::equals(argv[i], "-v") || utils::equals(argv[i], "-version") ||
            utils::equals(argv[i], "--version")) {
            printf("Pegasus Server %s (%s) %s\n",
                   PEGASUS_VERSION,
                   PEGASUS_GIT_COMMIT,
                   PEGASUS_BUILD_TYPE);
            dsn_exit(0);
        }
    }
    LOG_INFO("pegasus server starting, pid({}), version({})", getpid(), pegasus_server_rcsid());
    dsn_app_registration_pegasus();

    std::unique_ptr<command_deregister> server_info_cmd =
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

    dsn_run(argc, argv, true);

    return 0;
}
