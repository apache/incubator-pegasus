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

#include <fmt/core.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <stdio.h>
#include <map>
#include <string>
#include <vector>

#include "pegasus/git_commit.h"
#include "pegasus/version.h"
#include "server/server_utils.h"
#include "utils/command_manager.h"
#include "utils/process_utils.h"
#include "utils/strings.h"
#include "utils/time_utils.h"

using namespace dsn;

bool help(int argc, char **argv, const char *app_name)
{
    for (int i = 1; i < argc; ++i) {
        if (utils::equals(argv[i], "-v") || utils::equals(argv[i], "-version") ||
            utils::equals(argv[i], "--version")) {
            fmt::print(stdout,
                       "{} {} ({}) {}\n",
                       app_name,
                       PEGASUS_VERSION,
                       PEGASUS_GIT_COMMIT,
                       PEGASUS_BUILD_TYPE);
            return true;
        }
    }

    return false;
}

std::unique_ptr<command_deregister> register_server_info_cmd()
{
    return dsn::command_manager::instance().register_single_command(
        "server-info", "Query server information", "", [](const std::vector<std::string> &args) {
            nlohmann::json info;
            info["version"] = PEGASUS_VERSION;
            info["build_type"] = PEGASUS_BUILD_TYPE;
            info["git_SHA"] = PEGASUS_GIT_COMMIT;
            info["start_time"] =
                ::dsn::utils::time_s_to_date_time(dsn::utils::process_start_millis() / 1000);
            return info.dump();
        });
}

static char const rcsid[] = "$Version: Pegasus Server " PEGASUS_VERSION " (" PEGASUS_GIT_COMMIT ")"
#if defined(DSN_BUILD_TYPE)
                            " " STRINGIFY(DSN_BUILD_TYPE)
#endif
                                ", built by gcc " STRINGIFY(__GNUC__) "." STRINGIFY(
                                    __GNUC_MINOR__) "." STRINGIFY(__GNUC_PATCHLEVEL__)
#if defined(DSN_BUILD_HOSTNAME)
                                    ", built on " STRINGIFY(DSN_BUILD_HOSTNAME)
#endif
                                        ", built at " __DATE__ " " __TIME__ " $";

const char *pegasus_server_rcsid() { return rcsid; }
