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

#include "killer_handler_shell.h"

#include <stdio.h>
#include <cerrno>
#include <cstdlib>
#include <sstream> // IWYU pragma: keep

#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/process_utils.h"
#include "utils/safe_strerror_posix.h"
#include "utils/strings.h"

namespace pegasus {
namespace test {

DSN_DEFINE_string(killer.handler.shell, onebox_run_path, "~/pegasus/run.sh", "onebox run path");
DSN_DEFINE_validator(onebox_run_path,
                     [](const char *value) -> bool { return !dsn::utils::is_empty(value); });

bool killer_handler_shell::has_meta_dumped_core(int index)
{
    char find_core[1024];
    snprintf(
        find_core, 1024, "ls %s/onebox/meta%d | grep core | wc -l", FLAGS_onebox_run_path, index);

    std::stringstream output;
    int core_count;
    CHECK_EQ(dsn::utils::pipe_execute(find_core, output), 0);
    output >> core_count;

    return core_count != 0;
}

bool killer_handler_shell::has_replica_dumped_core(int index)
{
    char find_core[1024];
    snprintf(find_core,
             1024,
             "ls %s/onebox/replica%d | grep core | wc -l",
             FLAGS_onebox_run_path,
             index);

    std::stringstream output;
    int core_count;
    CHECK_EQ(dsn::utils::pipe_execute(find_core, output), 0);
    output >> core_count;

    return core_count != 0;
}

bool killer_handler_shell::kill_meta(int index)
{
    std::string cmd = generate_cmd(index, "meta", "stop");
    int res = system(cmd.c_str());
    LOG_INFO("kill meta command: {}", cmd);
    if (res != 0) {
        LOG_INFO("kill meta encounter error({})", dsn::utils::safe_strerror(errno));
        return false;
    }
    return check("meta", index, "stop");
}

bool killer_handler_shell::kill_replica(int index)
{
    std::string cmd = generate_cmd(index, "replica", "stop");
    int res = system(cmd.c_str());
    LOG_INFO("kill replica command: {}", cmd);
    if (res != 0) {
        LOG_INFO("kill meta encounter error({})", dsn::utils::safe_strerror(errno));
        return false;
    }
    return check("replica", index, "stop");
}

bool killer_handler_shell::kill_zookeeper(int index)
{
    // not implement
    return true;
}

bool killer_handler_shell::start_meta(int index)
{
    std::string cmd = generate_cmd(index, "meta", "start");
    int res = system(cmd.c_str());
    LOG_INFO("start meta command: {}", cmd);
    if (res != 0) {
        LOG_INFO("kill meta encounter error({})", dsn::utils::safe_strerror(errno));
        return false;
    }
    return check("meta", index, "start");
}

bool killer_handler_shell::start_replica(int index)
{
    std::string cmd = generate_cmd(index, "replica", "start");

    int res = system(cmd.c_str());
    LOG_INFO("start replica command: {}", cmd);
    if (res != 0) {
        LOG_INFO("kill meta encounter error({})", dsn::utils::safe_strerror(errno));
        return false;
    }
    return check("meta", index, "start");
}

bool killer_handler_shell::start_zookeeper(int index)
{
    // not implement.
    return true;
}

bool killer_handler_shell::kill_all_meta(std::unordered_set<int> &indexs)
{
    // not implement
    return false;
}

bool killer_handler_shell::kill_all_replica(std::unordered_set<int> &indexs)
{
    // not implement.
    return false;
}

bool killer_handler_shell::kill_all_zookeeper(std::unordered_set<int> &indexs)
{
    // not implement.
    return false;
}

bool killer_handler_shell::start_all_meta(std::unordered_set<int> &indexs)
{
    // not implement.
    return false;
}

bool killer_handler_shell::start_all_replica(std::unordered_set<int> &indexs)
{
    // not implement.
    return false;
}

bool killer_handler_shell::start_all_zookeeper(std::unordered_set<int> &indexs)
{
    // not implement.
    return false;
}

std::string
killer_handler_shell::generate_cmd(int index, const std::string &job, const std::string &action)
{
    std::stringstream res;
    res << "cd " << FLAGS_onebox_run_path << "; "
        << "bash run.sh";
    if (action == "stop")
        res << " stop_onebox_instance ";
    else
        res << " start_onebox_instance ";
    if (job == "replica")
        res << "-r " << index;
    else
        res << "-m " << index;
    return res.str();
}

// type = stop / start
bool killer_handler_shell::check(const std::string &job, int index, const std::string &type)
{
    // not implement, just return true
    return true;
}
}
} // end namespace
