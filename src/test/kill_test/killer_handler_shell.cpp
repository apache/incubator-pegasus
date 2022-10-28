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

#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <sstream>
#include <fstream>

#include "utils/api_utilities.h"
#include "utils/config_api.h"
#include "utils/fmt_logging.h"

namespace pegasus {
namespace test {

killer_handler_shell::killer_handler_shell()
{
    const char *section = "killer.handler.shell";
    _run_script_path = dsn_config_get_value_string(
        section, "onebox_run_path", "~/pegasus/run.sh", "onebox run path");
    CHECK(!_run_script_path.empty(), "");
}

bool killer_handler_shell::has_meta_dumped_core(int index)
{
    char find_core[1024];
    snprintf(find_core,
             1024,
             "ls %s/onebox/meta%d | grep core | wc -l",
             _run_script_path.c_str(),
             index);

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
             _run_script_path.c_str(),
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
    LOG_INFO("kill meta command: %s", cmd.c_str());
    if (res != 0) {
        LOG_INFO("kill meta encounter error(%s)", strerror(errno));
        return false;
    }
    return check("meta", index, "stop");
}

bool killer_handler_shell::kill_replica(int index)
{
    std::string cmd = generate_cmd(index, "replica", "stop");
    int res = system(cmd.c_str());
    LOG_INFO("kill replica command: %s", cmd.c_str());
    if (res != 0) {
        LOG_INFO("kill meta encounter error(%s)", strerror(errno));
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
    LOG_INFO("start meta command: %s", cmd.c_str());
    if (res != 0) {
        LOG_INFO("kill meta encounter error(%s)", strerror(errno));
        return false;
    }
    return check("meta", index, "start");
}

bool killer_handler_shell::start_replica(int index)
{
    std::string cmd = generate_cmd(index, "replica", "start");

    int res = system(cmd.c_str());
    LOG_INFO("start replica command: %s", cmd.c_str());
    if (res != 0) {
        LOG_INFO("kill meta encounter error(%s)", strerror(errno));
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
    res << "cd " << _run_script_path << "; "
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
