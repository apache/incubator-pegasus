// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "killer_handler_shell.h"
#include <cstdlib>
#include <sstream>
#include <fstream>
#include <dsn/c/api_utilities.h>

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "killer.handler.shell"

namespace pegasus {
namespace test {

killer_handler_shell::killer_handler_shell(std::shared_ptr<std::ofstream> &log_handler)
{
    const char *section = "killer.handler.shell";
    _run_script_path = dsn_config_get_value_string(
        section, "onebox_run_path", "~/pegasus/run.sh", "onebox run path");
    dassert(_run_script_path.size() > 0, "");
    _log_handler = log_handler;
}

bool killer_handler_shell::kill_meta(int index)
{
    std::ofstream &out = *_log_handler;
    out << "meta@" << index;
    std::string cmd = generate_cmd(index, "meta", "stop");
    int res = system(cmd.c_str());
    ddebug("kill meta command: %s", cmd.c_str());
    if (res != 0)
        return false;
    return check("meta", index, "stop");
}

bool killer_handler_shell::kill_replica(int index)
{
    std::ofstream &out = *_log_handler;
    out << "replica@" << index;
    std::string cmd = generate_cmd(index, "replica", "stop");
    ;
    int res = system(cmd.c_str());
    ddebug("kill replica command: %s", cmd.c_str());
    if (res != 0)
        return false;
    return check("replica", index, "stop");
}

bool killer_handler_shell::kill_zookeeper(int index)
{
    std::ofstream &out = *_log_handler;
    out << "zookeeper@" << index;
    // not implement
    return true;
}

bool killer_handler_shell::start_meta(int index)
{
    std::ofstream &out = *_log_handler;
    out << "meta@" << index;
    std::string cmd = generate_cmd(index, "meta", "start");
    ;
    int res = system(cmd.c_str());
    ddebug("start meta command: %s", cmd.c_str());
    if (res != 0)
        return false;
    return check("meta", index, "start");
}

bool killer_handler_shell::start_replica(int index)
{
    std::ofstream &out = *_log_handler;
    out << "replica@" << index;
    std::string cmd = generate_cmd(index, "replica", "start");
    ;
    int res = system(cmd.c_str());
    ddebug("start replica command: %s", cmd.c_str());
    if (res != 0)
        return false;
    return check("meta", index, "start");
}

bool killer_handler_shell::start_zookeeper(int index)
{
    std::ofstream &out = *_log_handler;
    out << "zookeeper@" << index;
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
        << "bash "
        << " run.sh";
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
