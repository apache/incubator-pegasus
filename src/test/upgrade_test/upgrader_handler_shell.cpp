// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "upgrader_handler_shell.h"
#include <cstdlib>
#include <cstring>
#include <cerrno>
#include <sstream>
#include <fstream>
#include <unistd.h>
#include <dsn/utility/config_api.h>
#include <dsn/c/api_utilities.h>

namespace pegasus {
namespace test {

upgrader_handler_shell::upgrader_handler_shell()
{
    const char *section = "upgrade.handler.shell";
    _run_script_path = dsn_config_get_value_string(
        section, "onebox_run_path", "~/pegasus/run.sh", "onebox run path");
    dassert(_run_script_path.size() > 0, "");
    _new_version_path = dsn_config_get_value_string(
        section, "new_version_path", "", "new server binary and library path");
    dassert(_new_version_path.size() > 0, "");
    _old_version_path = dsn_config_get_value_string(
        section, "old_version_path", "", "old server binary and library path");
    dassert(_old_version_path.size() > 0, "");
}

bool upgrader_handler_shell::has_meta_dumped_core(int index)
{
    char find_core[1024];
    snprintf(find_core,
             1024,
             "ls %s/onebox/meta%d | grep core | wc -l",
             _run_script_path.c_str(),
             index);

    std::stringstream output;
    int core_count;
    assert(dsn::utils::pipe_execute(find_core, output) == 0);
    output >> core_count;

    return core_count != 0;
}

bool upgrader_handler_shell::has_replica_dumped_core(int index)
{
    char find_core[1024];
    snprintf(find_core,
             1024,
             "ls %s/onebox/replica%d | grep core | wc -l",
             _run_script_path.c_str(),
             index);

    std::stringstream output;
    int core_count;
    assert(dsn::utils::pipe_execute(find_core, output) == 0);
    output >> core_count;

    return core_count != 0;
}

bool upgrader_handler_shell::upgrade_meta(int index)
{
    // not implement.
    return true;
}

bool upgrader_handler_shell::upgrade_replica(int index)
{
    std::list<std::string> cmds = generate_cmd(index, "replica", "upgrade");
    int try_times = 0;
    do {
        for (auto cmd : cmds) {
            int res = system(cmd.c_str());
            ddebug("upgrade replica command: %s", cmd.c_str());
            if (res != 0 && errno != 0) {
                ddebug("upgrade meta encounter error(%s)", strerror(errno));
                return false;
            }
            sleep(5);
        }
        if (check("replica", index, "upgrade"))
            return true;
    } while (++try_times < 3);
    return false;
}

bool upgrader_handler_shell::upgrade_zookeeper(int index)
{
    // not implement
    return true;
}

bool upgrader_handler_shell::downgrade_meta(int index)
{
    // not implement.
    return true;
}

bool upgrader_handler_shell::downgrade_replica(int index)
{
    std::list<std::string> cmds = generate_cmd(index, "replica", "downgrade");

    int try_times = 0;
    do {
        for (auto cmd : cmds) {
            int res = system(cmd.c_str());
            ddebug("downgrade replica command: %s", cmd.c_str());
            if (res != 0 && errno != 0) {
                ddebug("upgrade meta encounter error(%s)", strerror(errno));
                return false;
            }
            sleep(5);
        }
        if (check("replica", index, "downgrade"))
            return true;
    } while (++try_times < 3);
    return false;
}

bool upgrader_handler_shell::downgrade_zookeeper(int index)
{
    // not implement.
    return true;
}

bool upgrader_handler_shell::upgrade_all_meta(std::unordered_set<int> &indexs)
{
    // not implement
    return false;
}

bool upgrader_handler_shell::upgrade_all_replica(std::unordered_set<int> &indexs)
{
    // not implement.
    return false;
}

bool upgrader_handler_shell::upgrade_all_zookeeper(std::unordered_set<int> &indexs)
{
    // not implement.
    return false;
}

bool upgrader_handler_shell::downgrade_all_meta(std::unordered_set<int> &indexs)
{
    // not implement.
    return false;
}

bool upgrader_handler_shell::downgrade_all_replica(std::unordered_set<int> &indexs)
{
    // not implement.
    return false;
}

bool upgrader_handler_shell::downgrade_all_zookeeper(std::unordered_set<int> &indexs)
{
    // not implement.
    return false;
}

std::list<std::string>
upgrader_handler_shell::generate_cmd(int index, const std::string &job, const std::string &action)
{
    std::list<std::string> lst;

    std::stringstream res;
    res << "cd " << _run_script_path << "; ";

    res << "bash run.sh";
    res << " stop_onebox_instance ";
    if (job == "replica")
        res << "-r " << index << "; ";
    else
        res << "-m " << index << "; ";
    lst.push_back(res.str());

    res.str("");
    res << "cd " << _run_script_path << "/onebox/" << job << index << "; ";
    std::string version_path = "";
    if (action == "upgrade")
        version_path = _new_version_path;
    else
        version_path = _old_version_path;
    res << "ln -s -f " << version_path << "/pegasus_server; ";
    res << "export LD_LIBRARY_PATH=" << version_path << ":$LD_LIBRARY_PATH; ";
    res << "../replica" << index << "/pegasus_server config.ini -app_list ";
    if (job == "replica")
        res << "replica"
            << " &>result &";
    else
        res << "meta"
            << " &>result &";
    lst.push_back(res.str());

    return lst;
}

// type = upgrade / downgrade, but not used now
bool upgrader_handler_shell::check(const std::string &job, int index, const std::string &type)
{
    std::stringstream command;
    command << "ps aux | grep pegasus | grep " << job << index << " | grep -v grep | wc -l";
    std::stringstream output;
    int process_count = 0;

    int check_times = 5;
    do {
        sleep(1);
        dsn::utils::pipe_execute(command.str().c_str(), output);
        output >> process_count;
    } while (check_times-- > 0 and process_count == 1);

    return process_count == 1;
}
}
} // end namespace
