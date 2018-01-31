// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <unistd.h>
#include <libgen.h>
#include <iostream>
#include <cassert>
#include <memory>

#include "global_env.h"
global_env global_env::inst;

global_env::global_env()
{
    std::cout << "============" << std::endl << "start global_env()" << std::endl;
    get_dirs();
    get_hostip();
}

void global_env::get_dirs()
{
    const char *cmd1 = "ps aux | grep pegasus_server | grep meta1 | awk '{print $2}'";
    std::stringstream ss1;
    pipe_execute(cmd1, ss1);
    int meta1_pid;
    ss1 >> meta1_pid;
    std::cout << "meta1 pid: " << meta1_pid << std::endl;

    // get the dir of a process in onebox, say: $PEGASUS/onebox/meta1
    char cmd2[512];
    sprintf(cmd2, "readlink /proc/%d/cwd", meta1_pid);
    std::stringstream ss2;
    pipe_execute(cmd2, ss2);
    std::string meta1_dir;
    ss2 >> meta1_dir;
    std::cout << "meta1 dir: " << meta1_dir << std::endl;

    _pegasus_root = dirname(dirname((char *)meta1_dir.c_str()));
    std::cout << "project root: " << _pegasus_root << std::endl;
    assert(_pegasus_root != ".");

    char task_target[512];
    assert(getcwd(task_target, sizeof(task_target)) != nullptr);
    _working_dir = task_target;
    std::cout << "working dir: " << _working_dir << std::endl;
}

void global_env::get_hostip()
{
    std::stringstream output;
    pipe_execute("hostname -i", output);
    output >> _host_ip;
    std::cout << "host ip: " << _host_ip << std::endl;
}

/*static*/
void global_env::pipe_execute(const char *command, std::stringstream &output)
{
    std::array<char, 256> buffer;

    std::shared_ptr<FILE> command_pipe(popen(command, "r"), pclose);
    while (!feof(command_pipe.get())) {
        if (fgets(buffer.data(), 256, command_pipe.get()) != NULL)
            output << buffer.data();
    }
}
