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
    get_dirs();
    get_hostip();
}

void global_env::get_dirs()
{
    const char *cmd = "readlink /proc/`ps aux | grep pegasus_server | grep -v grep | grep @ | sed "
                      "-n \"1p\" | awk '{print $2}'`/cwd";
    std::stringstream ss;
    pipe_execute(cmd, ss);

    // get the dir of a process in onebox, say: $PEGASUS/onebox/meta1
    char task_target[512];
    ss >> task_target;

    _pegasus_root = dirname(dirname(task_target));
    std::cout << "get project root: " << _pegasus_root << std::endl;

    assert(getcwd(task_target, sizeof(task_target)) != nullptr);
    _working_dir = task_target;
}

void global_env::get_hostip()
{
    std::stringstream output;
    pipe_execute("hostname -i", output);
    output >> _host_ip;
    std::cout << "get host ip: " << _host_ip << std::endl;
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
