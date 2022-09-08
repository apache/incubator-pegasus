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

#include "global_env.h"

#include <arpa/inet.h>
#include <unistd.h>
#include <libgen.h>

#include <iostream>
#include <memory>

#include "dsn/dist/fmt_logging.h"
#include "dsn/utility/utils.h"
#include "dsn/tool-api/rpc_address.h"
#include "dsn/c/api_layer1.h"

global_env::global_env()
{
    std::cout << "============" << std::endl << "start global_env()" << std::endl;
    get_dirs();
    get_hostip();
}

void global_env::get_dirs()
{
    const char *cmd1 = "ps aux | grep '/meta1/pegasus_server' | grep -v grep | awk '{print $2}'";
    std::stringstream ss1;
    int ret = dsn::utils::pipe_execute(cmd1, ss1);
    std::cout << cmd1 << " output: " << ss1.str() << std::endl;
    dcheck_eq(ret, 0);
    int meta1_pid;
    ss1 >> meta1_pid;
    std::cout << "meta1 pid: " << meta1_pid << std::endl;

    // get the dir of a process in onebox, say: $PEGASUS/onebox/meta1
    char cmd2[512] = {0};
    sprintf(cmd2, "readlink /proc/%d/cwd", meta1_pid);
    std::stringstream ss2;
    ret = dsn::utils::pipe_execute(cmd2, ss2);
    std::cout << cmd2 << " output: " << ss2.str() << std::endl;
    dcheck_eq(ret, 0);
    std::string meta1_dir;
    ss2 >> meta1_dir;
    std::cout << "meta1 dir: " << meta1_dir << std::endl;

    _pegasus_root = dirname(dirname((char *)meta1_dir.c_str()));
    std::cout << "project root: " << _pegasus_root << std::endl;

    char task_target[512] = {0};
    dassert_f(getcwd(task_target, sizeof(task_target)), "");
    _working_dir = task_target;
    std::cout << "working dir: " << _working_dir << std::endl;
}

void global_env::get_hostip()
{
    uint32_t ip = dsn::rpc_address::ipv4_from_network_interface("lo");
    uint32_t ipnet = htonl(ip);
    char buffer[512] = {0};
    memset(buffer, 0, sizeof(buffer));
    dassert_f(inet_ntop(AF_INET, &ipnet, buffer, sizeof(buffer)), "");
    _host_ip = buffer;
    std::cout << "get ip: " << _host_ip << std::endl;
}
