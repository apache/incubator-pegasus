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

#include <gtest/gtest.h>

#include "dsn/dist/fmt_logging.h"
#include "dsn/utility/utils.h"
#include "dsn/tool-api/rpc_address.h"
#include "dsn/c/api_layer1.h"
#include "test/function_test/utils/utils.h"

global_env::global_env()
{
    get_dirs();
    get_hostip();
}

void global_env::get_dirs()
{
    std::string output1;
    ASSERT_NO_FATAL_FAILURE(run_cmd(
        "ps aux | grep '/meta1/pegasus_server' | grep -v grep | awk '{print $2}'", &output1));

    // get the dir of a process in onebox, say: $PEGASUS/onebox/meta1
    std::string output2;
    ASSERT_NO_FATAL_FAILURE(run_cmd("readlink /proc/" + output1 + "/cwd", &output2));

    _pegasus_root = dirname(dirname((char *)output2.c_str()));
    std::cout << "Pegasus project root: " << _pegasus_root << std::endl;

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
}
