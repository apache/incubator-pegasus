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

#pragma once

#include <map>
#include <memory>
#include <string>
#include "client/replication_ddl_client.h"
#include <pegasus/client.h>

#include "sds/sds.h"

struct command_executor;
struct shell_context
{
    std::string current_cluster_name;
    std::string current_app_name;
    std::vector<dsn::rpc_address> meta_list;
    std::unique_ptr<dsn::replication::replication_ddl_client> ddl_client;
    pegasus::pegasus_client *pg_client;
    bool escape_all;
    int timeout_ms;
    shell_context() : pg_client(nullptr), escape_all(false), timeout_ms(5000) {}
};

struct arguments
{
    int argc;
    sds *argv;
};

typedef bool (*executor)(command_executor *this_, shell_context *sc, arguments args);

struct command_executor
{
    const char *name;
    const char *description;
    const char *option_usage;
    executor exec;
};
