// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <map>
#include <memory>
#include <string>
#include <dsn/dist/replication/replication_ddl_client.h>
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
