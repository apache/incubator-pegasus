// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "shell/commands.h"
#include "shell/argh.h"

#include <fmt/ostream.h>
#include <dsn/utility/errors.h>
#include <dsn/utility/output_utils.h>
#include <dsn/utility/string_conv.h>
#include <dsn/dist/replication/duplication_common.h>

bool query_disk_info(command_executor *e, shell_context *sc, arguments args)
{
    // disk_info [-n|--node node_address] [-a|--app app_name] [-r|--resolve_ip]

    argh::parser cmd(args.argc, args.argv);
    if (cmd.pos_args().size() > 3) {
        fmt::print(stderr, "too many params\n");
        return false;
    }

    for (const auto &flag : cmd.flags()) {
        if (flag != "r" && flag != "resolve_ip") {
            fmt::print(stderr, "unknown flag {}\n", flag);
            return false;
        }
    }

    std::string node_address = cmd.params["node"];
    std::string app_name = cmd.params["app"];
    bool resolve_ip = cmd[{"-r", "--resolve_ip"}];

    if (node_address == "" && app_name != "") {
        fmt::print(stderr, "please input node_address!\n");
        return false;
    }

    auto err_resp = sc->ddl_client->query_disk_info(node_address, app_name, resolve_ip);
    dsn::error_s err = err_resp.get_error();
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
    }
    if (!err.is_ok()) {
        fmt::print(stderr, "querying disk_info error={}\n", err.description());
    } else if (node_address == "" && app_name == "") {
    }
}
else if (node_address != "" && app_name == "") {}
else if (node_address != "" && app_name != "") {}
return true;
}

// 伪代码
if ()