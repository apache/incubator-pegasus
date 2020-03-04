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
    // disk_info [-n|--node node_address] [-a|--app app_name]

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

    std::map<std::string, std::string> params = cmd.params();

    if (params.find("app") != params.end() && params.find("node") == params.end()) {
        fmt::print(stderr, "please input node_address!\n");
        return false;
    }

    std::map<dsn::rpc_address, dsn::replication::node_status::type> nodes;
    auto error = sc->ddl_client->list_nodes(::dsn::replication::node_status::NS_INVALID, nodes);
    if (error != dsn::ERR_OK) {
        std::cout << "list nodes failed, error=" << error.to_string() << std::endl;
        return true;
    }

    std::vector<dsn::rpc_address> target;
    if (params.find("node") != params.end()) {
        std::string node_address = params["node"];
        bool exist = false;
        for (const auto &node : nodes) {
            // TODO(jiashuo1) check and test ipv4_str value
            if (node.first.ipv4_str() == node_address) {
                target.emplace_back(node);
                exist = true;
            }
        }
        if (!exist) {
            fmt::print(stderr, "please input valid node_address!\n");
        } else {
            const auto &err_resps = sc->ddl_client->query_disk_info(target);
            const auto &resp = err_resps.back();
            if (params.find("app") != params.end()) {
                std::string app_name = params["app"];
                // TODO(jiashuo1) filter app and print
            } else {
                // TODO(jiashuo1) print all
            }
        }
    } else {
        for (const auto &node : nodes) {
            target.emplace_back(node);
        }
        const auto err_resps = sc->ddl_client->query_disk_info(target);
        for (const auto &err_resp : err_resps) {
            // TODO(jiashuo1) print all node without everl disk info
        }
    }
}
