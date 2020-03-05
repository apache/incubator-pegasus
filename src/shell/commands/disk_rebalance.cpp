// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "shell/commands.h"
#include "shell/argh.h"

#include <math.h>
#include <fmt/ostream.h>
#include <dsn/utility/errors.h>
#include <dsn/utility/output_utils.h>
#include <dsn/utility/string_conv.h>
#include <dsn/dist/replication/duplication_common.h>

bool query_disk_info(command_executor *e, shell_context *sc, arguments args)
{
    // disk_info [-n|--node node_address]

    argh::parser cmd(args.argc, args.argv);
    if (cmd.pos_args().size() > 1) {
        fmt::print(stderr, "too many params\n");
        return false;
    }

    std::map<dsn::rpc_address, dsn::replication::node_status::type> nodes;
    auto error = sc->ddl_client->list_nodes(::dsn::replication::node_status::NS_INVALID, nodes);
    if (error != dsn::ERR_OK) {
        std::cout << "list nodes failed, error=" << error.to_string() << std::endl;
        return false;
    }

    std::map<std::string, std::string> params = cmd.params();
    std::vector<dsn::rpc_address> targets;
    if (params.find("node") != params.end()) {
        std::string node_address = params["node"];
        bool exist = false;
        for (const auto &node : nodes) {
            // TODO(jiashuo1) check and test ipv4_str return value
            if (node.first.ipv4_str() == node_address) {
                targets.emplace_back(node);
                exist = true;
            }
        }
        if (!exist) {
            fmt::print(stderr, "please input valid node_address!\n");
            return false;
        }
    } else {
        for (const auto &node : nodes) {
            targets.emplace_back(node);
        }
    }

    const auto &err_resps = sc->ddl_client->query_disk_info(targets);

    dsn::utils::table_printer node_printer;
    node_printer.add_title("node");
    node_printer.add_column("capacity");
    node_printer.add_column("avalable");
    node_printer.add_column("ratio");
    node_printer.add_column("balance");

    for (const auto &err_resp : err_resps) {
        dsn::error_s err = err_resp.second.get_error();
        if (!err.is_ok()) {
            fmt::print(stderr,
                       "disk[{}] info skiped because request failed, error={}\n",
                       err_resp.first.ipv4_str(),
                       err.description());
        } else {
            const auto &resp = err_resp.second.get_value();
            int total_capacity_tatio =
                std::round((double)resp.total_available_mb / resp.total_capacity_mb);

            int temp;
            for (const auto &disk_info : resp.disk_infos) {
                temp += pow(
                    std::round((double)disk_info.disk_available_mb / disk_info.disk_capacity_mb) -
                        total_capacity_tatio,
                    2);
            }

            int balance = sqrt(temp);

            node_printer.add_row(err_resp.first.ipv4_str());
            node_printer.append_data(resp.total_capacity_mb);
            node_printer.append_data(resp.total_available_mb);
            node_printer.append_data(total_capacity_tatio);
            node_printer.append_data(balance);
        }
    }
    node_printer.output(std::cout);
    std::cout << std::endl;

    if (params.find("node") != params.end()) {
        const auto &err_resp = err_resps.begin();
        dsn::error_s err = err_resp->second.get_error();
        if (!err.is_ok()) {
            return false;
        } else {
            dsn::utils::table_printer disk_printer;
            disk_printer.add_title("disk");
            disk_printer.add_column("capacity");
            disk_printer.add_column("avalable");
            disk_printer.add_column("ratio");
            disk_printer.add_column("density");
            disk_printer.add_column("primary");
            disk_printer.add_column("secondary");
            disk_printer.add_column("replica");

            const auto &resp = err_resp->second.get_value();

            int total_capacity_tatio =
                std::round((double)resp.total_available_mb / resp.total_capacity_mb);
            for (const auto &disk_info : resp.disk_infos) {
                int disk_avalable_ratio =
                    std::round((double)disk_info.disk_available_mb / disk_info.disk_capacity_mb);
                int disk_density = disk_avalable_ratio - total_capacity_tatio;
                disk_printer.add_row(disk_info.tag);
                disk_printer.append_data(disk_info.disk_capacity_mb);
                disk_printer.append_data(disk_info.disk_available_mb);
                disk_printer.append_data(disk_avalable_ratio);
                disk_printer.append_data(disk_density);

                int primary_count;
                int secondary_count;
                for (const auto &replica_count : disk_info.holding_primary_replica_counts) {
                    primary_count += replica_count.second;
                }

                for (const auto &replica_count : disk_info.holding_secondary_replica_counts) {
                    secondary_count += replica_count.second;
                }

                disk_printer.append_data(primary_count);
                disk_printer.append_data(secondary_count);
                disk_printer.append_data(primary_count + secondary_count);
            }
            disk_printer.output(std::cout);
        }
    }
}
