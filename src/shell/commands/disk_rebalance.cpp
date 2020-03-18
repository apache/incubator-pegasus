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

bool fill_valid_targets(argh::parser &cmd,
                        bool query_one_node,
                        std::map<dsn::rpc_address, dsn::replication::node_status::type> &nodes,
                        std::vector<dsn::rpc_address> &targets)
{
    if (query_one_node) {
        std::string node_address = cmd(1).str();
        for (auto &node : nodes) {
            if (node.first.to_std_string() == node_address) {
                targets.emplace_back(node.first);
            }
        }

        if (targets.empty()) {
            fmt::print(stderr, "please input valid target node_address!\n");
            return false;
        }
    } else {
        for (const auto &node : nodes) {
            targets.emplace_back(node.first);
        }
    }
    return true;
}

bool query_disk_capacity(command_executor *e, shell_context *sc, arguments args)
{
    // disk_capacity [-n|--node str] [-d|--detail]
    std::vector<std::string> flags = {"-n", "--node", "-d", "--detail"};

    argh::parser cmd(args.argc, args.argv);
    if (cmd.size() > 2) {
        fmt::print(stderr, "too many params!\n");
        return false;
    }

    for (const auto &flag : cmd.flags()) {
        if (std::find(flags.begin(), flags.end(), flag) == flags.end()) {
            fmt::print(stderr, "unknown flag {}\n", flag);
            return false;
        }
    }

    bool query_one_node = cmd[{"-n", "--node"}];
    bool query_detail_info = cmd[{"-d", "--detail"}];

    if (query_one_node && !cmd(1)) {
        fmt::print(stderr, "missing param <node_address>\n");
        return false;
    }

    std::map<dsn::rpc_address, dsn::replication::node_status::type> nodes;
    auto error = sc->ddl_client->list_nodes(::dsn::replication::node_status::NS_INVALID, nodes);
    if (error != dsn::ERR_OK) {
        fmt::print(stderr, "list nodes failed, error={}\n", error.to_string());
        return false;
    }

    std::vector<dsn::rpc_address> targets;
    fill_valid_targets(cmd, query_one_node, nodes, targets);

    std::map<dsn::rpc_address, dsn::error_with<query_disk_info_response>> err_resps;
    sc->ddl_client->query_disk_info(targets, err_resps);

    dsn::utils::table_printer node_printer;
    node_printer.add_title("node");
    node_printer.add_column("total_capacity(MB)");
    node_printer.add_column("avalable_capacity(MB)");
    node_printer.add_column("avalable_ratio(%)");
    node_printer.add_column("capacity_balance");
    for (const auto &err_resp : err_resps) {
        dsn::error_s err = err_resp.second.get_error();
        if (err.is_ok()) {
            err = dsn::error_s::make(err_resp.second.get_value().err);
        }
        if (!err.is_ok()) {
            fmt::print(stderr,
                       "disk of node[{}] info skiped because request failed, error={}\n",
                       err_resp.first.to_std_string(),
                       err.description());
            if (query_one_node) {
                return false;
            }
        } else {
            dsn::utils::table_printer disk_printer;
            disk_printer.add_title("disk");
            disk_printer.add_column("total_capacity(MB)");
            disk_printer.add_column("avalable_capacity(MB)");
            disk_printer.add_column("avalable_ratio(%)");
            disk_printer.add_column("capacity_balance");

            const auto &resp = err_resp.second.get_value();
            int total_capacity_ratio =
                resp.total_capacity_mb == 0
                    ? 0
                    : std::round(resp.total_available_mb * 100.0 / resp.total_capacity_mb);
            if (query_detail_info) {
                for (const auto &disk_info : resp.disk_infos) {
                    int disk_available_ratio = disk_info.disk_capacity_mb == 0
                                                   ? 0
                                                   : std::round(disk_info.disk_available_mb *
                                                                100.0 / disk_info.disk_capacity_mb);
                    int disk_density = disk_available_ratio - total_capacity_ratio;
                    disk_printer.add_row(disk_info.tag);
                    disk_printer.append_data(disk_info.disk_capacity_mb);
                    disk_printer.append_data(disk_info.disk_available_mb);
                    disk_printer.append_data(disk_available_ratio);
                    disk_printer.append_data(disk_density);
                }
            }

            int variance = 0;
            for (const auto &disk_info : resp.disk_infos) {
                int disk_available_ratio = disk_info.disk_capacity_mb == 0
                                               ? 0
                                               : std::round(disk_info.disk_available_mb * 100.0 /
                                                            disk_info.disk_capacity_mb);
                variance += pow((disk_available_ratio - total_capacity_ratio), 2);
            }

            int capacity_balance = sqrt(variance);

            if (query_detail_info) {
                disk_printer.add_row("total");
                disk_printer.append_data(resp.total_capacity_mb);
                disk_printer.append_data(resp.total_available_mb);
                disk_printer.append_data(total_capacity_ratio);
                disk_printer.append_data(capacity_balance);
                fmt::print(stdout, "[{}]\n", err_resp.first.to_std_string());
                disk_printer.output(std::cout);
                std::cout << std::endl;
            } else {
                node_printer.add_row(err_resp.first.to_std_string());
                node_printer.append_data(resp.total_capacity_mb);
                node_printer.append_data(resp.total_available_mb);
                node_printer.append_data(total_capacity_ratio);
                node_printer.append_data(capacity_balance);
            }
        }
    }
    if (!query_detail_info) {
        node_printer.output(std::cout);
    }
    return true;
}

bool query_disk_replica(command_executor *e, shell_context *sc, arguments args)
{
    // disk_capacity [-n|--node ip:port][-a|app_name str]
    std::vector<std::string> flags = {"-n", "--node", "-a", "--app_name"};

    argh::parser cmd(args.argc, args.argv);
    if (cmd.size() > 3) {
        fmt::print(stderr, "too many params!\n");
        return false;
    }

    for (const auto &flag : cmd.flags()) {
        if (std::find(flags.begin(), flags.end(), flag) == flags.end()) {
            fmt::print(stderr, "unknown flag {}\n", flag);
            return false;
        }
    }

    bool query_one_node = cmd[{"-n", "--node"}];
    bool query_one_app = cmd[{"-a", "--app_name"}];

    if (query_one_node && !cmd(1)) {
        fmt::print(stderr, "missing param [-n|--node ip:port]\n");
        return false;
    }

    std::string app_name = std::string();
    if (query_one_app) {
        app_name = cmd(2).str();
    }

    std::map<dsn::rpc_address, dsn::replication::node_status::type> nodes;
    auto error = sc->ddl_client->list_nodes(::dsn::replication::node_status::NS_INVALID, nodes);
    if (error != dsn::ERR_OK) {
        fmt::print(stderr, "list nodes failed, error={}\n", error.to_string());
        return false;
    }

    std::vector<dsn::rpc_address> targets;
    fill_valid_targets(cmd, query_one_node, nodes, targets);
    std::map<dsn::rpc_address, dsn::error_with<query_disk_info_response>> err_resps;
    sc->ddl_client->query_disk_info(targets, err_resps, app_name);

    for (const auto &err_resp : err_resps) {
        dsn::error_s err = err_resp.second.get_error();
        if (err.is_ok()) {
            err = dsn::error_s::make(err_resp.second.get_value().err);
        }
        if (!err.is_ok()) {
            fmt::print(stderr,
                       "disk of node[{}] info skiped because request failed, error={}\n",
                       err_resp.first.to_std_string(),
                       err.description());
            if (query_one_node) {
                return false;
            }
        } else {
            dsn::utils::table_printer disk_printer;
            disk_printer.add_title("disk");
            disk_printer.add_column("primary_count");
            disk_printer.add_column("secondary_count");
            disk_printer.add_column("replica_count");

            const auto &resp = err_resp.second.get_value();
            for (const auto &disk_info : resp.disk_infos) {
                int primary_count = 0;
                int secondary_count = 0;
                for (const auto &replica_count : disk_info.holding_primary_replica_counts) {
                    primary_count += replica_count.second;
                }

                for (const auto &replica_count : disk_info.holding_secondary_replica_counts) {
                    secondary_count += replica_count.second;
                }
                disk_printer.add_row(disk_info.tag);
                disk_printer.append_data(primary_count);
                disk_printer.append_data(secondary_count);
                disk_printer.append_data(primary_count + secondary_count);

                fmt::print(stdout, "[{}]\n", err_resp.first.to_std_string());
                disk_printer.output(std::cout);
                std::cout << std::endl;
            }
        }
    }
    return true;
}
