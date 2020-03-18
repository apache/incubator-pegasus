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
                        std::string node_address,
                        std::map<dsn::rpc_address, dsn::replication::node_status::type> &nodes,
                        std::vector<dsn::rpc_address> &targets)
{
    if (!node_address.empty()) {
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
    // disk_capacity [-n|--node replica_server] [-o|--out file_name][-j|-json][-d|--detail]
    std::vector<std::string> flags = {"n", "node", "o", "out", "j", "json", "d", "detail"};

    argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);
    if (cmd.size() > 1) {
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
    bool output_to_file = cmd[{"-o", "--out"}];
    bool format_to_json = cmd[{"-j", "--json"}];
    bool query_detail_info = cmd[{"-d", "--detail"}];

    std::string node_address = cmd({"-n", "--node"}).str();
    if (query_one_node && node_address.empty()) {
        fmt::print(stderr, "missing param [-n|--node ip:port]\n");
        return false;
    }

    std::string file_name = cmd({"-o", "--out"}).str();
    if (output_to_file && file_name.empty()) {
        fmt::print(stderr, "missing param [-o|--out file_name]\n");
        return false;
    }

    std::map<dsn::rpc_address, dsn::replication::node_status::type> nodes;
    auto error = sc->ddl_client->list_nodes(::dsn::replication::node_status::NS_INVALID, nodes);
    if (error != dsn::ERR_OK) {
        fmt::print(stderr, "list nodes failed, error={}\n", error.to_string());
        return false;
    }

    std::vector<dsn::rpc_address> targets;
    fill_valid_targets(cmd, node_address, nodes, targets);

    std::map<dsn::rpc_address, dsn::error_with<query_disk_info_response>> err_resps;
    sc->ddl_client->query_disk_info(targets, err_resps);

    dsn::utils::table_printer node_printer;
    node_printer.add_title("node");
    node_printer.add_column("total_capacity(MB)");
    node_printer.add_column("avalable_capacity(MB)");
    node_printer.add_column("avalable_ratio(%)");
    node_printer.add_column("capacity_balance");

    std::streambuf *buf;
    std::ofstream of;
    if (!file_name.empty()) {
        of.open(file_name);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    dsn::utils::multi_table_printer multi_printer;
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
            if (!node_address.empty()) {
                return false;
            }
        } else {
            const auto &resp = err_resp.second.get_value();
            int total_capacity_ratio =
                resp.total_capacity_mb == 0
                    ? 0
                    : std::round(resp.total_available_mb * 100.0 / resp.total_capacity_mb);

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
                dsn::utils::table_printer disk_printer(err_resp.first.to_std_string());
                disk_printer.add_title("disk");
                disk_printer.add_column("total_capacity(MB)");
                disk_printer.add_column("avalable_capacity(MB)");
                disk_printer.add_column("avalable_ratio(%)");
                disk_printer.add_column("capacity_balance");

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
                disk_printer.add_row("total");
                disk_printer.append_data(resp.total_capacity_mb);
                disk_printer.append_data(resp.total_available_mb);
                disk_printer.append_data(total_capacity_ratio);
                disk_printer.append_data(capacity_balance);

                multi_printer.add(std::move(disk_printer));
            }

            node_printer.add_row(err_resp.first.to_std_string());
            node_printer.append_data(resp.total_capacity_mb);
            node_printer.append_data(resp.total_available_mb);
            node_printer.append_data(total_capacity_ratio);
            node_printer.append_data(capacity_balance);
        }
    }
    if (query_detail_info) {
        multi_printer.output(
            out, format_to_json ? tp_output_format::kJsonPretty : tp_output_format::kTabular);
    } else {
        node_printer.output(
            out, format_to_json ? tp_output_format::kJsonPretty : tp_output_format::kTabular);
    }
    return true;
}

bool query_disk_replica(command_executor *e, shell_context *sc, arguments args)
{
    // disk_capacity [-n|--node ip:port][-a|-app app_name][-o|--out file_name][-j|--json]
    std::vector<std::string> flags = {"n", "node", "a", "app_name", "o", "out", "j", "json"};

    argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);
    if (cmd.size() > 1) {
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
    bool query_one_app = cmd[{"-a", "--app"}];
    bool output_to_file = cmd[{"-o", "--out"}];
    bool format_to_json = cmd[{"-j", "--json"}];

    std::string node_address = cmd({"-n", "--node"}).str();
    if (query_one_node && node_address.empty()) {
        fmt::print(stderr, "missing param [-n|--node ip:port]\n");
        return false;
    }

    std::string app_name = cmd({"-a", "--app"}).str();
    if (query_one_app && app_name.empty()) {
        fmt::print(stderr, "missing param [-n|--app app_name]\n");
        return false;
    }

    std::string file_name = cmd({"-o", "--out"}).str();
    if (output_to_file && file_name.empty()) {
        fmt::print(stderr, "missing param [-o|--out file_name]\n");
        return false;
    }

    std::map<dsn::rpc_address, dsn::replication::node_status::type> nodes;
    auto error = sc->ddl_client->list_nodes(::dsn::replication::node_status::NS_INVALID, nodes);
    if (error != dsn::ERR_OK) {
        fmt::print(stderr, "list nodes failed, error={}\n", error.to_string());
        return false;
    }

    std::vector<dsn::rpc_address> targets;
    fill_valid_targets(cmd, node_address, nodes, targets);
    std::map<dsn::rpc_address, dsn::error_with<query_disk_info_response>> err_resps;
    sc->ddl_client->query_disk_info(targets, err_resps, app_name);

    std::streambuf *buf;
    std::ofstream of;
    if (!file_name.empty()) {
        of.open(file_name);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    dsn::utils::multi_table_printer multi_printer;
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
            if (!node_address.empty()) {
                return false;
            }
        } else {
            dsn::utils::table_printer disk_printer(err_resp.first.to_std_string());
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

                multi_printer.add(std::move(disk_printer));
            }
        }
    }
    multi_printer.output(
        out, format_to_json ? tp_output_format::kJsonPretty : tp_output_format::kTabular);
    return true;
}
