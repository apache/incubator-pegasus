// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "shell/commands.h"
#include "shell/argh.h"
#include "shell/command_output.h"
#include "shell/validate_utils.h"

#include <math.h>
#include <fmt/ostream.h>
#include <dsn/utility/errors.h>
#include <dsn/utility/output_utils.h>
#include <dsn/dist/replication/duplication_common.h>

bool query_disk_info(
    shell_context *sc,
    const argh::parser &cmd,
    const std::string &node_address,
    const std::string &app_name,
    /*out*/ std::map<dsn::rpc_address, dsn::error_with<query_disk_info_response>> &err_resps)
{
    std::map<dsn::rpc_address, dsn::replication::node_status::type> nodes;
    auto error = sc->ddl_client->list_nodes(::dsn::replication::node_status::NS_INVALID, nodes);
    if (error != dsn::ERR_OK) {
        fmt::print(stderr, "list nodes failed, error={}\n", error.to_string());
        return false;
    }

    std::vector<dsn::rpc_address> targets;
    for (const auto &node : nodes) {
        if (node_address.empty() || node_address == node.first.to_std_string()) {
            targets.push_back(node.first);
            if (!node_address.empty())
                break;
        }
    }

    if (targets.empty()) {
        fmt::print(stderr, "invalid target replica server address!\n");
        return false;
    }
    sc->ddl_client->query_disk_info(targets, app_name, err_resps);
    return true;
}

bool query_disk_capacity(command_executor *e, shell_context *sc, arguments args)
{
    // disk_capacity [-n|--node replica_server(ip:port)] [-o|--out file_name][-j|-json][-d|--detail]
    const std::set<std::string> &params = {"n", "node", "o", "out"};
    const std::set<std::string> &flags = {"j", "json", "d", "detail"};
    argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);
    if (!validate_cmd(cmd, params, flags)) {
        return false;
    }

    bool format_to_json = cmd[{"-j", "--json"}];
    bool query_detail_info = cmd[{"-d", "--detail"}];
    std::string node_address = cmd({"-n", "--node"}).str();
    std::string file_name = cmd({"-o", "--out"}).str();

    command_output out(file_name);
    if (!out.stream()) {
        return false;
    }

    std::map<dsn::rpc_address, dsn::error_with<query_disk_info_response>> err_resps;
    // passing empty app_name(app_name = "") means query all app disk info.
    if (!query_disk_info(sc, cmd, node_address, "", err_resps)) {
        return false;
    }

    dsn::utils::table_printer node_printer;
    node_printer.add_title("node");
    node_printer.add_column("total_capacity(MB)");
    node_printer.add_column("avalable_capacity(MB)");
    node_printer.add_column("avalable_ratio(%)");
    node_printer.add_column("capacity_balance");

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
            continue;
        }

        const auto &resp = err_resp.second.get_value();
        int total_capacity_ratio =
            resp.total_capacity_mb == 0
                ? 0
                : std::round(resp.total_available_mb * 100.0 / resp.total_capacity_mb);

        int variance = 0;
        for (const auto &disk_info : resp.disk_infos) {
            int disk_available_ratio =
                disk_info.disk_capacity_mb == 0
                    ? 0
                    : std::round(disk_info.disk_available_mb * 100.0 / disk_info.disk_capacity_mb);
            variance += std::pow((disk_available_ratio - total_capacity_ratio), 2);
        }

        int capacity_balance = std::sqrt(variance);

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
                                               : std::round(disk_info.disk_available_mb * 100.0 /
                                                            disk_info.disk_capacity_mb);
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
        } else {
            node_printer.add_row(err_resp.first.to_std_string());
            node_printer.append_data(resp.total_capacity_mb);
            node_printer.append_data(resp.total_available_mb);
            node_printer.append_data(total_capacity_ratio);
            node_printer.append_data(capacity_balance);
        }
    }
    if (query_detail_info) {
        multi_printer.output(*out.stream(),
                             format_to_json ? tp_output_format::kJsonPretty
                                            : tp_output_format::kTabular);
    } else {
        node_printer.output(*out.stream(),
                            format_to_json ? tp_output_format::kJsonPretty
                                           : tp_output_format::kTabular);
    }

    return true;
}

bool query_disk_replica(command_executor *e, shell_context *sc, arguments args)
{
    // disk_capacity [-n|--node replica_server(ip:port)][-a|-app app_name][-o|--out
    // file_name][-j|--json]
    const std::set<std::string> &params = {"n", "node", "a", "app", "o", "out"};
    const std::set<std::string> &flags = {"j", "json"};
    argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);
    if (!validate_cmd(cmd, params, flags)) {
        return false;
    }

    bool format_to_json = cmd[{"-j", "--json"}];
    std::string node_address = cmd({"-n", "--node"}).str();
    std::string app_name = cmd({"-a", "--app"}).str();
    std::string file_name = cmd({"-o", "--out"}).str();

    command_output out(file_name);
    if (!out.stream()) {
        return false;
    }

    std::map<dsn::rpc_address, dsn::error_with<query_disk_info_response>> err_resps;
    if (!query_disk_info(sc, cmd, node_address, app_name, err_resps)) {
        return false;
    }

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
            continue;
        }
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
        }
        multi_printer.add(std::move(disk_printer));
    }
    multi_printer.output(
        *out.stream(), format_to_json ? tp_output_format::kJsonPretty : tp_output_format::kTabular);

    return true;
}
