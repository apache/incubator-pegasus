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
    // disk_info [-n|--node node_address] [-a|--app app_replica_count]

    argh::parser cmd(args.argc, args.argv);
    if (cmd.pos_args().size() > 3) {
        fmt::print(stderr, "too many params\n");
        return false;
    }

    bool query_one_node = cmd[{"-n", "--node"}];
    bool query_app_replica_count = cmd[{"-a", "--app_replica"}];

    // only test
    fmt::print(stderr, "too many params {}\n", cmd.pos_args().size());
    for (auto n : cmd.pos_args()) {
        fmt::print(stderr, "params= {}\n", n.c_str());
    }

    std::map<dsn::rpc_address, dsn::replication::node_status::type> nodes;
    auto error = sc->ddl_client->list_nodes(::dsn::replication::node_status::NS_INVALID, nodes);
    if (error != dsn::ERR_OK) {
        std::cout << "list nodes failed, error=" << error.to_string() << std::endl;
        return false;
    }

    std::string node_address;
    std::vector<dsn::rpc_address> targets;
    if (query_one_node) {
        if (!cmd(1)) {
            fmt::print(stderr, "missing param <node_address>\n");
            return false;
        }
        node_address = cmd(1).str();
        // TODO(jiashuo1) will delete ip_port split.
        std::vector<std::string> ip_port;
        dsn::utils::split_args(node_address.c_str(), ip_port, ':');

        if (ip_port.size() < 2) {
            fmt::print(stderr, "please input valid node_address!\n");
            return false;
        }

        for (auto &node : nodes) {
            // TODO(jiashuo1) check and test ipv4_str return value
            if (node.first.ipv4_str() == ip_port[0] && node.first.port() == std::stoi(ip_port[1])) {
                targets.emplace_back(node.first);
            }
        }

        if (targets.empty()) {
            fmt::print(stderr, "please input valid target node_address!\n");
            return false;
        }
    } else {
        // TODO(jiashuo1) will move
        if (query_app_replica_count) {
            fmt::print(stderr, "please input node_address when query app replica count!\n");
            return false;
        }
        for (const auto &node : nodes) {
            targets.emplace_back(node.first);
        }
    }

    int app_id = 0;
    if (query_app_replica_count) {
        if (cmd(2)) {
            app_id = stoi(cmd(2).str());
        }
    }

    const auto &err_resps = sc->ddl_client->query_disk_info(targets, app_id);

    dsn::utils::table_printer node_printer;
    node_printer.add_title("node_address");
    node_printer.add_column("total_capacity(MB)");
    node_printer.add_column("avalable_capacity(MB)");
    node_printer.add_column("avalable_ratio(%)");
    node_printer.add_column("capacity_balance");

    int total_capacity_ratio = 0;
    for (const auto &err_resp : err_resps) {
        dsn::error_s err = err_resp.second.get_error();
        if (err.is_ok()) {
            err = dsn::error_s::make(err_resp.second.get_value().err);
        }
        if (!err.is_ok()) {
            fmt::print(stderr,
                       "disk[{}] info skiped because request failed, error={}\n",
                       err_resp.first.ipv4_str(),
                       err.description());
        } else {
            const auto &resp = err_resp.second.get_value();
            total_capacity_ratio =
                resp.total_capacity_mb == 0
                    ? 0
                    : std::round((double)resp.total_available_mb * 100 / resp.total_capacity_mb);

            int temp = 0;
            for (const auto &disk_info : resp.disk_infos) {
                int disk_available_ratio = disk_info.disk_capacity_mb == 0
                                               ? 0
                                               : std::round((double)disk_info.disk_available_mb *
                                                            100 / disk_info.disk_capacity_mb);
                temp += pow((disk_available_ratio - total_capacity_ratio), 2);
            }

            int capacity_balance = sqrt(temp);

            node_printer.add_row(err_resp.first.ipv4_str());
            node_printer.append_data(resp.total_capacity_mb);
            node_printer.append_data(resp.total_available_mb);
            node_printer.append_data(total_capacity_ratio);
            node_printer.append_data(capacity_balance);
        }
    }
    node_printer.output(std::cout);
    std::cout << std::endl;

    if (query_one_node) {
        const auto &err_resp = err_resps.begin();
        dsn::error_s err = err_resp->second.get_error();
        if (err.is_ok()) {
            err = dsn::error_s::make(err_resp->second.get_value().err);
        }
        if (!err.is_ok()) {
            return false;
        } else {
            dsn::utils::table_printer disk_printer;
            disk_printer.add_title("disk");
            const auto &resp = err_resp->second.get_value();
            if (query_app_replica_count) {
                disk_printer.add_column("primary_count");
                disk_printer.add_column("secondary_count");
                disk_printer.add_column("replica_count");

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

            } else {
                disk_printer.add_column("total_capacity(MB)");
                disk_printer.add_column("avalable_capacity(MB)");
                disk_printer.add_column("avalable_ratio(%)");
                disk_printer.add_column("disk_density");

                for (const auto &disk_info : resp.disk_infos) {
                    int disk_available_ratio =
                        disk_info.disk_capacity_mb == 0
                            ? 0
                            : std::round((double)disk_info.disk_available_mb * 100 /
                                         disk_info.disk_capacity_mb);
                    int disk_density = disk_available_ratio - total_capacity_ratio;
                    disk_printer.add_row(disk_info.tag);
                    disk_printer.append_data(disk_info.disk_capacity_mb);
                    disk_printer.append_data(disk_info.disk_available_mb);
                    disk_printer.append_data(disk_available_ratio);
                    disk_printer.append_data(disk_density);
                }
            }
            disk_printer.output(std::cout);
        }
    }
    return true;
}
