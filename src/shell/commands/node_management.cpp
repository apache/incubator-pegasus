// Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "shell/commands.h"

bool query_cluster_info(command_executor *e, shell_context *sc, arguments args)
{
    ::dsn::error_code err = sc->ddl_client->cluster_info("");
    if (err == ::dsn::ERR_OK)
        std::cout << "get cluster info succeed" << std::endl;
    else
        std::cout << "get cluster info failed, error=" << err.to_string() << std::endl;
    return true;
}

bool ls_nodes(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"detailed", no_argument, 0, 'd'},
                                           {"resolve_ip", no_argument, 0, 'r'},
                                           {"resource_usage", no_argument, 0, 'u'},
                                           {"json", no_argument, 0, 'j'},
                                           {"status", required_argument, 0, 's'},
                                           {"output", required_argument, 0, 'o'},
                                           {0, 0, 0, 0}};

    std::string status;
    std::string output_file;
    bool detailed = false;
    bool resolve_ip = false;
    bool resource_usage = false;
    bool json = false;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "drujs:o:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'd':
            detailed = true;
            break;
        case 'r':
            resolve_ip = true;
            break;
        case 'u':
            resource_usage = true;
            break;
        case 'j':
            json = true;
            break;
        case 's':
            status = optarg;
            break;
        case 'o':
            output_file = optarg;
            break;
        default:
            return false;
        }
    }

    dsn::utils::multi_table_printer mtp;
    if (!(status.empty() && output_file.empty())) {
        dsn::utils::table_printer tp("parameters");
        if (!status.empty())
            tp.add_row_name_and_data("status", status);
        if (!output_file.empty())
            tp.add_row_name_and_data("out_file", output_file);
        mtp.add(std::move(tp));
    }

    ::dsn::replication::node_status::type s = ::dsn::replication::node_status::NS_INVALID;
    if (!status.empty() && status != "all") {
        s = type_from_string(dsn::replication::_node_status_VALUES_TO_NAMES,
                             std::string("ns_") + status,
                             ::dsn::replication::node_status::NS_INVALID);
        verify_logged(s != ::dsn::replication::node_status::NS_INVALID,
                      "parse %s as node_status::type failed",
                      status.c_str());
    }

    std::map<dsn::rpc_address, dsn::replication::node_status::type> nodes;
    auto r = sc->ddl_client->list_nodes(s, nodes);
    if (r != dsn::ERR_OK) {
        std::cout << "list nodes failed, error=" << r.to_string() << std::endl;
        return true;
    }

    std::map<dsn::rpc_address, list_nodes_helper> tmp_map;
    int alive_node_count = 0;
    for (auto &kv : nodes) {
        if (kv.second == dsn::replication::node_status::NS_ALIVE)
            alive_node_count++;
        std::string status_str = dsn::enum_to_string(kv.second);
        status_str = status_str.substr(status_str.find("NS_") + 3);
        std::string node_name = kv.first.to_std_string();
        if (resolve_ip) {
            // TODO: put hostname_from_ip_port into common utils
            node_name = sc->ddl_client->hostname_from_ip_port(node_name.c_str());
        }
        tmp_map.emplace(kv.first, list_nodes_helper(node_name, status_str));
    }

    if (detailed) {
        std::vector<::dsn::app_info> apps;
        r = sc->ddl_client->list_apps(dsn::app_status::AS_AVAILABLE, apps);
        if (r != dsn::ERR_OK) {
            std::cout << "list apps failed, error=" << r.to_string() << std::endl;
            return true;
        }

        for (auto &app : apps) {
            int32_t app_id;
            int32_t partition_count;
            std::vector<dsn::partition_configuration> partitions;
            r = sc->ddl_client->list_app(app.app_name, app_id, partition_count, partitions);
            if (r != dsn::ERR_OK) {
                std::cout << "list app " << app.app_name << " failed, error=" << r.to_string()
                          << std::endl;
                return true;
            }

            for (const dsn::partition_configuration &p : partitions) {
                if (!p.primary.is_invalid()) {
                    auto find = tmp_map.find(p.primary);
                    if (find != tmp_map.end()) {
                        find->second.primary_count++;
                    }
                }
                for (const dsn::rpc_address &addr : p.secondaries) {
                    auto find = tmp_map.find(addr);
                    if (find != tmp_map.end()) {
                        find->second.secondary_count++;
                    }
                }
            }
        }
    }

    if (resource_usage) {
        std::vector<node_desc> nodes;
        if (!fill_nodes(sc, "replica-server", nodes)) {
            derror("get replica server node list failed");
            return true;
        }

        ::dsn::command command;
        command.cmd = "perf-counters";
        command.arguments.push_back(".*memused.res(MB)");
        command.arguments.push_back(".*rdb.block_cache.memory_usage");
        command.arguments.push_back(".*disk.available.total.ratio");
        command.arguments.push_back(".*disk.available.min.ratio");
        command.arguments.push_back(".*@.*");
        std::vector<std::pair<bool, std::string>> results;
        call_remote_command(sc, nodes, command, results);

        for (int i = 0; i < nodes.size(); ++i) {
            dsn::rpc_address node_addr = nodes[i].address;
            auto tmp_it = tmp_map.find(node_addr);
            if (tmp_it == tmp_map.end())
                continue;
            if (!results[i].first) {
                derror("query perf counter info from node %s failed", node_addr.to_string());
                return true;
            }
            dsn::perf_counter_info info;
            dsn::blob bb(results[i].second.data(), 0, results[i].second.size());
            if (!dsn::json::json_forwarder<dsn::perf_counter_info>::decode(bb, info)) {
                derror("decode perf counter info from node %s failed, result = %s",
                       node_addr.to_string(),
                       results[i].second.c_str());
                return true;
            }
            if (info.result != "OK") {
                derror("query perf counter info from node %s returns error, error = %s",
                       node_addr.to_string(),
                       info.result.c_str());
                return true;
            }
            list_nodes_helper &h = tmp_it->second;
            for (dsn::perf_counter_metric &m : info.counters) {
                if (m.name == "replica*server*memused.res(MB)")
                    h.memused_res_mb = m.value;
                else if (m.name == "replica*app.pegasus*rdb.block_cache.memory_usage")
                    h.block_cache_bytes = m.value;
                else if (m.name == "replica*eon.replica_stub*disk.available.total.ratio")
                    h.disk_available_total_ratio = m.value;
                else if (m.name == "replica*eon.replica_stub*disk.available.min.ratio")
                    h.disk_available_min_ratio = m.value;
                else {
                    int32_t app_id_x, partition_index_x;
                    std::string counter_name;
                    bool parse_ret = parse_app_pegasus_perf_counter_name(
                        m.name, app_id_x, partition_index_x, counter_name);
                    dassert(parse_ret, "name = %s", m.name.c_str());
                    if (counter_name == "rdb.memtable.memory_usage")
                        h.mem_tbl_bytes += m.value;
                    else if (counter_name == "rdb.index_and_filter_blocks.memory_usage")
                        h.mem_idx_bytes += m.value;
                }
            }
        }
    }

    // print configuration_list_nodes_response
    std::streambuf *buf;
    std::ofstream of;

    if (!output_file.empty()) {
        of.open(output_file);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    dsn::utils::table_printer tp("details");
    tp.add_title("address");
    tp.add_column("status");
    if (detailed) {
        tp.add_column("replica_count", tp_alignment::kRight);
        tp.add_column("primary_count", tp_alignment::kRight);
        tp.add_column("secondary_count", tp_alignment::kRight);
    }
    if (resource_usage) {
        tp.add_column("memused_res_mb", tp_alignment::kRight);
        tp.add_column("block_cache_mb", tp_alignment::kRight);
        tp.add_column("mem_tbl_mb", tp_alignment::kRight);
        tp.add_column("mem_idx_mb", tp_alignment::kRight);
        tp.add_column("disk_avl_total_ratio", tp_alignment::kRight);
        tp.add_column("disk_avl_min_ratio", tp_alignment::kRight);
    }
    for (auto &kv : tmp_map) {
        tp.add_row(kv.second.node_name);
        tp.append_data(kv.second.node_status);
        if (detailed) {
            tp.append_data(kv.second.primary_count + kv.second.secondary_count);
            tp.append_data(kv.second.primary_count);
            tp.append_data(kv.second.secondary_count);
        }
        if (resource_usage) {
            tp.append_data(kv.second.memused_res_mb);
            tp.append_data(kv.second.block_cache_bytes / (1 << 20U));
            tp.append_data(kv.second.mem_tbl_bytes / (1 << 20U));
            tp.append_data(kv.second.mem_idx_bytes / (1 << 20U));
            tp.append_data(kv.second.disk_available_total_ratio);
            tp.append_data(kv.second.disk_available_min_ratio);
        }
    }
    mtp.add(std::move(tp));

    dsn::utils::table_printer tp_count("summary");
    tp_count.add_row_name_and_data("total_node_count", nodes.size());
    tp_count.add_row_name_and_data("alive_node_count", alive_node_count);
    tp_count.add_row_name_and_data("unalive_node_count", nodes.size() - alive_node_count);
    mtp.add(std::move(tp_count));

    mtp.output(out, json ? tp_output_format::kJsonPretty : tp_output_format::kTabular);

    return true;
}

bool server_info(command_executor *e, shell_context *sc, arguments args)
{
    char *argv[args.argc + 1];
    memcpy(argv, args.argv, sizeof(char *) * args.argc);
    argv[args.argc] = (char *)"server-info";
    arguments new_args;
    new_args.argc = args.argc + 1;
    new_args.argv = argv;
    return remote_command(e, sc, new_args);
}

bool server_stat(command_executor *e, shell_context *sc, arguments args)
{
    char *argv[args.argc + 1];
    memcpy(argv, args.argv, sizeof(char *) * args.argc);
    argv[args.argc] = (char *)"server-stat";
    arguments new_args;
    new_args.argc = args.argc + 1;
    new_args.argv = argv;
    return remote_command(e, sc, new_args);
}

bool remote_command(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"node_type", required_argument, 0, 't'},
                                           {"node_list", required_argument, 0, 'l'},
                                           {0, 0, 0, 0}};

    std::string type;
    std::string nodes;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "t:l:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 't':
            type = optarg;
            break;
        case 'l':
            nodes = optarg;
            break;
        default:
            return false;
        }
    }

    if (!type.empty() && !nodes.empty()) {
        fprintf(stderr, "can not specify both node_type and node_list\n");
        return false;
    }

    if (type.empty() && nodes.empty()) {
        type = "all";
    }

    if (!type.empty() && type != "all" && type != "meta-server" && type != "replica-server") {
        fprintf(stderr, "invalid type, should be: all | meta-server | replica-server\n");
        return false;
    }

    if (optind == args.argc) {
        fprintf(stderr, "command not specified\n");
        return false;
    }

    ::dsn::command cmd;
    cmd.cmd = args.argv[optind];
    for (int i = optind + 1; i < args.argc; i++) {
        cmd.arguments.push_back(args.argv[i]);
    }

    std::vector<node_desc> node_list;
    if (!type.empty()) {
        if (!fill_nodes(sc, type, node_list)) {
            fprintf(stderr, "prepare nodes failed, type = %s\n", type.c_str());
            return true;
        }
    } else {
        std::vector<std::string> tokens;
        dsn::utils::split_args(nodes.c_str(), tokens, ',');
        if (tokens.empty()) {
            fprintf(stderr, "can't parse node from node_list\n");
            return true;
        }

        for (std::string &token : tokens) {
            dsn::rpc_address node;
            if (!node.from_string_ipv4(token.c_str())) {
                fprintf(stderr, "parse %s as a ip:port node failed\n", token.c_str());
                return true;
            }
            node_list.emplace_back("user-specified", node);
        }
    }

    fprintf(stderr, "COMMAND: %s", cmd.cmd.c_str());
    for (auto &s : cmd.arguments) {
        fprintf(stderr, " %s", s.c_str());
    }
    fprintf(stderr, "\n\n");

    std::vector<std::pair<bool, std::string>> results;
    call_remote_command(sc, node_list, cmd, results);

    int succeed = 0;
    int failed = 0;
    // TODO (yingchun) output is hard to read, need do some refactor
    for (int i = 0; i < node_list.size(); ++i) {
        node_desc &n = node_list[i];
        fprintf(stderr, "CALL [%s] [%s] ", n.desc.c_str(), n.address.to_string());
        if (results[i].first) {
            fprintf(stderr, "succeed: %s\n", results[i].second.c_str());
            succeed++;
        } else {
            fprintf(stderr, "failed: %s\n", results[i].second.c_str());
            failed++;
        }
    }

    fprintf(stderr, "\nSucceed count: %d\n", succeed);
    fprintf(stderr, "Failed count: %d\n", failed);

    return true;
}

bool flush_log(command_executor *e, shell_context *sc, arguments args)
{
    char *argv[args.argc + 1];
    memcpy(argv, args.argv, sizeof(char *) * args.argc);
    argv[args.argc] = (char *)"flush-log";
    arguments new_args;
    new_args.argc = args.argc + 1;
    new_args.argv = argv;
    return remote_command(e, sc, new_args);
}
