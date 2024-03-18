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

#include <getopt.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <algorithm>
// IWYU pragma: no_include <bits/getopt_core.h>
#include <chrono>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "client/replication_ddl_client.h"
#include "common/replication_enums.h"
#include "dsn.layer2_types.h"
#include "meta_admin_types.h"
#include "runtime/rpc/rpc_host_port.h"
#include "shell/command_executor.h"
#include "shell/command_helper.h"
#include "shell/command_utils.h"
#include "shell/commands.h"
#include "shell/sds/sds.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/flags.h"
#include "utils/math.h"
#include "utils/metrics.h"
#include "utils/output_utils.h"
#include "utils/ports.h"
#include "utils/strings.h"

DSN_DEFINE_uint32(shell, nodes_sample_interval_ms, 1000, "The interval between sampling metrics.");
DSN_DEFINE_validator(nodes_sample_interval_ms, [](uint32_t value) -> bool { return value > 0; });

bool query_cluster_info(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"resolve_ip", no_argument, 0, 'r'},
                                           {"json", no_argument, 0, 'j'},
                                           {"output", required_argument, 0, 'o'},
                                           {0, 0, 0, 0}};

    std::string out_file;
    bool resolve_ip = false;
    bool json = false;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c = getopt_long(args.argc, args.argv, "rjo:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'r':
            resolve_ip = true;
            break;
        case 'j':
            json = true;
            break;
        case 'o':
            out_file = optarg;
            break;
        default:
            return false;
        }
    }

    ::dsn::error_code err = sc->ddl_client->cluster_info(out_file, resolve_ip, json);
    if (err != ::dsn::ERR_OK) {
        std::cout << "get cluster info failed, error=" << err << std::endl;
    }
    return true;
}

namespace {

dsn::metric_filters resource_usage_filters()
{
    dsn::metric_filters filters;
    filters.with_metric_fields = {dsn::kMetricNameField, dsn::kMetricSingleValueField};
    filters.entity_types = {"server", "replica", "disk"};
    filters.entity_metrics = {"resident_mem_usage_mb",
                              "rdb_block_cache_mem_usage_bytes",
                              "rdb_memtable_mem_usage_bytes",
                              "rdb_index_and_filter_blocks_mem_usage_bytes",
                              "disk_capacity_total_mb",
                              "disk_capacity_avail_mb"};
    return filters;
}

dsn::error_s parse_resource_usage(const std::string &json_string, list_nodes_helper &stat)
{
    DESERIALIZE_METRIC_QUERY_BRIEF_SNAPSHOT(value, json_string, query_snapshot);

    int64_t total_capacity_mb = 0;
    int64_t total_available_mb = 0;
    stat.disk_available_min_ratio = 100;
    for (const auto &entity : query_snapshot.entities) {
        if (entity.type == "server") {
            for (const auto &m : entity.metrics) {
                if (m.name == "resident_mem_usage_mb") {
                    stat.memused_res_mb += m.value;
                } else if (m.name == "rdb_block_cache_mem_usage_bytes") {
                    stat.block_cache_bytes += m.value;
                }
            }
        } else if (entity.type == "replica") {
            for (const auto &m : entity.metrics) {
                if (m.name == "rdb_memtable_mem_usage_bytes") {
                    stat.mem_tbl_bytes += m.value;
                } else if (m.name == "rdb_index_and_filter_blocks_mem_usage_bytes") {
                    stat.mem_idx_bytes += m.value;
                }
            }
        } else if (entity.type == "disk") {
            int64_t capacity_mb = 0;
            int64_t available_mb = 0;
            for (const auto &m : entity.metrics) {
                if (m.name == "disk_capacity_total_mb") {
                    total_capacity_mb += m.value;
                    capacity_mb = m.value;
                } else if (m.name == "disk_capacity_avail_mb") {
                    total_available_mb += m.value;
                    available_mb = m.value;
                }
            }

            const auto available_ratio = dsn::utils::calc_percentage(available_mb, capacity_mb);
            stat.disk_available_min_ratio =
                std::min(stat.disk_available_min_ratio, available_ratio);
        }
    }

    stat.disk_available_total_ratio =
        dsn::utils::calc_percentage(total_available_mb, total_capacity_mb);

    return dsn::error_s::ok();
}

dsn::metric_filters profiler_latency_filters()
{
    dsn::metric_filters filters;
    filters.with_metric_fields = {dsn::kMetricNameField,
                                  dsn::kth_percentile_to_name(dsn::kth_percentile_type::P99)};
    filters.entity_types = {"profiler"};
    filters.entity_metrics = {"profiler_server_rpc_latency_ns"};
    return filters;
}

dsn::error_s parse_profiler_latency(const std::string &json_string, list_nodes_helper &stat)
{
    DESERIALIZE_METRIC_QUERY_BRIEF_SNAPSHOT(p99, json_string, query_snapshot);

    for (const auto &entity : query_snapshot.entities) {
        if (dsn_unlikely(entity.type != "profiler")) {
            return FMT_ERR(dsn::ERR_INVALID_DATA,
                           "non-replica entity should not be included: {}",
                           entity.type);
        }

        const auto &t = entity.attributes.find("task_name");
        if (dsn_unlikely(t == entity.attributes.end())) {
            return FMT_ERR(dsn::ERR_INVALID_DATA, "task_name field was not found");
        }

        double *latency = nullptr;
        const auto &task_name = t->second;
        if (task_name == "RPC_RRDB_RRDB_GET") {
            latency = &stat.get_p99;
        } else if (task_name == "RPC_RRDB_RRDB_PUT") {
            latency = &stat.put_p99;
        } else if (task_name == "RPC_RRDB_RRDB_MULTI_GET") {
            latency = &stat.multi_get_p99;
        } else if (task_name == "RPC_RRDB_RRDB_MULTI_PUT") {
            latency = &stat.multi_put_p99;
        } else if (task_name == "RPC_RRDB_RRDB_BATCH_GET") {
            latency = &stat.batch_get_p99;
        } else {
            continue;
        }

        for (const auto &m : entity.metrics) {
            if (m.name == "profiler_server_rpc_latency_ns") {
                *latency = m.p99;
            }
        }
    }

    return dsn::error_s::ok();
}

dsn::metric_filters rw_requests_filters()
{
    dsn::metric_filters filters;
    filters.with_metric_fields = {dsn::kMetricNameField, dsn::kMetricSingleValueField};
    filters.entity_types = {"replica"};
    filters.entity_metrics = {"get_requests",
                              "multi_get_requests",
                              "batch_get_requests",
                              "put_requests",
                              "multi_put_requests",
                              "read_capacity_units",
                              "write_capacity_units"};
    return filters;
}

} // anonymous namespace

bool ls_nodes(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"detailed", no_argument, 0, 'd'},
                                           {"resolve_ip", no_argument, 0, 'r'},
                                           {"resource_usage", no_argument, 0, 'u'},
                                           {"qps", no_argument, 0, 'q'},
                                           {"json", no_argument, 0, 'j'},
                                           {"status", required_argument, 0, 's'},
                                           {"output", required_argument, 0, 'o'},
                                           {"sample_interval_ms", required_argument, 0, 't'},
                                           {0, 0, 0, 0}};

    std::string status;
    std::string output_file;
    uint32_t sample_interval_ms = FLAGS_nodes_sample_interval_ms;
    bool detailed = false;
    bool resolve_ip = false;
    bool resource_usage = false;
    bool show_qps = false;
    bool show_latency = false;
    bool json = false;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c = getopt_long(args.argc, args.argv, "druqjs:o:t:", long_options, &option_index);
        if (c == -1) {
            // -1 means all command-line options have been parsed.
            break;
        }

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
        case 'q':
            show_qps = true;
            show_latency = true;
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
        case 't':
            RETURN_FALSE_IF_SAMPLE_INTERVAL_MS_INVALID();
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
        PRINT_AND_RETURN_FALSE_IF_NOT(s != ::dsn::replication::node_status::NS_INVALID,
                                      "parse {} as node_status::type failed",
                                      status);
    }

    std::map<dsn::host_port, dsn::replication::node_status::type> nodes;
    auto r = sc->ddl_client->list_nodes(s, nodes);
    if (r != dsn::ERR_OK) {
        std::cout << "list nodes failed, error=" << r << std::endl;
        return true;
    }

    std::map<dsn::host_port, list_nodes_helper> tmp_map;
    int alive_node_count = 0;
    for (auto &kv : nodes) {
        if (kv.second == dsn::replication::node_status::NS_ALIVE)
            alive_node_count++;
        std::string status_str = dsn::enum_to_string(kv.second);
        status_str = status_str.substr(status_str.find("NS_") + 3);
        const auto node_name = replication_ddl_client::node_name(kv.first, resolve_ip);
        tmp_map.emplace(kv.first, list_nodes_helper(node_name, status_str));
    }

    if (detailed) {
        std::vector<::dsn::app_info> apps;
        r = sc->ddl_client->list_apps(dsn::app_status::AS_AVAILABLE, apps);
        if (r != dsn::ERR_OK) {
            std::cout << "list apps failed, error=" << r << std::endl;
            return true;
        }

        for (auto &app : apps) {
            int32_t app_id;
            int32_t partition_count;
            std::vector<dsn::partition_configuration> partitions;
            r = sc->ddl_client->list_app(app.app_name, app_id, partition_count, partitions);
            if (r != dsn::ERR_OK) {
                std::cout << "list app " << app.app_name << " failed, error=" << r << std::endl;
                return true;
            }

            for (const dsn::partition_configuration &p : partitions) {
                if (!p.hp_primary.is_invalid()) {
                    auto find = tmp_map.find(p.hp_primary);
                    if (find != tmp_map.end()) {
                        find->second.primary_count++;
                    }
                }
                for (const auto &hp : p.hp_secondaries) {
                    auto find = tmp_map.find(hp);
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
            std::cout << "get replica server node list failed" << std::endl;
            return true;
        }

        const auto &results = get_metrics(nodes, resource_usage_filters().to_query_string());

        for (size_t i = 0; i < nodes.size(); ++i) {
            auto tmp_it = tmp_map.find(nodes[i].hp);
            if (tmp_it == tmp_map.end()) {
                continue;
            }

            RETURN_SHELL_IF_GET_METRICS_FAILED(results[i], nodes[i], "resource");

            auto &stat = tmp_it->second;
            RETURN_SHELL_IF_PARSE_METRICS_FAILED(
                parse_resource_usage(results[i].body(), stat), nodes[i], "resource");
        }
    }

    if (show_qps) {
        std::vector<node_desc> nodes;
        if (!fill_nodes(sc, "replica-server", nodes)) {
            std::cout << "get replica server node list failed" << std::endl;
            return true;
        }

        const auto &query_string = rw_requests_filters().to_query_string();
        const auto &results_start = get_metrics(nodes, query_string);
        std::this_thread::sleep_for(std::chrono::milliseconds(sample_interval_ms));
        const auto &results_end = get_metrics(nodes, query_string);

        for (size_t i = 0; i < nodes.size(); ++i) {
            auto tmp_it = tmp_map.find(nodes[i].hp);
            if (tmp_it == tmp_map.end()) {
                continue;
            }

            RETURN_SHELL_IF_GET_METRICS_FAILED(results_start[i], nodes[i], "starting rw requests");
            RETURN_SHELL_IF_GET_METRICS_FAILED(results_end[i], nodes[i], "ending rw requests");

            list_nodes_helper &stat = tmp_it->second;
            aggregate_stats_calcs calcs;
            calcs.create_increases<total_aggregate_stats>(
                "replica",
                stat_var_map({{"read_capacity_units", &stat.read_cu},
                              {"write_capacity_units", &stat.write_cu}}));
            calcs.create_rates<total_aggregate_stats>(
                "replica",
                stat_var_map({{"get_requests", &stat.get_qps},
                              {"multi_get_requests", &stat.multi_get_qps},
                              {"batch_get_requests", &stat.batch_get_qps},
                              {"put_requests", &stat.put_qps},
                              {"multi_put_requests", &stat.multi_put_qps}}));

            RETURN_SHELL_IF_PARSE_METRICS_FAILED(
                calcs.aggregate_metrics(results_start[i].body(), results_end[i].body()),
                nodes[i],
                "rw requests");
        }
    }

    if (show_latency) {
        std::vector<node_desc> nodes;
        if (!fill_nodes(sc, "replica-server", nodes)) {
            std::cout << "get replica server node list failed" << std::endl;
            return true;
        }

        const auto &results = get_metrics(nodes, profiler_latency_filters().to_query_string());

        for (size_t i = 0; i < nodes.size(); ++i) {
            auto tmp_it = tmp_map.find(nodes[i].hp);
            if (tmp_it == tmp_map.end()) {
                continue;
            }

            RETURN_SHELL_IF_GET_METRICS_FAILED(results[i], nodes[i], "profiler latency");

            auto &stat = tmp_it->second;
            RETURN_SHELL_IF_PARSE_METRICS_FAILED(
                parse_profiler_latency(results[i].body(), stat), nodes[i], "profiler latency");
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
    if (show_qps) {
        tp.add_column("get_qps", tp_alignment::kRight);
        tp.add_column("mget_qps", tp_alignment::kRight);
        tp.add_column("bget_qps", tp_alignment::kRight);
        tp.add_column("read_cu", tp_alignment::kRight);
        tp.add_column("put_qps", tp_alignment::kRight);
        tp.add_column("mput_qps", tp_alignment::kRight);
        tp.add_column("write_cu", tp_alignment::kRight);
    }
    if (show_latency) {
        tp.add_column("get_p99(ms)", tp_alignment::kRight);
        tp.add_column("mget_p99(ms)", tp_alignment::kRight);
        tp.add_column("bget_p99(ms)", tp_alignment::kRight);
        tp.add_column("put_p99(ms)", tp_alignment::kRight);
        tp.add_column("mput_p99(ms)", tp_alignment::kRight);
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
        if (show_qps) {
            tp.append_data(kv.second.get_qps);
            tp.append_data(kv.second.multi_get_qps);
            tp.append_data(kv.second.batch_get_qps);
            tp.append_data(kv.second.read_cu);
            tp.append_data(kv.second.put_qps);
            tp.append_data(kv.second.multi_put_qps);
            tp.append_data(kv.second.write_cu);
        }
        if (show_latency) {
            tp.append_data(kv.second.get_p99 / 1e6);
            tp.append_data(kv.second.multi_get_p99 / 1e6);
            tp.append_data(kv.second.batch_get_p99 / 1e6);
            tp.append_data(kv.second.put_p99 / 1e6);
            tp.append_data(kv.second.multi_put_p99 / 1e6);
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
                                           {"resolve_ip", no_argument, 0, 'r'},
                                           {0, 0, 0, 0}};

    std::string type;
    std::string nodes;
    optind = 0;
    bool resolve_ip = false;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "t:l:r", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 't':
            type = optarg;
            break;
        case 'l':
            nodes = optarg;
            break;
        case 'r':
            resolve_ip = true;
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

    std::string cmd = args.argv[optind];
    std::vector<std::string> arguments;
    for (int i = optind + 1; i < args.argc; i++) {
        arguments.push_back(args.argv[i]);
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
            const auto node = dsn::host_port::from_string(token);
            if (!node) {
                fprintf(stderr, "parse %s as a ip:port node failed\n", token.c_str());
                return true;
            }
            node_list.emplace_back("user-specified", node);
        }
    }

    fprintf(stderr, "COMMAND: %s", cmd.c_str());
    for (auto &s : arguments) {
        fprintf(stderr, " %s", s.c_str());
    }
    fprintf(stderr, "\n\n");

    std::vector<std::pair<bool, std::string>> results =
        call_remote_command(sc, node_list, cmd, arguments);

    int succeed = 0;
    int failed = 0;
    // TODO (yingchun) output is hard to read, need do some refactor
    for (int i = 0; i < node_list.size(); ++i) {
        const auto &node = node_list[i];
        const auto hostname = replication_ddl_client::node_name(node.hp, resolve_ip);
        fprintf(stderr, "CALL [%s] [%s] ", node.desc.c_str(), hostname.c_str());
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
