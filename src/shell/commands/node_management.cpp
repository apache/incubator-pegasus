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

#include <fmt/core.h>
#include <fmt/format.h>
#include <getopt.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <stdint.h>
#include <stdio.h>
#include <algorithm>
// IWYU pragma: no_include <bits/getopt_core.h>
#include <chrono>
#include <initializer_list>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "client/replication_ddl_client.h"
#include "common/json_helper.h"
#include "common/replication_enums.h"
#include "dsn.layer2_types.h"
#include "meta_admin_types.h"
#include "rpc/rpc_host_port.h"
#include "shell/argh.h"
#include "shell/command_executor.h"
#include "shell/command_helper.h"
#include "shell/command_utils.h"
#include "shell/commands.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/math.h"
#include "utils/metrics.h"
#include "utils/output_utils.h"
#include "utils/ports.h"

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

dsn::metric_filters server_stat_filters()
{
    dsn::metric_filters filters;
    filters.with_metric_fields = {dsn::kMetricNameField, dsn::kMetricSingleValueField};
    filters.entity_types = {"server"};
    filters.entity_metrics = {"virtual_mem_usage_mb", "resident_mem_usage_mb"};
    return filters;
}

struct meta_server_stats
{
    meta_server_stats() = default;

    double virt_mem_mb{0.0};
    double res_mem_mb{0.0};

    DEFINE_JSON_SERIALIZATION(virt_mem_mb, res_mem_mb)
};

std::pair<bool, std::string>
aggregate_meta_server_stats(const node_desc &node,
                            const dsn::metric_query_brief_value_snapshot &query_snapshot)
{
    aggregate_stats_calcs calcs;
    meta_server_stats stats;
    calcs.create_assignments<total_aggregate_stats>(
        "server",
        stat_var_map({{"virtual_mem_usage_mb", &stats.virt_mem_mb},
                      {"resident_mem_usage_mb", &stats.res_mem_mb}}));

    auto command_result = process_parse_metrics_result(
        calcs.aggregate_metrics(query_snapshot), node, "aggregate meta server stats");
    if (!command_result) {
        // Metrics failed to be aggregated.
        return std::make_pair(false, command_result.description());
    }

    return std::make_pair(true,
                          dsn::json::json_forwarder<meta_server_stats>::encode(stats).to_string());
}

struct replica_server_stats
{
    replica_server_stats() = default;

    double virt_mem_mb{0.0};
    double res_mem_mb{0.0};

    DEFINE_JSON_SERIALIZATION(virt_mem_mb, res_mem_mb)
};

std::pair<bool, std::string>
aggregate_replica_server_stats(const node_desc &node,
                               const dsn::metric_query_brief_value_snapshot &query_snapshot_start,
                               const dsn::metric_query_brief_value_snapshot &query_snapshot_end)
{
    aggregate_stats_calcs calcs;
    meta_server_stats stats;
    calcs.create_assignments<total_aggregate_stats>(
        "server",
        stat_var_map({{"virtual_mem_usage_mb", &stats.virt_mem_mb},
                      {"resident_mem_usage_mb", &stats.res_mem_mb}}));

    auto command_result = process_parse_metrics_result(
        calcs.aggregate_metrics(query_snapshot_start, query_snapshot_end),
        node,
        "aggregate replica server stats");
    if (!command_result) {
        // Metrics failed to be aggregated.
        return std::make_pair(false, command_result.description());
    }

    return std::make_pair(true,
                          dsn::json::json_forwarder<meta_server_stats>::encode(stats).to_string());
}

std::vector<std::pair<bool, std::string>> get_server_stats(const std::vector<node_desc> &nodes,
                                                           uint32_t sample_interval_ms)
{
    // Ask target node (meta or replica server) for the metrics of server stats.
    const auto &query_string = server_stat_filters().to_query_string();
    const auto &results_start = get_metrics(nodes, query_string);
    std::this_thread::sleep_for(std::chrono::milliseconds(sample_interval_ms));
    const auto &results_end = get_metrics(nodes, query_string);

    std::vector<std::pair<bool, std::string>> command_results;
    command_results.reserve(nodes.size());
    for (size_t i = 0; i < nodes.size(); ++i) {

#define SKIP_IF_PROCESS_RESULT_FALSE()                                                             \
    if (!command_result) {                                                                         \
        command_results.emplace_back(command_result, command_result.description());                \
        continue;                                                                                  \
    }

#define PROCESS_GET_METRICS_RESULT(result, what, ...)                                              \
    {                                                                                              \
        auto command_result = process_get_metrics_result(result, nodes[i], what, ##__VA_ARGS__);   \
        SKIP_IF_PROCESS_RESULT_FALSE()                                                             \
    }

        // Skip the metrics that failed to be fetched.
        PROCESS_GET_METRICS_RESULT(results_start[i], "starting server stats")
        PROCESS_GET_METRICS_RESULT(results_end[i], "ending server stats")

#undef PROCESS_GET_METRICS_RESULT

        dsn::metric_query_brief_value_snapshot query_snapshot_start;
        dsn::metric_query_brief_value_snapshot query_snapshot_end;
        {
            // Skip the metrics that failed to be deserialized.
            auto command_result = process_parse_metrics_result(
                deserialize_metric_query_2_samples(results_start[i].body(),
                                                   results_end[i].body(),
                                                   query_snapshot_start,
                                                   query_snapshot_end),
                nodes[i],
                "deserialize server stats");
            SKIP_IF_PROCESS_RESULT_FALSE()
        }

#undef SKIP_IF_PROCESS_RESULT_FALSE

        if (query_snapshot_end.role == "meta") {
            command_results.push_back(aggregate_meta_server_stats(nodes[i], query_snapshot_end));
            continue;
        }

        if (query_snapshot_end.role == "replica") {
            command_results.push_back(
                aggregate_replica_server_stats(nodes[i], query_snapshot_start, query_snapshot_end));
            continue;
        }

        command_results.emplace_back(
            false, fmt::format("role {} is unsupported", query_snapshot_end.role));
    }

    return command_results;
}

std::vector<std::pair<bool, std::string>> call_nodes(shell_context *sc,
                                                     const std::vector<node_desc> &nodes,
                                                     const std::string &command,
                                                     const std::vector<std::string> &arguments,
                                                     uint32_t sample_interval_ms)
{
    if (command == "server_stat") {
        return get_server_stats(nodes, sample_interval_ms);
    }

    return call_remote_command(sc, nodes, command, arguments);
}

} // anonymous namespace

bool ls_nodes(command_executor *, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"detailed", no_argument, nullptr, 'd'},
                                           {"resolve_ip", no_argument, nullptr, 'r'},
                                           {"resource_usage", no_argument, nullptr, 'u'},
                                           {"qps", no_argument, nullptr, 'q'},
                                           {"json", no_argument, nullptr, 'j'},
                                           {"status", required_argument, nullptr, 's'},
                                           {"output", required_argument, nullptr, 'o'},
                                           {"sample_interval_ms", required_argument, nullptr, 'i'},
                                           {nullptr, 0, nullptr, 0}};

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
        // TODO(wangdan): getopt_long() is not thread-safe (clang-tidy[concurrency-mt-unsafe]),
        // could use https://github.com/p-ranav/argparse instead.
        int c = getopt_long(args.argc, args.argv, "druqjs:o:i:", long_options, &option_index);
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
        case 'i':
            RETURN_FALSE_IF_SAMPLE_INTERVAL_MS_INVALID();
            break;
        default:
            return false;
        }
    }

    dsn::utils::multi_table_printer multi_printer;
    if (!(status.empty() && output_file.empty())) {
        dsn::utils::table_printer tp("parameters");
        if (!status.empty()) {
            tp.add_row_name_and_data("status", status);
        }
        if (!output_file.empty()) {
            tp.add_row_name_and_data("out_file", output_file);
        }
        multi_printer.add(std::move(tp));
    }

    ::dsn::replication::node_status::type s = ::dsn::replication::node_status::NS_INVALID;
    if (!status.empty() && status != "all") {
        s = type_from_string(dsn::replication::_node_status_VALUES_TO_NAMES,
                             std::string("ns_") + status,
                             ::dsn::replication::node_status::NS_INVALID);
        SHELL_PRINT_AND_RETURN_FALSE_IF_NOT(s != ::dsn::replication::node_status::NS_INVALID,
                                            "parse {} as node_status::type failed",
                                            status);
    }

    std::map<dsn::host_port, dsn::replication::node_status::type> status_by_hp;
    auto r = sc->ddl_client->list_nodes(s, status_by_hp);
    if (r != dsn::ERR_OK) {
        fmt::println("list nodes failed, error={}", r);
        return true;
    }

    std::map<dsn::host_port, list_nodes_helper> tmp_map;
    int alive_node_count = 0;
    for (auto &kv : status_by_hp) {
        if (kv.second == dsn::replication::node_status::NS_ALIVE)
            alive_node_count++;
        std::string status_str = dsn::enum_to_string(kv.second);
        status_str = status_str.substr(status_str.find("NS_") + 3);
        const auto node_name = replication_ddl_client::node_name(kv.first, resolve_ip);
        tmp_map.emplace(kv.first, list_nodes_helper(node_name, status_str));
    }

    if (detailed) {
        std::vector<::dsn::app_info> apps;
        const auto &result = sc->ddl_client->list_apps(dsn::app_status::AS_AVAILABLE, apps);
        if (!result) {
            fmt::println("list apps failed, error={}", result);
            return true;
        }

        for (auto &app : apps) {
            int32_t app_id;
            int32_t partition_count;
            std::vector<dsn::partition_configuration> pcs;
            r = sc->ddl_client->list_app(app.app_name, app_id, partition_count, pcs);
            if (r != dsn::ERR_OK) {
                fmt::println("list app {} failed, error={}", app.app_name, r);
                return true;
            }

            for (const auto &pc : pcs) {
                if (pc.hp_primary) {
                    auto find = tmp_map.find(pc.hp_primary);
                    if (find != tmp_map.end()) {
                        find->second.primary_count++;
                    }
                }
                for (const auto &secondary : pc.hp_secondaries) {
                    auto find = tmp_map.find(secondary);
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
            fmt::println("get replica server node list failed");
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
                parse_resource_usage(results[i].body(), stat), nodes[i], "parse resource usage");
        }
    }

    if (show_qps) {
        std::vector<node_desc> nodes;
        if (!fill_nodes(sc, "replica-server", nodes)) {
            fmt::println("get replica server node list failed");
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
                "aggregate rw requests");
        }
    }

    if (show_latency) {
        std::vector<node_desc> nodes;
        if (!fill_nodes(sc, "replica-server", nodes)) {
            fmt::println("get replica server node list failed");
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
            RETURN_SHELL_IF_PARSE_METRICS_FAILED(parse_profiler_latency(results[i].body(), stat),
                                                 nodes[i],
                                                 "parse profiler latency");
        }
    }

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
    multi_printer.add(std::move(tp));

    dsn::utils::table_printer tp_count("summary");
    tp_count.add_row_name_and_data("total_node_count", status_by_hp.size());
    tp_count.add_row_name_and_data("alive_node_count", alive_node_count);
    tp_count.add_row_name_and_data("unalive_node_count", status_by_hp.size() - alive_node_count);
    multi_printer.add(std::move(tp_count));

    dsn::utils::output(output_file, json, multi_printer);

    return true;
}

bool server_info(command_executor *e, shell_context *sc, arguments args)
{
    return remote_command(e, sc, args);
}

bool server_stat(command_executor *e, shell_context *sc, arguments args)
{
    return remote_command(e, sc, args);
}

bool flush_log(command_executor *e, shell_context *sc, arguments args)
{
    return remote_command(e, sc, args);
}

bool remote_command(command_executor *e, shell_context *sc, arguments args)
{
    // Command format: [remote_command] <command> [arguments...]
    //                                            [-t all|meta-server|replica-server]
    //                                            [-r|--resolve_ip]
    //                                            [-l host:port,host:port...]
    //                                            [-i|--sample_interval_ms num]
    argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);

    std::string command;
    std::vector<std::string> pos_args;
    int pos = 0;
    do {
        // Try to parse the positional args.
        const auto &pos_arg = cmd(pos++);
        if (!pos_arg) {
            break;
        }

        // Ignore the args that are useless to the command.
        static const std::set<std::string> kIgnoreArgs({"remote_command"});
        if (kIgnoreArgs.count(pos_arg.str()) == 1) {
            continue;
        }

        // Collect the positional args following by the command.
        if (!command.empty()) {
            pos_args.emplace_back(pos_arg.str());
            continue;
        }

        // Initialize the command.
        const std::map<std::string, std::string> kCmdsMapping(
            {{"server_info", "server-info"}, {"flush_log", "flush-log"}});
        const auto &it = kCmdsMapping.find(pos_arg.str());
        if (it != kCmdsMapping.end()) {
            // Use the mapped command.
            command = it->second;
        } else {
            command = pos_arg.str();
        }
    } while (true);

    if (command.empty()) {
        SHELL_PRINTLN_ERROR("missing <command>");
        return false;
    }
    const auto resolve_ip = cmd[{"-r", "--resolve_ip"}];
    auto node_type = cmd({"-t"}).str();
    std::vector<std::string> nodes_str;
    PARSE_OPT_STRS(nodes_str, "", {"-l"});

    if (!node_type.empty() && !nodes_str.empty()) {
        SHELL_PRINTLN_ERROR("can not specify both node_type and nodes_str");
        return false;
    }

    if (node_type.empty() && nodes_str.empty()) {
        node_type = "all";
    }

    static const std::set<std::string> kValidNodeTypes({"all", "meta-server", "replica-server"});
    if (!node_type.empty() && kValidNodeTypes.count(node_type) == 0) {
        SHELL_PRINTLN_ERROR("invalid node_type, should be in [{}]",
                            fmt::join(kValidNodeTypes, ", "));
        return false;
    }

    std::vector<node_desc> nodes;
    do {
        if (node_type.empty()) {
            for (const auto &node_str : nodes_str) {
                const auto node = dsn::host_port::from_string(node_str);
                if (!node) {
                    SHELL_PRINTLN_ERROR("parse '{}' as host:port failed", node_str);
                    return false;
                }
                nodes.emplace_back("user-specified", node);
            }
            break;
        }

        if (!fill_nodes(sc, node_type, nodes)) {
            SHELL_PRINTLN_ERROR("prepare nodes failed, node_type = {}", node_type);
            return false;
        }
    } while (false);

    nlohmann::json info;
    info["command"] = fmt::format("{} {}", command, fmt::join(pos_args, " "));

    uint32_t sample_interval_ms = 0;
    PARSE_OPT_UINT(
        sample_interval_ms, FLAGS_nodes_sample_interval_ms, {"-i", "--sample_interval_ms"});

    const auto &results = call_nodes(sc, nodes, command, pos_args, sample_interval_ms);
    CHECK_EQ(results.size(), nodes.size());

    int succeed = 0;
    int failed = 0;
    for (int i = 0; i < nodes.size(); ++i) {
        nlohmann::json node_info;
        node_info["role"] = nodes[i].desc;
        node_info["acked"] = results[i].first;
        try {
            // Treat the message as a JSON object by default.
            node_info["message"] = nlohmann::json::parse(results[i].second);
        } catch (nlohmann::json::exception &exp) {
            // Treat it as a string if failed to parse as a JSON object.
            node_info["message"] = results[i].second;
        }
        if (results[i].first) {
            succeed++;
        } else {
            failed++;
        }
        info["details"].emplace(replication_ddl_client::node_name(nodes[i].hp, resolve_ip),
                                node_info);
    }
    info["succeed_count"] = succeed;
    info["failed_count"] = failed;
    fmt::println(stdout, "{}", info.dump(2));
    return true;
}
