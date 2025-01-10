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

// IWYU pragma: no_include <bits/getopt_core.h>
// IWYU pragma: no_include <bits/std_abs.h>
#include <fmt/core.h>
#include <getopt.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <cstdint>
#include <fstream>
#include <initializer_list>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "client/replication_ddl_client.h"
#include "common/gpid.h"
#include "dsn.layer2_types.h"
#include "meta_admin_types.h"
#include "pegasus_utils.h"
#include "rpc/rpc_host_port.h"
#include "shell/argh.h"
#include "shell/command_executor.h"
#include "shell/command_helper.h"
#include "shell/command_utils.h"
#include "shell/commands.h"
#include "shell/sds/sds.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/flags.h"
#include "utils/metrics.h"
#include "utils/output_utils.h"
#include "utils/ports.h"
#include "utils/string_conv.h"
#include "utils/strings.h"
#include "utils_types.h"

DSN_DEFINE_uint32(shell, tables_sample_interval_ms, 1000, "The interval between sampling metrics.");
DSN_DEFINE_validator(tables_sample_interval_ms, [](uint32_t value) -> bool { return value > 0; });

double convert_to_ratio(double hit, double total)
{
    return std::abs(total) < 1e-6 ? 0 : hit / total;
}

bool ls_apps(command_executor *e, shell_context *sc, arguments args)
{
    // ls [-a|--all] [-d|--detailed] [-j|--json] [-o|--output file_name]
    // [-s|--status all|available|creating|dropping|dropped] "
    // [-p|--app_name_pattern str] [-m|--match_type all|exact|anywhere|prefix|postfix]"

    // All valid parameters and flags are given as follows.
    static const std::set<std::string> params = {
        "o", "output", "s", "status", "p", "app_name_pattern", "m", "match_type"};
    static const std::set<std::string> flags = {"a", "all", "d", "detailed", "j", "json"};

    argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);

    // Check if input parameters and flags are valid.
    const auto &check = validate_cmd(cmd, params, flags);
    if (!check) {
        SHELL_PRINTLN_ERROR("{}", check.description());
        return false;
    }

    const bool show_all = cmd[{"-a", "--all"}];
    const bool detailed = cmd[{"-d", "--detailed"}];
    const bool json = cmd[{"-j", "--json"}];

    const std::string output_file(cmd({"-o", "--output"}, "").str());

    const std::string status_str(cmd({"-s", "--status"}, "").str());
    auto status = dsn::app_status::AS_INVALID;
    if (status_str.empty()) {
        // `show_all` functions only when target `status` is not specified.
        if (!show_all) {
            // That `show_all` is not given means just showing available tables.
            status = dsn::app_status::AS_AVAILABLE;
        }
    } else if (status_str != "all") {
        status = type_from_string(dsn::_app_status_VALUES_TO_NAMES,
                                  fmt::format("as_{}", status_str),
                                  dsn::app_status::AS_INVALID);
        SHELL_PRINT_AND_RETURN_FALSE_IF_NOT(status != dsn::app_status::AS_INVALID,
                                            "parse {} as app_status::type failed",
                                            status_str);
    }

    // Read the parttern of table name with empty string as default.
    const std::string app_name_pattern(cmd({"-p", "--app_name_pattern"}, "").str());

    // Read the match type of the pattern for table name with "matching all" as default,
    // typically requesting all tables owned by this cluster.
    auto match_type = dsn::utils::pattern_match_type::PMT_MATCH_ALL;
    PARSE_OPT_ENUM(match_type, dsn::utils::pattern_match_type::PMT_INVALID, {"-m", "--match_type"});

    const auto &result = sc->ddl_client->list_apps(
        detailed, json, output_file, status, app_name_pattern, match_type);
    if (!result) {
        fmt::println("list apps failed, error={}", result);
    }

    return true;
}

bool query_app(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc <= 1)
        return false;

    static struct option long_options[] = {{"detailed", no_argument, 0, 'd'},
                                           {"resolve_ip", no_argument, 0, 'r'},
                                           {"output", required_argument, 0, 'o'},
                                           {"json", no_argument, 0, 'j'},
                                           {0, 0, 0, 0}};

    std::string app_name = args.argv[1];
    std::string out_file;
    bool detailed = false;
    bool resolve_ip = false;
    bool json = false;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "dro:j", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'd':
            detailed = true;
            break;
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

    if (app_name.empty()) {
        std::cout << "ERROR: null app name" << std::endl;
        return false;
    }

    ::dsn::error_code err =
        sc->ddl_client->list_app(app_name, detailed, json, out_file, resolve_ip);
    if (err != ::dsn::ERR_OK) {
        std::cout << "query app " << app_name << " failed, error=" << err << std::endl;
    }
    return true;
}

namespace {

dsn::metric_filters sst_stat_filters(int32_t table_id)
{
    dsn::metric_filters filters;
    filters.with_metric_fields = {dsn::kMetricNameField, dsn::kMetricSingleValueField};
    filters.entity_types = {"replica"};
    filters.entity_attrs = {"table_id", std::to_string(table_id)};
    filters.entity_metrics = {"rdb_total_sst_files", "rdb_total_sst_size_mb"};
    return filters;
}

dsn::error_s parse_sst_stat(const std::string &json_string,
                            std::map<int32_t, double> &count_map,
                            std::map<int32_t, double> &disk_map)
{
    DESERIALIZE_METRIC_QUERY_BRIEF_SNAPSHOT(value, json_string, query_snapshot);

    for (const auto &entity : query_snapshot.entities) {
        if (dsn_unlikely(entity.type != "replica")) {
            return FMT_ERR(dsn::ERR_INVALID_DATA,
                           "non-replica entity should not be included: {}",
                           entity.type);
        }

        int32_t partition_id;
        RETURN_NOT_OK(dsn::parse_metric_partition_id(entity.attributes, partition_id));

        for (const auto &m : entity.metrics) {
            if (m.name == "rdb_total_sst_files") {
                count_map[partition_id] = m.value;
            } else if (m.name == "rdb_total_sst_size_mb") {
                disk_map[partition_id] = m.value;
            }
        }
    }

    return dsn::error_s::ok();
}

} // anonymous namespace

bool app_disk(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc <= 1)
        return false;

    static struct option long_options[] = {{"resolve_ip", no_argument, 0, 'r'},
                                           {"detailed", no_argument, 0, 'd'},
                                           {"json", no_argument, 0, 'j'},
                                           {"output", required_argument, 0, 'o'},
                                           {0, 0, 0, 0}};

    std::string app_name = args.argv[1];
    std::string out_file;
    bool detailed = false;
    bool json = false;
    bool resolve_ip = false;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "drjo:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'd':
            detailed = true;
            break;
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
    if (app_name.empty()) {
        std::cout << "ERROR: null app name" << std::endl;
        return false;
    }

    std::streambuf *buf;
    std::ofstream of;

    if (!out_file.empty()) {
        of.open(out_file);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    dsn::utils::multi_table_printer mtp;
    dsn::utils::table_printer tp_params("parameters");
    if (!(app_name.empty() && out_file.empty())) {
        if (!app_name.empty())
            tp_params.add_row_name_and_data("app_name", app_name);
        if (!out_file.empty())
            tp_params.add_row_name_and_data("out_file", out_file);
    }
    tp_params.add_row_name_and_data("detailed", detailed);
    mtp.add(std::move(tp_params));

    int32_t app_id = 0;
    int32_t partition_count = 0;
    int32_t max_replica_count = 0;
    std::vector<dsn::partition_configuration> pcs;

    dsn::error_code err = sc->ddl_client->list_app(app_name, app_id, partition_count, pcs);
    if (err != ::dsn::ERR_OK) {
        std::cout << "ERROR: list app " << app_name << " failed, error=" << err << std::endl;
        return true;
    }
    if (!pcs.empty()) {
        max_replica_count = pcs[0].max_replica_count;
    }

    std::vector<node_desc> nodes;
    if (!fill_nodes(sc, "replica-server", nodes)) {
        std::cout << "ERROR: get replica server node list failed" << std::endl;
        return true;
    }

    const auto &results = get_metrics(nodes, sst_stat_filters(app_id).to_query_string());

    std::map<dsn::host_port, std::map<int32_t, double>> disk_map;
    std::map<dsn::host_port, std::map<int32_t, double>> count_map;
    for (size_t i = 0; i < nodes.size(); ++i) {
        RETURN_SHELL_IF_GET_METRICS_FAILED(results[i], nodes[i], "sst");

        RETURN_SHELL_IF_PARSE_METRICS_FAILED(
            parse_sst_stat(results[i].body(), count_map[nodes[i].hp], disk_map[nodes[i].hp]),
            nodes[i],
            "parse sst stats");
    }

    ::dsn::utils::table_printer tp_general("result");
    tp_general.add_row_name_and_data("app_name", app_name);
    tp_general.add_row_name_and_data("app_id", app_id);
    tp_general.add_row_name_and_data("partition_count", partition_count);
    tp_general.add_row_name_and_data("max_replica_count", max_replica_count);

    ::dsn::utils::table_printer tp_details("details");
    if (detailed) {
        tp_details.add_title("pidx");
        tp_details.add_column("ballot");
        tp_details.add_column("replica_count");
        tp_details.add_column("primary");
        tp_details.add_column("secondaries");
    }
    double disk_used_for_primary_replicas = 0;
    int primary_replicas_count = 0;
    double disk_used_for_all_replicas = 0;
    int all_replicas_count = 0;
    for (const auto &pc : pcs) {
        std::string primary_str("-");
        if (pc.hp_primary) {
            bool disk_found = false;
            double disk_value = 0;
            auto f1 = disk_map.find(pc.hp_primary);
            if (f1 != disk_map.end()) {
                auto &sub_map = f1->second;
                auto f2 = sub_map.find(pc.pid.get_partition_index());
                if (f2 != sub_map.end()) {
                    disk_found = true;
                    disk_value = f2->second;
                    disk_used_for_primary_replicas += disk_value;
                    primary_replicas_count++;
                    disk_used_for_all_replicas += disk_value;
                    all_replicas_count++;
                }
            }
            bool count_found = false;
            double count_value = 0;
            auto f3 = count_map.find(pc.hp_primary);
            if (f3 != count_map.end()) {
                auto &sub_map = f3->second;
                auto f4 = sub_map.find(pc.pid.get_partition_index());
                if (f4 != sub_map.end()) {
                    count_found = true;
                    count_value = f4->second;
                }
            }
            std::stringstream oss;
            oss << replication_ddl_client::node_name(pc.hp_primary, resolve_ip) << "(";
            if (disk_found)
                oss << disk_value;
            else
                oss << "-";
            oss << ",";
            if (count_found)
                oss << "#" << count_value;
            else
                oss << "-";
            oss << ")";
            primary_str = oss.str();
        }
        std::string secondary_str;
        {
            std::stringstream oss;
            oss << "[";
            for (int j = 0; j < pc.hp_secondaries.size(); j++) {
                if (j != 0)
                    oss << ",";
                bool found = false;
                double value = 0;
                auto f1 = disk_map.find(pc.hp_secondaries[j]);
                if (f1 != disk_map.end()) {
                    auto &sub_map = f1->second;
                    auto f2 = sub_map.find(pc.pid.get_partition_index());
                    if (f2 != sub_map.end()) {
                        found = true;
                        value = f2->second;
                        disk_used_for_all_replicas += value;
                        all_replicas_count++;
                    }
                }
                bool count_found = false;
                double count_value = 0;
                auto f3 = count_map.find(pc.hp_secondaries[j]);
                if (f3 != count_map.end()) {
                    auto &sub_map = f3->second;
                    auto f3 = sub_map.find(pc.pid.get_partition_index());
                    if (f3 != sub_map.end()) {
                        count_found = true;
                        count_value = f3->second;
                    }
                }

                oss << replication_ddl_client::node_name(pc.hp_secondaries[j], resolve_ip) << "(";
                if (found)
                    oss << value;
                else
                    oss << "-";
                oss << ",";
                if (count_found)
                    oss << "#" << count_value;
                else
                    oss << "-";
                oss << ")";
            }
            oss << "]";
            secondary_str = oss.str();
        }

        if (detailed) {
            tp_details.add_row(std::to_string(pc.pid.get_partition_index()));
            tp_details.append_data(pc.ballot);
            tp_details.append_data(fmt::format(
                "{}/{}", pc.hp_secondaries.size() + (pc.hp_primary ? 1 : 0), pc.max_replica_count));
            tp_details.append_data(primary_str);
            tp_details.append_data(secondary_str);
        }
    }
    tp_general.add_row_name_and_data("disk_used_for_primary_replicas(MB)",
                                     disk_used_for_primary_replicas);
    tp_general.add_row_name_and_data("disk_used_for_all_replicas(MB)", disk_used_for_all_replicas);
    tp_general.add_row_name_and_data("partitions not counted",
                                     std::to_string(partition_count - primary_replicas_count) +
                                         "/" + std::to_string(partition_count));
    tp_general.add_row_name_and_data(
        "replicas not counted",
        std::to_string(partition_count * max_replica_count - all_replicas_count) + "/" +
            std::to_string(partition_count * max_replica_count));
    mtp.add(std::move(tp_general));
    if (detailed) {
        mtp.add(std::move(tp_details));
    }
    mtp.output(out, json ? tp_output_format::kJsonPretty : tp_output_format::kTabular);

    return true;
}

bool app_stat(command_executor *, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"app_name", required_argument, nullptr, 'a'},
                                           {"only_qps", no_argument, nullptr, 'q'},
                                           {"only_usage", no_argument, nullptr, 'u'},
                                           {"json", no_argument, nullptr, 'j'},
                                           {"output", required_argument, nullptr, 'o'},
                                           {"sample_interval_ms", required_argument, nullptr, 'i'},
                                           {nullptr, 0, nullptr, 0}};

    std::string app_name;
    std::string out_file;
    bool only_qps = false;
    bool only_usage = false;
    bool json = false;
    uint32_t sample_interval_ms = FLAGS_tables_sample_interval_ms;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c = getopt_long(args.argc, args.argv, "a:qujo:i:", long_options, &option_index);
        if (c == -1) {
            // -1 means all command-line options have been parsed.
            break;
        }

        switch (c) {
        case 'a':
            app_name = optarg;
            break;
        case 'q':
            only_qps = true;
            break;
        case 'u':
            only_usage = true;
            break;
        case 'j':
            json = true;
            break;
        case 'o':
            out_file = optarg;
            break;
        case 'i':
            RETURN_FALSE_IF_SAMPLE_INTERVAL_MS_INVALID();
            break;
        default:
            return false;
        }
    }

    if (only_qps && only_usage) {
        std::cout << "ERROR: only_qps and only_usage should not be set at the same time"
                  << std::endl;
        return true;
    }

    std::vector<row_data> rows;
    if (!get_app_stat(sc, app_name, sample_interval_ms, rows)) {
        std::cout << "ERROR: query app stat from server failed" << std::endl;
        return true;
    }

    rows.emplace_back(fmt::format("(total:{})", rows.size() - 1));
    auto &sum = rows.back();
    for (size_t i = 0; i < rows.size() - 1; ++i) {
        row_data &row = rows[i];
        sum.partition_count += row.partition_count;
        sum.get_qps += row.get_qps;
        sum.multi_get_qps += row.multi_get_qps;
        sum.batch_get_qps += row.batch_get_qps;
        sum.put_qps += row.put_qps;
        sum.multi_put_qps += row.multi_put_qps;
        sum.remove_qps += row.remove_qps;
        sum.multi_remove_qps += row.multi_remove_qps;
        sum.incr_qps += row.incr_qps;
        sum.check_and_set_qps += row.check_and_set_qps;
        sum.check_and_mutate_qps += row.check_and_mutate_qps;
        sum.scan_qps += row.scan_qps;
        sum.recent_read_cu += row.recent_read_cu;
        sum.recent_write_cu += row.recent_write_cu;
        sum.recent_expire_count += row.recent_expire_count;
        sum.recent_filter_count += row.recent_filter_count;
        sum.recent_abnormal_count += row.recent_abnormal_count;
        sum.recent_write_throttling_delay_count += row.recent_write_throttling_delay_count;
        sum.recent_write_throttling_reject_count += row.recent_write_throttling_reject_count;
        sum.recent_read_throttling_delay_count += row.recent_read_throttling_delay_count;
        sum.recent_read_throttling_reject_count += row.recent_read_throttling_reject_count;
        sum.recent_backup_request_throttling_delay_count +=
            row.recent_backup_request_throttling_delay_count;
        sum.recent_backup_request_throttling_reject_count +=
            row.recent_backup_request_throttling_reject_count;
        sum.recent_write_splitting_reject_count += row.recent_write_splitting_reject_count;
        sum.recent_read_splitting_reject_count += row.recent_read_splitting_reject_count;
        sum.recent_write_bulk_load_ingestion_reject_count +=
            row.recent_write_bulk_load_ingestion_reject_count;
        sum.storage_mb += row.storage_mb;
        sum.storage_count += row.storage_count;
        sum.rdb_block_cache_hit_count += row.rdb_block_cache_hit_count;
        sum.rdb_block_cache_total_count += row.rdb_block_cache_total_count;
        sum.rdb_index_and_filter_blocks_mem_usage += row.rdb_index_and_filter_blocks_mem_usage;
        sum.rdb_memtable_mem_usage += row.rdb_memtable_mem_usage;
        sum.rdb_bf_seek_negatives += row.rdb_bf_seek_negatives;
        sum.rdb_bf_seek_total += row.rdb_bf_seek_total;
        sum.rdb_bf_point_positive_true += row.rdb_bf_point_positive_true;
        sum.rdb_bf_point_positive_total += row.rdb_bf_point_positive_total;
        sum.rdb_bf_point_negatives += row.rdb_bf_point_negatives;
    }

    std::streambuf *buf;
    std::ofstream of;

    if (!out_file.empty()) {
        of.open(out_file);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    ::dsn::utils::table_printer tp("app_stat", 2 /* tabular_width */, 3 /* precision */);
    tp.add_title(app_name.empty() ? "app_name" : "pidx");
    if (app_name.empty()) {
        tp.add_column("app_id", tp_alignment::kRight);
        tp.add_column("pcount", tp_alignment::kRight);
    }
    if (!only_usage) {
        tp.add_column("GET", tp_alignment::kRight);
        tp.add_column("MGET", tp_alignment::kRight);
        tp.add_column("BGET", tp_alignment::kRight);
        tp.add_column("PUT", tp_alignment::kRight);
        tp.add_column("MPUT", tp_alignment::kRight);
        tp.add_column("DEL", tp_alignment::kRight);
        tp.add_column("MDEL", tp_alignment::kRight);
        tp.add_column("INCR", tp_alignment::kRight);
        tp.add_column("CAS", tp_alignment::kRight);
        tp.add_column("CAM", tp_alignment::kRight);
        tp.add_column("SCAN", tp_alignment::kRight);
        tp.add_column("RCU", tp_alignment::kRight);
        tp.add_column("WCU", tp_alignment::kRight);
        tp.add_column("expire", tp_alignment::kRight);
        tp.add_column("filter", tp_alignment::kRight);
        tp.add_column("abnormal", tp_alignment::kRight);
        tp.add_column("delay", tp_alignment::kRight);
        tp.add_column("reject", tp_alignment::kRight);
    }
    if (!only_qps) {
        tp.add_column("file_mb", tp_alignment::kRight);
        tp.add_column("file_num", tp_alignment::kRight);
        tp.add_column("mem_tbl_mb", tp_alignment::kRight);
        tp.add_column("mem_idx_mb", tp_alignment::kRight);
    }
    tp.add_column("hit_rate", tp_alignment::kRight);
    tp.add_column("seek_n_rate", tp_alignment::kRight);
    tp.add_column("point_n_rate", tp_alignment::kRight);
    tp.add_column("point_fp_rate", tp_alignment::kRight);

    for (row_data &row : rows) {
        tp.add_row(row.row_name);
        if (app_name.empty()) {
            tp.append_data(row.app_id);
            tp.append_data(row.partition_count);
        }
        if (!only_usage) {
            tp.append_data(row.get_qps);
            tp.append_data(row.multi_get_qps);
            tp.append_data(row.batch_get_qps);
            tp.append_data(row.put_qps);
            tp.append_data(row.multi_put_qps);
            tp.append_data(row.remove_qps);
            tp.append_data(row.multi_remove_qps);
            tp.append_data(row.incr_qps);
            tp.append_data(row.check_and_set_qps);
            tp.append_data(row.check_and_mutate_qps);
            tp.append_data(row.scan_qps);
            tp.append_data(row.recent_read_cu);
            tp.append_data(row.recent_write_cu);
            tp.append_data(row.recent_expire_count);
            tp.append_data(row.recent_filter_count);
            tp.append_data(row.recent_abnormal_count);
            tp.append_data(row.recent_write_throttling_delay_count);
            tp.append_data(row.recent_write_throttling_reject_count);
        }
        if (!only_qps) {
            tp.append_data(row.storage_mb);
            tp.append_data((uint64_t)row.storage_count);
            tp.append_data(row.rdb_memtable_mem_usage / (1 << 20U));
            tp.append_data(row.rdb_index_and_filter_blocks_mem_usage / (1 << 20U));
        }
        tp.append_data(
            convert_to_ratio(row.rdb_block_cache_hit_count, row.rdb_block_cache_total_count));
        tp.append_data(convert_to_ratio(row.rdb_bf_seek_negatives, row.rdb_bf_seek_total));
        tp.append_data(
            convert_to_ratio(row.rdb_bf_point_negatives,
                             row.rdb_bf_point_negatives + row.rdb_bf_point_positive_total));
        tp.append_data(
            convert_to_ratio(row.rdb_bf_point_positive_total - row.rdb_bf_point_positive_true,
                             (row.rdb_bf_point_positive_total - row.rdb_bf_point_positive_true) +
                                 row.rdb_bf_point_negatives));
    }

    // TODO(wangdan): use dsn::utils::output() in output_utils.h instead.
    tp.output(out, json ? tp_output_format::kJsonPretty : tp_output_format::kTabular);

    return true;
}

bool create_app(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"partition_count", required_argument, 0, 'p'},
                                           {"replica_count", required_argument, 0, 'r'},
                                           {"fail_if_exist", no_argument, 0, 'f'},
                                           {"envs", required_argument, 0, 'e'},
                                           {0, 0, 0, 0}};

    if (args.argc < 2)
        return false;

    std::string app_name = args.argv[1];
    bool success_if_exist = true;

    int pc = 4, rc = 3;
    std::map<std::string, std::string> envs;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "p:r:fe:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'p':
            if (!dsn::buf2int32(optarg, pc)) {
                fprintf(stderr, "parse %s as partition_count failed\n", optarg);
                return false;
            }
            break;
        case 'r':
            if (!dsn::buf2int32(optarg, rc)) {
                fprintf(stderr, "parse %s as replica_count failed\n", optarg);
                return false;
            }
            break;
        case 'f':
            success_if_exist = false;
            break;
        case 'e':
            if (!::dsn::utils::parse_kv_map(optarg, envs, ',', '=')) {
                fprintf(stderr, "invalid envs: %s\n", optarg);
                return false;
            }
            break;
        default:
            return false;
        }
    }

    ::dsn::error_code err =
        sc->ddl_client->create_app(app_name, "pegasus", pc, rc, envs, false, success_if_exist);
    if (err == ::dsn::ERR_OK)
        std::cout << "create app \"" << pegasus::utils::c_escape_string(app_name) << "\" succeed"
                  << std::endl;
    else
        std::cout << "create app \"" << pegasus::utils::c_escape_string(app_name)
                  << "\" failed, error = " << err << std::endl;
    return true;
}

bool drop_app(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"reserve_seconds", required_argument, 0, 'r'},
                                           {0, 0, 0, 0}};

    if (args.argc < 2)
        return false;

    std::string app_name = args.argv[1];

    int reserve_seconds = 0;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "r:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'r':
            if (!dsn::buf2int32(optarg, reserve_seconds)) {
                fprintf(stderr, "parse %s as reserve_seconds failed\n", optarg);
                return false;
            }
            break;
        default:
            return false;
        }
    }

    std::cout << "reserve_seconds = " << reserve_seconds << std::endl;
    ::dsn::error_code err = sc->ddl_client->drop_app(app_name, reserve_seconds);
    if (err == ::dsn::ERR_OK)
        std::cout << "drop app " << app_name << " succeed" << std::endl;
    else
        std::cout << "drop app " << app_name << " failed, error=" << err << std::endl;
    return true;
}

bool rename_app(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc <= 2) {
        return false;
    }

    const std::string old_app_name = args.argv[1];
    const std::string new_app_name = args.argv[2];

    auto err_resp = sc->ddl_client->rename_app(old_app_name, new_app_name);
    auto err = err_resp.get_error();
    const auto &resp = err_resp.get_value();

    if (dsn_likely(err.is_ok())) {
        err = dsn::error_s::make(resp.err);
    }

    if (err.is_ok()) {
        fmt::print(stdout,
                   "rename app ok, old_app_name({}), new_app_name({})\n",
                   old_app_name,
                   new_app_name);
    } else {
        std::string error_message(resp.err.to_string());
        if (!resp.hint_message.empty()) {
            error_message += ", ";
            error_message += resp.hint_message;
        }
        fmt::print(stderr,
                   "rename app failed, old_app_name({}), new_app_name({}), failed: {}\n",
                   old_app_name,
                   new_app_name,
                   error_message);
    }
    return true;
}

bool recall_app(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc <= 1)
        return false;

    int id;
    std::string new_name = "";
    if (!dsn::buf2int32(args.argv[1], id)) {
        fprintf(stderr, "ERROR: parse %s as id failed\n", args.argv[1]);
        return false;
    }
    if (args.argc >= 3) {
        new_name = args.argv[2];
    }

    ::dsn::error_code err = sc->ddl_client->recall_app(id, new_name);
    if (dsn::ERR_OK == err)
        std::cout << "recall app " << id << " succeed" << std::endl;
    else
        std::cout << "recall app " << id << " failed, error=" << err << std::endl;
    return true;
}

bool get_app_envs(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"json", no_argument, 0, 'j'}, {0, 0, 0, 0}};
    bool json = false;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c = getopt_long(args.argc, args.argv, "j", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'j':
            json = true;
            break;
        default:
            return false;
        }
    }

    if (sc->current_app_name.empty()) {
        fprintf(stderr, "No app is using now\nUSAGE: use [app_name]\n");
        return true;
    }

    std::map<std::string, std::string> envs;
    ::dsn::error_code ret = sc->ddl_client->get_app_envs(sc->current_app_name, envs);
    if (ret != ::dsn::ERR_OK) {
        fprintf(stderr, "get app env failed with err = %s\n", ret.to_string());
        return true;
    }

    ::dsn::utils::table_printer tp("app_envs");
    for (auto &kv : envs) {
        tp.add_row_name_and_data(kv.first, kv.second);
    }
    tp.output(std::cout, json ? tp_output_format::kJsonPretty : tp_output_format::kTabular);

    return true;
}

bool set_app_envs(command_executor *e, shell_context *sc, arguments args)
{
    if (sc->current_app_name.empty()) {
        fprintf(stderr, "No app is using now\nUSAGE: use [app_name]\n");
        return true;
    }

    if (args.argc < 3) {
        return false;
    }

    if (((args.argc - 1) & 0x01) == 1) {
        // key & value count must equal 2*n(n >= 1)
        fprintf(stderr, "need speficy the value for key = %s\n", args.argv[args.argc - 1]);
        return true;
    }
    std::vector<std::string> keys;
    std::vector<std::string> values;
    int idx = 1;
    while (idx < args.argc) {
        keys.emplace_back(args.argv[idx++]);
        values.emplace_back(args.argv[idx++]);
    }

    auto err_resp = sc->ddl_client->set_app_envs(sc->current_app_name, keys, values);
    dsn::error_s err = err_resp.get_error();
    std::string hint_msg;
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
        hint_msg = err_resp.get_value().hint_message;
    }
    if (!err.is_ok()) {
        fmt::print(stderr, "set app envs failed with error {} [hint:\"{}\"]!\n", err, hint_msg);
    } else {
        fmt::print(stdout, "set app envs succeed\n");
    }

    return true;
}

bool del_app_envs(command_executor *e, shell_context *sc, arguments args)
{
    if (sc->current_app_name.empty()) {
        fprintf(stderr, "No app is using now\nUSAGE: use [app_name]\n");
        return true;
    }

    if (args.argc <= 1) {
        return false;
    }

    std::vector<std::string> keys;
    for (int idx = 1; idx < args.argc; idx++) {
        keys.emplace_back(args.argv[idx]);
    }

    ::dsn::error_code ret = sc->ddl_client->del_app_envs(sc->current_app_name, keys);

    if (ret != ::dsn::ERR_OK) {
        fprintf(stderr, "del app env failed with err = %s\n", ret.to_string());
    }
    return true;
}

bool clear_app_envs(command_executor *e, shell_context *sc, arguments args)
{
    if (sc->current_app_name.empty()) {
        fprintf(stderr, "No app is using now\nUSAGE: use [app_name]\n");
        return true;
    }

    static struct option long_options[] = {
        {"all", no_argument, 0, 'a'}, {"prefix", required_argument, 0, 'p'}, {0, 0, 0, 0}};

    bool clear_all = false;
    std::string prefix;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "ap:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            clear_all = true;
            break;
        case 'p':
            prefix = optarg;
            break;
        default:
            return false;
        }
    }
    if (!clear_all && prefix.empty()) {
        fprintf(stderr, "must specify one of --all and --prefix options\n");
        return false;
    }
    ::dsn::error_code ret = sc->ddl_client->clear_app_envs(sc->current_app_name, clear_all, prefix);
    if (ret != dsn::ERR_OK) {
        fprintf(stderr, "clear app envs failed with err = %s\n", ret.to_string());
    }
    return true;
}

bool get_max_replica_count(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"json", no_argument, 0, 'j'}, {0, 0, 0, 0}};

    if (args.argc < 2) {
        return false;
    }

    std::string app_name(args.argv[1]);

    bool json = false;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c = getopt_long(args.argc, args.argv, "j", long_options, &option_index);
        if (c == -1) {
            break;
        }

        switch (c) {
        case 'j':
            json = true;
            break;
        default:
            return false;
        }
    }

    auto err_resp = sc->ddl_client->get_max_replica_count(app_name);
    auto err = err_resp.get_error();
    const auto &resp = err_resp.get_value();

    if (err.is_ok()) {
        err = dsn::error_s::make(resp.err);
    }

    std::string escaped_app_name(pegasus::utils::c_escape_string(app_name));
    if (!err.is_ok()) {
        fmt::print(stderr, "get replica count of app({}) failed: {}\n", escaped_app_name, err);
        return true;
    }

    dsn::utils::table_printer tp("max_replica_count");
    tp.add_row_name_and_data("max_replica_count", resp.max_replica_count);
    tp.output(std::cout, json ? tp_output_format::kJsonPretty : tp_output_format::kTabular);

    return true;
}

bool set_max_replica_count(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 3) {
        return false;
    }

    int new_max_replica_count;
    if (!dsn::buf2int32(args.argv[2], new_max_replica_count)) {
        fmt::print(stderr, "parse '{}' as replica count failed\n", args.argv[2]);
        return false;
    }

    if (new_max_replica_count < 1) {
        fmt::print(stderr, "replica count should be >= 1\n");
        return false;
    }

    std::string app_name(args.argv[1]);
    std::string escaped_app_name(pegasus::utils::c_escape_string(app_name));
    std::string action(fmt::format(
        "set the replica count of app({}) to {}", escaped_app_name, new_max_replica_count));
    if (!confirm_unsafe_command(action)) {
        return true;
    }

    auto err_resp = sc->ddl_client->set_max_replica_count(app_name, new_max_replica_count);
    auto err = err_resp.get_error();
    const auto &resp = err_resp.get_value();

    if (dsn_likely(err.is_ok())) {
        err = dsn::error_s::make(resp.err);
    }

    if (err.is_ok()) {
        fmt::print(stdout,
                   "set replica count of app({}) from {} to {}: {}\n",
                   escaped_app_name,
                   resp.old_max_replica_count,
                   new_max_replica_count,
                   resp.hint_message.empty() ? "success" : resp.hint_message);
    } else {
        std::string error_message(resp.err.to_string());
        if (!resp.hint_message.empty()) {
            error_message += ", ";
            error_message += resp.hint_message;
        }

        fmt::print(
            stderr, "set replica count of app({}) failed: {}\n", escaped_app_name, error_message);
    }

    return true;
}
