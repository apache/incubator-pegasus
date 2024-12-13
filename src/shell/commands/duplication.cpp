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
#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "client/partition_resolver.h"
#include "client/replication_ddl_client.h"
#include "common//duplication_common.h"
#include "duplication_types.h"
#include "gutil/map_util.h"
#include "shell/argh.h"
#include "shell/command_executor.h"
#include "shell/command_helper.h"
#include "shell/command_utils.h"
#include "shell/commands.h"
#include "shell/sds/sds.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/fmt_logging.h"
#include "utils/output_utils.h"
#include "utils/string_conv.h"
#include "utils/time_utils.h"
#include "utils_types.h"

using dsn::replication::dupid_t;
using dsn::replication::duplication_status;

namespace {

struct list_dups_options
{
    // To list partition-level states for a duplication, typically progress info.
    bool list_partitions{false};

    // The given max gap between confirmed decree and last committed decree, any gap
    // larger than this would be considered as "unfinished".
    uint32_t progress_gap{0};

    // Whether partitions with "unfinished" progress should be shown.
    bool show_unfinishd{false};

    // Specify a file path to output listed duplication info. Empty value means stdout.
    std::string output_file{};

    // Whether output as json format.
    bool json{false};
};

// app id => (dup id => partition ids)
using selected_app_dups_map = std::map<int32_t, std::map<int32_t, std::set<int32_t>>>;

// dup status string => dup count
using dup_status_stat_map = std::map<std::string, size_t>;

// dup remote cluster => dup count
using dup_remote_cluster_stat_map = std::map<std::string, size_t>;

// app id => duplication_app_state
using ls_app_dups_map = std::map<int32_t, dsn::replication::duplication_app_state>;

struct list_dups_stat
{
    // Total number of returned tables with specified table name pattern.
    size_t total_app_count{0};

    // The number of returned tables that are duplicating.
    size_t duplicating_app_count{0};

    // The number of "unfinished" tables for duplication according to specified
    // `progress_gap`.
    size_t unfinished_app_count{0};

    // The number of listed duplications.
    size_t duplication_count{0};

    // The number of listed duplications for each dup status.
    dup_status_stat_map dup_status_stats{};

    // The number of listed duplications for each remote cluster.
    dup_remote_cluster_stat_map dup_remote_cluster_stats{};

    // Total number of returned partitions with specified table name pattern.
    size_t total_partition_count{0};

    // The number of returned partitions that are duplicating.
    size_t duplicating_partition_count{0};

    // The number of "unfinished" partitions for duplication according to specified
    // `progress_gap`.
    size_t unfinished_partition_count{0};

    // All partitions that are not "unfinished" according to specified `progress_gap`
    // organized as each table.
    selected_app_dups_map unfinished_apps{};
};

// Attach to printer the summary stats for listed duplications.
void attach_dups_stat(const list_dups_stat &stat, dsn::utils::multi_table_printer &multi_printer)
{
    dsn::utils::table_printer printer("summary");

    // Add stats for tables.
    printer.add_row_name_and_data("total_app_count", stat.total_app_count);
    printer.add_row_name_and_data("duplicating_app_count", stat.duplicating_app_count);
    printer.add_row_name_and_data("unfinished_app_count", stat.unfinished_app_count);

    // Add stats for duplications.
    printer.add_row_name_and_data("total_duplication_count", stat.duplication_count);
    for (const auto &[status, cnt] : stat.dup_status_stats) {
        printer.add_row_name_and_data(fmt::format("duplication_count_by_status({})", status), cnt);
    }
    for (const auto &[remote_cluster, cnt] : stat.dup_remote_cluster_stats) {
        printer.add_row_name_and_data(
            fmt::format("duplication_count_by_follower_cluster({})", remote_cluster), cnt);
    }

    // Add stats for partitions.
    printer.add_row_name_and_data("total_partition_count", stat.total_partition_count);
    printer.add_row_name_and_data("duplicating_partition_count", stat.duplicating_partition_count);
    printer.add_row_name_and_data("unfinished_partition_count", stat.unfinished_partition_count);

    multi_printer.add(std::move(printer));
}

// Stats for listed duplications.
void stat_dups(const ls_app_dups_map &app_states, uint32_t progress_gap, list_dups_stat &stat)
{
    // Record as the number of all listed tables.
    stat.total_app_count = app_states.size();

    for (const auto &[app_id, app] : app_states) {
        // Sum up as the total number of all listed partitions.
        stat.total_partition_count += app.partition_count;

        if (app.duplications.empty()) {
            // No need to stat other items since there is no duplications for this table.
            continue;
        }

        // There's at least 1 duplication for this table. Sum up for duplicating tables
        // with all partitions of each table marked as duplicating.
        ++stat.duplicating_app_count;
        stat.duplicating_partition_count += app.partition_count;

        // Use individual variables as counter for "unfinished" tables and partitions in
        // case one stat is calculated multiple times. Record 1 as the table and partition
        // are "unfinished", while keeping 0 as both are "finished".Initialize all of them
        // with 0 to sum up later.
        size_t unfinished_app_counter = 0;
        std::vector<size_t> unfinished_partition_counters(app.partition_count);

        for (const auto &[dup_id, dup] : app.duplications) {
            // Count for all duplication-level stats.
            ++stat.duplication_count;
            ++stat.dup_status_stats[dsn::replication::duplication_status_to_string(dup.status)];
            ++stat.dup_remote_cluster_stats[dup.remote];

            if (!dup.__isset.partition_states) {
                // Partition-level states are not set. Only to be compatible with old version
                // where there is no this field for duplication entry.
                continue;
            }

            for (const auto &[partition_id, partition_state] : dup.partition_states) {
                // Only in the status of `DS_LOG`could a duplication be considered as "finished".
                if (dup.status == duplication_status::DS_LOG) {
                    if (partition_state.last_committed_decree < partition_state.confirmed_decree) {
                        // This is unlikely to happen.
                        continue;
                    }

                    if (partition_state.last_committed_decree - partition_state.confirmed_decree <=
                        progress_gap) {
                        // This partition is defined as "finished".
                        continue;
                    }
                }

                // Just assign with 1 to dedup, in case calculated multiple times.
                unfinished_app_counter = 1;
                CHECK_LT(partition_id, unfinished_partition_counters.size());
                unfinished_partition_counters[partition_id] = 1;

                // Record the partitions that are still "unfinished".
                stat.unfinished_apps[app_id][dup_id].insert(partition_id);
            }
        }

        // Sum up for each "unfinished" partition.
        for (const auto &counter : unfinished_partition_counters) {
            stat.unfinished_partition_count += counter;
        }

        // Sum up if table is "unfinished".
        stat.unfinished_app_count += unfinished_app_counter;
    }
}

// Add table headers for listed duplications.
void add_titles_for_dups(bool list_partitions, dsn::utils::table_printer &printer)
{
    // Base columns for table-level and duplication-level info.
    printer.add_title("app_id");
    printer.add_column("app_name", tp_alignment::kRight);
    printer.add_column("dup_id", tp_alignment::kRight);
    printer.add_column("create_time", tp_alignment::kRight);
    printer.add_column("status", tp_alignment::kRight);
    printer.add_column("follower_cluster", tp_alignment::kRight);
    printer.add_column("follower_app_name", tp_alignment::kRight);

    if (list_partitions) {
        // Partition-level info.
        printer.add_column("partition_id", tp_alignment::kRight);
        printer.add_column("confirmed_decree", tp_alignment::kRight);
        printer.add_column("last_committed_decree", tp_alignment::kRight);
        printer.add_column("decree_gap", tp_alignment::kRight);
    }
}

// Add table rows only with table-level and duplicating-level columns for listed
// duplications.
void add_base_row_for_dups(int32_t app_id,
                           const std::string &app_name,
                           const dsn::replication::duplication_entry &dup,
                           dsn::utils::table_printer &printer)
{
    // The appending order should be consistent with that the column titles are added.
    printer.add_row(app_id);
    printer.append_data(app_name);
    printer.append_data(dup.dupid);

    std::string create_time;
    dsn::utils::time_ms_to_string(dup.create_ts, create_time);
    printer.append_data(create_time);

    printer.append_data(dsn::replication::duplication_status_to_string(dup.status));
    printer.append_data(dup.remote);
    printer.append_data(dup.__isset.remote_app_name ? dup.remote_app_name : app_name);
}

// Add table rows including table-level, duplicating-level and partition-level columns
// for listed duplications.
//
// `partition_selector` is used to filter partitions as needed. Empty value means all
// partitions for this duplication.
void add_row_for_dups(int32_t app_id,
                      const std::string &app_name,
                      const dsn::replication::duplication_entry &dup,
                      bool list_partitions,
                      std::function<bool(int32_t)> partition_selector,
                      dsn::utils::table_printer &printer)
{
    if (!list_partitions) {
        // Only add table-level and duplication-level columns.
        add_base_row_for_dups(app_id, app_name, dup, printer);
        return;
    }

    if (!dup.__isset.partition_states) {
        // Partition-level states are not set. Only to be compatible with old version
        // where there is no this field for duplication entry.
        return;
    }

    for (const auto &[partition_id, partition_state] : dup.partition_states) {
        if (partition_selector && !partition_selector(partition_id)) {
            // This partition is excluded according to the selector.
            continue;
        }

        // Add table-level and duplication-level columns.
        add_base_row_for_dups(app_id, app_name, dup, printer);

        // Add partition-level columns.
        printer.append_data(partition_id);
        printer.append_data(partition_state.confirmed_decree);
        printer.append_data(partition_state.last_committed_decree);
        printer.append_data(partition_state.last_committed_decree -
                            partition_state.confirmed_decree);
    }
}

// All partitions for the duplication would be selected into the printer.
void add_row_for_dups(int32_t app_id,
                      const std::string &app_name,
                      const dsn::replication::duplication_entry &dup,
                      bool list_partitions,
                      dsn::utils::table_printer &printer)
{
    add_row_for_dups(
        app_id, app_name, dup, list_partitions, std::function<bool(int32_t)>(), printer);
}

// Attach listed duplications to the printer.
void attach_dups(const ls_app_dups_map &app_states,
                 bool list_partitions,
                 dsn::utils::multi_table_printer &multi_printer)
{
    dsn::utils::table_printer printer("duplications");
    add_titles_for_dups(list_partitions, printer);

    for (const auto &[app_id, app] : app_states) {
        if (app.duplications.empty()) {
            // Skip if there is no duplications for this table.
            continue;
        }

        for (const auto &[_, dup] : app.duplications) {
            add_row_for_dups(app_id, app.app_name, dup, list_partitions, printer);
        }
    }

    multi_printer.add(std::move(printer));
}

// Attach selected duplications to the printer.
void attach_selected_dups(const ls_app_dups_map &app_states,
                          const selected_app_dups_map &selected_apps,
                          const std::string &topic,
                          dsn::utils::multi_table_printer &multi_printer)
{
    dsn::utils::table_printer printer(topic);

    // Show partition-level columns.
    add_titles_for_dups(true, printer);

    // Find the intersection between listed and selected tables.
    auto listed_app_iter = app_states.begin();
    auto selected_app_iter = selected_apps.begin();
    while (listed_app_iter != app_states.end() && selected_app_iter != selected_apps.end()) {
        if (listed_app_iter->first < selected_app_iter->first) {
            ++listed_app_iter;
            continue;
        }

        if (listed_app_iter->first > selected_app_iter->first) {
            ++selected_app_iter;
            continue;
        }

        // Find the intersection between listed and selected duplications.
        auto listed_dup_iter = listed_app_iter->second.duplications.begin();
        auto selected_dup_iter = selected_app_iter->second.begin();
        while (listed_dup_iter != listed_app_iter->second.duplications.end() &&
               selected_dup_iter != selected_app_iter->second.end()) {
            if (listed_dup_iter->first < selected_dup_iter->first) {
                ++listed_dup_iter;
                continue;
            }

            if (listed_dup_iter->first > selected_dup_iter->first) {
                ++selected_dup_iter;
                continue;
            }

            add_row_for_dups(
                listed_app_iter->first,
                listed_app_iter->second.app_name,
                listed_dup_iter->second,
                true,
                [selected_dup_iter](int32_t partition_id) {
                    return gutil::ContainsKey(selected_dup_iter->second, partition_id);
                },
                printer);

            ++listed_dup_iter;
            ++selected_dup_iter;
        }

        ++listed_app_iter;
        ++selected_app_iter;
    }

    multi_printer.add(std::move(printer));
}

// Print duplications.
void show_dups(const ls_app_dups_map &app_states, const list_dups_options &options)
{
    // Calculate stats for duplications.
    list_dups_stat stat;
    stat_dups(app_states, options.progress_gap, stat);

    dsn::utils::multi_table_printer multi_printer;

    // Attach listed duplications to printer.
    attach_dups(app_states, options.list_partitions, multi_printer);

    // Attach stats to printer.
    attach_dups_stat(stat, multi_printer);

    if (options.show_unfinishd) {
        // Attach unfinished duplications with partition-level info to printer. Use "unfinished"
        // as the selector to extract all "unfinished" partitions.
        attach_selected_dups(app_states, stat.unfinished_apps, "unfinished", multi_printer);
    }

    // Printer output info to target file/stdout.
    dsn::utils::output(options.output_file, options.json, multi_printer);
}

} // anonymous namespace

bool add_dup(command_executor *e, shell_context *sc, arguments args)
{
    // add_dup <app_name> <remote_cluster_name> [-s|--sst] [-a|--remote_app_name str]
    // [-r|--remote_replica_count num]

    argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);

    if (!cmd(1)) {
        SHELL_PRINTLN_ERROR("missing param <app_name>");
        return false;
    }
    std::string app_name = cmd(1).str();

    if (!cmd(2)) {
        SHELL_PRINTLN_ERROR("missing param <remote_cluster_name>");
        return false;
    }
    std::string remote_cluster_name = cmd(2).str();

    if (remote_cluster_name == sc->current_cluster_name) {
        SHELL_PRINTLN_ERROR("illegal operation: adding duplication to itself [remote: {}]",
                            remote_cluster_name);
        return true;
    }

    // Check if the boolean option is specified.
    const auto is_duplicating_checkpoint = cmd[{"-s", "--sst"}];

    // Read the app name of the remote cluster, if any.
    // Otherwise, use app_name as the remote_app_name.
    const std::string remote_app_name(cmd({"-a", "--remote_app_name"}, app_name).str());

    // 0 represents that remote_replica_count is missing, which means the replica count of
    // the remote app would be the same as the source app.
    uint32_t remote_replica_count = 0;
    PARSE_OPT_UINT(remote_replica_count, 0, {"-r", "--remote_replica_count"});

    fmt::println("trying to add duplication [app_name: {}, remote_cluster_name: {}, "
                 "is_duplicating_checkpoint: {}, remote_app_name: {}, remote_replica_count: {}]",
                 app_name,
                 remote_cluster_name,
                 is_duplicating_checkpoint,
                 remote_app_name,
                 remote_replica_count);

    auto err_resp = sc->ddl_client->add_dup(app_name,
                                            remote_cluster_name,
                                            is_duplicating_checkpoint,
                                            remote_app_name,
                                            remote_replica_count);
    auto err = err_resp.get_error();
    std::string hint;
    if (err) {
        err = dsn::error_s::make(err_resp.get_value().err);
        hint = err_resp.get_value().hint;
    }

    if (!err && err.code() != dsn::ERR_DUP_EXIST) {
        SHELL_PRINTLN_ERROR(
            "adding duplication failed [app_name: {}, remote_cluster_name: {}, "
            "is_duplicating_checkpoint: {}, remote_app_name: {}, remote_replica_count: {}, "
            "error: {}]",
            app_name,
            remote_cluster_name,
            is_duplicating_checkpoint,
            remote_app_name,
            remote_replica_count,
            err);

        if (!hint.empty()) {
            SHELL_PRINTLN_ERROR("detail:\n  {}", hint);
        }

        return true;
    }

    if (err.code() == dsn::ERR_DUP_EXIST) {
        SHELL_PRINT_WARNING("duplication has been existing");
    } else {
        SHELL_PRINT_OK("adding duplication succeed");
    }

    const auto &resp = err_resp.get_value();
    SHELL_PRINT_OK(" [app_name: {}, remote_cluster_name: {}, appid: {}, dupid: {}",
                   app_name,
                   remote_cluster_name,
                   resp.appid,
                   resp.dupid);

    if (err) {
        SHELL_PRINT_OK(", is_duplicating_checkpoint: {}", is_duplicating_checkpoint);
    }

    if (resp.__isset.remote_app_name) {
        SHELL_PRINT_OK(", remote_app_name: {}", resp.remote_app_name);
    }

    if (resp.__isset.remote_replica_count) {
        SHELL_PRINT_OK(", remote_replica_count: {}", resp.remote_replica_count);
    }

    SHELL_PRINTLN_OK("]");

    if (!resp.__isset.remote_app_name) {
        SHELL_PRINTLN_WARNING("WARNING: meta server does NOT support specifying remote_app_name, "
                              "remote_app_name might has been specified with '{}'",
                              app_name);
    }

    if (!resp.__isset.remote_replica_count) {
        SHELL_PRINTLN_WARNING(
            "WARNING: meta server does NOT support specifying remote_replica_count, "
            "remote_replica_count might has been specified with the replica count of '{}'",
            app_name);
    }

    return true;
}

// print error if parsing failed
bool string2dupid(const std::string &str, dupid_t *dup_id)
{
    bool ok = dsn::buf2int32(str, *dup_id);
    if (!ok) {
        SHELL_PRINTLN_ERROR("parsing {} as positive int failed", str);
        return false;
    }
    return true;
}

bool query_dup(command_executor *e, shell_context *sc, arguments args)
{
    // query_dup <app_name> [-d|--detail]

    argh::parser cmd(args.argc, args.argv);
    if (cmd.pos_args().size() > 2) {
        SHELL_PRINTLN_ERROR("too many params");
        return false;
    }
    for (const auto &flag : cmd.flags()) {
        if (flag != "d" && flag != "detail") {
            SHELL_PRINTLN_ERROR("unknown flag {}", flag);
            return false;
        }
    }

    if (!cmd(1)) {
        SHELL_PRINTLN_ERROR("missing param <app_name>");
        return false;
    }
    std::string app_name = cmd(1).str();

    // Check if the boolean option is specified.
    bool detail = cmd[{"-d", "--detail"}];

    auto err_resp = sc->ddl_client->query_dup(app_name);
    dsn::error_s err = err_resp.get_error();
    if (err) {
        err = dsn::error_s::make(err_resp.get_value().err);
    }
    if (!err) {
        SHELL_PRINTLN_ERROR("querying duplications of app [{}] failed, error={}", app_name, err);

        return true;
    }

    if (detail) {
        fmt::println("duplications of app [{}] in detail:", app_name);
        fmt::println("{}\n", duplication_query_response_to_string(err_resp.get_value()));

        return true;
    }

    const auto &resp = err_resp.get_value();
    fmt::println("duplications of app [{}] are listed as below:", app_name);

    dsn::utils::table_printer printer;
    printer.add_title("dup_id");
    printer.add_column("status");
    printer.add_column("remote cluster");
    printer.add_column("create time");

    for (auto info : resp.entry_list) {
        std::string create_time;
        dsn::utils::time_ms_to_string(info.create_ts, create_time);

        printer.add_row(info.dupid);
        printer.append_data(duplication_status_to_string(info.status));
        printer.append_data(info.remote);
        printer.append_data(create_time);

        printer.output(std::cout);
        std::cout << std::endl;
    }

    return true;
}

// List duplications of one or multiple tables with both duplication-level and partition-level
// info.
bool ls_dups(command_executor *e, shell_context *sc, arguments args)
{
    // dups [-a|--app_name_pattern str] [-m|--match_type str] [-p|--list_partitions]
    // [-g|--progress_gap num] [-u|--show_unfinishd] [-o|--output file_name] [-j|--json]

    // All valid parameters and flags are given as follows.
    static const std::set<std::string> params = {
        "a", "app_name_pattern", "m", "match_type", "g", "progress_gap", "o", "output"};
    static const std::set<std::string> flags = {
        "p", "list_partitions", "u", "show_unfinishd", "j", "json"};

    argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);

    // Check if input parameters and flags are valid.
    const auto &check = validate_cmd(cmd, params, flags);
    if (!check) {
        SHELL_PRINTLN_ERROR("{}", check.description());
        return false;
    }

    // Read the parttern of table name with empty string as default.
    const std::string app_name_pattern(cmd({"-a", "--app_name_pattern"}, "").str());

    // Read the match type of the pattern for table name with "matching all" as default, typically
    // requesting all tables owned by this cluster.
    auto match_type = dsn::utils::pattern_match_type::PMT_MATCH_ALL;
    PARSE_OPT_ENUM(match_type, dsn::utils::pattern_match_type::PMT_INVALID, {"-m", "--match_type"});

    // Initialize options for listing duplications.
    list_dups_options options;
    options.list_partitions = cmd[{"-p", "--list_partitions"}];
    PARSE_OPT_UINT(options.progress_gap, 0, {"-g", "--progress_gap"});
    options.show_unfinishd = cmd[{"-u", "--show_unfinishd"}];
    options.output_file = cmd({"-o", "--output"}, "").str();
    options.json = cmd[{"-j", "--json"}];

    ls_app_dups_map ls_app_dups;
    {
        const auto &result = sc->ddl_client->list_dups(app_name_pattern, match_type);
        auto status = result.get_error();
        if (status) {
            status = FMT_ERR(result.get_value().err, result.get_value().hint_message);
        }

        if (!status) {
            SHELL_PRINTLN_ERROR("list duplications failed, error={}", status);
            return true;
        }

        // Change the key from app name to id, to list tables in the order of app id.
        const auto &app_states = result.get_value().app_states;
        std::transform(
            app_states.begin(),
            app_states.end(),
            std::inserter(ls_app_dups, ls_app_dups.end()),
            [](const std::pair<std::string, dsn::replication::duplication_app_state> &app) {
                return std::make_pair(app.second.appid, app.second);
            });
    }

    show_dups(ls_app_dups, options);
    return true;
}

void handle_duplication_modify_response(
    const std::string &operation, const dsn::error_with<duplication_modify_response> &err_resp)
{
    dsn::error_s err = err_resp.get_error();
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
    }
    std::string hint;
    if (err.code() == dsn::ERR_OBJECT_NOT_FOUND) {
        hint = " [duplication not found]";
    }
    if (err.is_ok()) {
        SHELL_PRINTLN_OK("{} succeed", operation);
    } else {
        SHELL_PRINTLN_ERROR("{} failed, error={}{}", operation, err.description(), hint);
    }
}

bool change_dup_status(command_executor *e,
                       shell_context *sc,
                       const arguments &args,
                       duplication_status::type status)
{
    if (args.argc <= 2) {
        return false;
    }

    std::string app_name = args.argv[1];

    dupid_t dup_id;
    if (!string2dupid(args.argv[2], &dup_id)) {
        return false;
    }

    std::string operation;
    switch (status) {
    case duplication_status::DS_LOG:
        operation = "starting duplication";
        break;
    case duplication_status::DS_PAUSE:
        operation = "pausing duplication";
        break;
    case duplication_status::DS_REMOVED:
        operation = "removing duplication";
        break;
    default:
        LOG_FATAL("can't change duplication under status {}", status);
    }

    auto err_resp = sc->ddl_client->change_dup_status(app_name, dup_id, status);
    handle_duplication_modify_response(
        fmt::format("{}({}) for app {}", operation, dup_id, app_name), err_resp);
    return true;
}

bool remove_dup(command_executor *e, shell_context *sc, arguments args)
{
    return change_dup_status(e, sc, args, duplication_status::DS_REMOVED);
}

bool start_dup(command_executor *e, shell_context *sc, arguments args)
{
    return change_dup_status(e, sc, args, duplication_status::DS_LOG);
}

bool pause_dup(command_executor *e, shell_context *sc, arguments args)
{
    return change_dup_status(e, sc, args, duplication_status::DS_PAUSE);
}

bool set_dup_fail_mode(command_executor *e, shell_context *sc, arguments args)
{
    // set_dup_fail_mode <app_name> <dupid> <slow|skip>
    using namespace dsn::replication;

    argh::parser cmd(args.argc, args.argv);
    if (cmd.pos_args().size() > 4) {
        SHELL_PRINTLN_ERROR("too many params");
        return false;
    }
    std::string app_name = cmd(1).str();
    std::string dupid_str = cmd(2).str();
    dupid_t dup_id;
    if (!dsn::buf2int32(dupid_str, dup_id)) {
        SHELL_PRINTLN_ERROR("invalid dup_id {}", dupid_str);
        return false;
    }
    std::string fail_mode_str = cmd(3).str();
    if (fail_mode_str != "slow" && fail_mode_str != "skip") {
        SHELL_PRINTLN_ERROR("fail_mode must be \"slow\" or  \"skip\": {}", fail_mode_str);
        return false;
    }
    auto fmode = fail_mode_str == "slow" ? duplication_fail_mode::FAIL_SLOW
                                         : duplication_fail_mode::FAIL_SKIP;

    auto err_resp = sc->ddl_client->update_dup_fail_mode(app_name, dup_id, fmode);
    auto operation = fmt::format(
        "set duplication({}) fail_mode ({}) for app {}", dup_id, fail_mode_str, app_name);
    handle_duplication_modify_response(operation, err_resp);
    return true;
}
