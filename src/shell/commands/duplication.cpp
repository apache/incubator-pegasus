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
#include <stdint.h>
#include <initializer_list>
#include <iostream>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "client/partition_resolver.h"
#include "client/replication_ddl_client.h"
#include "common//duplication_common.h"
#include "duplication_types.h"
#include "shell/argh.h"
#include "shell/command_executor.h"
#include "shell/command_helper.h"
#include "shell/commands.h"
#include "shell/sds/sds.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/fmt_logging.h"
#include "utils/output_utils.h"
#include "utils/string_conv.h"
#include "utils/time_utils.h"

using dsn::replication::dupid_t;
using dsn::replication::duplication_status;

namespace {

struct list_dups_options
{
    bool list_partitions{false};
    uint32_t progress_gap{0};
    bool show_unfinishd{false};
};

using selected_app_dups_map = std::map<std::string, std::map<int32_t, std::set<int32_t>>>;

struct list_dups_stat
{
    size_t total_app_count{0};
    size_t duplicating_app_count{0};
    size_t unfinished_app_count{0};

    size_t total_partition_count{0};
    size_t duplicating_partition_count{0};
    size_t unfinished_partition_count{0};

    selected_app_dups_map unfinished_apps{};
};

void stat_dups(const std::map<std::string, dsn::replication::duplication_app_state> &app_states,
               uint32_t progress_gap,
               list_dups_stat &stat)
{
    stat.total_app_count = app_states.size();

    for (const auto &[app_name, app] : app_states) {
        stat.total_partition_count += app.partition_count;
        if (app.duplications.empty()) {
            continue;
        }

        ++stat.duplicating_app_count;
        stat.duplicating_partition_count += app.partition_count;

        size_t unfinished_app_counter = 0;
        std::vector<size_t> unfinished_partition_counters(app.partition_count);

        for (const auto &[dup_id, dup] : app.duplications) {
            if (!dup.__isset.partition_states) {
                continue;
            }

            for (const auto &[partition_id, partition_state] : dup.partition_states) {
                if (partition_state.last_committed_decree < partition_state.confirmed_decree) {
                    continue;
                }

                if (partition_state.last_committed_decree - partition_state.confirmed_decree <=
                    progress_gap) {
                    continue;
                }

                unfinished_app_counter = 1;

                CHECK_LT(partition_id, unfinished_partition_counters.size());
                unfinished_partition_counters[partition_id] = 1;

                stat.unfinished_apps[app_name][dup_id].insert(partition_id);
            }
        }

        for (const auto &counter : unfinished_partition_counters) {
            stat.unfinished_partition_count += counter;
        }

        stat.unfinished_app_count += unfinished_app_counter;
    }
}

void add_titles_for_dups(dsn::utils::table_printer &printer, bool list_partitions)
{
    printer.add_title("app_name");
    printer.add_column("dup_id", tp_alignment::kRight);
    printer.add_column("create_time", tp_alignment::kRight);
    printer.add_column("status", tp_alignment::kRight);
    printer.add_column("remote_cluster", tp_alignment::kRight);
    printer.add_column("remote_app_name", tp_alignment::kRight);

    if (list_partitions) {
        printer.add_column("partition_id", tp_alignment::kRight);
        printer.add_column("confirmed_decree", tp_alignment::kRight);
        printer.add_column("last_committed_decree", tp_alignment::kRight);
    }
}

void add_base_row_for_dups(dsn::utils::table_printer &printer,
                           const std::string &app_name,
                           const dsn::replication::duplication_entry &dup)
{
    printer.add_row(app_name);
    printer.append_data(dup.dupid);

    std::string create_time;
    dsn::utils::time_ms_to_string(dup.create_ts, create_time);
    printer.append_data(create_time);

    printer.append_data(dsn::replication::duplication_status_to_string(dup.status));
    printer.append_data(dup.remote);
    printer.append_data(dup.__isset.remote_app_name ? dup.remote_app_name : app_name);
}

void add_row_for_dups(dsn::utils::table_printer &printer,
                      bool list_partitions,
                      const std::string &app_name,
                      const dsn::replication::duplication_entry &dup)
{
    if (list_partitions) {
        add_base_row_for_dups(printer, app_name, dup);
        return;
    }

    for (const auto &[partition_id, partition_state] : dup.partition_states) {
        add_base_row_for_dups(printer, app_name, dup);
        printer.append_data(partition_id);
        printer.append_data(partition_state.confirmed_decree);
        printer.append_data(partition_state.last_committed_decree);
    }
}

void print_dups(const std::map<std::string, dsn::replication::duplication_app_state> &app_states,
                bool list_partitions)
{
    dsn::utils::table_printer printer("duplications");
    add_titles_for_dups(printer, list_partitions);

    for (const auto &[app_name, app] : app_states) {
        if (app.duplications.empty()) {
            continue;
        }

        for (const auto &[_, dup] : app.duplications) {
            if (!list_partitions) {
                continue;
            }

            if (!dup.__isset.partition_states) {
                continue;
            }

            add_row_for_dups(printer, list_partitions, app_name, dup);
        }
    }

    printer.output(std::cout);
    std::cout << std::endl;
}

void print_selected_dups(
    const std::map<std::string, dsn::replication::duplication_app_state> &app_states,
    const selected_app_dups_map &selected_apps)
{
}

void show_dups(const std::map<std::string, dsn::replication::duplication_app_state> &app_states,
               const list_dups_options &options)
{
    list_dups_stat stat;
    stat_dups(app_states, options.progress_gap, stat);

    print_dups(app_states, options.list_partitions);

    if (options.show_unfinishd) {
        print_selected_dups(app_states, stat.unfinished_apps);
        return;
    }
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

bool ls_dups(command_executor *e, shell_context *sc, arguments args)
{
    // dups [-a|--app_name_pattern str] [-m|--match_type str]
    // [-p|--list_partitions] [-g|--progress_gap num]
    // [-u|--show_unfinishd]

    static const std::set<std::string> params = {
        "a", "app_name_pattern", "m", "match_type", "g", "progress_gap"};
    static const std::set<std::string> flags = {
        "p", "list_partitions", "u", "show_unfinishd"};

    argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);

    const auto &check = validate_cmd(cmd, params, flags, empty_pos_args);
    if (!check) {
        SHELL_PRINTLN_ERROR("{}", check.description());
        return false;
    }

    const std::string app_name_pattern(cmd({"-a", "--app_name_pattern"}, "").str());

    auto match_type = dsn::utils::pattern_match_type::PMT_MATCH_ALL;
    PARSE_OPT_ENUM(match_type, dsn::utils::pattern_match_type::PMT_INVALID, {"-m", "--match_type"});

    list_dups_options options;
    options.list_partitions = cmd[{"-p", "--list_partitions"}];
    PARSE_OPT_UINT(options.progress_gap, 0, {"-g", "--progress_gap"});
    options.show_unfinishd = cmd[{"-u", "--show_unfinishd"}];

    const auto &result = sc->ddl_client->list_dups(app_name_pattern, match_type);
    auto status = result.get_error();
    if (status) {
        status = FMT_ERR(result.get_value().err, result.get_value().hint_message);
    }

    if (!status) {
        SHELL_PRINTLN_ERROR("list duplications failed, error={}", status);
        return true;
    }

    show_dups(result.get_value().app_states, options);
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
