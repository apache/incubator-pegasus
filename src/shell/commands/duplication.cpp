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
#include <stdio.h>
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

bool add_dup(command_executor *e, shell_context *sc, arguments args)
{
    // add_dup <app_name> <remote_cluster_name> [-s|--sst] [-a|--remote_app_name str]
    // [-r|--remote_replica_count num]

    argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);

    if (!cmd(1)) {
        fmt::print(stderr, "missing param <app_name>\n");
        return false;
    }
    std::string app_name = cmd(1).str();

    if (!cmd(2)) {
        fmt::print(stderr, "missing param <remote_cluster_name>\n");
        return false;
    }
    std::string remote_cluster_name = cmd(2).str();

    if (remote_cluster_name == sc->current_cluster_name) {
        fmt::print(stderr,
                   "illegal operation: adding duplication to itself [remote: {}]\n",
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

    fmt::print("trying to add duplication [app_name: {}, remote_cluster_name: {}, "
               "is_duplicating_checkpoint: {}, remote_app_name: {}, remote_replica_count: {}]\n",
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

    if (!err) {
        fmt::print(stderr,
                   "adding duplication failed [app_name: {}, remote_cluster_name: {}, "
                   "is_duplicating_checkpoint: {}, remote_app_name: {}, remote_replica_count: {}, "
                   "error: {}]\n",
                   app_name,
                   remote_cluster_name,
                   is_duplicating_checkpoint,
                   remote_app_name,
                   remote_replica_count,
                   err);

        if (!hint.empty()) {
            fmt::print(stderr, "detail:\n  {}\n", hint);
        }

        return true;
    }

    const auto &resp = err_resp.get_value();
    fmt::print(
        "adding duplication succeed [app_name: {}, remote_cluster_name: {}, appid: {}, dupid: "
        "{}, is_duplicating_checkpoint: {}",
        app_name,
        remote_cluster_name,
        resp.appid,
        resp.dupid,
        is_duplicating_checkpoint);

    if (resp.__isset.remote_app_name) {
        fmt::print(", remote_app_name: {}", resp.remote_app_name);
    }

    if (resp.__isset.remote_replica_count) {
        fmt::print(", remote_replica_count: {}", resp.remote_replica_count);
    }

    fmt::print("]\n");

    if (!resp.__isset.remote_app_name) {
        fmt::print("WARNING: meta server does NOT support specifying remote_app_name, "
                   "remote_app_name might has been specified with '{}'\n",
                   app_name);
    }

    if (!resp.__isset.remote_replica_count) {
        fmt::print("WARNING: meta server does NOT support specifying remote_replica_count, "
                   "remote_replica_count might has been specified with the replica count of '{}'\n",
                   app_name);
    }

    return true;
}

// print error if parsing failed
bool string2dupid(const std::string &str, dupid_t *dup_id)
{
    bool ok = dsn::buf2int32(str, *dup_id);
    if (!ok) {
        fmt::print(stderr, "parsing {} as positive int failed: {}\n", str);
        return false;
    }
    return true;
}

bool query_dup(command_executor *e, shell_context *sc, arguments args)
{
    // query_dup <app_name> [-d|--detail]

    argh::parser cmd(args.argc, args.argv);
    if (cmd.pos_args().size() > 2) {
        fmt::print(stderr, "too many params\n");
        return false;
    }
    for (const auto &flag : cmd.flags()) {
        if (flag != "d" && flag != "detail") {
            fmt::print(stderr, "unknown flag {}\n", flag);
            return false;
        }
    }

    if (!cmd(1)) {
        fmt::print(stderr, "missing param <app_name>\n");
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
        fmt::print(stderr, "querying duplications of app [{}] failed, error={}\n", app_name, err);

        return true;
    }

    if (detail) {
        fmt::print("duplications of app [{}] in detail:\n", app_name);
        fmt::print("{}\n\n", duplication_query_response_to_string(err_resp.get_value()));

        return true;
    }

    const auto &resp = err_resp.get_value();
    fmt::print("duplications of app [{}] are listed as below:\n", app_name);

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
        fmt::print("{} succeed\n", operation);
    } else {
        fmt::print(stderr, "{} failed, error={}{}\n", operation, err.description(), hint);
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
        fmt::print(stderr, "too many params\n");
        return false;
    }
    std::string app_name = cmd(1).str();
    std::string dupid_str = cmd(2).str();
    dupid_t dup_id;
    if (!dsn::buf2int32(dupid_str, dup_id)) {
        fmt::print(stderr, "invalid dup_id {}\n", dupid_str);
        return false;
    }
    std::string fail_mode_str = cmd(3).str();
    if (fail_mode_str != "slow" && fail_mode_str != "skip") {
        fmt::print(stderr, "fail_mode must be \"slow\" or  \"skip\": {}\n", fail_mode_str);
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
