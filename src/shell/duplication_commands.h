// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <fmt/ostream.h>
#include <dsn/utility/errors.h>
#include <dsn/dist/replication/duplication_common.h>

#include "command_executor.h"

inline bool add_dup(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc <= 2)
        return false;

    std::string app_name = args.argv[1];
    std::string remote_address = args.argv[2];
    auto err_resp = sc->ddl_client->add_dup(app_name, remote_address);
    if (!err_resp.is_ok()) {
        fmt::print("adding duplication for app [{}] failed, error={}\n",
                   app_name,
                   err_resp.get_error().description());
    } else {
        const auto &resp = err_resp.get_value();
        fmt::print(
            "Success for adding duplication [appid: {}, dupid: {}]\n", resp.appid, resp.dupid);
    }
    return true;
}

inline bool string2dupid(const std::string &str, dsn::replication::dupid_t *dup_id)
{
    bool ok = dsn::buf2int32(str, *dup_id);
    if (!ok) {
        fmt::print(stderr, "parsing {} as positive int failed: {}\n", str);
        return false;
    }
    return true;
}

inline bool query_dup(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc <= 1)
        return false;

    std::string app_name = args.argv[1];
    auto err_resp = sc->ddl_client->query_dup(app_name);
    if (!err_resp.is_ok()) {
        fmt::print("querying duplications of app [{}] failed, error={}\n",
                   app_name,
                   err_resp.get_error().description());
    } else {
        const auto &resp = err_resp.get_value();
        fmt::print("duplications of app [{}] are listed as below:\n", app_name);
        fmt::print("|{: ^16}|{: ^12}|{: ^24}|{: ^25}|\n",
                   "dup_id",
                   "status",
                   "remote cluster",
                   "create time");
        char create_time[25];
        for (auto info : resp.entry_list) {
            dsn::utils::time_ms_to_date_time(info.create_ts, create_time, sizeof(create_time));
            fmt::print("|{: ^16}|{: ^12}|{: ^24}|{: ^25}|\n",
                       info.dupid,
                       duplication_status_to_string(info.status),
                       info.remote_address,
                       create_time);
        }
    }
    return true;
}

inline bool query_dup_detail(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc <= 2)
        return false;

    std::string app_name = args.argv[1];

    dsn::replication::dupid_t dup_id;
    if (!string2dupid(args.argv[2], &dup_id)) {
        return false;
    }

    auto err_resp = sc->ddl_client->query_dup(app_name);
    if (!err_resp.is_ok()) {
        fmt::print("querying duplication of [app({}) dupid({})] failed, error={}\n",
                   app_name,
                   dup_id,
                   err_resp.get_error().description());
    } else {
        fmt::print("duplication [{}] of app [{}]:\n", dup_id, app_name);
        const auto &resp = err_resp.get_value();
        for (auto info : resp.entry_list) {
            if (info.dupid == dup_id) {
                fmt::print("{}\n", duplication_entry_to_string(info));
            }
        }
    }
    return true;
}

namespace internal {

inline bool change_dup_status(command_executor *e,
                              shell_context *sc,
                              const arguments &args,
                              duplication_status::type status)
{
    if (args.argc <= 2) {
        return false;
    }

    std::string app_name = args.argv[1];

    dsn::replication::dupid_t dup_id;
    if (!string2dupid(args.argv[2], &dup_id)) {
        return false;
    }

    std::string operation;
    switch (status) {
    case duplication_status::DS_START:
        operation = "starting duplication";
        break;
    case duplication_status::DS_PAUSE:
        operation = "pausing duplication";
        break;
    case duplication_status::DS_REMOVED:
        operation = "removing duplication";
        break;
    default:
        dfatal("unexpected duplication status %d", status);
    }

    auto err_resp = sc->ddl_client->change_dup_status(app_name, dup_id, status);
    if (err_resp.is_ok()) {
        fmt::print("{}({}) for app [{}] succeed\n", operation, dup_id, app_name);
    } else {
        fmt::print("{}({}) for app [{}] failed, error={}\n",
                   operation,
                   dup_id,
                   app_name,
                   err_resp.get_error().description());
    }
    return true;
}

} // namespace internal

inline bool remove_dup(command_executor *e, shell_context *sc, arguments args)
{
    return internal::change_dup_status(e, sc, args, duplication_status::DS_REMOVED);
}

inline bool start_dup(command_executor *e, shell_context *sc, arguments args)
{
    return internal::change_dup_status(e, sc, args, duplication_status::DS_START);
}

inline bool pause_dup(command_executor *e, shell_context *sc, arguments args)
{
    return internal::change_dup_status(e, sc, args, duplication_status::DS_PAUSE);
}
