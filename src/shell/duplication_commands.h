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
    dsn::error_code err = sc->ddl_client->add_dup(app_name, remote_address);
    if (err != dsn::ERR_OK) {
        fmt::print("adding duplication for app [{}] failed, error={}\n", app_name, err.to_string());
    }
    return true;
}

inline bool query_dup(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc <= 1)
        return false;

    std::string app_name = args.argv[1];
    dsn::error_code err = sc->ddl_client->query_dup(app_name);
    if (err != dsn::ERR_OK) {
        fmt::print(
            "querying duplications of app [{}] failed, error={}\n", app_name, err.to_string());
    }
    return true;
}

namespace internal {

inline bool get_dupid(const std::string &str, dsn::replication::dupid_t *dup_id)
{
    bool ok = dsn::buf2int32(str, *dup_id);
    if (!ok) {
        fmt::print(stderr, "parsing {} as positive int failed: {}\n", str);
        return false;
    }
    return true;
}

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
    if (!get_dupid(args.argv[2], &dup_id)) {
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

    dsn::error_code err = sc->ddl_client->change_dup_status(app_name, dup_id, status);
    if (err == dsn::ERR_OK) {
        fmt::print("{}({}) for app [{}] succeed\n", operation, dup_id, app_name);
    } else {
        fmt::print(
            "{}({}) for app [{}] failed, error=\n", operation, dup_id, app_name, err.to_string());
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
