// Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "shell/commands.h"

bool start_bulk_load(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"app_name", required_argument, 0, 'a'},
                                           {"cluster_name", required_argument, 0, 'c'},
                                           {"file_provider_type", required_argument, 0, 'p'},
                                           {0, 0, 0, 0}};
    std::string app_name;
    std::string cluster_name;
    std::string file_provider_type;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "a:c:p:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            app_name = optarg;
            break;
        case 'c':
            cluster_name = optarg;
            break;
        case 'p':
            file_provider_type = optarg;
            break;
        default:
            return false;
        }
    }

    if (app_name.empty()) {
        fprintf(stderr, "app_name should not be empty\n");
        return false;
    }
    if (cluster_name.empty()) {
        fprintf(stderr, "cluster_name should not be empty\n");
        return false;
    }

    if (file_provider_type.empty()) {
        fprintf(stderr, "file_provider_type should not be empty\n");
        return false;
    }

    auto err_resp = sc->ddl_client->start_bulk_load(app_name, cluster_name, file_provider_type);
    dsn::error_s err = err_resp.get_error();
    std::string hint_msg;
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
        hint_msg = err_resp.get_value().hint_msg;
    }
    if (!err.is_ok()) {
        fmt::print(stderr, "start bulk load failed, error={} [hint:\"{}\"]\n", err, hint_msg);
    } else {
        fmt::print(stdout, "start bulk load succeed\n");
    }

    return true;
}

// helper function for pause/restart bulk load
bool control_bulk_load_helper(command_executor *e,
                              shell_context *sc,
                              arguments args,
                              dsn::replication::bulk_load_control_type::type type)
{
    if (type != dsn::replication::bulk_load_control_type::BLC_PAUSE &&
        type != dsn::replication::bulk_load_control_type::BLC_RESTART) {
        return false;
    }

    static struct option long_options[] = {{"app_name", required_argument, 0, 'a'}, {0, 0, 0, 0}};
    std::string app_name;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "a:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            app_name = optarg;
            break;
        default:
            return false;
        }
    }

    if (app_name.empty()) {
        fprintf(stderr, "app_name should not be empty\n");
        return false;
    }

    auto err_resp = sc->ddl_client->control_bulk_load(app_name, type);
    dsn::error_s err = err_resp.get_error();
    std::string hint_msg;
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
        hint_msg = err_resp.get_value().hint_msg;
    }
    std::string type_str =
        type == dsn::replication::bulk_load_control_type::BLC_PAUSE ? "pause" : "restart";
    if (!err.is_ok()) {
        fmt::print(
            stderr, "{} bulk load failed, error={} [hint:\"{}\"]\n", type_str, err, hint_msg);
    } else {
        fmt::print(stdout, "{} bulk load succeed\n", type_str);
    }

    return true;
}

bool pause_bulk_load(command_executor *e, shell_context *sc, arguments args)
{
    return control_bulk_load_helper(
        e, sc, args, dsn::replication::bulk_load_control_type::BLC_PAUSE);
}

bool restart_bulk_load(command_executor *e, shell_context *sc, arguments args)
{
    return control_bulk_load_helper(
        e, sc, args, dsn::replication::bulk_load_control_type::BLC_RESTART);
}

bool cancel_bulk_load(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {
        {"app_name", required_argument, 0, 'a'}, {"forced", no_argument, 0, 'f'}, {0, 0, 0, 0}};
    std::string app_name;
    bool forced = false;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "a:f", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'a':
            app_name = optarg;
            break;
        case 'f':
            forced = true;
            break;
        default:
            return false;
        }
    }

    if (app_name.empty()) {
        fprintf(stderr, "app_name should not be empty\n");
        return false;
    }

    auto type = forced ? dsn::replication::bulk_load_control_type::BLC_FORCE_CANCEL
                       : dsn::replication::bulk_load_control_type::BLC_CANCEL;
    auto err_resp = sc->ddl_client->control_bulk_load(app_name, type);
    dsn::error_s err = err_resp.get_error();
    std::string hint_msg;
    if (err.is_ok()) {
        err = dsn::error_s::make(err_resp.get_value().err);
        hint_msg = err_resp.get_value().hint_msg;
    }
    if (!err.is_ok()) {
        fmt::print(stderr, "cancel bulk load failed, error={} [hint:\"{}\"]\n", err, hint_msg);
        if (err.code() == dsn::ERR_INVALID_STATE &&
            type == dsn::replication::bulk_load_control_type::BLC_CANCEL) {
            fmt::print(stderr, "you can force cancel bulk load by using \"-f\"\n");
        }
    } else {
        fmt::print(stdout, "cancel bulk load succeed\n");
    }

    return true;
}

bool query_bulk_load_status(command_executor *e, shell_context *sc, arguments args)
{
    // TODO(heyuchen): TBD
    return true;
}
