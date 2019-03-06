// Copyright (c) 2019, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "shell/commands.h"

bool use_app_as_current(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc == 1) {
        if (sc->current_app_name.empty()) {
            fprintf(stderr, "Current app not specified.\n");
            return false;
        } else {
            fprintf(stderr, "Current app: %s\n", sc->current_app_name.c_str());
            return true;
        }
    } else if (args.argc == 2) {
        sc->current_app_name = args.argv[1];
        fprintf(stderr, "OK\n");
        return true;
    } else {
        return false;
    }
}

bool process_escape_all(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc == 1) {
        fprintf(stderr, "Current escape_all: %s.\n", sc->escape_all ? "true" : "false");
        return true;
    } else if (args.argc == 2) {
        if (strcmp(args.argv[1], "true") == 0) {
            sc->escape_all = true;
            fprintf(stderr, "OK\n");
            return true;
        } else if (strcmp(args.argv[1], "false") == 0) {
            sc->escape_all = false;
            fprintf(stderr, "OK\n");
            return true;
        } else {
            fprintf(stderr, "ERROR: invalid parameter.\n");
            return false;
        }
    } else {
        return false;
    }
}

bool process_timeout(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc == 1) {
        fprintf(stderr, "Current timeout: %d ms.\n", sc->timeout_ms);
        return true;
    } else if (args.argc == 2) {
        int timeout;
        if (!dsn::buf2int32(args.argv[1], timeout)) {
            fprintf(stderr, "ERROR: parse %s as timeout failed\n", args.argv[1]);
            return false;
        }
        if (timeout <= 0) {
            fprintf(stderr, "ERROR: invalid timeout %s\n", args.argv[1]);
            return false;
        }
        sc->timeout_ms = timeout;
        fprintf(stderr, "OK\n");
        return true;
    } else {
        return false;
    }
}

extern void check_in_cluster(std::string cluster_name);

bool cc_command(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc == 2) {
        std::string cluster_name = args.argv[1];
        if (!cluster_name.empty()) {
            check_in_cluster(cluster_name);
            return true;
        }
    }
    return false;
}
