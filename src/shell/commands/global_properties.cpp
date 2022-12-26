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

#include "shell/commands.h"
#include "utils/strings.h"

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
        if (dsn::utils::iequals(args.argv[1], "true")) {
            sc->escape_all = true;
            fprintf(stderr, "OK\n");
            return true;
        } else if (dsn::utils::iequals(args.argv[1], "false")) {
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
