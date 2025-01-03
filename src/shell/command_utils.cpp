// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "command_utils.h"

#include <fmt/core.h>
#include <cstdio>
#include <memory>

#include "client/replication_ddl_client.h"
#include "command_executor.h"
#include "meta_admin_types.h"
#include "rpc/rpc_host_port.h"
#include "utils/error_code.h"

bool validate_ip(shell_context *sc,
                 const std::string &target_hp_str,
                 dsn::host_port &target_hp,
                 std::string &err_info)
{
    target_hp = dsn::host_port::from_string(target_hp_str);
    if (!target_hp) {
        err_info =
            fmt::format("invalid host:port '{}', can't transform it into host_port", target_hp_str);
        return false;
    }

    std::map<dsn::host_port, dsn::replication::node_status::type> ns_by_nodes;
    const auto error =
        sc->ddl_client->list_nodes(dsn::replication::node_status::NS_INVALID, ns_by_nodes);
    if (error != dsn::ERR_OK) {
        err_info = fmt::format("list nodes failed, error={}", error);
        return false;
    }

    for (const auto &[node, _] : ns_by_nodes) {
        if (target_hp == node) {
            return true;
        }
    }

    err_info = fmt::format("invalid host:port '{}', can't find it in the cluster", target_hp_str);
    return false;
}

bool confirm_unsafe_command(const std::string &action)
{
    const int max_attempts = 5;
    for (int attempts = 0; attempts < max_attempts; ++attempts) {
        fmt::print(stdout,
                   "PLEASE be CAUTIOUS with this operation ! "
                   "Are you sure to {} ? [y/n]: ",
                   action);

        int choice = fgetc(stdin);
        int len = 0;
        for (int c = choice; c != '\n' && c != EOF; ++len) {
            c = fgetc(stdin);
        }
        if (len != 1) {
            continue;
        }

        if (choice == 'y') {
            fmt::print(stdout, "you've chosen YES, we will continue ...\n");
            return true;
        } else if (choice == 'n') {
            fmt::print(stdout, "you've chosen NO, we will stop !\n");
            return false;
        }
    }

    fmt::print(stdout, "too many failed attempts, we will stop !\n");
    return false;
}
