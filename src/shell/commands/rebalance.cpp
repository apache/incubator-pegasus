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
#include <getopt.h>
#include <stdio.h>
#include <algorithm>
#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "client/replication_ddl_client.h"
#include "common/gpid.h"
#include "meta/load_balance_policy.h"
#include "meta_admin_types.h"
#include "rpc/rpc_host_port.h"
#include "shell/command_executor.h"
#include "shell/command_helper.h"
#include "shell/command_utils.h"
#include "shell/commands.h"
#include "shell/sds/sds.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"

bool set_meta_level(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc <= 1)
        return false;

    dsn::replication::meta_function_level::type l;
    l = type_from_string(_meta_function_level_VALUES_TO_NAMES,
                         std::string("fl_") + args.argv[1],
                         meta_function_level::fl_invalid);
    SHELL_PRINT_AND_RETURN_FALSE_IF_NOT(l != meta_function_level::fl_invalid,
                                        "parse {} as meta function level failed",
                                        args.argv[1]);

    configuration_meta_control_response resp = sc->ddl_client->control_meta_function_level(l);
    if (resp.err == dsn::ERR_OK) {
        std::cout << "control meta level ok, the old level is "
                  << _meta_function_level_VALUES_TO_NAMES.find(resp.old_level)->second << std::endl;
    } else {
        std::cout << "control meta level got error " << resp.err << std::endl;
    }
    return true;
}

bool get_meta_level(command_executor *e, shell_context *sc, arguments args)
{
    configuration_meta_control_response resp = sc->ddl_client->control_meta_function_level(
        dsn::replication::meta_function_level::fl_invalid);
    if (resp.err == dsn::ERR_OK) {
        std::cout << "current meta level is "
                  << _meta_function_level_VALUES_TO_NAMES.find(resp.old_level)->second << std::endl;
    } else {
        std::cout << "get meta level got error " << resp.err << std::endl;
    }
    return true;
}

bool propose(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"force", no_argument, 0, 'f'},
                                           {"gpid", required_argument, 0, 'g'},
                                           {"type", required_argument, 0, 'p'},
                                           {"target", required_argument, 0, 't'},
                                           {"node", required_argument, 0, 'n'},
                                           {0, 0, 0, 0}};

    dverify(args.argc >= 9);
    dsn::replication::configuration_balancer_request request;
    request.gpid.set_app_id(-1);
    dsn::host_port target, node;
    std::string proposal_type = "CT_";
    request.force = false;
    bool ans;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "fg:p:t:n:", long_options, &option_index);
        if (-1 == c)
            break;
        switch (c) {
        case 'f':
            request.force = true;
            break;
        case 'g':
            ans = request.gpid.parse_from(optarg);
            SHELL_PRINT_AND_RETURN_FALSE_IF_NOT(ans, "parse {} as gpid failed", optarg);
            break;
        case 'p':
            proposal_type += optarg;
            break;
        case 't':
            target = dsn::host_port::from_string(optarg);
            SHELL_PRINT_AND_RETURN_FALSE_IF_NOT(
                target, "parse {} as target_host_port failed", optarg);
            break;
        case 'n':
            node = dsn::host_port::from_string(optarg);
            SHELL_PRINT_AND_RETURN_FALSE_IF_NOT(target, "parse {}  as node failed", optarg);
            break;
        default:
            return false;
        }
    }

    SHELL_PRINT_AND_RETURN_FALSE_IF_NOT(target, "need set target by -t");
    SHELL_PRINT_AND_RETURN_FALSE_IF_NOT(node, "need set node by -n");
    SHELL_PRINT_AND_RETURN_FALSE_IF_NOT(request.gpid.get_app_id() != -1, "need set gpid by -g");

    config_type::type tp =
        type_from_string(_config_type_VALUES_TO_NAMES, proposal_type, config_type::CT_INVALID);
    SHELL_PRINT_AND_RETURN_FALSE_IF_NOT(
        tp != config_type::CT_INVALID, "parse {} as config_type failed.\n", proposal_type);
    request.action_list = {new_proposal_action(target, node, tp)};
    dsn::error_code err = sc->ddl_client->send_balancer_proposal(request);
    std::cout << "send proposal response: " << err << std::endl;
    return true;
}

bool balance(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"gpid", required_argument, 0, 'g'},
                                           {"type", required_argument, 0, 'p'},
                                           {"from", required_argument, 0, 'f'},
                                           {"to", required_argument, 0, 't'},
                                           {0, 0, 0, 0}};

    if (args.argc < 9)
        return false;

    dsn::replication::configuration_balancer_request request;
    request.gpid.set_app_id(-1);
    std::string balance_type;
    dsn::host_port from, to;
    bool ans;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "g:p:f:t:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'g':
            ans = request.gpid.parse_from(optarg);
            if (!ans) {
                fprintf(stderr, "parse %s as gpid failed\n", optarg);
                return false;
            }
            break;
        case 'p':
            balance_type = optarg;
            break;
        case 'f':
            from = dsn::host_port::from_string(optarg);
            if (!from) {
                fprintf(stderr, "parse %s as from_host_port failed\n", optarg);
                return false;
            }
            break;
        case 't':
            to = dsn::host_port::from_string(optarg);
            if (!to) {
                fprintf(stderr, "parse %s as target_address failed\n", optarg);
                return false;
            }
            break;
        default:
            return false;
        }
    }

    std::vector<configuration_proposal_action> &actions = request.action_list;
    actions.reserve(4);
    if (balance_type == "move_pri") {
        actions.emplace_back(
            new_proposal_action(from, from, config_type::CT_DOWNGRADE_TO_SECONDARY));
        actions.emplace_back(new_proposal_action(to, to, config_type::CT_UPGRADE_TO_PRIMARY));
    } else if (balance_type == "copy_pri") {
        actions.emplace_back(new_proposal_action(from, to, config_type::CT_ADD_SECONDARY_FOR_LB));
        actions.emplace_back(
            new_proposal_action(from, from, config_type::CT_DOWNGRADE_TO_SECONDARY));
        actions.emplace_back(new_proposal_action(to, to, config_type::CT_UPGRADE_TO_PRIMARY));
    } else if (balance_type == "copy_sec") {
        actions.emplace_back(
            new_proposal_action(dsn::host_port(), to, config_type::CT_ADD_SECONDARY_FOR_LB));
        actions.emplace_back(
            new_proposal_action(dsn::host_port(), from, config_type::CT_DOWNGRADE_TO_INACTIVE));
    } else {
        fprintf(stderr, "parse %s as a balance type failed\n", balance_type.c_str());
        return false;
    }

    if (!from) {
        fprintf(stderr, "need set from address by -f\n");
        return false;
    }
    if (!to) {
        fprintf(stderr, "need set target address by -t\n");
        return false;
    }
    if (request.gpid.get_app_id() == -1) {
        fprintf(stderr, "need set the gpid by -g\n");
        return false;
    }
    dsn::error_code ec = sc->ddl_client->send_balancer_proposal(request);
    std::cout << "send balance proposal result: " << ec << std::endl;
    return true;
}
