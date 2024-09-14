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
#include <boost/algorithm/string/trim.hpp>
#include <getopt.h>
#include <stdio.h>
#include <algorithm>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#include "client/replication_ddl_client.h"
#include "common/gpid.h"
#include "dsn.layer2_types.h"
#include "meta/load_balance_policy.h"
#include "meta_admin_types.h"
#include "rpc/rpc_host_port.h"
#include "shell/command_executor.h"
#include "shell/commands.h"
#include "utils/error_code.h"
#include "utils/string_conv.h"
#include "utils/strings.h"
#include "utils/time_utils.h"

bool recover(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"node_list_file", required_argument, 0, 'f'},
                                           {"node_list_str", required_argument, 0, 's'},
                                           {"wait_seconds", required_argument, 0, 'w'},
                                           {"skip_bad_nodes", no_argument, 0, 'b'},
                                           {"skip_lost_partitions", no_argument, 0, 'l'},
                                           {"output", required_argument, 0, 'o'},
                                           {0, 0, 0, 0}};

    std::string node_list_file;
    std::string node_list_str;
    int wait_seconds = 100;
    std::string output_file;
    bool skip_bad_nodes = false;
    bool skip_lost_partitions = false;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "f:s:w:o:bl", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'f':
            node_list_file = optarg;
            break;
        case 's':
            node_list_str = optarg;
            break;
        case 'w':
            if (!dsn::buf2int32(optarg, wait_seconds)) {
                fprintf(stderr, "parse %s as wait_seconds failed\n", optarg);
                return false;
            }
            break;
        case 'o':
            output_file = optarg;
            break;
        case 'b':
            skip_bad_nodes = true;
            break;
        case 'l':
            skip_lost_partitions = true;
            break;
        default:
            return false;
        }
    }

    if (wait_seconds <= 0) {
        fprintf(stderr, "invalid wait_seconds %d, should be positive number\n", wait_seconds);
        return false;
    }

    if (node_list_str.empty() && node_list_file.empty()) {
        fprintf(stderr, "should specify one of node_list_file/node_list_str\n");
        return false;
    }

    if (!node_list_str.empty() && !node_list_file.empty()) {
        fprintf(stderr, "should only specify one of node_list_file/node_list_str\n");
        return false;
    }

    std::vector<dsn::host_port> node_list;
    if (!node_list_str.empty()) {
        std::vector<std::string> tokens;
        dsn::utils::split_args(node_list_str.c_str(), tokens, ',');
        if (tokens.empty()) {
            fprintf(stderr, "can't parse node from node_list_str\n");
            return true;
        }

        for (std::string &token : tokens) {
            const auto node = dsn::host_port::from_string(token);
            if (!node) {
                fprintf(stderr, "parse %s as a host:port node failed\n", token.c_str());
                return true;
            }
            node_list.push_back(node);
        }
    } else {
        std::ifstream file(node_list_file);
        if (!file) {
            fprintf(stderr, "open file %s failed\n", node_list_file.c_str());
            return true;
        }

        std::string str;
        int lineno = 0;
        while (std::getline(file, str)) {
            lineno++;
            boost::trim(str);
            if (str.empty() || str[0] == '#' || str[0] == ';')
                continue;
            const auto node = dsn::host_port::from_string(str);
            if (!node) {
                fprintf(stderr,
                        "parse %s at file %s line %d as host:port failed\n",
                        str.c_str(),
                        node_list_file.c_str(),
                        lineno);
                return true;
            }
            node_list.push_back(node);
        }

        if (node_list.empty()) {
            fprintf(stderr, "no node specified in file %s\n", node_list_file.c_str());
            return true;
        }
    }

    dsn::error_code ec = sc->ddl_client->do_recovery(
        node_list, wait_seconds, skip_bad_nodes, skip_lost_partitions, output_file);
    if (!output_file.empty()) {
        std::cout << "recover complete with err = " << ec << std::endl;
    }
    return true;
}

dsn::host_port diagnose_recommend(const ddd_partition_info &pinfo)
{
    if (pinfo.config.hp_last_drops.size() < 2) {
        return dsn::host_port();
    }

    std::vector<dsn::host_port> last_two_nodes(pinfo.config.hp_last_drops.end() - 2,
                                               pinfo.config.hp_last_drops.end());
    std::vector<ddd_node_info> last_dropped;
    for (auto &node : last_two_nodes) {
        auto it = std::find_if(pinfo.dropped.begin(),
                               pinfo.dropped.end(),
                               [&node](const ddd_node_info &r) { return r.hp_node == node; });
        if (it->is_alive && it->is_collected)
            last_dropped.push_back(*it);
    }

    if (last_dropped.size() == 1) {
        const ddd_node_info &ninfo = last_dropped.back();
        if (ninfo.last_committed_decree >= pinfo.config.last_committed_decree) {
            return ninfo.hp_node;
        }
    } else if (last_dropped.size() == 2) {
        const ddd_node_info &secondary = last_dropped.front();
        const ddd_node_info &latest = last_dropped.back();

        // Select a best node to be the new primary, following the rule:
        //  - choose the node with the largest last committed decree
        //  - if last committed decree is the same, choose node with the largest ballot

        if (latest.last_committed_decree == secondary.last_committed_decree &&
            latest.last_committed_decree >= pinfo.config.last_committed_decree) {
            return latest.ballot >= secondary.ballot ? latest.hp_node : secondary.hp_node;
        }

        if (latest.last_committed_decree > secondary.last_committed_decree &&
            latest.last_committed_decree >= pinfo.config.last_committed_decree) {
            return latest.hp_node;
        }

        if (secondary.last_committed_decree > latest.last_committed_decree &&
            secondary.last_committed_decree >= pinfo.config.last_committed_decree) {
            return secondary.hp_node;
        }
    }

    return dsn::host_port();
}

bool ddd_diagnose(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"gpid", required_argument, 0, 'g'},
                                           {"diagnose", no_argument, 0, 'd'},
                                           {"auto_diagnose", no_argument, 0, 'a'},
                                           {"skip_prompt", no_argument, 0, 's'},
                                           {"output", required_argument, 0, 'o'},
                                           {0, 0, 0, 0}};

    std::string out_file;
    dsn::gpid id(-1, -1);
    bool diagnose = false;
    bool auto_diagnose = false;
    bool skip_prompt = false;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "g:daso:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'g':
            int pid;
            if (id.parse_from(optarg)) {
                // app_id.partition_index
            } else if (sscanf(optarg, "%d", &pid) == 1) {
                // app_id
                id.set_app_id(pid);
            } else {
                fprintf(stderr, "ERROR: invalid gpid %s\n", optarg);
                return false;
            }
            break;
        case 'd':
            diagnose = true;
            break;
        case 'a':
            auto_diagnose = true;
            break;
        case 's':
            skip_prompt = true;
            break;
        case 'o':
            out_file = optarg;
            break;
        default:
            return false;
        }
    }

    std::vector<ddd_partition_info> ddd_partitions;
    ::dsn::error_code ret = sc->ddl_client->ddd_diagnose(id, ddd_partitions);
    if (ret != dsn::ERR_OK) {
        fprintf(stderr, "ERROR: DDD diagnose failed with err = %s\n", ret.to_string());
        return true;
    }

    std::streambuf *buf;
    std::ofstream of;

    if (!out_file.empty()) {
        of.open(out_file);
        buf = of.rdbuf();
    } else {
        buf = std::cout.rdbuf();
    }
    std::ostream out(buf);

    out << "Total " << ddd_partitions.size() << " ddd partitions:" << std::endl;
    out << std::endl;
    int proposed_count = 0;
    int i = 0;
    for (const ddd_partition_info &pinfo : ddd_partitions) {
        out << "(" << ++i << ") " << pinfo.config.pid << std::endl;
        out << "    config: ballot(" << pinfo.config.ballot << "), "
            << "last_committed(" << pinfo.config.last_committed_decree << ")" << std::endl;
        out << "    ----" << std::endl;
        dsn::host_port latest_dropped, secondary_latest_dropped;
        if (pinfo.config.hp_last_drops.size() > 0) {
            latest_dropped = pinfo.config.hp_last_drops[pinfo.config.hp_last_drops.size() - 1];
        }
        if (pinfo.config.hp_last_drops.size() > 1) {
            secondary_latest_dropped =
                pinfo.config.hp_last_drops[pinfo.config.hp_last_drops.size() - 2];
        }
        int j = 0;
        for (const ddd_node_info &n : pinfo.dropped) {
            dsn::host_port hp_node;
            GET_HOST_PORT(n, node, hp_node);
            char time_buf[30] = {0};
            ::dsn::utils::time_ms_to_string(n.drop_time_ms, time_buf);
            out << "    dropped[" << j++ << "]: "
                << "node(" << hp_node << "), "
                << "drop_time(" << time_buf << "), "
                << "alive(" << (n.is_alive ? "true" : "false") << "), "
                << "collected(" << (n.is_collected ? "true" : "false") << "), "
                << "ballot(" << n.ballot << "), "
                << "last_committed(" << n.last_committed_decree << "), "
                << "last_prepared(" << n.last_prepared_decree << ")";
            if (hp_node == latest_dropped)
                out << "  <== the latest";
            else if (hp_node == secondary_latest_dropped)
                out << "  <== the secondary latest";
            out << std::endl;
        }
        out << "    ----" << std::endl;
        j = 0;
        for (const auto &r : pinfo.config.hp_last_drops) {
            out << "    last_drops[" << j++ << "]: "
                << "node(" << r.to_string() << ")";
            if (j == (int)pinfo.config.hp_last_drops.size() - 1)
                out << "  <== the secondary latest";
            else if (j == (int)pinfo.config.hp_last_drops.size())
                out << "  <== the latest";
            out << std::endl;
        }
        out << "    ----" << std::endl;
        out << "    ddd_reason: " << pinfo.reason << std::endl;
        if (diagnose) {
            out << "    ----" << std::endl;

            auto primary = diagnose_recommend(pinfo);
            out << "    recommend_primary: " << (!primary ? "none" : primary.to_string());
            if (primary == latest_dropped)
                out << "  <== the latest";
            else if (primary == secondary_latest_dropped)
                out << "  <== the secondary latest";
            out << std::endl;

            bool skip_this = false;
            if (primary && !auto_diagnose && !skip_prompt) {
                do {
                    std::cout << "    > Are you sure to use the recommend primary? [y/n/s(skip)]: ";
                    char c;
                    std::cin >> c;
                    if (c == 'y') {
                        break;
                    } else if (c == 'n') {
                        primary.reset();
                        break;
                    } else if (c == 's') {
                        skip_this = true;
                        std::cout << "    > You have choosed to skip diagnosing this partition."
                                  << std::endl;
                        break;
                    }
                } while (true);
            }

            if (!primary && !skip_prompt && !skip_this) {
                do {
                    std::cout << "    > Please input the primary node: ";
                    std::string node;
                    std::cin >> node;
                    primary = dsn::host_port::from_string(node);
                    if (primary) {
                        break;
                    }
                    std::cout << "    > Sorry, you have input an invalid node address."
                              << std::endl;
                } while (true);
            }

            if (primary && !skip_this) {
                dsn::replication::configuration_balancer_request request;
                request.gpid = pinfo.config.pid;
                request.action_list = {
                    new_proposal_action(primary, primary, config_type::CT_ASSIGN_PRIMARY)};
                request.force = false;
                dsn::error_code err = sc->ddl_client->send_balancer_proposal(request);
                out << "    propose_request: propose -g " << request.gpid
                    << " -p ASSIGN_PRIMARY -t " << primary << " -n " << primary << std::endl;
                out << "    propose_response: " << err << std::endl;
                proposed_count++;
            } else {
                out << "    propose_request: none" << std::endl;
            }
        }
        out << std::endl;
        out << "Proposed count: " << proposed_count << "/" << ddd_partitions.size() << std::endl;
        out << std::endl;
    }

    std::cout << "Diagnose ddd done." << std::endl;
    return true;
}
