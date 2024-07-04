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
#include <boost/cstdint.hpp>
#include <boost/lexical_cast.hpp>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <getopt.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <algorithm>
#include <cstdint>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "client/replication_ddl_client.h"
#include "shell/argh.h"
#include "shell/command_executor.h"
#include "shell/command_helper.h"
#include "shell/commands.h"
#include "shell/sds/sds.h"
#include "utils/error_code.h"
#include "utils/strings.h"

bool add_backup_policy(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"policy_name", required_argument, 0, 'p'},
                                           {"backup_provider_type", required_argument, 0, 'b'},
                                           {"app_ids", required_argument, 0, 'a'},
                                           {"backup_interval_seconds", required_argument, 0, 'i'},
                                           {"start_time", required_argument, 0, 's'},
                                           {"backup_history_cnt", required_argument, 0, 'c'},
                                           {0, 0, 0, 0}};

    std::string policy_name;
    std::string backup_provider_type;
    std::vector<int32_t> app_ids;
    int64_t backup_interval_seconds = 0;
    std::string start_time;
    int32_t backup_history_cnt = 0;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "p:b:a:i:s:c:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'p':
            policy_name = optarg;
            break;
        case 'b':
            backup_provider_type = optarg;
            ;
            break;
        case 'a': {
            std::vector<std::string> app_list;
            ::dsn::utils::split_args(optarg, app_list, ',');
            for (const auto &app : app_list) {
                int32_t id = atoi(app.c_str());
                if (id <= 0) {
                    fprintf(stderr, "invalid app_id, %d\n", id);
                    return false;
                } else {
                    app_ids.emplace_back(id);
                }
            }
        } break;
        case 'i':
            backup_interval_seconds = atoi(optarg);
            break;
        case 's':
            start_time = optarg;
            break;
        case 'c':
            backup_history_cnt = atoi(optarg);
            break;
        default:
            return false;
        }
    }

    if (policy_name.empty()) {
        fprintf(stderr, "policy_name should not be empty\n");
        return false;
    }
    if (backup_provider_type.empty()) {
        fprintf(stderr, "backup_provider_type should not be empty\n");
        return false;
    }

    if (backup_interval_seconds <= 0) {
        fprintf(stderr,
                "invalid backup_interval_seconds: %" PRId64 ", should > 0\n",
                backup_interval_seconds);
        return false;
    }

    if (backup_history_cnt <= 0) {
        fprintf(stderr, "invalid backup_history_cnt: %d, should > 0\n", backup_history_cnt);
        return false;
    }

    if (!start_time.empty()) {
        int32_t hour = 0, min = 0;
        if (sscanf(start_time.c_str(), "%d:%d", &hour, &min) != 2 || hour > 24 || hour < 0 ||
            min >= 60 || min < 0 || (hour == 24 && min > 0)) {
            fprintf(stderr,
                    "invalid start time: %s, should like this hour:minute\n",
                    start_time.c_str());
            return false;
        }
    }

    ::dsn::error_code ret = sc->ddl_client->add_backup_policy(policy_name,
                                                              backup_provider_type,
                                                              app_ids,
                                                              backup_interval_seconds,
                                                              backup_history_cnt,
                                                              start_time);
    if (ret != ::dsn::ERR_OK) {
        fprintf(stderr, "add backup policy failed, err = %s\n", ret.to_string());
    }
    return true;
}

bool ls_backup_policy(command_executor *e, shell_context *sc, arguments args)
{
    argh::parser cmd(args.argc, args.argv);
    const bool json = cmd[{"-j", "--json"}];

    ::dsn::error_code err = sc->ddl_client->ls_backup_policy(json);
    if (err != ::dsn::ERR_OK) {
        std::cout << "ls backup policy failed" << std::endl;
    }
    return true;
}

bool query_backup_policy(command_executor *e, shell_context *sc, arguments args)
{
    const std::string query_backup_policy_help =
        "<-p|--policy_name> [-b|--backup_info_cnt] [-j|--json]";
    argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);
    RETURN_FALSE_IF_NOT(!cmd.params().empty(),
                        "invalid command, should be in the form of '{}'",
                        query_backup_policy_help);

    std::vector<std::string> policy_names;
    PARSE_OPT_STRS(policy_names, "", {"-p", "--policy_name"});

    if (policy_names.empty()) {
        SHELL_PRINTLN_ERROR(
            "invalid command, policy_name should be in the form of 'val1,val2,val3' and "
            "should not be empty");
        return false;
    }

    std::set<std::string> str_set(policy_names.begin(), policy_names.end());
    if (str_set.size() != policy_names.size()) {
        SHELL_PRINTLN_ERROR("invalid command, policy_name has duplicate values");
        return false;
    }

    uint32_t backup_info_cnt;
    PARSE_OPT_UINT(backup_info_cnt, 3, {"-b", "--backup_info_cnt"});

    const bool json = cmd[{"-j", "--json"}];

    ::dsn::error_code ret =
        sc->ddl_client->query_backup_policy(policy_names, backup_info_cnt, json);
    if (ret != ::dsn::ERR_OK) {
        fprintf(stderr, "query backup policy failed, err = %s\n", ret.to_string());
    }

    return true;
}

bool modify_backup_policy(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"policy_name", required_argument, 0, 'p'},
                                           {"add_app", required_argument, 0, 'a'},
                                           {"remove_app", required_argument, 0, 'r'},
                                           {"backup_interval_seconds", required_argument, 0, 'i'},
                                           {"backup_history_count", required_argument, 0, 'c'},
                                           {"start_time", required_argument, 0, 's'},
                                           {0, 0, 0, 0}};

    std::string policy_name;
    std::vector<int32_t> add_appids;
    std::vector<int32_t> remove_appids;
    int64_t backup_interval_seconds = 0;
    int32_t backup_history_count = 0;
    std::string start_time;
    std::vector<std::string> app_id_strs;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "p:a:r:i:c:s:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'p':
            policy_name = optarg;
            break;
        case 'a':
            app_id_strs.clear();
            ::dsn::utils::split_args(optarg, app_id_strs, ',');
            for (const auto &s_appid : app_id_strs) {
                int32_t appid = boost::lexical_cast<int32_t>(s_appid);
                if (appid <= 0) {
                    fprintf(stderr, "add invalid app_id(%d) to policy\n", appid);
                    return false;
                } else {
                    add_appids.emplace_back(boost::lexical_cast<int32_t>(s_appid));
                }
            }
            break;
        case 'r':
            app_id_strs.clear();
            ::dsn::utils::split_args(optarg, app_id_strs, ',');
            for (const auto &s_appid : app_id_strs) {
                int32_t appid = boost::lexical_cast<int32_t>(s_appid);
                if (appid <= 0) {
                    fprintf(stderr, "remove invalid app_id(%d) from policy\n", appid);
                    return false;
                } else {
                    remove_appids.emplace_back(boost::lexical_cast<int32_t>(s_appid));
                }
            }
            break;
        case 'i':
            backup_interval_seconds = boost::lexical_cast<int64_t>(optarg);
            if (backup_interval_seconds < 0) {
                fprintf(stderr,
                        "invalid backup_interval_seconds(%" PRId64 ")\n",
                        backup_interval_seconds);
                return false;
            }
            break;
        case 'c':
            backup_history_count = boost::lexical_cast<int32_t>(optarg);
            if (backup_history_count < 0) {
                fprintf(stderr, "invalid backup_history_count(%d)\n", backup_history_count);
                return false;
            }
            break;
        case 's':
            start_time = optarg;
            break;
        default:
            return false;
        }
    }

    if (policy_name.empty()) {
        fprintf(stderr, "empty policy name\n");
        return false;
    }

    if (!start_time.empty()) {
        int32_t hour = 0, min = 0;
        if (sscanf(start_time.c_str(), "%d:%d", &hour, &min) != 2 || hour > 24 || hour < 0 ||
            min >= 60 || min < 0 || (hour == 24 && min > 0)) {
            fprintf(stderr,
                    "invalid start time: %s, should like this hour:minute\n",
                    start_time.c_str());
            return false;
        }
    }

    dsn::error_code ret = sc->ddl_client->update_backup_policy(policy_name,
                                                               add_appids,
                                                               remove_appids,
                                                               backup_interval_seconds,
                                                               backup_history_count,
                                                               start_time);
    if (ret != dsn::ERR_OK) {
        fprintf(stderr, "modify backup policy failed, with err = %s\n", ret.to_string());
    }
    return true;
}

const std::string disable_backup_policy_help = "<-p|--policy_name str> [-f|--force]";
bool disable_backup_policy(command_executor *e, shell_context *sc, arguments args)
{
    const argh::parser cmd(args.argc, args.argv, argh::parser::PREFER_PARAM_FOR_UNREG_OPTION);
    // TODO(yingchun): make the following code as a function.
    RETURN_FALSE_IF_NOT(cmd.pos_args().size() == 1 && cmd.pos_args()[0] == "disable_backup_policy",
                        "invalid command, should be in the form of '{}'",
                        disable_backup_policy_help);
    RETURN_FALSE_IF_NOT(cmd.flags().empty() ||
                            (cmd.flags().size() == 1 &&
                             (cmd.flags().count("force") == 1 || cmd.flags().count("f") == 1)),
                        "invalid command, should be in the form of '{}'",
                        disable_backup_policy_help);
    RETURN_FALSE_IF_NOT(cmd.params().size() == 1 && (cmd.params().begin()->first == "policy_name" ||
                                                     cmd.params().begin()->first == "p"),
                        "invalid command, should be in the form of '{}'",
                        disable_backup_policy_help);

    const std::string policy_name = cmd({"-p", "--policy_name"}).str();
    RETURN_FALSE_IF_NOT(!policy_name.empty(), "invalid command, policy_name should not be empty");

    const bool force = cmd[{"-f", "--force"}];

    const auto ret = sc->ddl_client->disable_backup_policy(policy_name, force);
    RETURN_FALSE_IF_NOT(
        ret == dsn::ERR_OK, "disable backup policy failed, with err = {}", ret.to_string());
    return true;
}

bool enable_backup_policy(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"policy_name", required_argument, 0, 'p'},
                                           {0, 0, 0, 0}};
    std::string policy_name;
    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "p:", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'p':
            policy_name = optarg;
            break;
        default:
            return false;
        }
    }

    if (policy_name.empty()) {
        fprintf(stderr, "empty policy name\n");
        return false;
    }

    ::dsn::error_code ret = sc->ddl_client->enable_backup_policy(policy_name);
    if (ret != dsn::ERR_OK) {
        fprintf(stderr, "enable backup policy failed, with err = %s\n", ret.to_string());
    }
    return true;
}

bool restore(command_executor *e, shell_context *sc, arguments args)
{
    static struct option long_options[] = {{"old_cluster_name", required_argument, 0, 'c'},
                                           {"old_policy_name", required_argument, 0, 'p'},
                                           {"old_app_name", required_argument, 0, 'a'},
                                           {"old_app_id", required_argument, 0, 'i'},
                                           {"new_app_name", required_argument, 0, 'n'},
                                           {"timestamp", required_argument, 0, 't'},
                                           {"backup_provider_type", required_argument, 0, 'b'},
                                           {"restore_path", required_argument, 0, 'r'},
                                           {"skip_bad_partition", no_argument, 0, 's'},
                                           {0, 0, 0, 0}};
    std::string old_cluster_name, old_policy_name;
    std::string old_app_name, new_app_name;
    std::string backup_provider_type;
    std::string restore_path;
    int32_t old_app_id = 0;
    int64_t timestamp = 0;
    bool skip_bad_partition = false;

    optind = 0;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "c:p:a:i:n:t:b:r:s", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'c':
            old_cluster_name = optarg;
            break;
        case 'p':
            old_policy_name = optarg;
            break;
        case 'a':
            old_app_name = optarg;
            break;
        case 'i':
            old_app_id = boost::lexical_cast<int32_t>(optarg);
            break;
        case 'n':
            new_app_name = optarg;
            break;
        case 't':
            timestamp = boost::lexical_cast<int64_t>(optarg);
            break;
        case 'b':
            backup_provider_type = optarg;
            break;
        case 'r':
            restore_path = optarg;
            break;
        case 's':
            skip_bad_partition = true;
            break;
        default:
            fprintf(stderr, "invalid parameter\n");
            return false;
        }
    }

    if (old_cluster_name.empty() || old_app_name.empty() || old_app_id <= 0 || timestamp <= 0 ||
        backup_provider_type.empty()) {
        fprintf(stderr, "invalid parameter\n");
        return false;
    }

    if (new_app_name.empty()) {
        // using old_app_name
        new_app_name = old_app_name;
    }

    ::dsn::error_code err = sc->ddl_client->do_restore(backup_provider_type,
                                                       old_cluster_name,
                                                       old_policy_name,
                                                       timestamp,
                                                       old_app_name,
                                                       old_app_id,
                                                       new_app_name,
                                                       skip_bad_partition,
                                                       restore_path);
    if (err != ::dsn::ERR_OK) {
        fprintf(stderr, "restore app failed with err(%s)\n", err.to_string());
    }
    return true;
}

bool query_restore_status(command_executor *e, shell_context *sc, arguments args)
{
    if (args.argc < 2) {
        fprintf(stderr, "invalid parameter\n");
        return false;
    }

    int32_t restore_app_id = boost::lexical_cast<int32_t>(args.argv[1]);
    if (restore_app_id <= 0) {
        fprintf(stderr, "invalid restore_app_id(%d)\n", restore_app_id);
        return false;
    }
    static struct option long_options[] = {{"detailed", no_argument, 0, 'd'}, {0, 0, 0, 0}};
    optind = 0;
    bool detailed = false;
    while (true) {
        int option_index = 0;
        int c;
        c = getopt_long(args.argc, args.argv, "d", long_options, &option_index);
        if (c == -1)
            break;
        switch (c) {
        case 'd':
            detailed = true;
            break;
        default:
            return false;
        }
    }
    ::dsn::error_code ret = sc->ddl_client->query_restore(restore_app_id, detailed);

    if (ret != ::dsn::ERR_OK) {
        fprintf(stderr,
                "query restore status failed, restore_app_id(%d), err = %s\n",
                restore_app_id,
                ret.to_string());
    }
    return true;
}
