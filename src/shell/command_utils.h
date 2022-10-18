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

#pragma once

#include <map>
#include <string>
#include <set>

#include "shell/argh.h"
#include "utils/fmt_logging.h"

namespace dsn {
class rpc_address;
}
struct shell_context;

inline bool validate_cmd(const argh::parser &cmd,
                         const std::set<std::string> &params,
                         const std::set<std::string> &flags)
{
    if (cmd.size() > 1) {
        fmt::print(stderr, "too many params!\n");
        return false;
    }

    for (const auto &param : cmd.params()) {
        if (params.find(param.first) == params.end()) {
            fmt::print(stderr, "unknown param {} = {}\n", param.first, param.second);
            return false;
        }
    }

    for (const auto &flag : cmd.flags()) {
        if (params.find(flag) != params.end()) {
            fmt::print(stderr, "missing value of {}\n", flag);
            return false;
        }

        if (flags.find(flag) == flags.end()) {
            fmt::print(stderr, "unknown flag {}\n", flag);
            return false;
        }
    }

    return true;
}

bool validate_ip(shell_context *sc,
                 const std::string &ip_str,
                 /*out*/ dsn::rpc_address &target_address,
                 /*out*/ std::string &err_info);

#define verify_logged(exp, ...)                                                                    \
    do {                                                                                           \
        if (!(exp)) {                                                                              \
            fprintf(stderr, __VA_ARGS__);                                                          \
            return false;                                                                          \
        }                                                                                          \
    } while (0)

inline int strcmp_ignore_case(const char *left, const char *right)
{
    int pos = 0;
    while (left[pos] && right[pos]) {
        char left_c = ::tolower(left[pos]);
        char right_c = ::tolower(right[pos]);
        if (left_c < right_c)
            return -1;
        if (left_c > right_c)
            return 1;
        ++pos;
    }
    if (left[pos]) {
        return 1;
    }
    return right[pos] ? -1 : 0;
}

template <typename EnumType>
EnumType type_from_string(const std::map<int, const char *> &type_maps,
                          const std::string &type_string,
                          const EnumType &default_type)
{
    for (auto it = type_maps.begin(); it != type_maps.end(); it++) {
        if (strcmp_ignore_case(type_string.c_str(), it->second) == 0) {
            return (EnumType)it->first;
        }
    }
    return default_type;
}

bool confirm_unsafe_command(const std::string &action);
