// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <map>
#include <string>
#include <set>

#include "shell/argh.h"
#include <dsn/dist/fmt_logging.h>
#include "command_executor.h"

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
