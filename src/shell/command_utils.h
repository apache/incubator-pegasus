// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <map>

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
