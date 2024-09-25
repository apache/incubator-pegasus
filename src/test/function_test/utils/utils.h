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

#include <sstream>

#include "utils/api_utilities.h"
#include "utils/fmt_logging.h"
#include "utils/rand.h"
#include "utils/strings.h"
#include "utils/process_utils.h"

#define RETRY_OPERATION(CLIENT_FUNCTION, RESULT)                                                   \
    do {                                                                                           \
        for (int i = 0; i < 60; ++i) {                                                             \
            RESULT = CLIENT_FUNCTION;                                                              \
            if (RESULT == PERR_OK) {                                                               \
                break;                                                                             \
            } else {                                                                               \
                std::this_thread::sleep_for(std::chrono::milliseconds(500));                       \
            }                                                                                      \
        }                                                                                          \
    } while (0)

inline std::string generate_random_string(uint32_t str_len = 20)
{
    static const std::string chars("abcdefghijklmnopqrstuvwxyz"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "1234567890");
    std::string result;
    for (int i = 0; i < str_len; i++) {
        result += chars[dsn::rand::next_u32(chars.size())];
    }
    return result;
}

inline std::string generate_hotkey(bool is_hotkey, int probability = 100, uint32_t str_len = 20)
{
    if (is_hotkey && (dsn::rand::next_u32(100) < probability)) {
        return "ThisisahotkeyThisisahotkey";
    }
    return generate_random_string(str_len);
}

inline std::vector<std::string> generate_str_vector_by_random(uint32_t single_str_len,
                                                              uint32_t arr_len,
                                                              bool random_value_size = false)
{
    std::vector<std::string> result;
    result.reserve(arr_len);
    for (int i = 0; i < arr_len; i++) {
        result.emplace_back(generate_random_string(
            random_value_size ? dsn::rand::next_u32(single_str_len) : single_str_len));
    }
    return result;
}

inline std::map<std::string, std::string>
generate_sortkey_value_map(const std::vector<std::string> sortkeys,
                           const std::vector<std::string> values)
{
    std::map<std::string, std::string> result;
    CHECK_EQ(sortkeys.size(), values.size());
    int len = sortkeys.size();
    for (int i = 0; i < len; i++) {
        result.emplace(std::make_pair(sortkeys[i], values[i]));
    }
    return result;
}

inline void
check_and_put(std::map<std::string, std::map<std::string, std::pair<std::string, uint32_t>>> &data,
              const std::string &hash_key,
              const std::string &sort_key,
              const std::string &value,
              uint32_t expire_ts_seconds)
{
    auto it1 = data.find(hash_key);
    if (it1 != data.end()) {
        auto it2 = it1->second.find(sort_key);
        ASSERT_EQ(it1->second.end(), it2)
            << "Duplicate: hash_key=" << hash_key << ", sort_key=" << sort_key
            << ", old_value=" << it2->second.first << ", new_value=" << value
            << ", old_expire_ts_seconds=" << it2->second.second
            << ", new_expire_ts_seconds=" << expire_ts_seconds;
    }
    data[hash_key][sort_key] = std::pair<std::string, uint32_t>(value, expire_ts_seconds);
}

inline void check_and_put(std::map<std::string, std::map<std::string, std::string>> &data,
                          const std::string &hash_key,
                          const std::string &sort_key,
                          const std::string &value)
{
    auto it1 = data.find(hash_key);
    if (it1 != data.end()) {
        auto it2 = it1->second.find(sort_key);
        ASSERT_EQ(it1->second.end(), it2)
            << "Duplicate: hash_key=" << hash_key << ", sort_key=" << sort_key
            << ", old_value=" << it2->second << ", new_value=" << value;
    }
    data[hash_key][sort_key] = value;
}

inline void check_and_put(std::map<std::string, std::string> &data,
                          const std::string &hash_key,
                          const std::string &sort_key,
                          const std::string &value)
{
    auto it1 = data.find(sort_key);
    ASSERT_EQ(data.end(), it1) << "Duplicate: hash_key=" << hash_key << ", sort_key=" << sort_key
                               << ", old_value=" << it1->second << ", new_value=" << value;
    data[sort_key] = value;
}

inline void compare(const std::pair<std::string, uint32_t> &expect,
                    const std::pair<std::string, uint32_t> &actual,
                    const std::string &hash_key,
                    const std::string sort_key)
{
    ASSERT_EQ(expect.first, actual.first)
        << "Diff value: hash_key=" << hash_key << ", sort_key=" << sort_key
        << ", expected_value=" << expect.first << ", expected_expire_ts_seconds=" << expect.second
        << ", actual_value=" << actual.first << ", actual_expire_ts_seconds=" << actual.second;

    ASSERT_TRUE(expect.second >= actual.second && expect.second - actual.second <= 1)
        << "Diff expire_ts_seconds: hash_key=" << hash_key << ", sort_key=" << sort_key
        << ", expected_value=" << expect.first << ", expected_expire_ts_seconds=" << expect.second
        << ", actual_value=" << actual.first << ", actual_expire_ts_seconds=" << actual.second;
}

inline void compare(const std::map<std::string, std::pair<std::string, uint32_t>> &expect,
                    const std::map<std::string, std::pair<std::string, uint32_t>> &actual,
                    const std::string &hash_key)
{
    for (auto it1 = actual.begin(), it2 = expect.begin();; ++it1, ++it2) {
        if (it1 == actual.end()) {
            ASSERT_EQ(expect.end(), it2)
                << "Only in expect: hash_key=" << hash_key << ", sort_key=" << it2->first
                << ", value=" << it2->second.first << ", expire_ts_seconds=" << it2->second.second;
            break;
        }
        ASSERT_NE(expect.end(), it2)
            << "Only in actual: hash_key=" << hash_key << ", sort_key=" << it1->first
            << ", value=" << it1->second.first << ", expire_ts_seconds=" << it1->second.second;
        ASSERT_EQ(it1->first, it2->first)
            << "Diff sort_key: hash_key=" << hash_key << ", actual_sort_key=" << it1->first
            << ", actual_value=" << it1->second.first
            << ", actual_expire_ts_seconds=" << it1->second.second
            << ", expected_sort_key=" << it2->first << ", expected_value=" << it2->second.first
            << ", expected_expire_ts_seconds=" << it2->second.second;
        ASSERT_NO_FATAL_FAILURE(compare(it1->second, it2->second, hash_key, it1->first));
    }
}

inline void compare(const std::map<std::string, std::string> &expect,
                    const std::map<std::string, std::string> &actual,
                    const std::string &hash_key)
{
    for (auto it1 = actual.begin(), it2 = expect.begin();; ++it1, ++it2) {
        if (it1 == actual.end()) {
            ASSERT_EQ(expect.end(), it2)
                << "Only in expect: hash_key=" << hash_key << ", sort_key=" << it2->first
                << ", value=" << it2->second;
            break;
        }
        ASSERT_NE(expect.end(), it2) << "Only in actual: hash_key=" << hash_key
                                     << ", sort_key=" << it1->first << ", value=" << it1->second;
        ASSERT_EQ(*it1, *it2) << "Diff: hash_key=" << hash_key << ", actual_sort_key=" << it1->first
                              << ", actual_value=" << it1->second
                              << ", expected_sort_key=" << it2->first
                              << ", expected_value=" << it2->second;
    }
}

template <typename T, typename U>
inline void compare(const T &expect, const U &actual)
{
    ASSERT_EQ(expect.size(), actual.size());
    for (auto it1 = actual.begin(), it2 = expect.begin();; ++it1, ++it2) {
        if (it1 == actual.end()) {
            ASSERT_EQ(expect.end(), it2) << "Only in expect: hash_key=" << it2->first;
            break;
        }
        ASSERT_NE(expect.end(), it2) << "Only in actual: hash_key=" << it1->first;
        ASSERT_EQ(it1->first, it2->first)
            << "Diff: actual_hash_key=" << it1->first << ", expected_hash_key=" << it2->first;
        ASSERT_NO_FATAL_FAILURE(compare(it1->second, it2->second, it1->first));
    }
}

inline int run_cmd(const std::string &cmd, std::string *output = nullptr)
{
    std::stringstream ss;
    int ret = dsn::utils::pipe_execute(cmd.c_str(), ss);
    if (output) {
        *output = dsn::utils::trim_string((char *)ss.str().c_str());
    }
    return ret;
}

inline void run_cmd_no_error(const std::string &cmd, std::string *output = nullptr)
{
    std::string tmp;
    int ret = run_cmd(cmd, &tmp);
    ASSERT_TRUE(ret == 0 || ret == 256) << "ret: " << ret << std::endl
                                        << "cmd: " << cmd << std::endl
                                        << "output: " << tmp;
    if (output) {
        *output = tmp;
    }
}
