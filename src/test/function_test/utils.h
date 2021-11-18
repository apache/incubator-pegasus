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

#include <dsn/utility/rand.h>

#define RETRY_OPERATION(CLIENT_FUNCTION, RESULT)                                                   \
    do {                                                                                           \
        for (int i = 0; i < 60; ++i) {                                                             \
            RESULT = CLIENT_FUNCTION;                                                              \
            if (RESULT == 0) {                                                                     \
                break;                                                                             \
            } else {                                                                               \
                std::this_thread::sleep_for(std::chrono::milliseconds(500));                       \
            }                                                                                      \
        }                                                                                          \
    } while (0)

inline std::string
generate_hash_key_by_random(bool is_hotkey, int probability = 100, uint32_t str_len = 20)
{
    if (is_hotkey && (dsn::rand::next_u32(100) < probability)) {
        return "ThisisahotkeyThisisahotkey";
    }
    static const std::string chars("abcdefghijklmnopqrstuvwxyz"
                                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
                                   "1234567890"
                                   "!@#$%^&*()"
                                   "`~-_=+[{]{\\|;:'\",<.>/? ");
    std::string result;
    for (int i = 0; i < str_len; i++) {
        result += chars[dsn::rand::next_u32(chars.size())];
    }
    return result;
}

inline std::vector<std::string>
generate_str_vector_by_random(uint32_t length, uint32_t arr_len, uint32_t single_str_len)
{
    std::vector<std::string> result;
    result.reserve(arr_len);
    for (int i = 0; i < arr_len; i++) {
        result.emplace_back(generate_hash_key_by_random(false,100,single_str_len));
    }
    return result;
}