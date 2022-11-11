/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <iostream>
#include <list>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>

namespace dsn {
namespace utils {

inline bool is_empty(const char *str) { return str == nullptr || *str == '\0'; }

// Split the `input` string by the only character `separator` into tokens. Leading and trailing
// spaces of each token will be stripped. Once the token is empty, or become empty after
// stripping, an empty string will be added into `output` if `keep_place_holder` is enabled.
//
// There are several overloaded `split_args` functions in the following all of which are the
// same except that the split tokens are collected to the different containers.
void split_args(const char *input,
                std::vector<std::string> &output,
                char separator = ' ',
                bool keep_place_holder = false);

void split_args(const char *input,
                std::list<std::string> &output,
                char separator = ' ',
                bool keep_place_holder = false);

void split_args(const char *input,
                std::unordered_set<std::string> &output,
                char separator = ' ',
                bool keep_place_holder = false);

// kv_map sample (when item_splitter = ',' and kv_splitter = ':'):
//   k1:v1,k2:v2,k3:v3
// we say that 'k1:v1' is an item.
// return false if:
//   - bad format: no kv_splitter found in any non-empty item
//   - allow_dup_key = false and the same key appears for more than once
// if allow_dup_key = true and the same key appears for more than once,
// the last value will be returned.
bool parse_kv_map(const char *args,
                  /*out*/ std::map<std::string, std::string> &kv_map,
                  char item_splitter,
                  char kv_splitter,
                  bool allow_dup_key = false);

// format sample (when item_splitter = ',' and kv_splitter = ':'):
//   k1:v1,k2:v2,k3:v3
void kv_map_to_stream(const std::map<std::string, std::string> &kv_map,
                      /*out*/ std::ostream &oss,
                      char item_splitter,
                      char kv_splitter);
std::string kv_map_to_string(const std::map<std::string, std::string> &kv_map,
                             char item_splitter,
                             char kv_splitter);

std::string
replace_string(std::string subject, const std::string &search, const std::string &replace);

std::string get_last_component(const std::string &input, const char splitters[]);

char *trim_string(char *s);

// calculate the md5 checksum of buffer
std::string string_md5(const char *buffer, unsigned int length);
} // namespace utils
} // namespace dsn
