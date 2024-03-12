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

#include <cstdint>
#include <iosfwd>
#include <string>
#include <vector>

/// load a ini configuration file, and replace specific strings in file with arguments.
///
/// the rules of replacement is as follows:
///     1. arguments are with format of k1=v1,k2=v2,k3=v3
///     2. if a string if file is %k1%, it will be replaced with v1.
///        %k2%, %k3% will be replaced to v2 and v3 in a similar fashion
///
/// for example:
///
/// the file content:
/// "
/// [example_section]
/// %replace_key% = real_value
/// real_port = %host_port%
/// "
///
/// and the arguments "replace_key=key,host_port=8080
///
/// so the loaded config will be:
/// "
/// [example_section]
/// key = real_value
/// real_port = 8080
///
/// return true if load config file succeed, otherwise false
///
/// please call this function at the beginning of your process, otherwise non of the
/// dsn_config_xxx function works
///
/// the function is not thread safe.
bool dsn_config_load(const char *file, const char *arguments);

/// dump the global configuration
void dsn_config_dump(std::ostream &os);

/// the section format:
/// [section]
/// key = value
///
/// this function get the value of a key in some section.
/// if <key, value> is not present in section, then return default_value.
/// dsptr is the description of the key-value pairs.
///
/// this function is not thread safe if get/set are concurrently called
const char *dsn_config_get_value_string(const char *section,
                                        const char *key,
                                        const char *default_value,
                                        const char *dsptr);

/// get value of a key in some section, the value should be "true" or "false"
/// this function is not thread safe if dsn_config_set is concurrently called
bool dsn_config_get_value_bool(const char *section,
                               const char *key,
                               bool default_value,
                               const char *dsptr);

/// get value of a key in some section, the value should be decimal in range of uint64_t
/// this function is not thread safe if dsn_config_set is concurrently called
uint64_t dsn_config_get_value_uint64(const char *section,
                                     const char *key,
                                     uint64_t default_value,
                                     const char *dsptr);

/// get value of a key in some section, the value should be decimal in range of int64_t
/// this function is not thread safe if dsn_config_set is concurrently called
int64_t dsn_config_get_value_int64(const char *section,
                                   const char *key,
                                   int64_t default_value,
                                   const char *dsptr);

/// get value of a key in some section, the value should be decimal in range of double
/// this function is not thread safe if dsn_config_set is concurrently called
double dsn_config_get_value_double(const char *section,
                                   const char *key,
                                   double default_value,
                                   const char *dsptr);

/// get the names of all sections
/// this function is not thread safe if dsn_config_set is concurrently called
void dsn_config_get_all_sections(/*out*/ std::vector<std::string> &sections);

/// get all keys in some specific section
/// this function is not thread safe if dsn_config_set is concurrently called
void dsn_config_get_all_keys(const char *section, /*out*/ std::vector<std::string> &keys);

/// set value for a key of some section.
/// if the section doesn't exsit, a new one will be created.
/// if the key doesn't exist, a new one will be created
///
/// multiple concurrent set are thread safe.
///
/// any of dsn_config_get_xxx may corrupt if called concurrently with dsn_config_set
void dsn_config_set(const char *section, const char *key, const char *value, const char *dsptr);
