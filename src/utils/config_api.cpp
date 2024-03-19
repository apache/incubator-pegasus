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

#include "utils/config_api.h"

#include <algorithm>

#include "utils/configuration.h"

dsn::configuration g_config;

bool dsn_config_load(const char *file, const char *arguments)
{
    return g_config.load(file, arguments);
}

void dsn_config_dump(std::ostream &os) { g_config.dump(os); }

const char *dsn_config_get_value_string(const char *section,
                                        const char *key,
                                        const char *default_value,
                                        const char *dsptr)
{
    return g_config.get_string_value(section, key, default_value, dsptr);
}

bool dsn_config_get_value_bool(const char *section,
                               const char *key,
                               bool default_value,
                               const char *dsptr)
{
    return g_config.get_value<bool>(section, key, default_value, dsptr);
}

uint64_t dsn_config_get_value_uint64(const char *section,
                                     const char *key,
                                     uint64_t default_value,
                                     const char *dsptr)
{
    return g_config.get_value<uint64_t>(section, key, default_value, dsptr);
}

int64_t dsn_config_get_value_int64(const char *section,
                                   const char *key,
                                   int64_t default_value,
                                   const char *dsptr)
{
    return g_config.get_value<int64_t>(section, key, default_value, dsptr);
}

double dsn_config_get_value_double(const char *section,
                                   const char *key,
                                   double default_value,
                                   const char *dsptr)
{
    return g_config.get_value<double>(section, key, default_value, dsptr);
}

void dsn_config_get_all_sections(/*out*/ std::vector<std::string> &sections)
{
    g_config.get_all_sections(sections);
}

void dsn_config_get_all_keys(const char *section, std::vector<std::string> &keys)
{
    std::vector<const char *> key_ptrs;
    g_config.get_all_keys(section, key_ptrs);
    for (const char *p : key_ptrs)
        keys.emplace_back(std::string(p));
}

void dsn_config_set(const char *section, const char *key, const char *value, const char *dsptr)
{
    g_config.set(section, key, value, dsptr);
}
