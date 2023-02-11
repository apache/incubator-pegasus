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

#include <assert.h>
#include <inttypes.h>
#include <cstdio>
#include <cstdlib>
#include <iosfwd>
#include <list>
#include <map>
#include <mutex>
#include <string>
#include <vector>

#include "string_conv.h"

namespace dsn {

class configuration
{
public:
    configuration();

    ~configuration();

    // arguments: k1=v1;k2=v2;k3=v3; ...
    // e.g.,
    //    port = %port%
    //    timeout = %timeout%
    // arguments: port=23466;timeout=1000
    bool load(const char *file_name, const char *arguments = nullptr);

    void get_all_sections(std::vector<std::string> &sections);

    void get_all_section_ptrs(std::vector<const char *> &sections);

    void get_all_keys(const char *section, std::vector<const char *> &keys);

    const char *get_string_value(const char *section,
                                 const char *key,
                                 const char *default_value,
                                 const char *dsptr);

    std::list<std::string>
    get_string_value_list(const char *section, const char *key, char splitter, const char *dsptr);

    void set(const char *section, const char *key, const char *value, const char *dsptr);

    bool has_section(const char *section);

    bool has_key(const char *section, const char *key);

    const char *get_file_name() const { return _file_name.c_str(); }

    bool set_warning(bool warn)
    {
        bool old = _warning;
        _warning = warn;
        return old;
    }

    void dump(std::ostream &os);

    // ---------------------- commmon routines ----------------------------------

    template <typename T>
    T get_value(const char *section, const char *key, T default_value, const char *dsptr);

private:
    bool get_string_value_internal(const char *section,
                                   const char *key,
                                   const char *default_value,
                                   const char **ov,
                                   const char *dsptr);

private:
    struct conf
    {
        std::string section;
        std::string key;
        std::string value;
        int line;

        bool present;
        std::string dsptr;
    };

    typedef std::map<std::string, std::map<std::string, conf *>> config_map;
    std::mutex _lock;
    config_map _configs;

    std::string _file_name;
    std::string _file_data;
    bool _warning;
};

template <>
inline std::string configuration::get_value<std::string>(const char *section,
                                                         const char *key,
                                                         std::string default_value,
                                                         const char *dsptr)
{
    return get_string_value(section, key, default_value.c_str(), dsptr);
}

template <>
inline double configuration::get_value<double>(const char *section,
                                               const char *key,
                                               double default_value,
                                               const char *dsptr)
{
    const char *value;
    char defaultstr[32];
    sprintf(defaultstr, "%lf", default_value);

    if (!get_string_value_internal(section, key, defaultstr, &value, dsptr)) {
        if (_warning) {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%lf'\n",
                   section,
                   key,
                   default_value);
        }

        return default_value;
    } else {
        return atof(value);
    }
}

template <>
inline int64_t configuration::get_value<int64_t>(const char *section,
                                                 const char *key,
                                                 int64_t default_value,
                                                 const char *dsptr)
{
    const char *value;
    char defaultstr[32];
    sprintf(defaultstr, "%" PRId64, default_value);

    if (!get_string_value_internal(section, key, defaultstr, &value, dsptr)) {
        if (_warning) {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%" PRId64
                   "'\n",
                   section,
                   key,
                   default_value);
        }

        return default_value;
    } else {
        int64_t result = default_value;
        bool suc = dsn::buf2int64(value, result);
        assert(suc || result == default_value);
        return result;
    }
}

template <>
inline uint64_t configuration::get_value<uint64_t>(const char *section,
                                                   const char *key,
                                                   uint64_t default_value,
                                                   const char *dsptr)
{
    const char *value;
    char defaultstr[32];
    sprintf(defaultstr, "%" PRIu64, default_value);

    if (!get_string_value_internal(section, key, defaultstr, &value, dsptr)) {
        if (_warning) {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%" PRIu64
                   "'\n",
                   section,
                   key,
                   default_value);
        }

        return default_value;
    } else {
        uint64_t result = default_value;
        bool suc = dsn::buf2uint64(value, result);
        assert(suc || result == default_value);
        return result;
    }
}

template <>
inline bool configuration::get_value<bool>(const char *section,
                                           const char *key,
                                           bool default_value,
                                           const char *dsptr)
{
    const char *value;
    const char *defaultstr = (default_value ? "true" : "false");

    if (!get_string_value_internal(section, key, defaultstr, &value, dsptr)) {
        if (_warning) {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%s'\n",
                   section,
                   key,
                   default_value ? "true" : "false");
        }
        return default_value;
    } else {
        bool result = default_value;
        bool suc = dsn::buf2bool(value, result);
        assert(suc || result == default_value);
        return result;
    }
}

} // namespace dsn
