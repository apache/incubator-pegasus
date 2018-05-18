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

#include <memory>
#include <vector>
#include <map>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <list>
#include <mutex>

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
inline long long configuration::get_value<long long>(const char *section,
                                                     const char *key,
                                                     long long default_value,
                                                     const char *dsptr)
{
    const char *value;
    char defaultstr[32];
    sprintf(defaultstr, "%lld", default_value);

    if (!get_string_value_internal(section, key, defaultstr, &value, dsptr)) {
        if (_warning) {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%lld'\n",
                   section,
                   key,
                   default_value);
        }

        return default_value;
    } else {
        if (strlen(value) > 2 && (value[0] == '0' && (value[1] == 'x' || value[1] == 'X'))) {
            long long unsigned int v;
            sscanf(value, "0x%llx", &v);
            return v;
        } else
            return atoll(value);
    }
}

template <>
inline long configuration::get_value<long>(const char *section,
                                           const char *key,
                                           long default_value,
                                           const char *dsptr)
{
    const char *value;
    char defaultstr[32];
    sprintf(defaultstr, "%d", (int)default_value);

    if (!get_string_value_internal(section, key, defaultstr, &value, dsptr)) {
        if (_warning) {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%ld'\n",
                   section,
                   key,
                   default_value);
        }
        return default_value;
    } else {
        if (strlen(value) > 2 && (value[0] == '0' && (value[1] == 'x' || value[1] == 'X'))) {
            long v;
            sscanf(value, "0x%lx", &v);
            return v;
        } else
            return (long)(atoi(value));
    }
}

template <>
inline unsigned long long configuration::get_value<unsigned long long>(
    const char *section, const char *key, unsigned long long default_value, const char *dsptr)
{
    return (unsigned long long)(get_value<long long>(section, key, default_value, dsptr));
}

template <>
inline unsigned long configuration::get_value<unsigned long>(const char *section,
                                                             const char *key,
                                                             unsigned long default_value,
                                                             const char *dsptr)
{
    return (unsigned long)(get_value<long>(section, key, default_value, dsptr));
}

template <>
inline int configuration::get_value<int>(const char *section,
                                         const char *key,
                                         int default_value,
                                         const char *dsptr)
{
    return static_cast<int>(get_value<long>(section, key, default_value, dsptr));
}

template <>
inline unsigned int configuration::get_value<unsigned int>(const char *section,
                                                           const char *key,
                                                           unsigned int default_value,
                                                           const char *dsptr)
{
    return (unsigned int)(get_value<long long>(section, key, default_value, dsptr));
}

template <>
inline short configuration::get_value<short>(const char *section,
                                             const char *key,
                                             short default_value,
                                             const char *dsptr)
{
    return (short)(get_value<long>(section, key, default_value, dsptr));
}

template <>
inline unsigned short configuration::get_value<unsigned short>(const char *section,
                                                               const char *key,
                                                               unsigned short default_value,
                                                               const char *dsptr)
{
    return (unsigned short)(get_value<long long>(section, key, default_value, dsptr));
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
    } else if (strcmp(value, "true") == 0 || strcmp(value, "TRUE") == 0) {
        return true;
    } else {
        return false;
    }
}

} // end namespace
