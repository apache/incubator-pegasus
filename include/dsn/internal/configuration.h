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
# pragma once

# include <memory>
# include <vector>
# include <map>
# include <cstdlib>
# include <cstring>
# include <string>
# include <list>


namespace dsn {

class configuration;
typedef std::shared_ptr<configuration> configuration_ptr;
typedef void (*config_file_change_notifier)(configuration_ptr);

class configuration
{
public:
    template <typename T> static configuration* create(const char* file_name)
    {
        return new T(file_name);
    }

public:
    configuration(const char* file_name);

    ~configuration(void);

    void get_all_sections(std::vector<std::string>& sections);

    void get_all_keys(const char* section, std::vector<std::string>& keys);

    std::string get_string_value(const char* section, const char* key, const char* default_value);

    std::list<std::string> get_string_value_list(const char* section, const char* key, char splitter);

    void register_config_change_notification(config_file_change_notifier notifier);

    bool has_section(const char* section, bool warn_if_not = true);

    bool has_key(const char* section, const char* key);

    const char* get_file_name() const { return _file_name.c_str(); }

    // ---------------------- commmon routines ----------------------------------
    
    template<typename T> T get_value(const char* section, const char* key, T default_value);

private:
    bool get_string_value_internal(const char* section, const char* key, const char* default_value, std::string& ov);

private:
    struct conf
    {
        const char* section;
        const char* key;
        const char* value;
        int         line;
    };

    typedef std::map<std::string, std::map<std::string, conf>> config_map;
    std::string                             _file_name;
    config_map                              _configs;
    std::list<config_file_change_notifier>  _notifiers;
    std::shared_ptr<char>                   _file_data;
};

template<> inline double configuration::get_value<double>(const char* section, const char* key, double default_value)
{
    std::string value;
    if (!get_string_value_internal(section, key, "", value))
    {
        printf("WARNING: configuration '[%s] %s' is not defined, default value is '%lf'\n",
            section,
            key,
            default_value
            );

        return default_value;
    }   
    else
    {
        return atof(value.c_str());
    }
}


template<> inline long long configuration::get_value<long long>(const char* section, const char* key, long long default_value)
{
    std::string value;
    if (!get_string_value_internal(section, key, "", value))
    {
        printf("WARNING: configuration '[%s] %s' is not defined, default value is '%lld'\n",
            section,
            key,
            default_value
            );

        return default_value;
    }
    else
    {
        if (value.length() > 2 && (value.substr(0, 2) == "0x" || value.substr(0, 2) == "0X"))
        {
            long long unsigned int v;
            sscanf(value.c_str(), "0x%llx", &v);
            return v;
        }
        else
            return (long long)(atol(value.c_str()));
    }
}

template<> inline long configuration::get_value<long>(const char* section, const char* key, long default_value)
{
    std::string value;
    if (!get_string_value_internal(section, key, "", value))
    {
        printf("WARNING: configuration '[%s] %s' is not defined, default value is '%ld'\n",
            section,
            key,
            default_value
            );

        return default_value;
    }
    else
    {
        if (value.length() > 2 && (value.substr(0, 2) == "0x" || value.substr(0, 2) == "0X"))
        {
            int v;
            sscanf(value.c_str(), "0x%x", &v);
            return v;
        }
        else
            return (long)(atoi(value.c_str()));
    }
}

template<> inline int configuration::get_value<int>(const char* section, const char* key, int default_value)
{
    return static_cast<int>(get_value<long>(section, key, default_value));
}

template<> inline unsigned int configuration::get_value<unsigned int>(const char* section, const char* key, unsigned int default_value)
{
    return (unsigned int)(get_value<long long>(section, key, default_value));
}

template<> inline short configuration::get_value<short>(const char* section, const char* key, short default_value)
{
    return (short)(get_value<long>(section, key, default_value));
}

template<> inline unsigned short configuration::get_value<unsigned short>(const char* section, const char* key, unsigned short default_value)
{
    return (unsigned short)(get_value<long long>(section, key, default_value));
}

template<> inline bool configuration::get_value<bool>(const char* section, const char* key, bool default_value)
{
    std::string value;
    if (!get_string_value_internal(section, key, "", value))
    {
        printf("WARNING: configuration '[%s] %s' is not defined, default value is '%s'\n",
            section,
            key,
            default_value ? "true" : "false"
            );

        return default_value;
    }
    else if (strcmp(value.c_str(), "true") == 0 || strcmp(value.c_str(), "TRUE") == 0)
    {
        return true;
    }
    else
    {
        return false;
    }
}


} // end namespace
