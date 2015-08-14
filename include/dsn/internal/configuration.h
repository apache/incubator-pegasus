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
# include <cstdio>
# include <cstdlib>
# include <cstring>
# include <string>
# include <list>
# include <mutex>


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
    // arguments: k1=v1;k2=v2;k3=v3; ...
    // e.g.,
    //    port = %port%
    //    timeout = %timeout%
    // arguments: port=23466;timeout=1000
    configuration(const char* file_name, const char* arguments = nullptr);

    ~configuration(void);

    void get_all_sections(std::vector<std::string>& sections);

    void get_all_keys(const char* section, std::vector<const char*>& keys);

    const char* get_string_value(const char* section, const char* key, const char* default_value, const char* dsptr);

    std::list<std::string> get_string_value_list(const char* section, const char* key, char splitter, const char* dsptr);

    void register_config_change_notification(config_file_change_notifier notifier);

    bool has_section(const char* section);

    bool has_key(const char* section, const char* key);

    const char* get_file_name() const { return _file_name.c_str(); }

    bool set_warning(bool warn) { bool old = _warning; _warning = warn; return old; }

    void dump(std::ostream& os);

    // ---------------------- commmon routines ----------------------------------
    
    template<typename T> T get_value(const char* section, const char* key, T default_value, const char* dsptr);

private:
    bool get_string_value_internal(const char* section, const char* key, const char* default_value, const char** ov, const char* dsptr);

private:
    struct conf
    {
        std::string section;
        std::string key;
        std::string value;
        int         line;

        bool        present;
        std::string dsptr;
    };

    typedef std::map<std::string, std::map<std::string, conf*>> config_map;    
    std::mutex                              _lock;
    config_map                              _configs;
    
    std::string                             _file_name;
    std::list<config_file_change_notifier>  _notifiers;
    std::string                             _file_data;
    bool                                    _warning;    
};

template<> inline std::string configuration::get_value<std::string>(const char* section, const char* key, std::string default_value, const char* dsptr)
{
    return get_string_value(section, key, default_value.c_str(), dsptr);
}

template<> inline double configuration::get_value<double>(const char* section, const char* key, double default_value, const char* dsptr)
{
    const char* value;
    char defaultstr[32];
    sprintf(defaultstr, "%lf", default_value);

    if (!get_string_value_internal(section, key, defaultstr, &value, dsptr))
    {
        if (_warning)
        {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%lf'\n",
                section,
                key,
                default_value
                );
        }
        
        return default_value;
    }   
    else
    {
        return atof(value);
    }
}


template<> inline long long configuration::get_value<long long>(const char* section, const char* key, long long default_value, const char* dsptr)
{
    const char* value;
    char defaultstr[32];
    sprintf(defaultstr, "%lld", default_value);

    if (!get_string_value_internal(section, key, defaultstr, &value, dsptr))
    {
        if (_warning)
        {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%lld'\n",
                section,
                key,
                default_value
                );
        }

        return default_value;
    }
    else
    {
        if (strlen(value) > 2 && (value[0] == '0' && (value[1] == 'x' || value[1] == 'X')))
        {
            long long unsigned int v;
            sscanf(value, "0x%llx", &v);
            return v;
        }
        else
            return atoll(value);
    }
}

template<> inline long configuration::get_value<long>(const char* section, const char* key, long default_value, const char* dsptr)
{
    const char* value;
    char defaultstr[32];
    sprintf(defaultstr, "%d", (int)default_value);

    if (!get_string_value_internal(section, key, defaultstr, &value, dsptr))
    {
        if (_warning)
        {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%ld'\n",
                section,
                key,
                default_value
                );
        }
        return default_value;
    }
    else
    {
        if (strlen(value) > 2 && (value[0] == '0' && (value[1] == 'x' || value[1] == 'X')))
        {
            int v;
            sscanf(value, "0x%x", &v);
            return v;
        }
        else
            return (long)(atoi(value));
    }
}

template<> inline unsigned long long configuration::get_value<unsigned long long>(const char* section, const char* key, unsigned long long default_value, const char* dsptr)
{
    return (unsigned long long)(get_value<long long>(section, key, default_value, dsptr));
}


template<> inline unsigned long configuration::get_value<unsigned long>(const char* section, const char* key, unsigned long default_value, const char* dsptr)
{
    return (unsigned long)(get_value<long>(section, key, default_value, dsptr));
}

template<> inline int configuration::get_value<int>(const char* section, const char* key, int default_value, const char* dsptr)
{
    return static_cast<int>(get_value<long>(section, key, default_value, dsptr));
}

template<> inline unsigned int configuration::get_value<unsigned int>(const char* section, const char* key, unsigned int default_value, const char* dsptr)
{
    return (unsigned int)(get_value<long long>(section, key, default_value, dsptr));
}

template<> inline short configuration::get_value<short>(const char* section, const char* key, short default_value, const char* dsptr)
{
    return (short)(get_value<long>(section, key, default_value, dsptr));
}

template<> inline unsigned short configuration::get_value<unsigned short>(const char* section, const char* key, unsigned short default_value, const char* dsptr)
{
    return (unsigned short)(get_value<long long>(section, key, default_value, dsptr));
}

template<> inline bool configuration::get_value<bool>(const char* section, const char* key, bool default_value, const char* dsptr)
{
    const char* value;
    const char* defaultstr = (default_value ? "true" : "false");

    if (!get_string_value_internal(section, key, defaultstr, &value, dsptr))
    {
        if (_warning)
        {
            printf("WARNING: configuration '[%s] %s' is not defined, default value is '%s'\n",
                section,
                key,
                default_value ? "true" : "false"
                );
        }
        return default_value;
    }
    else if (strcmp(value, "true") == 0 || strcmp(value, "TRUE") == 0)
    {
        return true;
    }
    else
    {
        return false;
    }
}


# define CONFIG_BEGIN(t_struct) \
    inline bool read_config(\
    const char* section, \
    __out_param t_struct& val, \
    t_struct* default_value = nullptr\
    ) \
{

# define CONFIG_END \
    return true; \
}

// type fld = xyz
# define CONFIG_FLD(real_type, config_type, fld, default_fld_value, dsptr) \
    val.fld = (real_type)dsn_config_get_value_##config_type(section, #fld, default_value ? default_value->fld : default_fld_value, dsptr);

# define CONFIG_FLD_STRING(fld, default_fld_value, dsptr) \
    val.fld = dsn_config_get_value_string(section, #fld, (val.fld.length() > 0 && val.fld != std::string(default_fld_value)) ? \
         val.fld.c_str() : (default_value ? default_value->fld.c_str() : default_fld_value), dsptr);

// customized_id<type> fld = xyz
# define CONFIG_FLD_ID(type, fld, default_fld_value, defined_before_read_config, dsptr) \
{\
    std::string v = dsn_config_get_value_string(section, #fld, "", dsptr); \
    if (v == "") {    \
        if (!defined_before_read_config){\
                if (default_value) val.fld = default_value->fld; \
                    else val.fld = default_fld_value; \
        }\
    }\
    else {\
    if (!type::is_exist(v.c_str())) {\
        printf("invalid enum configuration '[%s] %s'", section, #fld); \
        return false; \
            }\
            else \
        val.fld = type(v.c_str()); \
    }\
}

// enum type fld = xyz
# define CONFIG_FLD_ENUM(type, fld, default_fld_value, invalid_enum, defined_before_read_config, dsptr) \
{\
    std::string v = dsn_config_get_value_string(section, #fld, "", dsptr); \
    if (v == "") {    \
        if (!defined_before_read_config){ \
            if (default_value) val.fld = default_value->fld; \
            else val.fld = default_fld_value; \
        }\
    }\
    else {\
    auto v2 = enum_from_string(v.c_str(), invalid_enum);\
    if (v2 == invalid_enum) {\
        printf("invalid enum configuration '[%s] %s'", section, #fld); \
        return false; \
            }\
            else \
        val.fld = v2; \
    }\
}

// list<customized_id<type>> fld = x,y,z
# define CONFIG_FLD_ID_LIST(type, fld, dsptr) \
{ \
    val.fld.clear();\
    std::string vv = dsn_config_get_value_string(section, #fld, "", dsptr); \
    std::list<std::string> lv; \
    ::dsn::utils::split_args(vv.c_str(), lv, ','); \
    for (auto& v : lv) {\
        if (!type::is_exist(v.c_str())) {\
            printf("invalid enum configuration '[%s] %s'", section, #fld); \
            return false; \
                } \
                else \
            val.fld.push_back(type(v.c_str())); \
        } \
    if (val.fld.size() == 0 && default_value) \
        val.fld = default_value->fld; \
}

// list<type> fld = x,y,z
# define CONFIG_FLD_STRING_LIST(fld, dsptr) \
    {\
    std::string vv = dsn_config_get_value_string(section, #fld, "", dsptr); \
    ::dsn::utils::split_args(vv.c_str(), val.fld, ','); \
    if (val.fld.size() == 0 && default_value) \
        val.fld = default_value->fld;\
    }

// cb: std::list<std::string>& => fld value
# define CONFIG_FLD_INT_LIST(fld, dsptr) \
   { \
    std::string vv = dsn_config_get_value_string(section, #fld, "", dsptr); \
    std::list<std::string> lv; \
    ::dsn::utils::split_args(vv.c_str(), lv, ','); \
    if (lv.size() == 0 && default_value) \
        val.fld = default_value->fld; \
    else {\
        for (auto& s : lv) { val.fld.push_back(atoi(s.c_str())); } \
    }\
   }

} // end namespace
