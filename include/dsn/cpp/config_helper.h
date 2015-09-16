

# pragma once

# include <dsn/service_api_c.h>
# include <dsn/cpp/utils.h>

# define CONFIG_BEGIN(t_struct) \
    inline bool read_config(\
    const char* section, \
    /*out*/ t_struct& val, \
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
