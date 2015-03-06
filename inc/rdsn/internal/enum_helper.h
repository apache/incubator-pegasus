# pragma once

# include <map>
# include <string>
# include <mutex>

namespace rdsn {

// an invalid enum value must be provided so as to be the default value when parsing failed
#define ENUM_BEGIN(type, invalid_value)    \
    static inline enum_helper_xxx<type>* RegisterEnu_##type() {\
        enum_helper_xxx<type>* helper = new enum_helper_xxx<type>(invalid_value);

#define ENUM_REG(e) helper->register_enum(#e, e);

#define ENUM_END(type) return helper; \
    } \
    inline type enum_from_string(const char* s, type invalid_value) {\
        return enum_helper_xxx<type>::instance(RegisterEnu_##type).parse(s); \
    }\
    inline const char* enum_to_string(type val)  {\
        return enum_helper_xxx<type>::instance(RegisterEnu_##type).to_string(val); \
    }

template<typename TEnum>
class enum_helper_xxx
{
private:
    struct EnumContext
    {
        std::string name;
    };

public:
    enum_helper_xxx(TEnum invalid) : _invalid(invalid) {}

    void register_enum(const char* name, TEnum v)
    {
        _nameToValue[std::string(name)] = v;

        EnumContext ctx;
        ctx.name.assign(name);
        _valueToContext[v] = ctx;
    }

    TEnum parse(const std::string& name)
    {
        auto it = _nameToValue.find(name);
        return it != _nameToValue.end() ? it->second : _invalid;
    }

    const char* to_string(TEnum v)
    {
        auto it = _valueToContext.find(v);
        if (it != _valueToContext.end())
        {
            return it->second.name.c_str();
        }
        else
        {
            return "Unknown";
        }
    }
    
    static enum_helper_xxx& instance(enum_helper_xxx<TEnum>* (*registor)())
    {
        if (_instance == nullptr)
        {
            static std::once_flag flag;
            std::call_once(flag, [&]() {
                _instance = registor();
            });
        }
        return *_instance;
    }

private:
    static enum_helper_xxx *_instance;

private:
    TEnum                        _invalid;
    std::map<TEnum, EnumContext> _valueToContext;
    std::map<std::string, TEnum> _nameToValue;
};

template<typename TEnum> enum_helper_xxx<TEnum>* enum_helper_xxx<TEnum>::_instance = 0;

} // end namespace
