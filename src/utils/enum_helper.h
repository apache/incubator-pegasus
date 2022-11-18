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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <map>
#include <string>
#include <mutex>
#include <memory>

// an invalid enum value must be provided so as to be the default value when parsing failed
#define ENUM_BEGIN2(type, name, invalid_value)                                                     \
    static inline ::dsn::enum_helper_xxx<type> *RegisterEnu_##name()                               \
    {                                                                                              \
        ::dsn::enum_helper_xxx<type> *helper = new ::dsn::enum_helper_xxx<type>(invalid_value);

#define ENUM_BEGIN(type, invalid_value) ENUM_BEGIN2(type, type, invalid_value)

#define ENUM_REG2(type, name) helper->register_enum(#name, type::name);
#define ENUM_REG_WITH_CUSTOM_NAME(type, name) helper->register_enum(#name, type);
#define ENUM_REG(e) helper->register_enum(#e, e);

#define ENUM_END2(type, name)                                                                      \
    return helper;                                                                                 \
    }                                                                                              \
    inline type enum_from_string(const char *s, type invalid_value)                                \
    {                                                                                              \
        return ::dsn::enum_helper_xxx<type>::instance(RegisterEnu_##name).parse(s);                \
    }                                                                                              \
    inline const char *enum_to_string(type val)                                                    \
    {                                                                                              \
        return ::dsn::enum_helper_xxx<type>::instance(RegisterEnu_##name).to_string(val);          \
    }

#define ENUM_END(type) ENUM_END2(type, type)

namespace dsn {

template <typename TEnum>
class enum_helper_xxx
{
private:
    struct EnumContext
    {
        std::string name;
    };

public:
    enum_helper_xxx(TEnum invalid) : _invalid(invalid) {}

    void register_enum(const char *name, TEnum v)
    {
        _nameToValue[std::string(name)] = v;

        EnumContext ctx;
        ctx.name.assign(name);
        _valueToContext[v] = ctx;
    }

    TEnum parse(const std::string &name)
    {
        auto it = _nameToValue.find(name);
        return it != _nameToValue.end() ? it->second : _invalid;
    }

    const char *to_string(TEnum v)
    {
        auto it = _valueToContext.find(v);
        if (it != _valueToContext.end()) {
            return it->second.name.c_str();
        } else {
            return "Unknown";
        }
    }

    static enum_helper_xxx &instance(enum_helper_xxx<TEnum> *(*registor)())
    {
        if (_instance == nullptr) {
            static std::once_flag flag;
            std::call_once(flag, [&]() { _instance.reset(registor()); });
        }
        return *_instance;
    }

private:
    static std::unique_ptr<enum_helper_xxx<TEnum>> _instance;

private:
    TEnum _invalid;
    std::map<TEnum, EnumContext> _valueToContext;
    std::map<std::string, TEnum> _nameToValue;
};

template <typename TEnum>
std::unique_ptr<enum_helper_xxx<TEnum>> enum_helper_xxx<TEnum>::_instance;

} // namespace dsn
