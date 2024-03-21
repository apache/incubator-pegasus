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

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <utility>

#include "utils/ports.h"

namespace dsn {
template <typename TEnum>
class enum_helper_xxx;
} // namespace dsn

// an invalid enum value must be provided so as to be the default value when parsing failed
#define ENUM_BEGIN2(type, name, invalid_value)                                                     \
    static inline ::dsn::enum_helper_xxx<type> *RegisterEnu_##name()                               \
    {                                                                                              \
        ::dsn::enum_helper_xxx<type> *helper = new ::dsn::enum_helper_xxx<type>(invalid_value);

#define ENUM_BEGIN(type, invalid_value) ENUM_BEGIN2(type, type, invalid_value)

#define ENUM_REG2(type, name) helper->register_enum(#name, type::name);
#define ENUM_REG_WITH_CUSTOM_NAME(type, name) helper->register_enum(#name, type);
#define ENUM_REG(e) helper->register_enum(#e, e);

// The argument `type invalid_value`, albeit unused, has to be provided since `enum_from_string`
// could be overloaded by different enumeration types.
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

// Google Style (https://google.github.io/styleguide/cppguide.html#Enumerator_Names) has recommended
// using k-prefixed camelCase to name an enumerator instead of MACRO_CASE. That is, use kEnumName,
// rather than ENUM_NAME.
//
// On the other hand, the string representation for an enumerator is often needed. After declaring
// the enumerator, ENUM_REG* macros would also be used to register the string representation, which
// has some drawbacks:
// * the enumerator has to appear again, leading to redundant code;
// * once there are numerous enumerators, ENUM_REG* tend to be forgotten and the registers for the
// string representation would be missing.
//
// To solve these problems, ENUM_CONST* macros are introduced:
// * firstly, naming for the string representation should be UpperCamelCase style (namely EnumName);
// * ENUM_CONST() could be used to generate the enumerators according to the string representation;
// * only support `enum class` declarations;
// * ENUM_CONST_DEF() could be used to declare enumerators in the enum class;
// * ENUM_CONST_REG_STR could be used to register the string representation.
//
// The usage of these macros would be described as below. For example, Status::code of rocksdb
// (include/rocksdb/status.h) could be defined by ENUM_CONST* as following steps:
//
// 1. List string representation for each enumerator by a user-defined macro:
// --------------------------------------------------------------------------
/*
 * #define ENUM_FOREACH_STATUS_CODE(DEF)    \
 *    DEF(Ok)                               \
 *    DEF(NotFound)                         \
 *    DEF(Corruption)                       \
 *    DEF(IOError)
 */
// If each enumerator needs to be bound to a value, define the macro as below, where `status_code`
// is the name of enum class:
/*
 * #define ENUM_FOREACH_STATUS_CODE(DEF)    \
 *    DEF(Ok, 100, status_code)             \
 *    DEF(NotFound, 101, status_code)       \
 *    DEF(Corruption, 102, status_code)     \
 *    DEF(IOError, 103, status_code)
 */
// 2. Declare an enum class by above user-defined macro, with an Invalid and an Count enumerator if
// necessary:
// ------------------------------------------------------------------------------------------------
// enum class status_code
// {
//     ENUM_FOREACH_STATUS_CODE(ENUM_CONST_DEF) kCount, kInvalidCode,
// };
//
// 3. Define another user-defined macro to register string representations:
// ------------------------------------------------------------------------
// #define ENUM_CONST_REG_STR_STATUS_CODE(str) ENUM_CONST_REG_STR(status_code, str)
//
// If each enumerator needs to be bound to a value, define the macro as below, and for the reason
// please see the comments for the definition of ENUM_CONST_DEF:
// #define ENUM_CONST_REG_STR_STATUS_CODE(str, ...) ENUM_CONST_REG_STR(status_code, str)
//
// 4. Define converters between enumerators and string representations:
// --------------------------------------------------------------------
// ENUM_BEGIN(status_code, status_code::kInvalidCode)
// ENUM_FOREACH_STATUS_CODE(ENUM_CONST_REG_STR_STATUS_CODE)
// ENUM_END(status_code)
//
// Call converters between enumerators and string representations:
// auto code = enum_from_string("Ok", status_code::kInvalidCode);
// const char *str = enum_to_string(status_code::kOk);
//
// 5. Define converters from values with any type to enumerators:
// ------------------------------------------------------------------
// For values with long type:
// ENUM_CONST_DEF_FROM_VAL_FUNC(long, status_code, ENUM_FOREACH_STATUS_CODE)
//
// For values with uint64_t:
// ENUM_CONST_DEF_FROM_VAL_FUNC(uint64_t, status_code, ENUM_FOREACH_STATUS_CODE)
//
// Call the converter to get the enumerator corresponding to the value:
// long val = 100;
// auto code = enum_from_val(val, status_code::kInvalidCode);
//
// 6. Define converters from enumerators to values with any type:
// ------------------------------------------------------------------
// For values with long type:
// ENUM_CONST_DEF_TO_VAL_FUNC(long, status_code, ENUM_FOREACH_STATUS_CODE)
//
// For values with uint64_t:
// ENUM_CONST_DEF_TO_VAL_FUNC(uint64_t, status_code, ENUM_FOREACH_STATUS_CODE)
//
// Define an invalid value for long type:
// const long kInvalidStatus = -1;
//
// Call the converter to get the value corresponding to the enumerator:
// auto code = status_code::kOk;
// auto val = enum_to_val(code, kInvalidStatus);

#define ENUM_CONST(str) k##str

// To define an enumerator, we could use a macro with some arguments. However, not all of the
// macros have identical arguments. To make each macro accept the same arguments, we could:
// * keep the arguments of all macros in the same order
// * use variable arguments as the placeholders that represent the missing arguments
//
// To be specific, examples of variadic macros are as follows, all of which have the same sequence
// of variable arguments: `str, val, enum_class, ...`:
// * ENUM_CONST_DEF(str, ...)
// * ENUM_CONST_DEF_FROM_VAL_ITEM(str, from_val, enum_class, ...)
// * ENUM_CONST_DEF_TO_VAL_ITEM(str, to_val, enum_class, ...)
#define ENUM_CONST_DEF(str, ...) ENUM_CONST(str),

#define ENUM_CONST_REG_STR(enum_class, str)                                                        \
    helper->register_enum(#str, enum_class::ENUM_CONST(str));

#define ENUM_CONST_DEF_MAPPER(direction, from_type, to_type, enum_foreach, enum_def)               \
    inline to_type enum_##direction##_val(const from_type &from_val,                               \
                                          const to_type &invalid_to_val)                           \
    {                                                                                              \
        static const std::unordered_map<from_type, to_type> kMap = {enum_foreach(enum_def)};       \
                                                                                                   \
        const auto &iter = kMap.find(from_val);                                                    \
        if (dsn_unlikely(iter == kMap.end())) {                                                    \
            return invalid_to_val;                                                                 \
        }                                                                                          \
                                                                                                   \
        return iter->second;                                                                       \
    }

#define ENUM_CONST_DEF_FROM_VAL_ITEM(str, from_val, enum_class, ...)                               \
    {from_val, enum_class::ENUM_CONST(str)},
#define ENUM_CONST_DEF_FROM_VAL_FUNC(from_type, enum_class, enum_foreach)                          \
    ENUM_CONST_DEF_MAPPER(from, from_type, enum_class, enum_foreach, ENUM_CONST_DEF_FROM_VAL_ITEM)

#define ENUM_CONST_DEF_TO_VAL_ITEM(str, to_val, enum_class, ...)                                   \
    {enum_class::ENUM_CONST(str), to_val},
#define ENUM_CONST_DEF_TO_VAL_FUNC(to_type, enum_class, enum_foreach)                              \
    ENUM_CONST_DEF_MAPPER(to, enum_class, to_type, enum_foreach, ENUM_CONST_DEF_TO_VAL_ITEM)

namespace dsn {

template <typename TEnum>
constexpr auto enum_to_int(TEnum e)
{
    return static_cast<std::underlying_type_t<TEnum>>(e);
}

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
