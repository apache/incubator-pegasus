// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <dsn/utility/flags.h>
#include <dsn/utility/config_api.h>
#include <dsn/utility/singleton.h>
#include <dsn/c/api_utilities.h>
#include <boost/optional/optional.hpp>

#include <map>

namespace dsn {

enum value_type
{
    FV_BOOL = 0,
    FV_INT32 = 1,
    FV_UINT32 = 2,
    FV_INT64 = 3,
    FV_UINT64 = 4,
    FV_DOUBLE = 5,
    FV_STRING = 6,
    FV_MAX_INDEX = 6,
};

using validator_fn = std::function<void()>;

class flag_data
{
public:
#define FLAG_DATA_LOAD_CASE(type, type_enum, suffix)                                               \
    case type_enum:                                                                                \
        value<type>() = dsn_config_get_value_##suffix(_section, _name, value<type>(), _desc);      \
        if (_validator) {                                                                          \
            _validator();                                                                          \
        }                                                                                          \
        break

    void load()
    {
        switch (_type) {
            FLAG_DATA_LOAD_CASE(int32_t, FV_INT32, int64);
            FLAG_DATA_LOAD_CASE(int64_t, FV_INT64, int64);
            FLAG_DATA_LOAD_CASE(uint32_t, FV_UINT32, uint64);
            FLAG_DATA_LOAD_CASE(uint64_t, FV_UINT64, uint64);
            FLAG_DATA_LOAD_CASE(bool, FV_BOOL, bool);
            FLAG_DATA_LOAD_CASE(double, FV_DOUBLE, double);
            FLAG_DATA_LOAD_CASE(const char *, FV_STRING, string);
        }
    }

    flag_data(const char *section, const char *name, const char *desc, value_type type, void *val)
        : _type(type), _val(val), _section(section), _name(name), _desc(desc)
    {
    }

    void set_validator(validator_fn &validator) { _validator = std::move(validator); }
    const validator_fn &validator() const { return _validator; }

private:
    template <typename T>
    T &value()
    {
        return *reinterpret_cast<T *>(_val);
    }

private:
    const value_type _type;
    void *const _val;
    const char *_section;
    const char *_name;
    const char *_desc;
    validator_fn _validator;
};

class flag_registry : public utils::singleton<flag_registry>
{
public:
    void add_flag(const char *name, flag_data flag) { _flags.emplace(name, flag); }

    void add_validator(const char *name, validator_fn &validator)
    {
        auto it = _flags.find(name);
        dassert(it != _flags.end(), "flag \"%s\" does not exist", name);
        flag_data &flag = it->second;
        if (!flag.validator()) {
            flag.set_validator(validator);
        }
    }

    void load_from_config()
    {
        for (auto &kv : _flags) {
            flag_data &flag = kv.second;
            flag.load();
        }
    }

private:
    friend class utils::singleton<flag_registry>;
    flag_registry() = default;

private:
    std::map<std::string, flag_data> _flags;
};

#define FLAG_REG_CONSTRUCTOR(type, type_enum)                                                      \
    flag_registerer::flag_registerer(                                                              \
        const char *section, const char *name, const char *desc, type *val)                        \
    {                                                                                              \
        flag_registry::instance().add_flag(name, flag_data(section, name, desc, type_enum, val));  \
    }

FLAG_REG_CONSTRUCTOR(int32_t, FV_INT32);
FLAG_REG_CONSTRUCTOR(uint32_t, FV_UINT32);
FLAG_REG_CONSTRUCTOR(int64_t, FV_INT64);
FLAG_REG_CONSTRUCTOR(uint64_t, FV_UINT64);
FLAG_REG_CONSTRUCTOR(bool, FV_BOOL);
FLAG_REG_CONSTRUCTOR(double, FV_DOUBLE);
FLAG_REG_CONSTRUCTOR(const char *, FV_STRING);

flag_validator::flag_validator(const char *name, validator_fn validator)
{
    flag_registry::instance().add_validator(name, validator);
}

/*extern*/ void flags_initialize() { flag_registry::instance().load_from_config(); }

} // namespace dsn
