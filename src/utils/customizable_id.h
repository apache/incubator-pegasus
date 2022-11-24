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

#include <string>
#include <unordered_map>
#include <vector>

#include "utils/ports.h"
#include "utils/singleton.h"
#include "utils/command_manager.h"

namespace dsn {
namespace utils {

#define DEFINE_CUSTOMIZED_ID(T, name) __selectany const T name(#name);
#define DEFINE_CUSTOMIZED_ID_LONG(T, name, ...) __selectany const T name(#name, __VA_ARGS__);

#define DEFINE_CUSTOMIZED_ID_TYPE(T)                                                               \
    struct T##_                                                                                    \
    {                                                                                              \
    };                                                                                             \
    typedef dsn::utils::customized_id<T##_> T;

template <typename T>
class customized_id_mgr : public dsn::utils::singleton<customized_id_mgr<T>>
{
public:
    customized_id_mgr() : _names(199) { register_commands(); }
    int get_id(const char *name) const;
    int get_id(const std::string &name) const;
    const char *get_name(int id) const;
    int register_id(const char *name);
    int max_value() const { return static_cast<int>(_names2.size()) - 1; }
    void register_commands() {}

private:
    std::unordered_map<std::string, int> _names;
    std::vector<std::string> _names2;
    std::vector<std::unique_ptr<command_deregister>> _cmds;
};

template <typename T>
struct customized_id
{
    customized_id(const char *name);
    customized_id(const customized_id &source);
    operator int() const;
    operator T() const { return T(_internal_code); }
    const char *to_string() const;
    void reset(const customized_id &r);

    static int max_value();
    static const char *to_string(int code);
    static bool is_exist(const char *name);
    static customized_id from_string(const char *name, customized_id invalid_value);

    friend std::ostream &operator<<(std::ostream &os, const customized_id &id)
    {
        return os << id.to_string();
    }

protected:
    static int assign(const char *xxx);
    customized_id(int code);

protected:
    int _internal_code;

    // private:
    //    // no assignment operator
    //    customized_id<T>& operator=(const customized_id<T>& source);
};

// -------------------------- inline implementation ----------------------------

template <typename T>
customized_id<T>::customized_id(const char *name) : _internal_code(assign(name))
{
}

template <typename T>
customized_id<T>::customized_id(const customized_id &source) : _internal_code(source._internal_code)
{
}

template <typename T>
customized_id<T>::operator int() const
{
    return _internal_code;
}

template <typename T>
const char *customized_id<T>::to_string() const
{
    return customized_id_mgr<T>::instance().get_name(_internal_code);
}

template <typename T>
void customized_id<T>::reset(const customized_id<T> &r)
{
    _internal_code = r._internal_code;
}

template <typename T>
int customized_id<T>::max_value()
{
    return customized_id_mgr<T>::instance().max_value();
}

template <typename T>
const char *customized_id<T>::to_string(int code)
{
    return customized_id_mgr<T>::instance().get_name(code);
}

template <typename T>
bool customized_id<T>::is_exist(const char *name)
{
    return customized_id_mgr<T>::instance().get_id(name) != -1;
}

template <typename T>
customized_id<T> customized_id<T>::from_string(const char *name, customized_id invalid_value)
{
    int id = customized_id_mgr<T>::instance().get_id(name);
    if (id == -1)
        return invalid_value;
    else
        return customized_id<T>(id);
}

template <typename T>
int customized_id<T>::assign(const char *name)
{
    return customized_id_mgr<T>::instance().register_id(name);
}

template <typename T>
customized_id<T>::customized_id(int code) : _internal_code(code)
{
}

template <typename T>
int customized_id_mgr<T>::get_id(const char *name) const
{
    auto it = _names.find(std::string(name));
    if (it == _names.end())
        return -1;
    else
        return it->second;
}

template <typename T>
int customized_id_mgr<T>::get_id(const std::string &name) const
{
    auto it = _names.find(name);
    if (it == _names.end())
        return -1;
    else
        return it->second;
}

template <typename T>
const char *customized_id_mgr<T>::get_name(int id) const
{
    if (id < static_cast<int>(_names2.size()))
        return _names2[id].c_str();
    else
        return "unknown";
}

template <typename T>
int customized_id_mgr<T>::register_id(const char *name)
{
    int id = get_id(name);
    if (-1 != id) {
        return id;
    }

    int code = static_cast<int>(_names.size());
    _names[std::string(name)] = code;
    _names2.push_back(std::string(name));
    return code;
}
}
} // end namespace dsn::utils
