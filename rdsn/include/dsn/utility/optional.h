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
 *     A naive implementation of optional type. Mainly for avoiding boost dependency.
 *
 * Revision history:
 *     2016-01-15, Tianyi Wang, first version
 *     2016-01-25, Tianyi Wang, add none placeholder
 */

#pragma once
#include <utility>

namespace dsn {
struct none_placeholder_t
{
};
constexpr none_placeholder_t none{};

template <typename T>
class optional
{
    bool _is_some;
    char _data_placeholder[sizeof(T)];

public:
    optional() : _is_some(false) {}
    /*implicit*/ optional(none_placeholder_t) : optional() {}
    /*implicit*/ optional(const optional &that) : _is_some(true)
    {
        new (_data_placeholder) T{reinterpret_cast<const T &>(that._data_placeholder)};
    }

    /*implicit*/ optional(optional &&that) : _is_some(true)
    {
        new (_data_placeholder) T{std::move(reinterpret_cast<T &&>(that._data_placeholder))};
        that.reset();
    }
    template <typename... Args>
    /*implicit*/ optional(Args &&... args) : _is_some(true)
    {
        new (_data_placeholder) T{std::forward<Args>(args)...};
    }

    // please use explicit reset
    optional &operator=(const optional &that) = delete;

    bool is_some() const { return _is_some; }
    bool is_none() const { return !_is_some; }
    const T &unwrap_or(const T &def) const
    {
        if (_is_some) {
            return unwrap();
        } else {
            return def;
        }
    }
    T &unwrap() { return reinterpret_cast<T &>(_data_placeholder); }
    const T &unwrap() const { return reinterpret_cast<const T &>(_data_placeholder); }
    void reset()
    {
        if (_is_some) {
            reinterpret_cast<T *>(_data_placeholder)->~T();
            _is_some = false;
        }
    }
    template <typename... Args>
    void reset(Args &&... args)
    {
        if (_is_some) {
            reinterpret_cast<T *>(_data_placeholder)->~T();
        } else {
            _is_some = true;
        }
        new (_data_placeholder) T{std::forward<Args>(args)...};
    }
    ~optional() { reset(); }
};
}
