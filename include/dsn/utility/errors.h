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

#include <dsn/cpp/auto_codes.h>
#include <dsn/utility/smart_pointers.h>

#include <sstream>

namespace dsn {

// error_s gives a detailed description of the error tagged by error_code.
// For example:
//
//   error_s open_file(std::string file_name) {
//       if(file_name.empty()) {
//           return error_s::make(ERR_INVALID_PARAMETERS, "file name should not be empty");
//       }
//       return error_s::ok();
//   }
//
//   error_s err = open_file("");
//   if (!err.is_ok()) {
//       std::cerr << s.description() << std::endl;
//       // print: "ERR_INVALID_PARAMETERS: file name should not be empty"
//   }
//
class error_s
{
public:
    constexpr error_s() noexcept = default;

    ~error_s() = default;

    // copyable
    error_s(const error_s &rhs) noexcept { copy(rhs); }
    error_s &operator=(const error_s &rhs) noexcept
    {
        copy(rhs);
        return (*this);
    }

    // movable
    error_s(error_s &&rhs) noexcept = default;
    error_s &operator=(error_s &&) noexcept = default;

    static inline error_s make(error_code code, std::string reason)
    {
        return error_s(code, reason);
    }

    static inline error_s make(error_code code)
    {
        // fast path
        if (code == ERR_OK) {
            return {};
        }
        return make(code, "");
    }

    // Return a success status.
    // This function is almost zero-cost since the returned object contains
    // merely a null pointer.
    static inline error_s ok() { return error_s(); }

    inline bool is_ok() const
    {
        if (info_) {
            return info_->code == ERR_OK;
        }
        return true;
    }

    std::string description() const
    {
        if (!info_) {
            return ERR_OK.to_string();
        }
        std::string code = info_->code.to_string();
        return info_->msg.empty() ? code : code + ": " + info_->msg;
    }

    error_code code() const { return info_ ? error_code(info_->code) : ERR_OK; }

    error_s &operator<<(const char str[])
    {
        if (info_) {
            info_->msg.append(str);
            // It's fine for operator<< being applied to an OK Status.
        }
        return (*this);
    }

    template <class T>
    error_s &operator<<(T v)
    {
        if (info_) {
            std::ostringstream oss;
            oss << v;
            (*this) << oss.str().c_str();
        }
        return *this;
    }

public:
    friend std::ostream &operator<<(std::ostream &os, const error_s &s)
    {
        return os << s.description();
    }

private:
    error_s(const error_code &code, std::string &msg) noexcept : info_(new error_info(code, msg)) {}

    struct error_info
    {
        error_code code;
        std::string msg;

        error_info(const error_code &c, std::string &s) : code(c), msg(std::move(s)) {}
    };

    void copy(const error_s &rhs)
    {
        if (!rhs.info_) {
            info_.release();
        } else if (!info_) {
            info_ = make_unique<error_info>(rhs.info_->code, rhs.info_->msg);
        } else {
            info_->code = rhs.info_->code;
            info_->msg = rhs.info_->msg;
        }
    }

private:
    std::unique_ptr<error_info> info_;
};

// error_with is used to return an error or a value.
// For example:
//
//   error_with<int> result = ...;
//   if (!s.is_ok()) {
//       cerr << s.get_error().description()) << endl;
//   } else {
//       cerr << s.get_value() << endl;
//   }
//
template <typename T>
class error_with
{
public:
    // for ok case
    error_with(const T &value) : _value(new T(value)) {}
    error_with(T &&value) : _value(new T(std::move(value))) {}

    // for error case
    error_with(error_s &&err) : _err(std::move(err)) { assert(!_err.is_ok()); }
    error_with(const error_s &status) : _err(status) { assert(!_err.is_ok()); }

    const T &get_value() const
    {
        assert(_err.is_ok());
        return *_value;
    }

    T &get_value()
    {
        assert(_err.is_ok());
        return *_value;
    }

    const error_s &get_error() const { return _err; }

    error_s &get_error() { return _err; }

    bool is_ok() const { return _err.is_ok(); }

private:
    error_s _err;
    std::unique_ptr<T> _value;
};

} // namespace dsn

#define FMT_ERR(ec, msg, args...) error_s::make(ec, fmt::format(msg, ##args))

#define RETURN_NOT_OK(s)                                                                           \
    do {                                                                                           \
        const ::dsn::error_s &_s = (s);                                                            \
        if (!_s.is_ok())                                                                           \
            return _s;                                                                             \
    } while (false);
