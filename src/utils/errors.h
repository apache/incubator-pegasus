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

#include <sstream>

#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/fmt_utils.h"
#include "utils/ports.h"
#include <string_view>

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
//       std::cerr << err.description() << std::endl;
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
        if (this == &rhs) {
            return *this;
        }

        copy(rhs);
        return *this;
    }

    // movable
    error_s(error_s &&) noexcept = default;
    error_s &operator=(error_s &&) noexcept = default;

    static error_s make(error_code code, std::string_view reason) { return {code, reason}; }

    static error_s make(error_code code)
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
    static error_s ok() { return error_s(); }

    bool is_ok() const
    {
        if (_info) {
            return _info->code == ERR_OK;
        }
        return true;
    }

    explicit operator bool() const noexcept { return is_ok(); }

    std::string description() const
    {
        if (!_info) {
            return ERR_OK.to_string();
        }

        std::string msg(_info->code.to_string());
        if (!_info->msg.empty()) {
            fmt::format_to(std::back_inserter(msg), ": {}", _info->msg);
        }

        return msg;
    }

    [[nodiscard]] error_code code() const { return _info ? _info->code : ERR_OK; }

    [[nodiscard]] std::string message() const { return _info ? _info->msg : ""; }

    error_s &operator<<(const char str[])
    {
        if (_info) {
            _info->msg.append(" << ");
            _info->msg.append(str);
            // It's fine for operator<< being applied to an OK Status.
        }
        return (*this);
    }

    template <class T>
    error_s &operator<<(T v)
    {
        if (_info) {
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

    friend bool operator==(const error_s &lhs, const error_s &rhs)
    {
        if (lhs._info && rhs._info) {
            return lhs._info->code == rhs._info->code && lhs._info->msg == rhs._info->msg;
        }
        return lhs._info == rhs._info;
    }

private:
    error_s(error_code code, std::string_view msg) noexcept
        : _info(std::make_unique<error_info>(code, msg))
    {
    }

    struct error_info
    {
        error_code code;
        std::string msg; // TODO(wutao1): use raw char* to improve performance?

        error_info(error_code c, std::string_view s) noexcept : code(c), msg(s) {}
    };

    void copy(const error_s &rhs)
    {
        if (rhs._info == _info) {
            return;
        }
        if (!rhs._info) {
            _info.reset();
        } else if (!_info) {
            _info = std::make_unique<error_info>(rhs._info->code, rhs._info->msg);
        } else {
            _info->code = rhs._info->code;
            _info->msg = rhs._info->msg;
        }
    }

    std::unique_ptr<error_info> _info;
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
        CHECK(_err.is_ok(), get_error().description());
        return *_value;
    }

    T &get_value()
    {
        CHECK(_err.is_ok(), get_error().description());
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

USER_DEFINED_STRUCTURE_FORMATTER(::dsn::error_s);

#define FMT_ERR(ec, msg, args...) ::dsn::error_s::make(ec, fmt::format(msg, ##args))

#define RETURN_NOT_OK(s)                                                                           \
    do {                                                                                           \
        const ::dsn::error_s &_s = (s);                                                            \
        if (dsn_unlikely(!_s)) {                                                                   \
            return _s;                                                                             \
        }                                                                                          \
    } while (false)

#define RETURN_EC_NOT_OK(s)                                                                        \
    do {                                                                                           \
        const ::dsn::error_s &_s = (s);                                                            \
        if (dsn_unlikely(!_s)) {                                                                   \
            return _s.code();                                                                      \
        }                                                                                          \
    } while (false)

#define RETURN_ES_NOT_OK_MSG(s, ...)                                                               \
    do {                                                                                           \
        const ::dsn::error_s &_s = (s);                                                            \
        if (dsn_unlikely(!_s)) {                                                                   \
            fmt::println(stderr, "{}: {}", _s, fmt::format(__VA_ARGS__));                          \
            return _s;                                                                             \
        }                                                                                          \
    } while (false)

#define RETURN_EC_NOT_OK_MSG(s, ...)                                                               \
    do {                                                                                           \
        const ::dsn::error_s &_s = (s);                                                            \
        if (dsn_unlikely(!_s)) {                                                                   \
            fmt::println(stderr, "{}: {}", _s, fmt::format(__VA_ARGS__));                          \
            return _s.code();                                                                      \
        }                                                                                          \
    } while (false)

#define RETURN_EW_NOT_OK_MSG(s, T, ...)                                                            \
    do {                                                                                           \
        ::dsn::error_s _s = (s);                                                                   \
        if (dsn_unlikely(!_s)) {                                                                   \
            fmt::println(stderr, "{}: {}", _s, fmt::format(__VA_ARGS__));                          \
            return dsn::error_with<T>(std::move(_s));                                              \
        }                                                                                          \
    } while (false)

#define CHECK_OK(s, ...)                                                                           \
    do {                                                                                           \
        const ::dsn::error_s &_s = (s);                                                            \
        CHECK(_s.is_ok(), fmt::format(__VA_ARGS__));                                               \
    } while (false)

#define RETURN_ERRS_NOT_TRUE(exp, code, ...)                                                       \
    do {                                                                                           \
        if (dsn_unlikely(!(exp))) {                                                                \
            return dsn::error_s::make(code, fmt::format(__VA_ARGS__));                             \
        }                                                                                          \
    } while (false)

#ifndef NDEBUG
#define DCHECK_OK CHECK_OK
#else
#define DCHECK_OK(s, ...)
#endif
