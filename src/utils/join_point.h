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

#include <absl/utility/utility.h>
#include <functional>
#include <list>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

namespace dsn {

// A join_point instance is a set of lambdas with the identical function signature.
// It's typically used for creating hooks at the specific execution points,
// for example:
//   - on rpc session establishes
//   - on process begins to exits
//   - ...
// Using join_point, we can inject the behavior in these cases in non-intrusive way.
//
// NOTE: "Join point" is a concept in Aspect-Oriented-Programming. Each "advice" is
// an extension on the join-point. It's similar with the "Interceptor Pattern".
//   - https://en.wikipedia.org/wiki/Advice_(programming)

template <typename R, typename... Args>
class join_point_base
{
public:
    explicit join_point_base(const char *name) : _name(name) {}

    virtual ~join_point_base()
    {
        _advice_entries.clear();
        _ret_advice_entries.clear();
    }

    using ReturnedAdviceT = R(Args...);
    using AdviceT = void(Args...);

    // TODO(wutao): call it add_returned_advice()
    void put_native(std::function<ReturnedAdviceT> fn) { _ret_advice_entries.push_front(fn); }

    // TODO(wutao): call it add_advice()
    void put_back(std::function<AdviceT> fn, const char * /*unused*/)
    {
        _advice_entries.push_back(std::move(fn));
    }

    void put_front(std::function<AdviceT> fn, const char * /*unused*/)
    {
        _advice_entries.push_front(std::move(fn));
    }

    const char *name() const { return _name.c_str(); }

protected:
    std::list<std::function<ReturnedAdviceT>> _ret_advice_entries;
    std::list<std::function<AdviceT>> _advice_entries;
    const std::string _name;

private:
    friend class join_point_test;
};

template <typename R, typename... Args>
class join_point final : public join_point_base<R, Args...>
{
public:
    using BaseType = join_point_base<R, Args...>;
    static_assert(!std::is_void<R>::value, "type R must not be a void");

    explicit join_point(const char *name) : BaseType(name) {}

    // Execute the hooks sequentially.
    R execute(Args... args, R default_return_value)
    {
        R ret = default_return_value;
        for (auto &func : BaseType::_ret_advice_entries) {
            ret = absl::apply(func, std::make_tuple(std::forward<Args>(args)...));
        }
        for (auto &func : BaseType::_advice_entries) {
            absl::apply(func, std::make_tuple(std::forward<Args>(args)...));
        }
        return ret;
    }
};

template <typename... Args>
class join_point<void, Args...> final : public join_point_base<void, Args...>
{
public:
    using BaseType = join_point_base<void, Args...>;

    explicit join_point(const char *name) : BaseType(name) {}

    // Execute the hooks sequentially.
    void execute(Args... args)
    {
        for (auto &func : BaseType::_advice_entries) {
            absl::apply(func, std::make_tuple(std::forward<Args>(args)...));
        }
    }
};

} // end namespace dsn
