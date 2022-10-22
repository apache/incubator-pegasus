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
 *     Function traits to help extract the type of various callbacks
 *
 * Revision history:
 *     2016-01-15, Tianyi Wang, first version
 */

#pragma once

#include <type_traits>

namespace dsn {
template <typename T>
struct function_traits : public function_traits<decltype(&T::operator())>
{
};
template <typename ReturnType, typename... Args>
struct function_traits<ReturnType(Args...)>
{
    using return_t = ReturnType;
    static constexpr size_t const arity = sizeof...(Args);
    template <size_t i>
    using arg_t = typename std::tuple_element<i, std::tuple<Args...>>::type;
};

template <typename ReturnType, typename... Args>
struct function_traits<ReturnType (*)(Args...)> : public function_traits<ReturnType(Args...)>
{
};

template <typename ClassType, typename ReturnType, typename... Args>
struct function_traits<ReturnType (ClassType::*)(Args...)>
    : public function_traits<ReturnType(Args...)>
{
    typedef ClassType &owner_type;
};

template <typename ClassType, typename ReturnType, typename... Args>
struct function_traits<ReturnType (ClassType::*)(Args...) const>
    : public function_traits<ReturnType(Args...)>
{
    typedef const ClassType &owner_type;
};

template <typename ClassType, typename ReturnType, typename... Args>
struct function_traits<ReturnType (ClassType::*)(Args...) volatile>
    : public function_traits<ReturnType(Args...)>
{
    typedef volatile ClassType &owner_type;
};

template <typename ClassType, typename ReturnType, typename... Args>
struct function_traits<ReturnType (ClassType::*)(Args...) const volatile>
    : public function_traits<ReturnType(Args...)>
{
    typedef const volatile ClassType &owner_type;
};

template <typename FunctionType>
struct function_traits<std::function<FunctionType>> : public function_traits<FunctionType>
{
};
template <typename T>
struct function_traits<T &> : public function_traits<T>
{
};
template <typename T>
struct function_traits<const T &> : public function_traits<T>
{
};
template <typename T>
struct function_traits<volatile T &> : public function_traits<T>
{
};
template <typename T>
struct function_traits<const volatile T &> : public function_traits<T>
{
};
template <typename T>
struct function_traits<T &&> : public function_traits<T>
{
};
template <typename T>
struct function_traits<const T &&> : public function_traits<T>
{
};
template <typename T>
struct function_traits<volatile T &&> : public function_traits<T>
{
};
template <typename T>
struct function_traits<const volatile T &&> : public function_traits<T>
{
};
}
