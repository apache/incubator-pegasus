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

#include <stddef.h>
#include <stdint.h>
#include <algorithm> // IWYU pragma: keep
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

namespace dsn {
class rpc_address;
} // namespace dsn

#define TIME_MS_MAX 0xffffffff

// The COMPILE_ASSERT macro can be used to verify that a compile time
// expression is true. For example, you could use it to verify the
// size of a static array:
//
//   COMPILE_ASSERT(ARRAYSIZE(content_type_names) == CONTENT_NUM_TYPES,
//                  content_type_names_incorrect_size);
//
// or to make sure a struct is smaller than a certain size:
//
//   COMPILE_ASSERT(sizeof(foo) < 128, foo_too_large);
//
// The second argument to the macro is the name of the variable. If
// the expression is false, most compilers will issue a warning/error
// containing the name of the variable.
struct CompileAssert
{
};

#define COMPILE_ASSERT(expr, msg) static const CompileAssert msg[bool(expr) ? 1 : -1]

namespace dsn {
namespace utils {

template <typename T>
std::shared_ptr<T> make_shared_array(size_t size)
{
    return std::shared_ptr<T>(new T[size], std::default_delete<T[]>());
}

// get host name from ip series
// if can't get a hostname from ip(maybe no hostname or other errors), return false, and
// hostname_result will be invalid value
// if multiple hostname got and all of them are resolvable return true, otherwise return false.
// and the hostname_result will be "hostname1,hostname2(or ip_address or )..."
// we only support ipv4 currently
// check if a.b.c.d:port can be resolved to hostname:port. If it can be resolved, return true
// and hostname_result
// will be the hostname, or it will be ip address or error message

// valid a.b.c.d -> return TRUE && hostname_result=hostname | invalid a.b.c.d:port1 -> return
// FALSE
// && hostname_result=a.b.c.d
bool hostname_from_ip(const char *ip, std::string *hostname_result);

// valid a.b.c.d：port -> return TRUE && hostname_result=hostname:port | invalid a.b.c.d:port1
// ->
// return FALSE  && hostname_result=a.b.c.d:port
bool hostname_from_ip_port(const char *ip_port, std::string *hostname_result);

// valid a.b.c.d,e.f.g.h -> return TRUE && hostname_result_list=hostname1,hostname2 | invalid
// a.b.c.d,e.f.g.h -> return TRUE && hostname_result_list=a.b.c.d,e.f.g.h
bool list_hostname_from_ip(const char *ip_port_list, std::string *hostname_result_list);

// valid a.b.c.d:port1,e.f.g.h:port2 -> return TRUE &&
// hostname_result_list=hostname1:port1,hostname2:port2 | invalid a.b.c.d:port1,e.f.g.h:port2 ->
// return TRUE && hostname_result_list=a.b.c.d:port1,e.f.g.h:port2
bool list_hostname_from_ip_port(const char *ip_port_list, std::string *hostname_result_list);

// valid_ipv4_rpc_address return TRUE && hostname_result=hostname:port | invalid_ipv4 -> return
// FALSE
bool hostname(const dsn::rpc_address &address, std::string *hostname_result);

// valid_ip_network_order -> return TRUE && hostname_result=hostname	|
// invalid_ip_network_order -> return FALSE
bool hostname_from_ip(uint32_t ip, std::string *hostname_result);

template <typename A, typename B>
std::multimap<B, A> flip_map(const std::map<A, B> &source)
{
    std::multimap<B, A> target;
    std::transform(source.begin(),
                   source.end(),
                   std::inserter(target, target.begin()),
                   [](const std::pair<A, B> &p) { return std::pair<B, A>(p.second, p.first); });
    return target;
}

template <typename T>
std::set<T> get_intersection(const std::set<T> &set1, const std::set<T> &set2)
{
    std::set<T> intersection;
    std::set_intersection(set1.begin(),
                          set1.end(),
                          set2.begin(),
                          set2.end(),
                          std::inserter(intersection, intersection.begin()));
    return intersection;
}
} // namespace utils
} // namespace dsn
