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

#include <functional>
#include <memory>

#include <dsn/tool-api/rpc_address.h>
#include <dsn/utility/string_view.h>

#define TIME_MS_MAX 0xffffffff

namespace dsn {
namespace utils {

template <typename T>
std::shared_ptr<T> make_shared_array(size_t size)
{
    return std::shared_ptr<T>(new T[size], std::default_delete<T[]>());
}

void time_ms_to_string(uint64_t ts_ms, char *str); // yyyy-MM-dd hh:mm:ss.SSS

void time_ms_to_date(uint64_t ts_ms, char *str, int len); // yyyy-MM-dd

void time_ms_to_date_time(uint64_t ts_ms, char *str, int len); // yyyy-MM-dd hh:mm:ss

void time_ms_to_date_time(uint64_t ts_ms,
                          int32_t &hour,
                          int32_t &min,
                          int32_t &sec); // time to hour, min, sec

uint64_t get_current_physical_time_ns();

// get unix timestamp of today's zero o'clock.
// eg. `1525881600` returned when called on May 10, 2018, CST
int64_t get_unix_sec_today_midnight();

// `hh:mm` (range in [00:00, 23:59]) to seconds since 00:00:00
// eg. `01:00` => `3600`
// Return: -1 when invalid
int hh_mm_to_seconds(dsn::string_view hhmm);

// local time `hh:mm` to unix timestamp.
// eg. `18:10` => `1525947000` when called on May 10, 2018, CST
// Return: -1 when invalid
int64_t hh_mm_today_to_unix_sec(string_view hhmm_of_day);

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
}
}
