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

#include <dsn/utility/utils.h>

#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <array>
#include <chrono>
#include <fstream>
#include <iostream>
#include <memory>
#include <random>

#include <dsn/service_api_cpp.h>
#include <dsn/utility/singleton.h>
#include <sys/stat.h>
#include <sys/types.h>

#if defined(__linux__)
#include <sys/syscall.h>
#include <unistd.h>
#elif defined(__FreeBSD__)
#include <sys/thr.h>
#elif defined(__APPLE__)
#include <pthread.h>
#endif

namespace dsn {
namespace utils {

uint64_t get_current_physical_time_ns()
{
    auto now = std::chrono::high_resolution_clock::now();
    auto nanos =
        std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
    return nanos;
}

// len >= 24
void time_ms_to_string(uint64_t ts_ms, char *str)
{
    auto t = (time_t)(ts_ms / 1000);
    struct tm tmp;
    auto ret = localtime_r(&t, &tmp);
    sprintf(str,
            "%04d-%02d-%02d %02d:%02d:%02d.%03u",
            ret->tm_year + 1900,
            ret->tm_mon + 1,
            ret->tm_mday,
            ret->tm_hour,
            ret->tm_min,
            ret->tm_sec,
            static_cast<uint32_t>(ts_ms % 1000));
}

// len >= 11
void time_ms_to_date(uint64_t ts_ms, char *str, int len)
{
    auto t = (time_t)(ts_ms / 1000);
    struct tm tmp;
    auto ret = localtime_r(&t, &tmp);
    strftime(str, len, "%Y-%m-%d", ret);
}

// len >= 20
void time_ms_to_date_time(uint64_t ts_ms, char *str, int len)
{
    auto t = (time_t)(ts_ms / 1000);
    struct tm tmp;
    auto ret = localtime_r(&t, &tmp);
    strftime(str, len, "%Y-%m-%d %H:%M:%S", ret);
}

void time_ms_to_date_time(uint64_t ts_ms, int32_t &hour, int32_t &min, int32_t &sec)
{
    auto t = (time_t)(ts_ms / 1000);
    struct tm tmp;
    auto ret = localtime_r(&t, &tmp);
    hour = ret->tm_hour;
    min = ret->tm_min;
    sec = ret->tm_sec;
}

int64_t get_unix_sec_today_midnight()
{
    time_t t = time(nullptr);
    struct tm tmp;
    auto ret = localtime_r(&t, &tmp);
    ret->tm_hour = 0;
    ret->tm_min = 0;
    ret->tm_sec = 0;
    return static_cast<int64_t>(mktime(ret));
}

int hh_mm_to_seconds(dsn::string_view hhmm)
{
    int hour = 0, min = 0, sec = -1;
    if (::sscanf(hhmm.data(), "%d:%d", &hour, &min) == 2 && (0 <= hour && hour <= 23) &&
        (0 <= min && min <= 59)) {
        sec = 3600 * hour + 60 * min;
    }
    return sec;
}

int64_t hh_mm_today_to_unix_sec(string_view hhmm_of_day)
{
    int sec_of_day = hh_mm_to_seconds(hhmm_of_day);
    if (sec_of_day == -1) {
        return -1;
    }

    return get_unix_sec_today_midnight() + sec_of_day;
}
bool hostname_from_ip(uint32_t ip, std::string *hostname_result)
{
    struct sockaddr_in addr_in;
    addr_in.sin_family = AF_INET;
    addr_in.sin_port = 0;
    addr_in.sin_addr.s_addr = ip;
    char hostname[256];
    int err = getnameinfo((struct sockaddr *)(&addr_in),
                          sizeof(struct sockaddr),
                          hostname,
                          sizeof(hostname),
                          nullptr,
                          0,
                          NI_NAMEREQD);
    if (err != 0) {
        struct in_addr net_addr;
        net_addr.s_addr = ip;
        char ip_str[256];
        inet_ntop(AF_INET, &net_addr, ip_str, sizeof(ip_str));
        if (err == EAI_SYSTEM) {
            dwarn("got error %s when try to resolve %s", strerror(errno), ip_str);
        } else {
            dwarn("return error(%s) when try to resolve %s", gai_strerror(err), ip_str);
        }
        return false;
    } else {
        *hostname_result = std::string(hostname);
        return true;
    }
}

bool hostname_from_ip(const char *ip, std::string *hostname_result)
{
    uint32_t ip_addr;
    if (inet_pton(AF_INET, ip, &ip_addr) != 1) {
        // inet_pton() returns 1 on success (network address was successfully converted)
        *hostname_result = ip;
        return false;
    }
    if (!hostname_from_ip(ip_addr, hostname_result)) {
        *hostname_result = ip;
        return false;
    }
    return true;
}

bool hostname_from_ip_port(const char *ip_port, std::string *hostname_result)
{
    dsn::rpc_address addr;
    if (!addr.from_string_ipv4(ip_port)) {
        dwarn("invalid ip_port(%s)", ip_port);
        *hostname_result = ip_port;
        return false;
    }
    if (!hostname(addr, hostname_result)) {
        *hostname_result = ip_port;
        return false;
    }
    return true;
}

bool hostname(const rpc_address &address, std::string *hostname_result)
{
    if (address.type() != HOST_TYPE_IPV4) {
        return false;
    }
    if (hostname_from_ip(htonl(address.ip()), hostname_result)) {
        *hostname_result += ":" + std::to_string(address.port());
        return true;
    }
    return false;
}

bool list_hostname_from_ip(const char *ip_list, std::string *hostname_result_list)
{
    std::vector<std::string> splitted_ip;
    dsn::utils::split_args(ip_list, splitted_ip, ',');

    if (splitted_ip.empty()) {
        dwarn("invalid ip_list(%s)", ip_list);
        *hostname_result_list = *ip_list;
        return false;
    }

    std::string temp;
    std::stringstream result;
    bool all_ok = true;
    for (int i = 0; i < splitted_ip.size(); ++i) {
        result << (i ? "," : "");
        if (hostname_from_ip(splitted_ip[i].c_str(), &temp)) {
            result << temp;
        } else {
            result << splitted_ip[i].c_str();
            all_ok = false;
        }
    }
    *hostname_result_list = result.str();
    return all_ok;
}

bool list_hostname_from_ip_port(const char *ip_port_list, std::string *hostname_result_list)
{
    std::vector<std::string> splitted_ip_port;
    dsn::utils::split_args(ip_port_list, splitted_ip_port, ',');

    if (splitted_ip_port.empty()) {
        dwarn("invalid ip_list(%s)", ip_port_list);
        *hostname_result_list = *ip_port_list;
        return false;
    }

    std::string temp;
    std::stringstream result;
    bool all_ok = true;
    for (int i = 0; i < splitted_ip_port.size(); ++i) {
        result << (i ? "," : "");
        if (hostname_from_ip_port(splitted_ip_port[i].c_str(), &temp)) {
            result << temp;
        } else {
            result << splitted_ip_port[i].c_str();
            all_ok = false;
        }
    }
    *hostname_result_list = result.str();
    return all_ok;
}
}
}
