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

#include <chrono>
#include <cstdio>

#include "string_view.h"

namespace dsn {
namespace utils {

static struct tm *get_localtime(uint64_t ts_ms, struct tm *tm_buf)
{
    auto t = (time_t)(ts_ms / 1000);
    return localtime_r(&t, tm_buf);
}

// get time string, which format is yyyy-MM-dd hh:mm:ss.SSS
// NOTE: using char* as output is usually unsafe. Please use std::string as the output argument
// as long as it's possible.
extern void time_ms_to_string(uint64_t ts_ms, char *str);
extern void time_ms_to_string(uint64_t ts_ms, std::string &str);

// get date string with format of 'yyyy-MM-dd' from given timestamp
inline void time_ms_to_date(uint64_t ts_ms, char *str, int len)
{
    struct tm tmp;
    strftime(str, len, "%Y-%m-%d", get_localtime(ts_ms, &tmp));
}

// get date string with format of 'yyyy-MM-dd hh:mm:ss' from given timestamp(ms)
inline void time_ms_to_date_time(uint64_t ts_ms, char *str, int len)
{
    struct tm tmp;
    strftime(str, len, "%Y-%m-%d %H:%M:%S", get_localtime(ts_ms, &tmp));
}

// get date string with format of 'yyyy-MM-dd hh:mm:ss' from given timestamp(s)
inline std::string time_s_to_date_time(uint64_t unix_seconds)
{
    char buffer[128];
    utils::time_ms_to_date_time(unix_seconds * 1000, buffer, 128);
    return std::string(buffer);
}

// parse hour/min/sec from the given timestamp
inline void time_ms_to_date_time(uint64_t ts_ms, int32_t &hour, int32_t &min, int32_t &sec)
{
    struct tm tmp;
    auto ret = get_localtime(ts_ms, &tmp);
    hour = ret->tm_hour;
    min = ret->tm_min;
    sec = ret->tm_sec;
}

// get current physical timestamp in ns
inline uint64_t get_current_physical_time_ns()
{
    auto now = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::nanoseconds>(now.time_since_epoch()).count();
}

// get current physical timestamp in s
inline uint64_t get_current_physical_time_s() { return get_current_physical_time_ns() * 1e-9; }

// get unix timestamp of today's zero o'clock.
// eg. `1525881600` returned when called on May 10, 2018, CST
inline int64_t get_unix_sec_today_midnight()
{
    time_t t = time(nullptr);
    struct tm tmp;
    auto ret = localtime_r(&t, &tmp);
    ret->tm_hour = 0;
    ret->tm_min = 0;
    ret->tm_sec = 0;
    return static_cast<int64_t>(mktime(ret));
}

// `hh:mm` (range in [00:00, 23:59]) to seconds since 00:00:00
// eg. `01:00` => `3600`
// Return: -1 when invalid
inline int hh_mm_to_seconds(dsn::string_view hhmm)
{
    int hour = 0, min = 0, sec = -1;
    if (::sscanf(hhmm.data(), "%d:%d", &hour, &min) == 2 && (0 <= hour && hour <= 23) &&
        (0 <= min && min <= 59)) {
        sec = 3600 * hour + 60 * min;
    }
    return sec;
}

// local time `hh:mm` to unix timestamp.
// eg. `18:10` => `1525947000` when called on May 10, 2018, CST
// Return: -1 when invalid
inline int64_t hh_mm_today_to_unix_sec(string_view hhmm_of_day)
{
    int sec_of_day = hh_mm_to_seconds(hhmm_of_day);
    if (sec_of_day == -1) {
        return -1;
    }

    return get_unix_sec_today_midnight() + sec_of_day;
}

} // namespace utils
} // namespace dsn
