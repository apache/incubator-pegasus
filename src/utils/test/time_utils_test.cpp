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

#include "utils/time_utils.h"

#include "gtest/gtest.h"

namespace dsn {
namespace utils {

TEST(time_utils, hh_mm_to_seconds)
{
    ASSERT_EQ(hh_mm_to_seconds("00:00"), 0);
    ASSERT_EQ(hh_mm_to_seconds("23:59"), 86340);
    ASSERT_EQ(hh_mm_to_seconds("1:1"), 3660);
    ASSERT_EQ(hh_mm_to_seconds("01:1"), 3660);
    ASSERT_EQ(hh_mm_to_seconds("1:01"), 3660);
    ASSERT_EQ(hh_mm_to_seconds("01:01"), 3660);

    ASSERT_EQ(hh_mm_to_seconds("23"), -1);
    ASSERT_EQ(hh_mm_to_seconds("23:"), -1);
    ASSERT_EQ(hh_mm_to_seconds(":59"), -1);
    ASSERT_EQ(hh_mm_to_seconds("-1:00"), -1);
    ASSERT_EQ(hh_mm_to_seconds("24:00"), -1);
    ASSERT_EQ(hh_mm_to_seconds("01:-1"), -1);
    ASSERT_EQ(hh_mm_to_seconds("01:60"), -1);
    ASSERT_EQ(hh_mm_to_seconds("a:00"), -1);
    ASSERT_EQ(hh_mm_to_seconds("01:b"), -1);
    ASSERT_EQ(hh_mm_to_seconds("01b"), -1);
}

TEST(time_utils, get_unix_sec_today_midnight)
{
    ASSERT_LT(0, get_unix_sec_today_midnight());
    ASSERT_LE(get_unix_sec_today_midnight(), time(nullptr));
    ASSERT_GE(time(nullptr) - get_unix_sec_today_midnight(), 0);
    ASSERT_LT(time(nullptr) - get_unix_sec_today_midnight(), 86400);
}

TEST(time_utils, hh_mm_today_to_unix_sec)
{
    ASSERT_EQ(get_unix_sec_today_midnight() + hh_mm_to_seconds("0:0"),
              hh_mm_today_to_unix_sec("0:0"));
    ASSERT_EQ(get_unix_sec_today_midnight() + hh_mm_to_seconds("23:59"),
              hh_mm_today_to_unix_sec("23:59"));

    ASSERT_EQ(hh_mm_today_to_unix_sec("23"), -1);
    ASSERT_EQ(hh_mm_today_to_unix_sec("23:"), -1);
    ASSERT_EQ(hh_mm_today_to_unix_sec(":59"), -1);
    ASSERT_EQ(hh_mm_today_to_unix_sec("-1:00"), -1);
    ASSERT_EQ(hh_mm_today_to_unix_sec("24:00"), -1);
    ASSERT_EQ(hh_mm_today_to_unix_sec("01:-1"), -1);
    ASSERT_EQ(hh_mm_today_to_unix_sec("01:60"), -1);
    ASSERT_EQ(hh_mm_today_to_unix_sec("a:00"), -1);
    ASSERT_EQ(hh_mm_today_to_unix_sec("01:b"), -1);
    ASSERT_EQ(hh_mm_today_to_unix_sec("01b"), -1);
}

TEST(time_utils, get_current_physical_time_ns)
{
    int64_t ts_ns = get_current_physical_time_ns();
    ASSERT_LT(0, ts_ns);
    ASSERT_GE(get_current_physical_time_ns() - ts_ns, 0);
    ASSERT_LT(get_current_physical_time_ns() - ts_ns, 1e7); // < 10 ms
}

TEST(time_utils, time_ms_to_string)
{
    char buf[64] = {0};
    time_ms_to_string(1605091506136, buf);
    // time differ between time zones,
    // the real time 2020-11-11 18:45:06.136 (UTC+8)
    // so it must be 2020-11-1x xx:45:06.136
    ASSERT_EQ(std::string(buf).substr(0, 9), "2020-11-1");
    ASSERT_EQ(std::string(buf).substr(13, 10), ":45:06.136");
}

} // namespace utils
} // namespace dsn
