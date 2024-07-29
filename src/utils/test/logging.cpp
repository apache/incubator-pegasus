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

#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "spdlog/common.h"
#include "utils/fmt_logging.h"
#include "utils/timer.h"

TEST(LoggingTest, LOG) { LOG(spdlog::level::info, "in TEST(LoggingTest, GlobalLog)"); }

TEST(LoggingTest, LOGWithBigString)
{
    std::string big_str(128000, 'x');
    LOG(spdlog::level::info, big_str.c_str());
}

TEST(LoggingTest, LogMacro)
{
    std::string str1 = "This is a test";
    std::string str2 = "\\x00%d\\x00\\x01%n/nm";

    LOG_DEBUG("LOG: sortkey = {}", str1);
    LOG_DEBUG("LOG: sortkey = {}", str2);
    LOG_INFO("LOG: sortkey = {}", str2);
    LOG_WARNING("LOG: sortkey = {}", str2);
    LOG_ERROR("LOG: sortkey = {}", str2);
    ASSERT_DEATH(LOG_FATAL("LOG: sortkey = {}", str2), "LOG: sortkey =");
}

TEST(LoggingTest, TestLogTiming)
{
    LOG_INFO("common info log");
    LOG_TIMING_PREFIX_IF(info, true, "prefix", "foo test{}", 0) {}
    LOG_TIMING_IF(info, true, "no_prefix foo test{}", 1) {}
    LOG_TIMING_PREFIX(info, "prefix", "foo test{}", 2) {}
    LOG_TIMING(info, "foo test{}", 3){}
    {
        SCOPED_LOG_TIMING(info, "bar {}", 0);
        SCOPED_LOG_SLOW_EXECUTION(info, 1, "bar {}", 1);
        SCOPED_LOG_SLOW_EXECUTION_PREFIX(info, 1, "prefix", "bar {}", 1);
    }
    LOG_SLOW_EXECUTION(info, 1, "baz {}", 0) {}

    // Previous implementations of the above macro confused clang-tidy's use-after-move
    // check and generated false positives.
    std::string s1 = "hello";
    std::string s2;
    LOG_SLOW_EXECUTION(info, 1, "baz")
    {
        LOG_INFO(s1);
        s2 = std::move(s1);
    }
    LOG_INFO("common info log");

    ASSERT_EQ("hello", s2);
}
