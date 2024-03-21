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

#include <iostream>
#include <string>
#include <utility>

#include "gtest/gtest.h"
#include "utils/api_utilities.h"
#include "utils/fail_point.h"
#include "utils/fmt_logging.h"
#include "utils/timer.h"

TEST(LoggingTest, GlobalLog)
{
    std::cout << "logging start level = " << enum_to_string(get_log_start_level()) << std::endl;
    global_log(__FILENAME__, __FUNCTION__, __LINE__, LOG_LEVEL_INFO, "in TEST(core, logging)");
}

TEST(LoggingTest, GlobalLogBig)
{
    std::string big_str(128000, 'x');
    global_log(__FILENAME__, __FUNCTION__, __LINE__, LOG_LEVEL_INFO, big_str.c_str());
}

TEST(LoggingTest, LogMacro)
{
    struct test_case
    {
        log_level_t level;
        std::string str;
    } tests[] = {{LOG_LEVEL_DEBUG, "This is a test"},
                 {LOG_LEVEL_DEBUG, "\\x00%d\\x00\\x01%n/nm"},
                 {LOG_LEVEL_INFO, "\\x00%d\\x00\\x01%n/nm"},
                 {LOG_LEVEL_WARNING, "\\x00%d\\x00\\x01%n/nm"},
                 {LOG_LEVEL_ERROR, "\\x00%d\\x00\\x01%n/nm"},
                 {LOG_LEVEL_FATAL, "\\x00%d\\x00\\x01%n/nm"}};

    dsn::fail::setup();
    dsn::fail::cfg("coredump_for_fatal_log", "void(false)");

    for (auto test : tests) {
        // Test logging_provider::log.
        LOG(test.level, "LOG: sortkey = {}", test.str);
    }

    dsn::fail::teardown();
}

TEST(LoggingTest, TestLogTiming)
{
    LOG_TIMING_PREFIX_IF(INFO, true, "prefix", "foo test{}", 0) {}
    LOG_TIMING_IF(INFO, true, "no_prefix foo test{}", 1) {}
    LOG_TIMING_PREFIX(INFO, "prefix", "foo test{}", 2) {}
    LOG_TIMING(INFO, "foo test{}", 3){}
    {
        SCOPED_LOG_TIMING(INFO, "bar {}", 0);
        SCOPED_LOG_SLOW_EXECUTION(INFO, 1, "bar {}", 1);
        SCOPED_LOG_SLOW_EXECUTION_PREFIX(INFO, 1, "prefix", "bar {}", 1);
    }
    LOG_SLOW_EXECUTION(INFO, 1, "baz {}", 0) {}

    // Previous implementations of the above macro confused clang-tidy's use-after-move
    // check and generated false positives.
    std::string s1 = "hello";
    std::string s2;
    LOG_SLOW_EXECUTION(INFO, 1, "baz")
    {
        LOG_INFO(s1);
        s2 = std::move(s1);
    }

    ASSERT_EQ("hello", s2);
}
