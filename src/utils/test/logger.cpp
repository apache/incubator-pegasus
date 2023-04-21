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

#include <errno.h>
// IWYU pragma: no_include <gtest/gtest-message.h>
// IWYU pragma: no_include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <stdarg.h>
#include <stdio.h>
#include <unistd.h>
#include <algorithm>
#include <memory>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/logging_provider.h"
#include "utils/ports.h"
#include "utils/safe_strerror_posix.h"
#include "utils/simple_logger.h"

using std::vector;
using std::string;

using namespace dsn;
using namespace dsn::tools;

static const int simple_logger_gc_gap = 20;

static void get_log_file_index(vector<int> &log_index)
{
    vector<string> sub_list;
    string path = "./";
    ASSERT_TRUE(utils::filesystem::get_subfiles(path, sub_list, false));

    for (auto &ptr : sub_list) {
        auto &&name = utils::filesystem::get_file_name(ptr);
        if (name.length() <= 8 || name.substr(0, 4) != "log.")
            continue;
        int index;
        if (1 != sscanf(name.c_str(), "log.%d.txt", &index))
            continue;
        log_index.push_back(index);
    }
}

static void clear_files(vector<int> &log_index)
{
    char file[256] = {};
    for (auto i : log_index) {
        snprintf_p(file, 256, "log.%d.txt", i);
        dsn::utils::filesystem::remove_path(string(file));
    }
}

static void prepare_test_dir()
{
    const char *dir = "./test";
    string dr(dir);
    ASSERT_TRUE(dsn::utils::filesystem::create_directory(dr));
    ASSERT_EQ(0, ::chdir(dir));
}

static void finish_test_dir()
{
    const char *dir = "./test";
    ASSERT_EQ(0, ::chdir("..")) << "chdir failed, err = " << utils::safe_strerror(errno);
    ASSERT_TRUE(utils::filesystem::remove_path(dir)) << "remove_directory " << dir << " failed";
}

void log_print(logging_provider *logger, const char *fmt, ...)
{
    va_list vl;
    va_start(vl, fmt);
    logger->dsn_logv(__FILE__, __FUNCTION__, __LINE__, LOG_LEVEL_DEBUG, fmt, vl);
    va_end(vl);
}

TEST(tools_common, simple_logger)
{
    // Deregister commands to avoid re-register error.
    dsn::logging_provider::instance()->deregister_commands();

    {
        auto logger = std::make_unique<screen_logger>(true);
        log_print(logger.get(), "%s", "test_print");
        std::thread t([](screen_logger *lg) { log_print(lg, "%s", "test_print"); }, logger.get());
        t.join();

        logger->flush();
    }

    prepare_test_dir();
    // create multiple files
    for (unsigned int i = 0; i < simple_logger_gc_gap + 10; ++i) {
        auto logger = std::make_unique<simple_logger>("./");
        for (unsigned int i = 0; i != 1000; ++i) {
            log_print(logger.get(), "%s", "test_print");
        }
        logger->flush();
    }

    vector<int> index;
    get_log_file_index(index);
    ASSERT_TRUE(!index.empty());
    sort(index.begin(), index.end());
    ASSERT_EQ(simple_logger_gc_gap, index.size());
    clear_files(index);
    finish_test_dir();
}
