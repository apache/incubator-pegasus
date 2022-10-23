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
 *     Unit-test for logger.
 *
 * Revision history:
 *     Nov., 2015, @shengofsun (Weijie Sun), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "utils/simple_logger.h"
#include <gtest/gtest.h>
#include "utils/filesystem.h"

using namespace dsn;
using namespace dsn::tools;

static const int simple_logger_gc_gap = 20;

static void get_log_file_index(std::vector<int> &log_index)
{
    std::vector<std::string> sub_list;
    std::string path = "./";
    if (!utils::filesystem::get_subfiles(path, sub_list, false)) {
        ASSERT_TRUE(false);
    }

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

static void clear_files(std::vector<int> &log_index)
{
    char file[256];
    memset(file, 0, sizeof(file));
    for (auto i : log_index) {
        snprintf_p(file, 256, "log.%d.txt", i);
        dsn::utils::filesystem::remove_path(std::string(file));
    }
}

static void prepare_test_dir()
{
    const char *dir = "./test";
    std::string dr(dir);
    dsn::utils::filesystem::create_directory(dr);
    chdir(dir);
}

static void finish_test_dir()
{
    const char *dir = "./test";
    chdir("..");
    rmdir(dir);
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
    // cases for print_header
    screen_logger *logger = new screen_logger("./");
    log_print(logger, "%s", "test_print");
    std::thread t([](screen_logger *lg) { log_print(lg, "%s", "test_print"); }, logger);
    t.join();

    logger->flush();
    delete logger;

    prepare_test_dir();
    // create multiple files
    for (unsigned int i = 0; i < simple_logger_gc_gap + 10; ++i) {
        simple_logger *logger = new simple_logger("./");
        // in this case stdout is useless
        for (unsigned int i = 0; i != 1000; ++i)
            log_print(logger, "%s", "test_print");
        logger->flush();

        delete logger;
    }

    std::vector<int> index;
    get_log_file_index(index);
    EXPECT_TRUE(!index.empty());
    sort(index.begin(), index.end());
    EXPECT_EQ(simple_logger_gc_gap, index.size());
    clear_files(index);
    finish_test_dir();
}
