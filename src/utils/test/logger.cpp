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

#include <boost/algorithm/string/predicate.hpp>
#include <errno.h>
#include <fmt/core.h>
#include <stdio.h>
#include <unistd.h>
#include <algorithm>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "utils/api_utilities.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/logging_provider.h"
#include "utils/safe_strerror_posix.h"
#include "utils/simple_logger.h"

namespace dsn {
namespace tools {

DSN_DECLARE_uint64(max_number_of_log_files_on_disk);

namespace {

void get_log_file_index(std::vector<int> &log_index)
{
    std::vector<std::string> sub_list;
    ASSERT_TRUE(dsn::utils::filesystem::get_subfiles("./", sub_list, false));

    for (const auto &path : sub_list) {
        const auto &name = dsn::utils::filesystem::get_file_name(path);
        if (!boost::algorithm::starts_with(name, "log.")) {
            continue;
        }
        if (!boost::algorithm::ends_with(name, ".txt")) {
            continue;
        }

        int index;
        if (1 != sscanf(name.c_str(), "log.%d.txt", &index)) {
            continue;
        }
        log_index.push_back(index);
    }
}

// Don't name the dir with "./test", otherwise the whole utils test dir would be removed.
const std::string kTestDir("./test_logger");

void prepare_test_dir()
{
    ASSERT_TRUE(dsn::utils::filesystem::create_directory(kTestDir));
    ASSERT_EQ(0, ::chdir(kTestDir.c_str()));
}

void remove_test_dir()
{
    ASSERT_EQ(0, ::chdir("..")) << "chdir failed, err = " << dsn::utils::safe_strerror(errno);
    ASSERT_TRUE(dsn::utils::filesystem::remove_path(kTestDir)) << "remove_directory " << kTestDir
                                                               << " failed";
}

} // anonymous namespace

#define LOG_PRINT(logger, ...)                                                                     \
    (logger)->log(                                                                                 \
        __FILE__, __FUNCTION__, __LINE__, LOG_LEVEL_DEBUG, fmt::format(__VA_ARGS__).c_str())

TEST(LoggerTest, SimpleLogger)
{
    // Deregister commands to avoid re-register error.
    dsn::logging_provider::instance()->deregister_commands();

    {
        auto logger = std::make_unique<screen_logger>(true);
        LOG_PRINT(logger.get(), "{}", "test_print");
        std::thread t([](screen_logger *lg) { LOG_PRINT(lg, "{}", "test_print"); }, logger.get());
        t.join();

        logger->flush();
    }

    prepare_test_dir();

    // Create redundant log files to test if their number could be restricted.
    for (unsigned int i = 0; i < FLAGS_max_number_of_log_files_on_disk + 10; ++i) {
        auto logger = std::make_unique<simple_logger>("./");
        for (unsigned int i = 0; i != 1000; ++i) {
            LOG_PRINT(logger.get(), "{}", "test_print");
        }
        logger->flush();
    }

    std::vector<int> index;
    get_log_file_index(index);
    ASSERT_FALSE(index.empty());
    ASSERT_EQ(FLAGS_max_number_of_log_files_on_disk, index.size());

    remove_test_dir();
}

} // namespace tools
} // namespace dsn
