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

#include <fmt/core.h>
#include <unistd.h>
#include <memory>
#include <regex>
#include <set>
#include <string>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "gutil/map_util.h"
#include "utils/api_utilities.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/simple_logger.h"
#include "utils/test_macros.h"

DSN_DECLARE_uint64(max_number_of_log_files_on_disk);

namespace dsn {
namespace tools {
class logger_test : public testing::Test
{
public:
    void SetUp() override
    {
        std::string cwd;
        ASSERT_TRUE(dsn::utils::filesystem::get_current_directory(cwd));
        // NOTE: Don't name the dir with "test", otherwise the whole utils test dir would be
        // removed.
        test_dir = dsn::utils::filesystem::path_combine(cwd, "logger_test");

        NO_FATALS(prepare_test_dir());
        std::set<std::string> files;
        NO_FATALS(get_log_files(files));
        NO_FATALS(clear_files(files));
    }

    void get_log_files(std::set<std::string> &file_names)
    {
        std::vector<std::string> sub_list;
        ASSERT_TRUE(utils::filesystem::get_subfiles(test_dir, sub_list, false));

        file_names.clear();
        std::regex pattern(R"(SimpleLogger\.log\.[0-9]{8}_[0-9]{6}_[0-9]{3})");
        for (const auto &path : sub_list) {
            std::string name(utils::filesystem::get_file_name(path));
            if (std::regex_match(name, pattern)) {
                ASSERT_TRUE(gutil::InsertIfNotPresent(&file_names, name));
            }
        }
    }

    void compare_log_files(const std::set<std::string> &before_files,
                           const std::set<std::string> &after_files)
    {
        ASSERT_FALSE(after_files.empty());

        // One new log file is created.
        if (after_files.size() == before_files.size() + 1) {
            // All the file names are the same.
            for (auto it1 = before_files.begin(), it2 = after_files.begin();
                 it1 != before_files.end();
                 ++it1, ++it2) {
                ASSERT_EQ(*it1, *it2);
            }
            // The number of log files is the same, but they have rolled.
        } else if (after_files.size() == before_files.size()) {
            auto it1 = before_files.begin();
            auto it2 = after_files.begin();
            // The first file is different, the one in 'before_files' is older.
            ASSERT_NE(*it1, *it2);

            // The rest of the files are the same.
            for (++it1; it1 != before_files.end(); ++it1, ++it2) {
                ASSERT_EQ(*it1, *it2);
            }
        } else {
            ASSERT_TRUE(false) << "Invalid number of log files, before=" << before_files.size()
                               << ", after=" << after_files.size();
        }
    }

    void clear_files(const std::set<std::string> &file_names)
    {
        for (const auto &file_name : file_names) {
            ASSERT_TRUE(dsn::utils::filesystem::remove_path(file_name));
        }
    }

    void prepare_test_dir()
    {
        ASSERT_TRUE(dsn::utils::filesystem::create_directory(test_dir)) << test_dir;
    }

    void remove_test_dir()
    {
        ASSERT_TRUE(dsn::utils::filesystem::remove_path(test_dir)) << test_dir;
    }

public:
    std::string test_dir;
};

#define LOG_PRINT(logger, ...)                                                                     \
    (logger)->log(                                                                                 \
        __FILE__, __FUNCTION__, __LINE__, LOG_LEVEL_DEBUG, fmt::format(__VA_ARGS__).c_str())

TEST_F(logger_test, screen_logger_test)
{
    auto logger = std::make_unique<screen_logger>(nullptr, nullptr);
    LOG_PRINT(logger.get(), "{}", "test_print");
    std::thread t([](screen_logger *lg) { LOG_PRINT(lg, "{}", "test_print"); }, logger.get());
    t.join();
    logger->flush();
}

TEST_F(logger_test, redundant_log_test)
{
    // Create redundant log files to test if their number could be restricted.
    for (unsigned int i = 0; i < FLAGS_max_number_of_log_files_on_disk + 10; ++i) {
        std::set<std::string> before_files;
        NO_FATALS(get_log_files(before_files));

        auto logger = std::make_unique<simple_logger>(test_dir.c_str(), "SimpleLogger");
        for (unsigned int i = 0; i != 1000; ++i) {
            LOG_PRINT(logger.get(), "{}", "test_print");
        }
        logger->flush();

        std::set<std::string> after_files;
        NO_FATALS(get_log_files(after_files));
        NO_FATALS(compare_log_files(before_files, after_files));
        ::usleep(2000);
    }

    std::set<std::string> files;
    NO_FATALS(get_log_files(files));
    ASSERT_FALSE(files.empty());
    ASSERT_EQ(FLAGS_max_number_of_log_files_on_disk, files.size());
}

} // namespace tools
} // namespace dsn
