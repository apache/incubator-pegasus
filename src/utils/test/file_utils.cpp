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

#include <rocksdb/env.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <stdint.h>
#include <time.h>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "test_util/test_util.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"

class file_utils : public pegasus::encrypt_data_test_base
{
public:
    void file_utils_test_setup()
    {
        std::string path = "./file_utils_temp.txt";
        ASSERT_TRUE(dsn::utils::filesystem::remove_path(path));
        ASSERT_FALSE(dsn::utils::filesystem::file_exists(path));

        path = "./file_utils_temp";
        ASSERT_TRUE(dsn::utils::filesystem::remove_path(path));
        ASSERT_FALSE(dsn::utils::filesystem::directory_exists(path));
    }

    void file_utils_test_get_process_image_path()
    {
        std::string imagepath;
        ASSERT_TRUE(dsn::utils::filesystem::get_current_directory(imagepath));
        imagepath = dsn::utils::filesystem::path_combine(imagepath, "dsn_utils_tests");

        std::string path;
        ASSERT_EQ(dsn::ERR_OK, dsn::utils::filesystem::get_current_process_image_path(path));
    }

    void file_utils_test_get_normalized_path()
    {
        struct same_normalized_paths
        {
            std::string path;
        } same_normalized_path_tests[] = {{"\\\\?\\"},
                                          {"c:\\"},
                                          {"c:"},
                                          {"\\\\?\\c:\\"},
                                          {"\\\\?\\c:"},
                                          {"c:\\a"},
                                          {"c:\\\\a"},
                                          {"c:\\\\a\\"},
                                          {"c:\\\\a\\\\"},
                                          {"\\\\?\\c:\\a"},
                                          {"\\\\?\\c:\\\\a"},
                                          {"\\\\?\\c:\\\\a\\"},
                                          {"\\\\?\\c:\\\\a\\\\"},
                                          {"\\"},
                                          {"\\\\"},
                                          {"\\\\\\"},
                                          {"\\\\a"},
                                          {"\\\\\\a"},
                                          {"\\\\a\\"},
                                          {"\\\\\\a\\"},
                                          {"\\\\\\a\\\\"},
                                          {"/"},
                                          {"c:/a"},
                                          {"."},
                                          {"./a"},
                                          {"./a/b"},
                                          {".."},
                                          {"../a"},
                                          {"../a/b"}};
        for (const auto &test : same_normalized_path_tests) {
            std::string npath;
            dsn::utils::filesystem::get_normalized_path(test.path, npath);
            ASSERT_EQ(test.path, npath);
        }

        struct normalized_paths
        {
            std::string path;
            std::string normalized_path;
        } normalized_path_tests[] = {
            {"//", "/"},
            {"//?/", "/?"},
            {"//a", "/a"},
            {"//a/", "/a"},
            {"//a//", "/a"},
            {"c:/", "c:"},
            {"c://", "c:"},
            {"c:/a/", "c:/a"},
            {"c://a/", "c:/a"},
            {"c://a//", "c:/a"},
            {"/////////////////////////////////////////////////////////////////", "/"},
            {"/////////////////////////////////////////////////////////////////a/////////////////"
             "b///"
             "////////",
             "/a/b"},
            {"./", "."},
            {".//a", "./a"},
            {"./a/", "./a"},
            {"./a/b/", "./a/b"},
            {".///a////b///", "./a/b"},
            {"../", ".."},
            {"..//a", "../a"},
            {"../a/", "../a"},
            {"../a/b/", "../a/b"},
            {"..///a////b///", "../a/b"}};
        for (const auto &test : normalized_path_tests) {
            std::string npath;
            dsn::utils::filesystem::get_normalized_path(test.path, npath);
            ASSERT_EQ(test.normalized_path, npath) << test.path;
        }
    }

    void file_utils_test_get_current_directory()
    {
        std::string path;
        ASSERT_TRUE(dsn::utils::filesystem::get_current_directory(path));
        ASSERT_TRUE(!path.empty());
    }

    void file_utils_test_path_combine()
    {
        struct combine_paths
        {
            std::string path1;
            std::string path2;
            std::string combined_path;
        } tests[] = {{"", "", ""},
                     {"c:", "Windows\\explorer.exe", "c:/Windows\\explorer.exe"},
                     {"c:", "\\Windows\\explorer.exe", "c:/Windows\\explorer.exe"},
                     {"c:\\", "\\Windows\\explorer.exe", "c:\\/Windows\\explorer.exe"},
                     {"/bin", "ls", "/bin/ls"},
                     {"/bin/", "ls", "/bin/ls"},
                     {"/bin", "/ls", "/bin/ls"},
                     {"/bin/", "/ls", "/bin/ls"}};
        for (const auto &test : tests) {
            std::string path = dsn::utils::filesystem::path_combine(test.path1, test.path2);
            ASSERT_EQ(test.combined_path, path) << test.path1 << " + " << test.path2;
        }
    }

    void file_utils_test_get_file_name()
    {
        struct combine_paths
        {
            std::string path;
            std::string file_name;
        } tests[] = {{"", ""},
                     {"c:", "c:"},
                     {"c:\\", ""},
                     {"c:1.txt", "c:1.txt"},
                     {"c:\\1.txt", "1.txt"},
                     {"c:\\Windows\\1.txt", "1.txt"},
                     {"/bin/", ""},
                     {"/bin/ls", "ls"}};
        for (const auto &test : tests) {
            std::string file_name = dsn::utils::filesystem::get_file_name(test.path);
            ASSERT_EQ(test.file_name, file_name) << test.path;
        }
    }

    void file_utils_test_create()
    {
        std::string path = "./file_utils_temp.txt";
        ASSERT_TRUE(dsn::utils::filesystem::create_file(path));
        ASSERT_TRUE(dsn::utils::filesystem::file_exists(path));

        time_t current_time = ::time(nullptr);
        ASSERT_NE(current_time, 1);

        auto s = rocksdb::WriteStringToFile(
            dsn::utils::PegasusEnv(dsn::utils::FileDataType::kNonSensitive),
            rocksdb::Slice("Hello world!"),
            path,
            /* should_sync */ true);
        ASSERT_TRUE(s.ok()) << s.ToString();

        time_t last_write_time;
        ASSERT_TRUE(dsn::utils::filesystem::last_write_time(path, last_write_time));
        ASSERT_NE(last_write_time, -1);
        ASSERT_GE(last_write_time, current_time);

        path = "./file_utils_temp";
        ASSERT_TRUE(dsn::utils::filesystem::create_directory(path));
        ASSERT_TRUE(dsn::utils::filesystem::directory_exists(path));

        path = "./file_utils_temp/a/b/c/d//";
        ASSERT_TRUE(dsn::utils::filesystem::create_directory(path));
        ASSERT_TRUE(dsn::utils::filesystem::directory_exists(path));

        struct create_files
        {
            std::string filename;
        } tests[] = {{"./file_utils_temp/a/1.txt"},
                     {"./file_utils_temp/a/2.txt"},
                     {"./file_utils_temp/b/c/d/1.txt"}};
        for (const auto &test : tests) {
            ASSERT_TRUE(dsn::utils::filesystem::create_file(test.filename)) << test.filename;
            ASSERT_TRUE(dsn::utils::filesystem::file_exists(test.filename)) << test.filename;
        }
    }

    void file_utils_test_file_size()
    {
        std::string path = "./file_utils_temp.txt";
        int64_t sz;
        ASSERT_TRUE(
            dsn::utils::filesystem::file_size(path, dsn::utils::FileDataType::kNonSensitive, sz));
        ASSERT_EQ(12, sz);

        path = "./file_utils_temp2.txt";
        ASSERT_FALSE(
            dsn::utils::filesystem::file_size(path, dsn::utils::FileDataType::kNonSensitive, sz));
    }

    void file_utils_test_path_exists()
    {
        std::string path = "/";
        ASSERT_TRUE(dsn::utils::filesystem::path_exists(path));
        ASSERT_TRUE(dsn::utils::filesystem::directory_exists(path));
        ASSERT_FALSE(dsn::utils::filesystem::file_exists(path));

        path = "./not_exists_not_exists";
        ASSERT_FALSE(dsn::utils::filesystem::path_exists(path));

        path = "/bin/ls";
        ASSERT_TRUE(dsn::utils::filesystem::path_exists(path));
        ASSERT_FALSE(dsn::utils::filesystem::directory_exists(path));
        ASSERT_TRUE(dsn::utils::filesystem::file_exists(path));
    }

    void file_utils_test_get_paths()
    {
        std::string path = ".";
        std::vector<std::string> file_list;
        ASSERT_TRUE(dsn::utils::filesystem::get_subfiles(path, file_list, false));
        ASSERT_GE(file_list.size(), 2);
        file_list.clear();

        path = ".";
        ASSERT_TRUE(dsn::utils::filesystem::get_subfiles(path, file_list, true));
        ASSERT_GE(file_list.size(), 3);
        file_list.clear();

        path = "../../";
        ASSERT_TRUE(dsn::utils::filesystem::get_subfiles(path, file_list, true));
        ASSERT_GE(file_list.size(), 3);
        file_list.clear();

        path = "./file_utils_temp/";
        ASSERT_TRUE(dsn::utils::filesystem::get_subfiles(path, file_list, true));
        ASSERT_EQ(file_list.size(), 3);
        file_list.clear();

        path = "./file_utils_temp/";
        ASSERT_TRUE(dsn::utils::filesystem::get_subdirectories(path, file_list, true));
        ASSERT_EQ(file_list.size(), 7);
        file_list.clear();

        path = "./file_utils_temp/";
        ASSERT_TRUE(dsn::utils::filesystem::get_subdirectories(path, file_list, false));
        ASSERT_EQ(file_list.size(), 2);
        file_list.clear();

        path = "./file_utils_temp/";
        ASSERT_TRUE(dsn::utils::filesystem::get_subpaths(path, file_list, true));
        ASSERT_EQ(file_list.size(), 10);
        file_list.clear();

        path = "./file_utils_temp/";
        ASSERT_TRUE(dsn::utils::filesystem::get_subpaths(path, file_list, false));
        ASSERT_EQ(file_list.size(), 2);
        file_list.clear();

        path = "./file_utils_temp/a/";
        ASSERT_TRUE(dsn::utils::filesystem::get_subfiles(path, file_list, false));
        ASSERT_EQ(file_list.size(), 2);
        file_list.clear();

        path = "./file_utils_temp/a/";
        ASSERT_TRUE(dsn::utils::filesystem::get_subpaths(path, file_list, false));
        ASSERT_EQ(file_list.size(), 3);
        file_list.clear();
    }

    void file_utils_test_rename()
    {
        std::string path = "./file_utils_temp/b/c/d/1.txt";
        std::string path2 = "./file_utils_temp/b/c/d/2.txt";
        ASSERT_TRUE(dsn::utils::filesystem::rename_path(path, path2));
        ASSERT_FALSE(dsn::utils::filesystem::file_exists(path));
        ASSERT_TRUE(dsn::utils::filesystem::file_exists(path2));
        ASSERT_FALSE(dsn::utils::filesystem::rename_path(path, path2));
    }

    void file_utils_test_remove()
    {
        std::string path = "./file_utils_temp.txt";
        ASSERT_TRUE(dsn::utils::filesystem::remove_path(path));
        ASSERT_FALSE(dsn::utils::filesystem::file_exists(path));

        path = "./file_utils_temp/a/2.txt";
        ASSERT_TRUE(dsn::utils::filesystem::remove_path(path));
        ASSERT_TRUE(dsn::utils::filesystem::remove_path(path));

        path = "./file_utils_temp/";
        ASSERT_TRUE(dsn::utils::filesystem::remove_path(path));
        ASSERT_FALSE(dsn::utils::filesystem::directory_exists(path));
    }

    void file_utils_test_cleanup() {}
};

INSTANTIATE_TEST_SUITE_P(, file_utils, ::testing::Values(false, true));

TEST_P(file_utils, basic)
{
    file_utils_test_setup();
    file_utils_test_get_process_image_path();
    file_utils_test_get_normalized_path();
    file_utils_test_get_current_directory();
    file_utils_test_path_combine();
    file_utils_test_get_file_name();
    file_utils_test_create();
    file_utils_test_file_size();
    file_utils_test_path_exists();
    file_utils_test_get_paths();
    file_utils_test_rename();
    file_utils_test_remove();
    file_utils_test_cleanup();
}
