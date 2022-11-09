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

#include <gtest/gtest.h>
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/rpc_address.h"
#include "utils/utils.h"
#include "utils/filesystem.h"
#include <fstream>

static void file_utils_test_setup()
{
    std::string path;
    bool ret;

    path = "./file_utils_temp.txt";
    ret = dsn::utils::filesystem::remove_path(path);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::file_exists(path);
    EXPECT_FALSE(ret);

    path = "./file_utils_temp";
    ret = dsn::utils::filesystem::remove_path(path);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::directory_exists(path);
    EXPECT_FALSE(ret);
}

static void file_utils_test_get_process_image_path()
{
    std::string path;
    std::string imagepath;
    dsn::error_code ret;
    // int pid;

    if (!dsn::utils::filesystem::get_current_directory(imagepath)) {
        EXPECT_TRUE(false);
    }
    imagepath = dsn::utils::filesystem::path_combine(imagepath, "dsn_utils_tests");

    ret = dsn::utils::filesystem::get_current_process_image_path(path);
    EXPECT_TRUE(ret == dsn::ERR_OK);
    // TODO: not always true when running dir is not where the test resides
    // EXPECT_TRUE(path == imagepath); // e: vs E:
}

static void file_utils_test_get_normalized_path()
{
    int ret;
    std::string path;
    std::string npath;

    path = "\\\\?\\";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == path);

    path = "c:\\";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == path);

    path = "c:";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == path);

    path = "\\\\?\\c:\\";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == path);

    path = "\\\\?\\c:";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == path);

    path = "c:\\a";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == path);

    path = "c:\\\\a";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "c:\\a");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "c:\\\\a\\";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "c:\\a");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "c:\\\\a\\\\";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "c:\\a");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "\\\\?\\c:\\a";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == path);

    path = "\\\\?\\c:\\\\a";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\?\\c:\\a");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "\\\\?\\c:\\\\a\\";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\?\\c:\\a");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "\\\\?\\c:\\\\a\\\\";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\?\\c:\\a");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "\\";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == path);

    path = "\\\\";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == path);

    path = "\\\\\\";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "\\\\a";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == path);

    path = "\\\\\\a";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\a");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "\\\\a\\";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\a");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "\\\\\\a\\";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\a");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "\\\\\\a\\\\";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\a");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "/";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "//";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\");
#else
    EXPECT_TRUE(npath == "/");
#endif

    path = "//?/";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\?\\");
#else
    EXPECT_TRUE(npath == "/?");
#endif

    path = "//a";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\a");
#else
    EXPECT_TRUE(npath == "/a");
#endif

    path = "//a/";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\a");
#else
    EXPECT_TRUE(npath == "/a");
#endif

    path = "//a//";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\a");
#else
    EXPECT_TRUE(npath == "/a");
#endif

    path = "c:/";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "c:\\");
#else
    EXPECT_TRUE(npath == "c:");
#endif

    path = "c://";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "c:\\");
#else
    EXPECT_TRUE(npath == "c:");
#endif

    path = "c:/a";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "c:\\a");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "c:/a/";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "c:\\a");
#else
    EXPECT_TRUE(npath == "c:/a");
#endif

    path = "c://a/";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "c:\\a");
#else
    EXPECT_TRUE(npath == "c:/a");
#endif

    path = "c://a//";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "c:\\a");
#else
    EXPECT_TRUE(npath == "c:/a");
#endif

    path = "/////////////////////////////////////////////////////////////////";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\");
#else
    EXPECT_TRUE(npath == "/");
#endif

    path = "/////////////////////////////////////////////////////////////////a/////////////////b///"
           "////////";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "\\\\a\\b");
#else
    EXPECT_TRUE(npath == "/a/b");
#endif

    path = ".";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == path);

    path = "./";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == ".");

    path = "./a";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == ".\\a");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = ".//a";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == ".\\a");
#else
    EXPECT_TRUE(npath == "./a");
#endif

    path = "./a/";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == ".\\a");
#else
    EXPECT_TRUE(npath == "./a");
#endif

    path = "./a/b";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == ".\\a\\b");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "./a/b/";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == ".\\a\\b");
#else
    EXPECT_TRUE(npath == "./a/b");
#endif

    path = ".///a////b///";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == ".\\a\\b");
#else
    EXPECT_TRUE(npath == "./a/b");
#endif

    path = "..";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == path);

    path = "../";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
    EXPECT_TRUE(npath == "..");

    path = "../a";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "..\\a");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "..//a";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "..\\a");
#else
    EXPECT_TRUE(npath == "../a");
#endif

    path = "../a/";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "..\\a");
#else
    EXPECT_TRUE(npath == "../a");
#endif

    path = "../a/b";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "..\\a\\b");
#else
    EXPECT_TRUE(npath == path);
#endif

    path = "../a/b/";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "..\\a\\b");
#else
    EXPECT_TRUE(npath == "../a/b");
#endif

    path = "..///a////b///";
    ret = dsn::utils::filesystem::get_normalized_path(path, npath);
    EXPECT_TRUE(ret == 0);
#ifdef _WIN32
    EXPECT_TRUE(npath == "..\\a\\b");
#else
    EXPECT_TRUE(npath == "../a/b");
#endif
}

static void file_utils_test_get_current_directory()
{
    std::string path;
    bool ret;

    path = "";
    ret = dsn::utils::filesystem::get_current_directory(path);
    EXPECT_TRUE(ret);
    EXPECT_TRUE(!path.empty());
}

static void file_utils_test_path_combine()
{
    std::string path;
    std::string path1;
    std::string path2;

    path1 = "";
    path2 = "";
    path = dsn::utils::filesystem::path_combine(path1, path2);
    EXPECT_TRUE(path == "");

    path1 = "c:";
    path2 = "Windows\\explorer.exe";
    path = dsn::utils::filesystem::path_combine(path1, path2);
#ifdef _WIN32
    EXPECT_TRUE(path == "c:Windows\\explorer.exe");
#else
    EXPECT_TRUE(path == "c:/Windows\\explorer.exe");
#endif

    path1 = "c:";
    path2 = "\\Windows\\explorer.exe";
    path = dsn::utils::filesystem::path_combine(path1, path2);
#ifdef _WIN32
    EXPECT_TRUE(path == "c:\\Windows\\explorer.exe");
#else
    EXPECT_TRUE(path == "c:/Windows\\explorer.exe");
#endif

    path1 = "c:\\";
    path2 = "\\Windows\\explorer.exe";
    path = dsn::utils::filesystem::path_combine(path1, path2);
#ifdef _WIN32
    EXPECT_TRUE(path == "c:\\Windows\\explorer.exe");
#else
    EXPECT_TRUE(path == "c:\\/Windows\\explorer.exe");
#endif

    path1 = "/bin";
    path2 = "ls";
    path = dsn::utils::filesystem::path_combine(path1, path2);
#ifdef _WIN32
    EXPECT_TRUE(path == "\\bin\\ls");
#else
    EXPECT_TRUE(path == "/bin/ls");
#endif

    path1 = "/bin/";
    path2 = "ls";
    path = dsn::utils::filesystem::path_combine(path1, path2);
#ifdef _WIN32
    EXPECT_TRUE(path == "\\bin\\ls");
#else
    EXPECT_TRUE(path == "/bin/ls");
#endif

    path1 = "/bin";
    path2 = "/ls";
    path = dsn::utils::filesystem::path_combine(path1, path2);
#ifdef _WIN32
    EXPECT_TRUE(path == "\\bin\\ls");
#else
    EXPECT_TRUE(path == "/bin/ls");
#endif

    path1 = "/bin/";
    path2 = "/ls";
    path = dsn::utils::filesystem::path_combine(path1, path2);
#ifdef _WIN32
    EXPECT_TRUE(path == "\\bin\\ls");
#else
    EXPECT_TRUE(path == "/bin/ls");
#endif
}

static void file_utils_test_get_file_name()
{
    std::string path1;
    std::string path2;

    path1 = "";
    path2 = dsn::utils::filesystem::get_file_name(path1);
    EXPECT_TRUE(path2 == "");

    path1 = "c:";
    path2 = dsn::utils::filesystem::get_file_name(path1);
#ifdef _WIN32
    EXPECT_TRUE(path2 == "");
#else
    EXPECT_TRUE(path2 == "c:");
#endif

    path1 = "c:\\";
    path2 = dsn::utils::filesystem::get_file_name(path1);
    EXPECT_TRUE(path2 == "");

    path1 = "c:1.txt";
    path2 = dsn::utils::filesystem::get_file_name(path1);
#ifdef _WIN32
    EXPECT_TRUE(path2 == "1.txt");
#else
    EXPECT_TRUE(path2 == "c:1.txt");
#endif

    path1 = "c:\\1.txt";
    path2 = dsn::utils::filesystem::get_file_name(path1);
    EXPECT_TRUE(path2 == "1.txt");

    path1 = "c:\\Windows\\1.txt";
    path2 = dsn::utils::filesystem::get_file_name(path1);
    EXPECT_TRUE(path2 == "1.txt");

    path1 = "/bin/";
    path2 = dsn::utils::filesystem::get_file_name(path1);
    EXPECT_TRUE(path2 == "");

    path1 = "/bin/ls";
    path2 = dsn::utils::filesystem::get_file_name(path1);
    EXPECT_TRUE(path2 == "ls");
}

static void file_utils_test_create()
{
    std::string path;
    bool ret;

    path = "./file_utils_temp.txt";
    ret = dsn::utils::filesystem::create_file(path);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::file_exists(path);
    EXPECT_TRUE(ret);

    time_t current_time = ::time(nullptr);
    EXPECT_TRUE(current_time != 1);

    std::ofstream myfile(path.c_str(), std::ios::out | std::ios::app | std::ios::binary);
    EXPECT_TRUE(myfile.is_open());
    myfile << "Hello world!";
    myfile.close();

    time_t last_write_time;
    ret = dsn::utils::filesystem::last_write_time(path, last_write_time);
    EXPECT_TRUE(ret);
    EXPECT_TRUE((last_write_time != -1) && (last_write_time >= current_time));

    path = "./file_utils_temp";
    ret = dsn::utils::filesystem::create_directory(path);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::directory_exists(path);
    EXPECT_TRUE(ret);

    path = "./file_utils_temp/a/b/c/d//";
    ret = dsn::utils::filesystem::create_directory(path);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::directory_exists(path);
    EXPECT_TRUE(ret);

    path = "./file_utils_temp/a/1.txt";
    ret = dsn::utils::filesystem::create_file(path);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::file_exists(path);
    EXPECT_TRUE(ret);

    path = "./file_utils_temp/a/1.txt";
    ret = dsn::utils::filesystem::create_file(path);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::file_exists(path);
    EXPECT_TRUE(ret);

    path = "./file_utils_temp/a/2.txt";
    ret = dsn::utils::filesystem::create_file(path);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::file_exists(path);
    EXPECT_TRUE(ret);

    path = "./file_utils_temp/b/c/d/1.txt";
    ret = dsn::utils::filesystem::create_file(path);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::file_exists(path);
    EXPECT_TRUE(ret);
}

static void file_utils_test_file_size()
{
    std::string path;
    int64_t sz;
    bool ret;

    path = "./file_utils_temp.txt";
    ret = dsn::utils::filesystem::file_size(path, sz);
    EXPECT_TRUE(ret);
    EXPECT_TRUE(sz == 12);

    path = "./file_utils_temp2.txt";
    ret = dsn::utils::filesystem::file_size(path, sz);
    EXPECT_FALSE(ret);
}

static void file_utils_test_path_exists()
{
    std::string path;
    bool ret;

    path = "c:\\";
    ret = dsn::utils::filesystem::path_exists(path);
#ifdef _WIN32
    EXPECT_TRUE(ret);
#else
    EXPECT_FALSE(ret);
#endif

    path = "c:\\";
    ret = dsn::utils::filesystem::directory_exists(path);
#ifdef _WIN32
    EXPECT_TRUE(ret);
#else
    EXPECT_FALSE(ret);
#endif

    path = "c:\\";
    ret = dsn::utils::filesystem::file_exists(path);
#ifdef _WIN32
    EXPECT_FALSE(ret);
#else
    EXPECT_FALSE(ret);
#endif

    path = "/";
    ret = dsn::utils::filesystem::path_exists(path);
    EXPECT_TRUE(ret);

    path = "/";
    ret = dsn::utils::filesystem::directory_exists(path);
    EXPECT_TRUE(ret);

    path = "/";
    ret = dsn::utils::filesystem::file_exists(path);
    EXPECT_FALSE(ret);

    path = "./not_exists_not_exists";
    ret = dsn::utils::filesystem::path_exists(path);
    EXPECT_FALSE(ret);

    path = "c:\\Windows\\System32\\notepad.exe";
    ret = dsn::utils::filesystem::path_exists(path);
#ifdef _WIN32
    EXPECT_TRUE(ret);
#else
    EXPECT_FALSE(ret);
#endif

    path = "c:\\Windows\\System32\\notepad.exe";
    ret = dsn::utils::filesystem::directory_exists(path);
    EXPECT_FALSE(ret);

    path = "c:\\Windows\\System32\\notepad.exe";
    ret = dsn::utils::filesystem::file_exists(path);
#ifdef _WIN32
    EXPECT_TRUE(ret);
#else
    EXPECT_FALSE(ret);
#endif

    path = "/bin/ls";
    ret = dsn::utils::filesystem::path_exists(path);
#ifdef _WIN32
    EXPECT_FALSE(ret);
#else
    EXPECT_TRUE(ret);
#endif

    path = "/bin/ls";
    ret = dsn::utils::filesystem::directory_exists(path);
    EXPECT_FALSE(ret);

    path = "/bin/ls";
    ret = dsn::utils::filesystem::file_exists(path);
#ifdef _WIN32
    EXPECT_FALSE(ret);
#else
    EXPECT_TRUE(ret);
#endif
}

static void file_utils_test_get_paths()
{
    std::string path;
    bool ret;
    std::vector<std::string> file_list;

    path = ".";
    ret = dsn::utils::filesystem::get_subfiles(path, file_list, false);
    EXPECT_TRUE(ret);
#ifdef _WIN32
    EXPECT_TRUE(file_list.size() >= 3);
#else
    EXPECT_TRUE(file_list.size() >= 2);
#endif
    file_list.clear();

    path = ".";
    ret = dsn::utils::filesystem::get_subfiles(path, file_list, true);
    EXPECT_TRUE(ret);
    EXPECT_TRUE(file_list.size() >= 3);
    file_list.clear();

    path = "../../";
    ret = dsn::utils::filesystem::get_subfiles(path, file_list, true);
    EXPECT_TRUE(ret);
    EXPECT_TRUE(file_list.size() >= 3);
    file_list.clear();

    path = "./file_utils_temp/";
    ret = dsn::utils::filesystem::get_subfiles(path, file_list, true);
    EXPECT_TRUE(ret);
    EXPECT_TRUE(file_list.size() == 3);
    file_list.clear();

    path = "./file_utils_temp/";
    ret = dsn::utils::filesystem::get_subdirectories(path, file_list, true);
    EXPECT_TRUE(ret);
    EXPECT_TRUE(file_list.size() == 7);
    file_list.clear();

    path = "./file_utils_temp/";
    ret = dsn::utils::filesystem::get_subdirectories(path, file_list, false);
    EXPECT_TRUE(ret);
    EXPECT_TRUE(file_list.size() == 2);
    file_list.clear();

    path = "./file_utils_temp/";
    ret = dsn::utils::filesystem::get_subpaths(path, file_list, true);
    EXPECT_TRUE(ret);
    EXPECT_TRUE(file_list.size() == 10);
    file_list.clear();

    path = "./file_utils_temp/";
    ret = dsn::utils::filesystem::get_subpaths(path, file_list, false);
    EXPECT_TRUE(ret);
    EXPECT_TRUE(file_list.size() == 2);
    file_list.clear();

    path = "./file_utils_temp/a/";
    ret = dsn::utils::filesystem::get_subfiles(path, file_list, false);
    EXPECT_TRUE(ret);
    EXPECT_TRUE(file_list.size() == 2);
    file_list.clear();

    path = "./file_utils_temp/a/";
    ret = dsn::utils::filesystem::get_subpaths(path, file_list, false);
    EXPECT_TRUE(ret);
    EXPECT_TRUE(file_list.size() == 3);
    file_list.clear();
}

static void file_utils_test_rename()
{
    std::string path;
    std::string path2;
    bool ret;

    path = "./file_utils_temp/b/c/d/1.txt";
    path2 = "./file_utils_temp/b/c/d/2.txt";
    ret = dsn::utils::filesystem::rename_path(path, path2);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::file_exists(path);
    EXPECT_FALSE(ret);
    ret = dsn::utils::filesystem::file_exists(path2);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::rename_path(path, path2);
    EXPECT_FALSE(ret);
}

static void file_utils_test_remove()
{
    std::string path;
    std::vector<std::string> file_list;
    bool ret;

    path = "./file_utils_temp.txt";
    ret = dsn::utils::filesystem::remove_path(path);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::file_exists(path);
    EXPECT_FALSE(ret);

    path = "./file_utils_temp/a/2.txt";
    ret = dsn::utils::filesystem::remove_path(path);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::remove_path(path);
    EXPECT_TRUE(ret);

    path = "./file_utils_temp/";
    ret = dsn::utils::filesystem::remove_path(path);
    EXPECT_TRUE(ret);
    ret = dsn::utils::filesystem::directory_exists(path);
    EXPECT_FALSE(ret);
}

static void file_utils_test_cleanup() {}

TEST(core, file_utils)
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
