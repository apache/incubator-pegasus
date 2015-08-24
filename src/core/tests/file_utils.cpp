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

# include <gtest/gtest.h>
# include <dsn/cpp/utils.h>


static void test_setup()
{
	std::string path;
	bool ret;

	path = "./file_utils_temp.txt";
	ret = dsn::utils::remove(path);
	EXPECT_TRUE(ret);
	ret = dsn::utils::file_exists(path);
	EXPECT_FALSE(ret);

	path = "./file_utils_temp";
	ret = dsn::utils::remove(path);
	ret = dsn::utils::directory_exists(path);
	EXPECT_FALSE(ret);
}

static void test_get_normalized_path()
{
	bool ret;
	std::string path;
	std::string npath;
	
	path = "\\\\?\\";
	ret = dsn::utils::get_normalized_path(path, npath);
    EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == path);

	path = "c:\\";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == path);

	path = "c:";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == path);

	path = "\\\\?\\c:\\";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == path);

	path = "\\\\?\\c:";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == path);

	path = "c:\\a";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == path);

	path = "c:\\\\a";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "c:\\a");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "c:\\\\a\\";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "c:\\a");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "c:\\\\a\\\\";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "c:\\a");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "\\\\?\\c:\\a";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == path);

	path = "\\\\?\\c:\\\\a";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\?\\c:\\a");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "\\\\?\\c:\\\\a\\";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\?\\c:\\a");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "\\\\?\\c:\\\\a\\\\";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\?\\c:\\a");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "\\";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == path);

	path = "\\\\";
		ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == path);

	path = "\\\\\\";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "\\\\a";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == path);

	path = "\\\\\\a";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\a");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "\\\\a\\";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\a");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "\\\\\\a\\";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\a");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "\\\\\\a\\\\";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\a");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "/";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "//";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\");
#else
	EXPECT_TRUE(npath == "/");
#endif

	path = "//?/";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\?\\");
#else
	EXPECT_TRUE(npath == "/?");
#endif

	path = "//a";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\a");
#else
	EXPECT_TRUE(npath == "/a");
#endif

	path = "//a/";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\a");
#else
	EXPECT_TRUE(npath == "/a");
#endif

	path = "//a//";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\a");
#else
	EXPECT_TRUE(npath == "/a");
#endif

	path = "c:/";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "c:\\");
#else
	EXPECT_TRUE(npath == "c:");
#endif

	path = "c://";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "c:\\");
#else
	EXPECT_TRUE(npath == "c:");
#endif

	path = "c:/a";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "c:\\a");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "c:/a/";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "c:\\a");
#else
	EXPECT_TRUE(npath == "c:/a");
#endif

	path = "c://a/";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "c:\\a");
#else
	EXPECT_TRUE(npath == "c:/a");
#endif

	path = "c://a//";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "c:\\a");
#else
	EXPECT_TRUE(npath == "c:/a");
#endif

	path = "/////////////////////////////////////////////////////////////////";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\");
#else
	EXPECT_TRUE(npath == "/");
#endif


	path = "/////////////////////////////////////////////////////////////////a/////////////////b///////////";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "\\\\a\\b");
#else
	EXPECT_TRUE(npath == "/a/b");
#endif

	path = ".";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == path);

	path = "./";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == ".");

	path = "./a";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == ".\\a");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = ".//a";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == ".\\a");
#else
	EXPECT_TRUE(npath == "./a");
#endif

	path = "./a/";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == ".\\a");
#else
	EXPECT_TRUE(npath == "./a");
#endif

	path = "./a/b";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == ".\\a\\b");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "./a/b/";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == ".\\a\\b");
#else
	EXPECT_TRUE(npath == "./a/b");
#endif

	path = ".///a////b///";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == ".\\a\\b");
#else
	EXPECT_TRUE(npath == "./a/b");
#endif

	path = "..";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == path);

	path = "../";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(npath == "..");

	path = "../a";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "..\\a");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "..//a";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "..\\a");
#else
	EXPECT_TRUE(npath == "../a");
#endif

	path = "../a/";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "..\\a");
#else
	EXPECT_TRUE(npath == "../a");
#endif

	path = "../a/b";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "..\\a\\b");
#else
	EXPECT_TRUE(npath == path);
#endif

	path = "../a/b/";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "..\\a\\b");
#else
	EXPECT_TRUE(npath == "../a/b");
#endif

	path = "..///a////b///";
	ret = dsn::utils::get_normalized_path(path, npath);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(npath == "..\\a\\b");
#else
	EXPECT_TRUE(npath == "../a/b");
#endif
}

static void test_create()
{
	std::string path;
	bool ret;

	path = "./file_utils_temp.txt";
	ret = dsn::utils::create_file(path);
	EXPECT_TRUE(ret);
	ret = dsn::utils::file_exists(path);
	EXPECT_TRUE(ret);

	path = "./file_utils_temp";
	ret = dsn::utils::create_directory(path);
	EXPECT_TRUE(ret);
	ret = dsn::utils::directory_exists(path);
	EXPECT_TRUE(ret);

	path = "./file_utils_temp/a/b/c/d//";
	ret = dsn::utils::create_directory(path);
	EXPECT_TRUE(ret);
	ret = dsn::utils::directory_exists(path);
	EXPECT_TRUE(ret);

	path = "./file_utils_temp/a/1.txt";
	ret = dsn::utils::create_file(path);
	EXPECT_TRUE(ret);
	ret = dsn::utils::file_exists(path);
	EXPECT_TRUE(ret);

	path = "./file_utils_temp/a/1.txt";
	ret = dsn::utils::create_file(path);
	EXPECT_TRUE(ret);
	ret = dsn::utils::file_exists(path);
	EXPECT_TRUE(ret);

	path = "./file_utils_temp/a/2.txt";
	ret = dsn::utils::create_file(path);
	EXPECT_TRUE(ret);
	ret = dsn::utils::file_exists(path);
	EXPECT_TRUE(ret);

	path = "./file_utils_temp/b/c/d/1.txt";
	ret = dsn::utils::create_file(path);
	EXPECT_TRUE(ret);
	ret = dsn::utils::file_exists(path);
	EXPECT_TRUE(ret);
}

static void test_path_exists()
{
	std::string path;
	bool ret;

	path = "c:\\";
	ret = dsn::utils::path_exists(path);
#ifdef _WIN32
	EXPECT_TRUE(ret);
#else
	EXPECT_FALSE(ret);
#endif

	path = "c:\\";
	ret = dsn::utils::directory_exists(path);
#ifdef _WIN32
	EXPECT_TRUE(ret);
#else
	EXPECT_FALSE(ret);
#endif

	path = "c:\\";
	ret = dsn::utils::file_exists(path);
#ifdef _WIN32
	EXPECT_FALSE(ret);
#else
	EXPECT_FALSE(ret);
#endif

	path = "/";
	ret = dsn::utils::path_exists(path);
	EXPECT_TRUE(ret);

	path = "/";
	ret = dsn::utils::directory_exists(path);
	EXPECT_TRUE(ret);

	path = "/";
	ret = dsn::utils::file_exists(path);
	EXPECT_FALSE(ret);

	path = "./not_exists_not_exists";
	ret = dsn::utils::path_exists(path);
	EXPECT_FALSE(ret);

	path = "c:\\Windows\\System32\\notepad.exe";
	ret = dsn::utils::path_exists(path);
#ifdef _WIN32
	EXPECT_TRUE(ret);
#else
	EXPECT_FALSE(ret);
#endif

	path = "c:\\Windows\\System32\\notepad.exe";
	ret = dsn::utils::directory_exists(path);
	EXPECT_FALSE(ret);

	path = "c:\\Windows\\System32\\notepad.exe";
	ret = dsn::utils::file_exists(path);
#ifdef _WIN32
	EXPECT_TRUE(ret);
#else
	EXPECT_FALSE(ret);
#endif

	path = "/bin/ls";
	ret = dsn::utils::path_exists(path);
#ifdef _WIN32
	EXPECT_FALSE(ret);
#else
	EXPECT_TRUE(ret);
#endif

	path = "/bin/ls";
	ret = dsn::utils::directory_exists(path);
	EXPECT_FALSE(ret);

	path = "/bin/ls";
	ret = dsn::utils::file_exists(path);
#ifdef _WIN32
	EXPECT_FALSE(ret);
#else
	EXPECT_TRUE(ret);
#endif
}

static void test_get_files()
{
	std::string path;
	bool ret;
	std::vector<std::string> file_list;

	path = ".";
	ret = dsn::utils::get_files(path, file_list, false);
	EXPECT_TRUE(ret);
#ifdef _WIN32
	EXPECT_TRUE(file_list.size() >= 3);
#else
	EXPECT_TRUE(file_list.size() >= 2);
#endif
	file_list.clear();

	path = ".";
	ret = dsn::utils::get_files(path, file_list, true);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(file_list.size() >= 3);
	file_list.clear();

	path = "../../";
	ret = dsn::utils::get_files(path, file_list, true);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(file_list.size() >= 3);
	file_list.clear();

	path = "./file_utils_temp/";
	ret = dsn::utils::get_files(path, file_list, true);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(file_list.size() == 3);
	file_list.clear();

	path = "./file_utils_temp/a/";
	ret = dsn::utils::get_files(path, file_list, false);
	EXPECT_TRUE(ret);
	EXPECT_TRUE(file_list.size() == 2);
	file_list.clear();
}

static void test_remove()
{
	std::string path;
	std::vector<std::string> file_list;
	bool ret;

	path = "./file_utils_temp.txt";
	ret = dsn::utils::remove(path);
	EXPECT_TRUE(ret);
	ret = dsn::utils::file_exists(path);
	EXPECT_FALSE(ret);

	path = "./file_utils_temp/a/2.txt";
	ret = dsn::utils::remove(path);
	EXPECT_TRUE(ret);
	ret = dsn::utils::remove(path);
	EXPECT_TRUE(ret);

	path = "./file_utils_temp/";
	ret = dsn::utils::remove(path);
	EXPECT_TRUE(ret);
	ret = dsn::utils::directory_exists(path);
	EXPECT_FALSE(ret);
}

static void test_cleanup()
{
}

TEST(core, file_utils_test)
{
	test_setup();
	test_get_normalized_path();
	test_create();
	test_path_exists();
	test_get_files();
	test_remove();
	test_cleanup();
}