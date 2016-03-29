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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include <dsn/cpp/utils.h>
# include <gtest/gtest.h>
# include <iostream>
# include <vector>
# include "stdlib.h"

//#define DSN_IDL_TESTS_DEBUG

enum Language {lang_cpp, lang_csharp};
enum IDL{idl_protobuf, idl_thrift};
enum Format{format_binary, format_json};

std::string DSN_ROOT;
std::string RESOURCE_ROOT;

std::string file(const std::string &val)
{
    std::string nval;
    dsn::utils::filesystem::get_normalized_path(val, nval);
    return nval;
}

std::string file(const char* val)
{
    return file(std::string(val));
}

std::string combine(const std::string &dir, const char* sub)
{
    return dsn::utils::filesystem::path_combine(file(dir), file(sub));
}

std::string combine(const std::string &dir,const std::string &sub)
{
    return dsn::utils::filesystem::path_combine(file(dir), file(sub));
}

void execute(std::string cmd, bool &result)
{
#ifdef DSN_IDL_TESTS_DEBUG
    std::cout << cmd << std::endl;
#else
    cmd = cmd + std::string(" >> log");
#endif
    bool ret = !((bool)system(cmd.c_str()));
    result = result && ret;
}

void copy_file(const std::string& src, const std::string& dst, bool &result)
{
#ifdef _WIN32
    std::string cmd = std::string("copy /Y ");
#else
    std::string cmd = std::string("cp -f ");
#endif
    cmd += src + " " + dst;
    execute(cmd, result);
}

void create_dir(const char* dir, bool &result)
{
    bool ret = dsn::utils::filesystem::create_directory(file(dir));
    result = result && ret;
}

void rm_dir(const char* dir, bool &result)
{
#ifdef _WIN32
    std::string cmd = std::string("rd /S /Q ") + file(dir);
#else
    std::string cmd = std::string("rm -rf ") + file(dir);
#endif
    execute(cmd, result);
}

void cmake(Language lang, bool &result)
{
    create_dir("builder", result);
    std::string cmake_cmd = std::string("cd builder && cmake ") + file("../src");
#ifdef _WIN32
    cmake_cmd += std::string(" -DCMAKE_GENERATOR_PLATFORM=x64");
#endif
    execute(cmake_cmd, result);
    if (lang == lang_cpp)
    {
#ifdef _WIN32
        execute(std::string("msbuild ") + file("builder/counter.sln"), result);
        execute(file("builder/bin/counter/Debug/counter.exe") + " " + file("builder/bin/counter/config.ini"), result);
#else
        execute(std::string("cd builder && make "), result);
        execute(file("builder/bin/counter/counter") + " " + file("builder/bin/counter/config.ini"), result);
#endif
    }
    else
    {
        execute(file("builder/bin/counter/counter.exe") + " " + file("builder/bin/counter/config.ini"), result);
    }
}

bool test_code_generation(Language lang, IDL idl, Format format)
{
    bool result = true;
#ifdef _WIN32
    std::string codegen_bash("bin/dsn.cg.bat");
#else
    std::string codegen_bash("bin/dsn.cg.sh");
#endif
    std::string codegen_cmd = combine(DSN_ROOT, codegen_bash)\
        + std::string(" counter.")\
        + (idl == idl_protobuf ? "proto" : "thrift")\
        + (lang == lang_cpp ? " cpp" : " csharp")\
        + " src "\
        + (format == format_binary ? "binary" : "json")\
        + " single";
    create_dir("src", result);
    execute(codegen_cmd, result);
    std::vector<std::string> src_files;
    std::string src_subdir(idl == idl_protobuf ? "protobuf" : "thrift");
    std::string src_root(lang == lang_cpp ? "repo/cpp" : "repo/csharp");
    if (lang == lang_cpp)
    {
        src_files.push_back("counter.main.cpp");
        src_files.push_back("test.common.cpp");
        src_files.push_back("test.common.h");
        src_files.push_back("config.ini");
        src_files.push_back(combine(src_subdir, "counter.app.example.h"));
        src_files.push_back(combine(src_subdir, "counter.server.impl.h"));
    } else
    {
        src_files.push_back("counter.main.cs");
        src_files.push_back("test.helper.cs");
        src_files.push_back("config.ini");
        src_files.push_back(combine(src_subdir, "counter.app.example.cs"));
        src_files.push_back(combine(src_subdir, "counter.server.impl.cs"));
    }
    for (auto i : src_files)
    {
        copy_file(combine(combine(RESOURCE_ROOT, src_root), i), file("src"), result);
    }
    cmake(lang, result);
    rm_dir("data", result);
    rm_dir("builder", result);
    rm_dir("src", result);
    return result;
}

bool prepare()
{
    bool ret = true;
    std::vector<std::string> idl_files;
    idl_files.push_back("repo/counter.proto");
    idl_files.push_back("repo/counter.proto.annotations");
    idl_files.push_back("repo/counter.thrift");
    idl_files.push_back("repo/counter.thrift.annotations");
    for (auto i : idl_files)
    {
        copy_file(combine(RESOURCE_ROOT, i), file("./"), ret);
    }
    return ret;
}

TEST(TEST_PROTOBUF_HELPER, CPP_BINARY)
{
    EXPECT_TRUE(test_code_generation(lang_cpp, idl_protobuf, format_binary));
}

TEST(TEST_PROTOBUF_HELPER, CPP_JSON)
{
    EXPECT_TRUE(test_code_generation(lang_cpp, idl_protobuf, format_json));
}

#ifdef WIN32

TEST(TEST_PROTOBUF_HELPER, CSHARP_BINARY)
{
    EXPECT_TRUE(test_code_generation(lang_csharp, idl_protobuf, format_binary));
}

TEST(TEST_PROTOBUF_HELPER, CSHARP_JSON)
{
    EXPECT_TRUE(test_code_generation(lang_csharp, idl_protobuf, format_json));
}

#endif

TEST(TEST_THRIFT_HELPER, CPP_BINARY)
{
    EXPECT_TRUE(test_code_generation(lang_cpp, idl_thrift, format_binary));
}

TEST(TEST_THRIFT_HELPER, CPP_JSON)
{
    EXPECT_TRUE(test_code_generation(lang_cpp, idl_thrift, format_json));
}

#ifdef WIN32

TEST(TEST_THRIFT_HELPER, CSHARP_BINARY)
{
    EXPECT_TRUE(test_code_generation(lang_csharp, idl_thrift, format_binary));
}

TEST(TEST_THRIFT_HELPER, CSHARP_JSON)
{
    EXPECT_TRUE(test_code_generation(lang_csharp, idl_thrift, format_json));
}

#endif

GTEST_API_ int main(int argc, char **argv)
{
    if (argc != 3)
    {
        std::cout << "invalid parameters" << std::endl;
        return 1;
    }
    // DSN_ROOT is the path where directory "rdsn" exists
    // RESOURCE_ROOT is the path where directory "repo" exists
    DSN_ROOT = std::string(argv[1]);
    RESOURCE_ROOT = std::string(argv[2]);
    if (!prepare())
    {
        return 1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
