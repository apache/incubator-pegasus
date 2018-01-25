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

#include <dsn/utility/utils.h>
#include <dsn/utility/filesystem.h>
#include <gtest/gtest.h>
#include <dsn/cpp/serialization.h>

#include "idl_test.types.h"

#include <iostream>
#include <vector>
#include "stdlib.h"

//#define DSN_IDL_TESTS_DEBUG

enum Language
{
    lang_cpp,
    lang_csharp
};
enum IDL
{
    idl_protobuf,
    idl_thrift
};
enum Format
{
    format_binary,
    format_json
};

std::string DSN_ROOT;
std::string RESOURCE_ROOT;

std::string file(const std::string &val)
{
    std::string nval;
    dsn::utils::filesystem::get_normalized_path(val, nval);
    return nval;
}

std::string file(const char *val) { return file(std::string(val)); }

std::string combine(const std::string &dir, const char *sub)
{
    return dsn::utils::filesystem::path_combine(file(dir), file(sub));
}

std::string combine(const std::string &dir, const std::string &sub)
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

void copy_file(const std::string &src, const std::string &dst, bool &result)
{
#ifdef _WIN32
    std::string cmd = std::string("copy /Y ");
#else
    std::string cmd = std::string("cp -f ");
#endif
    cmd += src + " " + dst;
    execute(cmd, result);
}

void create_dir(const char *dir, bool &result)
{
    bool ret = dsn::utils::filesystem::create_directory(file(dir));
    result = result && ret;
}

void rm_dir(const char *dir, bool &result)
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
    if (lang == lang_cpp) {
#ifdef _WIN32
        execute(std::string("msbuild ") + file("builder/counter.sln"), result);
        execute(file("builder/bin/counter/Debug/counter.exe") + " " +
                    file("builder/bin/counter/config.ini"),
                result);
#else
        execute(std::string("cd builder && make "), result);
        execute(file("builder/bin/counter/counter") + " " + file("builder/bin/counter/config.ini"),
                result);
#endif
    } else {
        execute(file("builder/bin/counter/counter.exe") + " " +
                    file("builder/bin/counter/config.ini"),
                result);
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
    std::string codegen_cmd = combine(DSN_ROOT, codegen_bash) + std::string(" counter.") +
                              (idl == idl_protobuf ? "proto" : "thrift") +
                              (lang == lang_cpp ? " cpp" : " csharp") + " src " +
                              (format == format_binary ? "binary" : "json") + " single";
    create_dir("src", result);
    execute(codegen_cmd, result);
    std::vector<std::string> src_files;
    std::string src_root(lang == lang_cpp ? "repo/cpp" : "repo/csharp");
    if (lang == lang_cpp) {
        src_files.push_back("counter.main.cpp");
    } else {
        src_files.push_back("counter.main.cs");
    }
    for (auto i : src_files) {
        copy_file(combine(combine(RESOURCE_ROOT, src_root), i), file("src"), result);
    }
    cmake(lang, result);
    rm_dir("data", result);
    rm_dir("builder", result);
    rm_dir("src", result);
    return result;
}

template <typename T>
void thrift_basic_type_serialization_checker(std::vector<T> &data, Format fmt)
{
    const int bufsize = 2000;
    char buf[bufsize];
    for (const T &i : data) {
        T input = i;
        dsn::blob b(buf, 0, bufsize);
        dsn::binary_writer writer(b);
        if (fmt == format_binary) {
            dsn::marshall_thrift_binary(writer, input);
        } else {
            dsn::marshall_thrift_json(writer, input);
        }
        dsn::binary_reader reader(b);
        T output;
        if (fmt == format_binary) {
            dsn::unmarshall_thrift_binary(reader, output);
        } else {
            dsn::unmarshall_thrift_json(reader, output);
        }
        EXPECT_TRUE(input == output);
    }
}
void test_thrift_basic_type_serialization(Format fmt)
{
    std::vector<bool> data_bool_t{true, false};
    thrift_basic_type_serialization_checker(data_bool_t, fmt);

    std::vector<int8_t> data_int8_t{
        std::numeric_limits<int8_t>::min(), std::numeric_limits<int8_t>::max(), 0, 1, -1, 13, -13};
    thrift_basic_type_serialization_checker(data_int8_t, fmt);

    std::vector<int16_t> data_int16_t{std::numeric_limits<int16_t>::min(),
                                      std::numeric_limits<int16_t>::max(),
                                      0,
                                      1,
                                      -1,
                                      13,
                                      -13};
    thrift_basic_type_serialization_checker(data_int16_t, fmt);

    std::vector<int32_t> data_int32_t{std::numeric_limits<int32_t>::min(),
                                      std::numeric_limits<int32_t>::max(),
                                      0,
                                      1,
                                      -1,
                                      13,
                                      -13};
    thrift_basic_type_serialization_checker(data_int32_t, fmt);

    std::vector<int64_t> data_int64_t{std::numeric_limits<int64_t>::min(),
                                      std::numeric_limits<int64_t>::max(),
                                      0,
                                      1,
                                      -1,
                                      13,
                                      -13};
    thrift_basic_type_serialization_checker(data_int32_t, fmt);

    std::vector<std::string> data_string_t{
        std::string("hello"), std::string("world"), std::string("")};
    thrift_basic_type_serialization_checker(data_string_t, fmt);

    // TODO:: test double type

    std::vector<std::vector<int32_t>> data_vec_int32_t{data_int32_t, std::vector<int32_t>()};
    thrift_basic_type_serialization_checker(data_vec_int32_t, fmt);

    std::map<int, std::string> m1;
    m1[1] = "hello";
    m1[233] = "world";
    m1[-22] = "";
    std::map<int, std::string> m2;
    std::vector<std::map<int, std::string>> data_map_int32_str_t{m1, m2};
    thrift_basic_type_serialization_checker(data_map_int32_str_t, fmt);
}

void check_thrift_generated_type_serialization(const dsn::idl::test::test_thrift_item &input,
                                               Format fmt)
{
    const int bufsize = 2000;
    char buf[bufsize];
    dsn::blob b(buf, 0, bufsize);
    dsn::binary_writer writer(b);
    if (fmt == format_binary) {
        dsn::marshall_thrift_binary(writer, input);
    } else {
        dsn::marshall_thrift_json(writer, input);
    }
    dsn::binary_reader reader(b);
    dsn::idl::test::test_thrift_item output;
    if (fmt == format_binary) {
        dsn::unmarshall_thrift_binary(reader, output);
    } else {
        dsn::unmarshall_thrift_json(reader, output);
    }
    EXPECT_EQ(input.bool_item, output.bool_item);
    EXPECT_EQ(input.byte_item, output.byte_item);
    EXPECT_EQ(input.i16_item, output.i16_item);
    EXPECT_EQ(input.i32_item, output.i32_item);
    EXPECT_EQ(input.i64_item, output.i64_item);
    EXPECT_EQ(input.list_i32_item, output.list_i32_item);
    EXPECT_EQ(input.set_i32_item, output.set_i32_item);
    EXPECT_EQ(input.map_i32_item, output.map_i32_item);
    EXPECT_DOUBLE_EQ(input.double_item, output.double_item);
}

void test_thrift_generated_type_serialization(Format fmt)
{
    const int container_n = 10;
    dsn::idl::test::test_thrift_item item;

    item.bool_item = false;
    item.byte_item = 0;
    item.i16_item = 0;
    item.i32_item = 0;
    item.i64_item = 0;
    item.double_item = 0.0;
    check_thrift_generated_type_serialization(item, fmt);

    item.bool_item = true;
    item.byte_item = std::numeric_limits<int8_t>::max();
    item.i16_item = std::numeric_limits<int16_t>::max();
    item.i32_item = std::numeric_limits<int32_t>::max();
    item.i64_item = std::numeric_limits<int64_t>::max();
    item.double_item = 123.321;
    item.string_item = "hello world";
    for (int i = 0; i < container_n; i++) {
        item.list_i32_item.push_back(i);
    }
    for (int i = 0; i < container_n; i++) {
        item.set_i32_item.insert(item.set_i32_item.begin(), i + 1);
    }
    for (int i = 0; i < container_n; i++) {
        item.map_i32_item[i] = i * 2;
    }
    check_thrift_generated_type_serialization(item, fmt);
}

void check_protobuf_generated_type_serialization(const dsn::idl::test::test_protobuf_item &input,
                                                 Format fmt)
{
    const int bufsize = 2000;
    char buf[bufsize];
    dsn::blob b(buf, 0, bufsize);
    dsn::binary_writer writer(b);
    if (fmt == format_binary) {
        dsn::marshall_protobuf_binary(writer, input);
    } else {
        dsn::marshall_protobuf_json(writer, input);
    }
    dsn::binary_reader reader(b);
    dsn::idl::test::test_protobuf_item output;
    if (fmt == format_binary) {
        dsn::unmarshall_protobuf_binary(reader, output);
    } else {
        dsn::unmarshall_protobuf_json(reader, output);
    }
    EXPECT_EQ(input.bool_item(), output.bool_item());
    EXPECT_EQ(input.int32_item(), output.int32_item());
    EXPECT_EQ(input.int64_item(), output.int64_item());
    EXPECT_EQ(input.uint32_item(), output.uint32_item());
    EXPECT_EQ(input.uint64_item(), output.uint64_item());
    EXPECT_EQ(input.string_item(), output.string_item());
    EXPECT_FLOAT_EQ(input.float_item(), output.float_item());
    EXPECT_DOUBLE_EQ(input.double_item(), output.double_item());
    for (int i = 0; i < input.repeated_int32_item_size(); i++) {
        EXPECT_EQ(input.repeated_int32_item().Get(i), output.repeated_int32_item().Get(i));
    }
    EXPECT_EQ(input.map_int32_item_size(), output.map_int32_item_size());
    for (auto i = input.map_int32_item().begin(); i != input.map_int32_item().end(); i++) {
        EXPECT_TRUE(output.map_int32_item().find(i->first) != output.map_int32_item().end());
        if (output.map_int32_item().find(i->first) != output.map_int32_item().end()) {
            EXPECT_EQ(i->second, output.map_int32_item().at(i->first));
        }
    }
}

void test_protobuf_generated_type_serialization(Format fmt)
{
    const int container_n = 10;
    dsn::idl::test::test_protobuf_item item;

    check_protobuf_generated_type_serialization(item, fmt);

    item.set_bool_item(true);
    item.set_int32_item(std::numeric_limits<int32_t>::max());
    item.set_int64_item(std::numeric_limits<int64_t>::max());
    item.set_uint32_item(std::numeric_limits<uint32_t>::max());
    item.set_uint64_item(std::numeric_limits<uint64_t>::max());
    item.set_float_item(123.321);
    item.set_double_item(1234.4321);
    item.set_string_item("hello world");
    for (int i = 0; i < container_n; i++) {
        item.add_repeated_int32_item(i);
    }
    for (int i = 0; i < container_n; i++) {
        auto mp = item.mutable_map_int32_item();
        (*mp)[i] = 2 * i;
    }
    check_protobuf_generated_type_serialization(item, fmt);
}

bool prepare()
{
    bool ret = true;
    std::vector<std::string> idl_files;
    idl_files.push_back("repo/counter.proto");
    idl_files.push_back("repo/counter.proto.annotations");
    idl_files.push_back("repo/counter.thrift");
    idl_files.push_back("repo/counter.thrift.annotations");
    for (auto i : idl_files) {
        copy_file(combine(RESOURCE_ROOT, i), file("./"), ret);
    }
    return ret;
}

/*
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
*/

TEST(thrift_helper, cpp_binary_basic_type_serialization)
{
    test_thrift_basic_type_serialization(format_binary);
}

TEST(thrift_helper, cpp_binary_generated_type_serialization)
{
    test_thrift_generated_type_serialization(format_binary);
}

TEST(thrift_helper, cpp_json_basic_type_serialization)
{
    test_thrift_basic_type_serialization(format_json);
}

TEST(thrift_helper, cpp_json_generated_type_serialization)
{
    test_thrift_generated_type_serialization(format_json);
}

TEST(thrift_helper, cpp_binary_code_generation)
{
    EXPECT_TRUE(test_code_generation(lang_cpp, idl_thrift, format_binary));
}

TEST(thrift_helper, cpp_json_code_generation)
{
    EXPECT_TRUE(test_code_generation(lang_cpp, idl_thrift, format_json));
}

TEST(protobuf_helper, cpp_binary_generated_type_serialization)
{
    test_protobuf_generated_type_serialization(format_binary);
}

TEST(protobuf_helper, cpp_json_generated_type_serialization)
{
    test_protobuf_generated_type_serialization(format_json);
}

TEST(protobuf_helper, cpp_binary_code_generation)
{
    EXPECT_TRUE(test_code_generation(lang_cpp, idl_protobuf, format_binary));
}

TEST(protobuf_helper, cpp_json_code_generation)
{
    EXPECT_TRUE(test_code_generation(lang_cpp, idl_protobuf, format_json));
}

/*
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
*/

GTEST_API_ int main(int argc, char **argv)
{
    if (argc < 3) {
        std::cout << "invalid parameters" << std::endl;
        return 1;
    }
    // DSN_ROOT is the path where directory "rdsn" exists
    // RESOURCE_ROOT is the path where directory "repo" exists
    DSN_ROOT = std::string(argv[1]);
    RESOURCE_ROOT = std::string(argv[2]);
    if (!prepare()) {
        return 1;
    }
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
