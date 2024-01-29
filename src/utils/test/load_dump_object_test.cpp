/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "utils/load_dump_object.h"

#include <nlohmann/detail/macro_scope.hpp>
#include <rocksdb/status.h>
#include <algorithm>
#include <cstdint>

#include "gtest/gtest.h"

namespace dsn {
namespace utils {
struct nlohmann_json_struct;
struct rapid_json_struct;

#define STRUCT_CONTENT(T)                                                                          \
    int64_t a;                                                                                     \
    std::string b;                                                                                 \
    std::vector<int64_t> c;                                                                        \
    bool operator==(const T &other) const { return a == other.a && b == other.b && c == other.c; }

struct nlohmann_json_struct
{
    STRUCT_CONTENT(nlohmann_json_struct);
};
NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE(nlohmann_json_struct, a, b, c);

class load_dump_object : public testing::TestWithParam<FileDataType>
{
public:
    load_dump_object() : type_(GetParam()) {}

protected:
    FileDataType type_;
};

INSTANTIATE_TEST_SUITE_P(,
                         load_dump_object,
                         ::testing::Values(FileDataType::kNonSensitive, FileDataType::kSensitive));

TEST_P(load_dump_object, nlohmann_json_struct_normal_test)
{
    const std::string path("nlohmann_json_struct_test");
    nlohmann_json_struct obj;
    obj.a = 123;
    obj.b = "hello world";
    obj.c = std::vector<int64_t>({1, 3, 5, 2, 4});
    ASSERT_EQ(ERR_OK, dump_njobj_to_file(obj, type_, path));
    nlohmann_json_struct obj2;
    ASSERT_EQ(ERR_OK, load_njobj_from_file(path, type_, &obj2));
    ASSERT_EQ(obj, obj2);
}

TEST_P(load_dump_object, nlohmann_json_struct_load_failed_test)
{
    const std::string path("nlohmann_json_struct_test_bad");
    ASSERT_TRUE(filesystem::remove_path(path));

    nlohmann_json_struct obj;
    ASSERT_EQ(ERR_PATH_NOT_FOUND, load_njobj_from_file(path, type_, &obj));

    auto s = rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(type_),
                                        rocksdb::Slice("invalid data"),
                                        path,
                                        /* should_sync */ true);
    ASSERT_TRUE(s.ok()) << s.ToString();

    ASSERT_EQ(ERR_CORRUPTION, load_njobj_from_file(path, type_, &obj));
}

struct rapid_json_struct
{
    STRUCT_CONTENT(rapid_json_struct);
    DEFINE_JSON_SERIALIZATION(a, b, c);
};

TEST_P(load_dump_object, rapid_json_struct_test)
{
    const std::string path("rapid_json_struct_test");
    rapid_json_struct obj;
    obj.a = 123;
    obj.b = "hello world";
    obj.c = std::vector<int64_t>({1, 3, 5, 2, 4});
    ASSERT_EQ(ERR_OK, dump_rjobj_to_file(obj, type_, path));
    rapid_json_struct obj2;
    ASSERT_EQ(ERR_OK, load_rjobj_from_file(path, type_, &obj2));
    ASSERT_EQ(obj, obj2);
}

TEST_P(load_dump_object, rapid_json_struct_load_failed_test)
{
    const std::string path("rapid_json_struct_test_bad");
    ASSERT_TRUE(filesystem::remove_path(path));

    rapid_json_struct obj;
    ASSERT_EQ(ERR_PATH_NOT_FOUND, load_rjobj_from_file(path, type_, &obj));

    auto s = rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(type_),
                                        rocksdb::Slice("invalid data"),
                                        path,
                                        /* should_sync */ true);
    ASSERT_TRUE(s.ok()) << s.ToString();

    ASSERT_EQ(ERR_CORRUPTION, load_rjobj_from_file(path, type_, &obj));
}

} // namespace utils
} // namespace dsn
