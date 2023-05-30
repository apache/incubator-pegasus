// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "utils/enum_helper.h"

#include <gtest/gtest.h>

namespace dsn {

#define ENUM_FOREACH_TEST_TYPE(DEF)                                                                \
    DEF(EnumeratorOne)                                                                             \
    DEF(EnumeratorTwo)                                                                             \
    DEF(EnumeratorThree)

enum class test_enum_const_type
{
    ENUM_FOREACH_TEST_TYPE(ENUM_CONST_DEF) kInvalidType,
};

#define ENUM_CONST_REG_STR_TEST_TYPE(str) ENUM_CONST_REG_STR(test_enum_const_type, str)

ENUM_BEGIN(test_enum_const_type, test_enum_const_type::kInvalidType)
ENUM_FOREACH_TEST_TYPE(ENUM_CONST_REG_STR_TEST_TYPE)
ENUM_END(test_enum_const_type)

using enum_const_from_string_case = std::tuple<std::string, test_enum_const_type>;

class EnumConstFromStringTest : public testing::TestWithParam<enum_const_from_string_case>
{
};

TEST_P(EnumConstFromStringTest, EnumFromString)
{
    std::string str;
    test_enum_const_type expected_type;
    std::tie(str, expected_type) = GetParam();

    auto actual_type = enum_from_string(str.c_str(), test_enum_const_type::kInvalidType);
    EXPECT_EQ(expected_type, actual_type);
}

const std::vector<enum_const_from_string_case> enum_const_from_string_tests = {
    {"EnumeratorOne", test_enum_const_type::kEnumeratorOne},
    {"EnumeratorTwo", test_enum_const_type::kEnumeratorTwo},
    {"EnumeratorThree", test_enum_const_type::kEnumeratorThree},
    {"EnumeratorUndefined", test_enum_const_type::kInvalidType},
};

INSTANTIATE_TEST_CASE_P(EnumHelperTest,
                        EnumConstFromStringTest,
                        testing::ValuesIn(enum_const_from_string_tests));

using enum_const_to_string_case = std::tuple<test_enum_const_type, std::string>;

class EnumConstToStringTest : public testing::TestWithParam<enum_const_to_string_case>
{
};

TEST_P(EnumConstToStringTest, EnumToString)
{
    test_enum_const_type type;
    std::string expected_str;
    std::tie(type, expected_str) = GetParam();

    std::string actual_str(enum_to_string(type));
    EXPECT_EQ(expected_str, actual_str);
}

const std::vector<enum_const_to_string_case> enum_const_to_string_tests = {
    {test_enum_const_type::kEnumeratorOne, "EnumeratorOne"},
    {test_enum_const_type::kEnumeratorTwo, "EnumeratorTwo"},
    {test_enum_const_type::kEnumeratorThree, "EnumeratorThree"},
    {test_enum_const_type::kInvalidType, "Unknown"},
};

INSTANTIATE_TEST_CASE_P(EnumHelperTest,
                        EnumConstToStringTest,
                        testing::ValuesIn(enum_const_to_string_tests));

} // namespace dsn
