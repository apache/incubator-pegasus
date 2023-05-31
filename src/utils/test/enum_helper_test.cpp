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

// IWYU pragma: no_include <gtest/gtest-message.h>
// IWYU pragma: no_include <gtest/gtest-param-test.h>
// IWYU pragma: no_include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <vector>

namespace dsn {

enum class command_type
{
    START,
    RESTART,
    STOP,
    COUNT,
    INVALID_TYPE,
};

ENUM_BEGIN(command_type, command_type::INVALID_TYPE)
ENUM_REG2(command_type, START)
ENUM_REG_WITH_CUSTOM_NAME(command_type::RESTART, restart)
ENUM_REG(command_type::STOP)
ENUM_END(command_type)

using command_type_enum_from_string_case = std::tuple<std::string, command_type>;

class CommandTypeEnumFromStringTest
    : public testing::TestWithParam<command_type_enum_from_string_case>
{
};

TEST_P(CommandTypeEnumFromStringTest, EnumFromString)
{
    std::string str;
    command_type expected_type;
    std::tie(str, expected_type) = GetParam();

    auto actual_type = enum_from_string(str.c_str(), command_type::INVALID_TYPE);
    EXPECT_EQ(expected_type, actual_type);
}

const std::vector<command_type_enum_from_string_case> command_type_enum_from_string_tests = {
    {"START", command_type::START},
    {"Start", command_type::INVALID_TYPE},
    {"start", command_type::INVALID_TYPE},
    {"command_type::START", command_type::INVALID_TYPE},
    {"command_type::Start", command_type::INVALID_TYPE},
    {"command_type::start", command_type::INVALID_TYPE},
    {"RESTART", command_type::INVALID_TYPE},
    {"Restart", command_type::INVALID_TYPE},
    {"restart", command_type::RESTART},
    {"command_type::RESTART", command_type::INVALID_TYPE},
    {"command_type::Restart", command_type::INVALID_TYPE},
    {"command_type::restart", command_type::INVALID_TYPE},
    {"STOP", command_type::INVALID_TYPE},
    {"Stop", command_type::INVALID_TYPE},
    {"stop", command_type::INVALID_TYPE},
    {"command_type::STOP", command_type::STOP},
    {"command_type::Stop", command_type::INVALID_TYPE},
    {"command_type::stop", command_type::INVALID_TYPE},
    {"COUNT", command_type::INVALID_TYPE}, // Since COUNT was not registered with specified string
    {"UNDEFINE_TYPE", command_type::INVALID_TYPE},
};

INSTANTIATE_TEST_CASE_P(EnumHelperTest,
                        CommandTypeEnumFromStringTest,
                        testing::ValuesIn(command_type_enum_from_string_tests));

using command_type_enum_to_string_case = std::tuple<command_type, std::string>;

class CommandTypeEnumToStringTest : public testing::TestWithParam<command_type_enum_to_string_case>
{
};

TEST_P(CommandTypeEnumToStringTest, EnumToString)
{
    command_type type;
    std::string expected_str;
    std::tie(type, expected_str) = GetParam();

    std::string actual_str(enum_to_string(type));
    EXPECT_EQ(expected_str, actual_str);
}

const std::vector<command_type_enum_to_string_case> command_type_enum_to_string_tests = {
    {command_type::START, "START"},
    {command_type::RESTART, "restart"},
    {command_type::STOP, "command_type::STOP"},
    {command_type::COUNT, "Unknown"}, // Since COUNT was not registered with specified string
    {command_type::INVALID_TYPE, "Unknown"},
};

INSTANTIATE_TEST_CASE_P(EnumHelperTest,
                        CommandTypeEnumToStringTest,
                        testing::ValuesIn(command_type_enum_to_string_tests));

#define ENUM_FOREACH_STATUS_CODE(DEF)                                                              \
    DEF(Ok)                                                                                        \
    DEF(NotFound)                                                                                  \
    DEF(Corruption)                                                                                \
    DEF(IOError)

enum class status_code
{
    ENUM_FOREACH_STATUS_CODE(ENUM_CONST_DEF) kCount,
    kInvalidCode,
};

#define ENUM_CONST_REG_STR_STATUS_CODE(str) ENUM_CONST_REG_STR(status_code, str)

ENUM_BEGIN(status_code, status_code::kInvalidCode)
ENUM_FOREACH_STATUS_CODE(ENUM_CONST_REG_STR_STATUS_CODE)
ENUM_END(status_code)

using status_code_enum_from_string_case = std::tuple<std::string, status_code>;

class StatusCodeEnumFromStringTest
    : public testing::TestWithParam<status_code_enum_from_string_case>
{
};

TEST_P(StatusCodeEnumFromStringTest, EnumFromString)
{
    std::string str;
    status_code expected_code;
    std::tie(str, expected_code) = GetParam();

    auto actual_code = enum_from_string(str.c_str(), status_code::kInvalidCode);
    EXPECT_EQ(expected_code, actual_code);
}

const std::vector<status_code_enum_from_string_case> status_code_enum_from_string_tests = {
    {"OK", status_code::kInvalidCode},
    {"Ok", status_code::kOk},
    {"ok", status_code::kInvalidCode},
    {"status_code::OK", status_code::kInvalidCode},
    {"status_code::Ok", status_code::kInvalidCode},
    {"status_code::ok", status_code::kInvalidCode},
    {"NOTFOUND", status_code::kInvalidCode},
    {"NotFound", status_code::kNotFound},
    {"notfound", status_code::kInvalidCode},
    {"status_code::NOTFOUND", status_code::kInvalidCode},
    {"status_code::NotFound", status_code::kInvalidCode},
    {"status_code::notfound", status_code::kInvalidCode},
    {"CORRUPTION", status_code::kInvalidCode},
    {"Corruption", status_code::kCorruption},
    {"corruption", status_code::kInvalidCode},
    {"status_code::CORRUPTION", status_code::kInvalidCode},
    {"status_code::Corruption", status_code::kInvalidCode},
    {"status_code::corruption", status_code::kInvalidCode},
    {"IOERROR", status_code::kInvalidCode},
    {"IOError", status_code::kIOError},
    {"ioerror", status_code::kInvalidCode},
    {"status_code::IOERROR", status_code::kInvalidCode},
    {"status_code::IOError", status_code::kInvalidCode},
    {"status_code::ioerror", status_code::kInvalidCode},
    {"Count", status_code::kInvalidCode}, // Since kCount was not registered with specified string
    {"UndefinedCode", status_code::kInvalidCode},
};

INSTANTIATE_TEST_CASE_P(EnumHelperTest,
                        StatusCodeEnumFromStringTest,
                        testing::ValuesIn(status_code_enum_from_string_tests));

using status_code_enum_to_string_case = std::tuple<status_code, std::string>;

class StatusCodeEnumToStringTest : public testing::TestWithParam<status_code_enum_to_string_case>
{
};

TEST_P(StatusCodeEnumToStringTest, EnumToString)
{
    status_code code;
    std::string expected_str;
    std::tie(code, expected_str) = GetParam();

    std::string actual_str(enum_to_string(code));
    EXPECT_EQ(expected_str, actual_str);
}

const std::vector<status_code_enum_to_string_case> status_code_enum_to_string_tests = {
    {status_code::kOk, "Ok"},
    {status_code::kNotFound, "NotFound"},
    {status_code::kCorruption, "Corruption"},
    {status_code::kIOError, "IOError"},
    {status_code::kCount, "Unknown"}, // Since kCount was not registered with specified string
    {status_code::kInvalidCode, "Unknown"},
};

INSTANTIATE_TEST_CASE_P(EnumHelperTest,
                        StatusCodeEnumToStringTest,
                        testing::ValuesIn(status_code_enum_to_string_tests));

} // namespace dsn
