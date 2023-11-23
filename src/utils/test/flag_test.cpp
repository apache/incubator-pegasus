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

#include <fmt/core.h>
#include <stdint.h>
#include <iostream>
#include <string>

#include "gtest/gtest.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/flags.h"

namespace dsn {
namespace utils {

DSN_DEFINE_int32(flag_test, test_int32, 5, "");
DSN_TAG_VARIABLE(test_int32, FT_MUTABLE);

DSN_DEFINE_uint32(flag_test, test_uint32, 5, "");
DSN_TAG_VARIABLE(test_uint32, FT_MUTABLE);

DSN_DEFINE_int64(flag_test, test_int64, 5, "");
DSN_TAG_VARIABLE(test_int64, FT_MUTABLE);

DSN_DEFINE_uint64(flag_test, test_uint64, 5, "");
DSN_TAG_VARIABLE(test_uint64, FT_MUTABLE);

DSN_DEFINE_double(flag_test, test_double, 5.0, "");
DSN_TAG_VARIABLE(test_double, FT_MUTABLE);

DSN_DEFINE_bool(flag_test, test_bool, true, "");
DSN_TAG_VARIABLE(test_bool, FT_MUTABLE);

DSN_DEFINE_string(flag_test, test_string_immutable, "immutable_string", "");

DSN_DEFINE_int32(flag_test, test_validator, 10, "");
DSN_TAG_VARIABLE(test_validator, FT_MUTABLE);
DSN_DEFINE_validator(test_validator, [](int32_t test_validator) -> bool {
    if (test_validator < 0) {
        return false;
    }
    return true;
});

DSN_DEFINE_bool(flag_test, condition_a, false, "");
DSN_TAG_VARIABLE(condition_a, FT_MUTABLE);

DSN_DEFINE_bool(flag_test, condition_b, false, "");
DSN_TAG_VARIABLE(condition_b, FT_MUTABLE);

DSN_DEFINE_group_validator(inconsistent_conditions, [](std::string &message) -> bool {
    return !FLAGS_condition_a || !FLAGS_condition_b;
});

DSN_DEFINE_int32(flag_test, min_value, 1, "");
DSN_TAG_VARIABLE(min_value, FT_MUTABLE);
DSN_DEFINE_validator(min_value, [](int32_t value) -> bool { return value > 0; });

DSN_DEFINE_int32(flag_test, max_value, 5, "");
DSN_TAG_VARIABLE(max_value, FT_MUTABLE);
DSN_DEFINE_validator(max_value, [](int32_t value) -> bool { return value <= 10; });

DSN_DEFINE_group_validator(min_max, [](std::string &message) -> bool {
    if (FLAGS_min_value > FLAGS_max_value) {
        message = fmt::format("min({}) should be <= max({})", FLAGS_min_value, FLAGS_max_value);
        return false;
    }
    return true;
});

DSN_DEFINE_int32(flag_test, small_value, 0, "");
DSN_TAG_VARIABLE(small_value, FT_MUTABLE);

DSN_DEFINE_int32(flag_test, medium_value, 5, "");
DSN_TAG_VARIABLE(medium_value, FT_MUTABLE);

DSN_DEFINE_int32(flag_test, large_value, 10, "");
DSN_TAG_VARIABLE(large_value, FT_MUTABLE);

DSN_DEFINE_group_validator(small_medium_large, [](std::string &message) -> bool {
    if (FLAGS_small_value >= FLAGS_medium_value) {
        message =
            fmt::format("small({}) should be < medium({})", FLAGS_small_value, FLAGS_medium_value);
        return false;
    }

    if (FLAGS_medium_value >= FLAGS_large_value) {
        message =
            fmt::format("medium({}) should be < large({})", FLAGS_medium_value, FLAGS_large_value);
        return false;
    }

    return true;
});

DSN_DEFINE_int32(flag_test, lesser, 0, "");
DSN_TAG_VARIABLE(lesser, FT_MUTABLE);

DSN_DEFINE_int32(flag_test, greater_0, 5, "");
DSN_TAG_VARIABLE(greater_0, FT_MUTABLE);

DSN_DEFINE_int32(flag_test, greater_1, 10, "");
DSN_TAG_VARIABLE(greater_1, FT_MUTABLE);

DSN_DEFINE_group_validator(lesser_greater_0, [](std::string &message) -> bool {
    if (FLAGS_lesser >= FLAGS_greater_0) {
        message =
            fmt::format("lesser({}) should be < greater_0({})", FLAGS_lesser, FLAGS_greater_0);
        return false;
    }

    return true;
});

DSN_DEFINE_group_validator(lesser_greater_1, [](std::string &message) -> bool {
    if (FLAGS_lesser >= FLAGS_greater_1) {
        message =
            fmt::format("lesser({}) should be < greater_1({})", FLAGS_lesser, FLAGS_greater_1);
        return false;
    }

    return true;
});

TEST(flag_test, update_config)
{
    auto res = update_flag("test_int32", "3");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_test_int32, 3);

    res = update_flag("test_uint32", "3");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_test_uint32, 3);

    res = update_flag("test_int64", "3");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_test_int64, 3);

    res = update_flag("test_uint64", "3");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_test_uint64, 3);

    res = update_flag("test_double", "3.0");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_test_double, 3.0);

    res = update_flag("test_bool", "false");
    ASSERT_TRUE(res.is_ok());
    ASSERT_FALSE(FLAGS_test_bool);

    // string modifications are not supported
    res = update_flag("test_string_immutable", "update_string");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_STREQ("immutable_string", FLAGS_test_string_immutable);

    // test flag is not exist
    res = update_flag("test_not_exist", "test_string");
    ASSERT_EQ(res.code(), ERR_OBJECT_NOT_FOUND);

    // test to update invalid value
    res = update_flag("test_int32", "3ab");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(FLAGS_test_int32, 3);

    // validation succeed
    res = update_flag("test_validator", "5");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_test_validator, 5);

    // validation failed
    res = update_flag("test_validator", "-1");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(FLAGS_test_validator, 5);

    // successful detection with consistent conditions
    res = update_flag("condition_a", "true");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_condition_a, true);

    // failed detection with mutually exclusive conditions
    res = update_flag("condition_b", "true");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(FLAGS_condition_b, false);
    std::cout << res.description() << std::endl;

    // successful detection between 2 flags
    res = update_flag("max_value", "6");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_max_value, 6);

    // failed detection between 2 flags with each individual validation
    res = update_flag("min_value", "0");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(FLAGS_min_value, 1);
    std::cout << res.description() << std::endl;

    // failed detection between 2 flags within a grouped validator
    res = update_flag("min_value", "7");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(FLAGS_min_value, 1);
    std::cout << res.description() << std::endl;

    // successful detection among 3 flags within a grouped validator
    res = update_flag("medium_value", "6");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_medium_value, 6);

    // failed detection among 3 flags within a grouped validator
    res = update_flag("medium_value", "0");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(FLAGS_medium_value, 6);
    std::cout << res.description() << std::endl;

    // failed detection among 3 flags within a grouped validator
    res = update_flag("medium_value", "10");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(FLAGS_medium_value, 6);
    std::cout << res.description() << std::endl;

    // successful detection among 3 flags within both 2 grouped validators
    res = update_flag("lesser", "1");
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(FLAGS_lesser, 1);

    // failed detection among 3 flags within one of 2 grouped validators
    res = update_flag("lesser", "6");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(FLAGS_lesser, 1);
    std::cout << res.description() << std::endl;

    // failed detection among 3 flags within both 2 grouped validators
    res = update_flag("lesser", "11");
    ASSERT_EQ(res.code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(FLAGS_lesser, 1);
    std::cout << res.description() << std::endl;
}

DSN_DEFINE_int32(flag_test, has_tag, 5, "");
DSN_TAG_VARIABLE(has_tag, FT_MUTABLE);

DSN_DEFINE_int32(flag_test, no_tag, 5, "");

TEST(flag_test, tag_flag)
{
    // has tag
    auto res = has_tag("has_tag", flag_tag::FT_MUTABLE);
    ASSERT_TRUE(res);

    // doesn't has tag
    res = has_tag("no_tag", flag_tag::FT_MUTABLE);
    ASSERT_FALSE(res);

    // flag is not exist
    res = has_tag("no_flag", flag_tag::FT_MUTABLE);
    ASSERT_FALSE(res);
}

DSN_DEFINE_int32(flag_test, get_flag_int32, 5, "test get_flag_int32");
DSN_TAG_VARIABLE(get_flag_int32, FT_MUTABLE);
DSN_DEFINE_uint32(flag_test, get_flag_uint32, 5, "test get_flag_uint32");
DSN_TAG_VARIABLE(get_flag_uint32, FT_MUTABLE);
DSN_DEFINE_int64(flag_test, get_flag_int64, 5, "test get_flag_int64");
DSN_TAG_VARIABLE(get_flag_int64, FT_MUTABLE);
DSN_DEFINE_uint64(flag_test, get_flag_uint64, 5, "test get_flag_uint64");
DSN_TAG_VARIABLE(get_flag_uint64, FT_MUTABLE);
DSN_DEFINE_double(flag_test, get_flag_double, 5.12, "test get_flag_double");
DSN_TAG_VARIABLE(get_flag_double, FT_MUTABLE);
DSN_DEFINE_bool(flag_test, get_flag_bool, true, "test get_flag_bool");
DSN_TAG_VARIABLE(get_flag_bool, FT_MUTABLE);
DSN_DEFINE_string(flag_test, get_flag_string, "flag_string", "test get_flag_string");
DSN_TAG_VARIABLE(get_flag_string, FT_MUTABLE);

TEST(flag_test, get_config)
{
    auto res = get_flag_str("get_flag_not_exist");
    ASSERT_EQ(res.get_error().code(), ERR_OBJECT_NOT_FOUND);

    std::string test_app = "get_flag_int32";
    res = get_flag_str(test_app);
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(
        res.get_value(),
        R"({"name":")" + test_app +
            R"(","section":"flag_test","type":"FV_INT32","tags":"flag_tag::FT_MUTABLE","description":"test get_flag_int32","value":")" +
            std::to_string(FLAGS_get_flag_int32) + R"("})" + "\n");

    test_app = "get_flag_uint32";
    res = get_flag_str(test_app);
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(
        res.get_value(),
        R"({"name":")" + test_app +
            R"(","section":"flag_test","type":"FV_UINT32","tags":"flag_tag::FT_MUTABLE","description":"test get_flag_uint32","value":")" +
            std::to_string(FLAGS_get_flag_uint32) + R"("})" + "\n");

    test_app = "get_flag_int64";
    res = get_flag_str(test_app);
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(
        res.get_value(),
        R"({"name":")" + test_app +
            R"(","section":"flag_test","type":"FV_INT64","tags":"flag_tag::FT_MUTABLE","description":"test get_flag_int64","value":")" +
            std::to_string(FLAGS_get_flag_int64) + R"("})" + "\n");

    test_app = "get_flag_uint64";
    res = get_flag_str(test_app);
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(
        res.get_value(),
        R"({"name":")" + test_app +
            R"(","section":"flag_test","type":"FV_UINT64","tags":"flag_tag::FT_MUTABLE","description":"test get_flag_uint64","value":")" +
            std::to_string(FLAGS_get_flag_uint64) + R"("})" + "\n");

    test_app = "get_flag_double";
    res = get_flag_str(test_app);
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(
        res.get_value(),
        R"({"name":")" + test_app +
            R"(","section":"flag_test","type":"FV_DOUBLE","tags":"flag_tag::FT_MUTABLE","description":"test get_flag_double","value":"5.12"})" +
            "\n");

    test_app = "get_flag_bool";
    res = get_flag_str(test_app);
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(
        res.get_value(),
        R"({"name":")" + test_app +
            R"(","section":"flag_test","type":"FV_BOOL","tags":"flag_tag::FT_MUTABLE","description":"test get_flag_bool","value":"true"})"
            "\n");

    test_app = "get_flag_string";
    res = get_flag_str(test_app);
    ASSERT_TRUE(res.is_ok());
    ASSERT_EQ(
        res.get_value(),
        R"({"name":")" + test_app +
            R"(","section":"flag_test","type":"FV_STRING","tags":"flag_tag::FT_MUTABLE","description":"test get_flag_string","value":")" +
            FLAGS_get_flag_string + R"("})" + "\n");
}
} // namespace utils
} // namespace dsn
