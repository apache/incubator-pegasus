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

#include <cstdlib>
#include <string>
#include <vector>
#include <climits>
#include <map>

#include <dsn/service_api_c.h>
#include <unistd.h>
#include "include/pegasus/client.h"
#include <gtest/gtest.h>

#include "test/function_test/utils/test_util.h"

using namespace ::pegasus;

class incr : public test_util
{
};

TEST_F(incr, unexist_key)
{
    int ret = client->del("incr_test_unexist_key", "");
    ASSERT_EQ(0, ret);

    int64_t new_value_int;
    ret = client->incr("incr_test_unexist_key", "", 100, new_value_int);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(100, new_value_int);

    std::string new_value_str;
    ret = client->get("incr_test_unexist_key", "", new_value_str);
    ASSERT_EQ(0, ret);
    ASSERT_EQ("100", new_value_str);

    ret = client->del("incr_test_unexist_key", "");
    ASSERT_EQ(0, ret);
}

TEST_F(incr, empty_key)
{
    int ret = client->set("incr_test_empty_key", "", "");
    ASSERT_EQ(0, ret);

    int64_t new_value_int;
    ret = client->incr("incr_test_empty_key", "", 100, new_value_int);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(100, new_value_int);

    std::string new_value_str;
    ret = client->get("incr_test_empty_key", "", new_value_str);
    ASSERT_EQ(0, ret);
    ASSERT_EQ("100", new_value_str);

    ret = client->del("incr_test_empty_key", "");
    ASSERT_EQ(0, ret);
}

TEST_F(incr, negative_value)
{
    int ret = client->set("incr_test_negative_value", "", "-100");
    ASSERT_EQ(0, ret);

    int64_t new_value_int;
    ret = client->incr("incr_test_negative_value", "", -1, new_value_int);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(-101, new_value_int);

    std::string new_value_str;
    ret = client->get("incr_test_negative_value", "", new_value_str);
    ASSERT_EQ(0, ret);
    ASSERT_EQ("-101", new_value_str);

    ret = client->del("incr_test_negative_value", "");
    ASSERT_EQ(0, ret);
}

TEST_F(incr, increase_zero)
{
    int ret = client->set("incr_test_increase_zero", "", "100");
    ASSERT_EQ(0, ret);

    int64_t new_value_int;
    ret = client->incr("incr_test_increase_zero", "", 0, new_value_int);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(100, new_value_int);

    std::string new_value_str;
    ret = client->get("incr_test_increase_zero", "", new_value_str);
    ASSERT_EQ(0, ret);
    ASSERT_EQ("100", new_value_str);

    ret = client->del("incr_test_increase_zero", "");
    ASSERT_EQ(0, ret);
}

TEST_F(incr, multiple_increment)
{
    int ret = client->set("incr_test_multiple_increment", "", "100");
    ASSERT_EQ(0, ret);

    int64_t new_value_int;
    ret = client->incr("incr_test_multiple_increment", "", 1, new_value_int);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(101, new_value_int);

    std::string new_value_str;
    ret = client->get("incr_test_multiple_increment", "", new_value_str);
    ASSERT_EQ(0, ret);
    ASSERT_EQ("101", new_value_str);

    ret = client->incr("incr_test_multiple_increment", "", 2, new_value_int);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(103, new_value_int);

    ret = client->get("incr_test_multiple_increment", "", new_value_str);
    ASSERT_EQ(0, ret);
    ASSERT_EQ("103", new_value_str);

    ret = client->del("incr_test_multiple_increment", "");
    ASSERT_EQ(0, ret);
}

TEST_F(incr, invalid_old_data)
{
    int ret = client->set("incr_test_invalid_old_data", "", "aaa");
    ASSERT_EQ(0, ret);

    int64_t new_value_int;
    ret = client->incr("incr_test_invalid_old_data", "", 1, new_value_int);
    ASSERT_EQ(PERR_INVALID_ARGUMENT, ret);
    ASSERT_EQ(0, new_value_int);

    std::string new_value_str;
    ret = client->get("incr_test_invalid_old_data", "", new_value_str);
    ASSERT_EQ(0, ret);
    ASSERT_EQ("aaa", new_value_str);

    ret = client->del("incr_test_invalid_old_data", "");
    ASSERT_EQ(0, ret);
}

TEST_F(incr, out_of_range_old_data)
{
    int ret = client->set(
        "incr_test_out_of_range_old_data", "", "10000000000000000000000000000000000000");
    ASSERT_EQ(0, ret);

    int64_t new_value_int;
    ret = client->incr("incr_test_out_of_range_old_data", "", 1, new_value_int);
    ASSERT_EQ(PERR_INVALID_ARGUMENT, ret);
    ASSERT_EQ(0, new_value_int);

    std::string new_value_str;
    ret = client->get("incr_test_out_of_range_old_data", "", new_value_str);
    ASSERT_EQ(0, ret);
    ASSERT_EQ("10000000000000000000000000000000000000", new_value_str);

    ret = client->del("incr_test_out_of_range_old_data", "");
    ASSERT_EQ(0, ret);
}

TEST_F(incr, up_overflow)
{
    int ret = client->set("incr_test_up_overflow", "", "1");
    ASSERT_EQ(0, ret);

    int64_t new_value_int;
    ret = client->incr("incr_test_up_overflow", "", LLONG_MAX, new_value_int);
    ASSERT_EQ(PERR_INVALID_ARGUMENT, ret);
    ASSERT_EQ(1, new_value_int);

    std::string new_value_str;
    ret = client->get("incr_test_up_overflow", "", new_value_str);
    ASSERT_EQ(0, ret);
    ASSERT_EQ("1", new_value_str);

    ret = client->del("incr_test_up_overflow", "");
    ASSERT_EQ(0, ret);
}

TEST_F(incr, down_overflow)
{
    int ret = client->set("incr_test_down_overflow", "", "-1");
    ASSERT_EQ(0, ret);

    int64_t new_value_int;
    ret = client->incr("incr_test_down_overflow", "", LLONG_MIN, new_value_int);
    ASSERT_EQ(PERR_INVALID_ARGUMENT, ret);
    ASSERT_EQ(-1, new_value_int);

    std::string new_value_str;
    ret = client->get("incr_test_down_overflow", "", new_value_str);
    ASSERT_EQ(0, ret);
    ASSERT_EQ("-1", new_value_str);

    ret = client->del("incr_test_down_overflow", "");
    ASSERT_EQ(0, ret);
}

TEST_F(incr, preserve_ttl)
{
    int ret = client->set("incr_test_preserve_ttl", "", "100", 5000, 3);
    ASSERT_EQ(0, ret);

    int64_t new_value_int;
    ret = client->incr("incr_test_preserve_ttl", "", 1, new_value_int);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(101, new_value_int);

    int ttl_seconds;
    ret = client->ttl("incr_test_preserve_ttl", "", ttl_seconds);
    ASSERT_EQ(0, ret);
    ASSERT_GE(3, ttl_seconds);

    sleep(4);

    std::string new_value_str;
    ret = client->get("incr_test_preserve_ttl", "", new_value_str);
    ASSERT_EQ(PERR_NOT_FOUND, ret);

    ret = client->del("incr_test_preserve_ttl", "");
    ASSERT_EQ(0, ret);
}

TEST_F(incr, reset_ttl)
{
    /// reset after old value ttl timeout
    int ret = client->set("incr_test_reset_ttl", "", "100", 5000, 3);
    ASSERT_EQ(0, ret);

    sleep(4);

    int64_t new_value_int;
    ret = client->incr("incr_test_reset_ttl", "", 1, new_value_int);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, new_value_int);

    int ttl_seconds;
    ret = client->ttl("incr_test_reset_ttl", "", ttl_seconds);
    ASSERT_EQ(0, ret);
    ASSERT_GE(-1, ttl_seconds);

    ret = client->del("incr_test_reset_ttl", "");
    ASSERT_EQ(0, ret);

    /// reset with new ttl
    ret = client->set("incr_test_reset_ttl", "", "100");
    ASSERT_EQ(0, ret);

    ret = client->ttl("incr_test_reset_ttl", "", ttl_seconds);
    ASSERT_EQ(0, ret);
    ASSERT_GE(-1, ttl_seconds);

    ret = client->incr("incr_test_reset_ttl", "", 1, new_value_int, 5000, 10);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(101, new_value_int);

    ret = client->ttl("incr_test_reset_ttl", "", ttl_seconds);
    ASSERT_EQ(0, ret);
    ASSERT_LT(0, ttl_seconds);
    ASSERT_GE(10, ttl_seconds);

    ret = client->del("incr_test_reset_ttl", "");
    ASSERT_EQ(0, ret);

    /// reset with no ttl
    ret = client->set("incr_test_reset_ttl", "", "200", 5000, 10);
    ASSERT_EQ(0, ret);

    ret = client->ttl("incr_test_reset_ttl", "", ttl_seconds);
    ASSERT_EQ(0, ret);
    ASSERT_LT(0, ttl_seconds);
    ASSERT_GE(10, ttl_seconds);

    ret = client->incr("incr_test_reset_ttl", "", 1, new_value_int, 5000, -1);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(201, new_value_int);

    ret = client->ttl("incr_test_reset_ttl", "", ttl_seconds);
    ASSERT_EQ(0, ret);
    ASSERT_GE(-1, ttl_seconds);

    ret = client->del("incr_test_reset_ttl", "");
    ASSERT_EQ(0, ret);
}
