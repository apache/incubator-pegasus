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
#include <unistd.h>
#include <cstdint>
#include <limits>
#include <set>
#include <string>
#include <vector>

#include "atomic_write_test.h"
#include "gtest/gtest.h"
#include "include/pegasus/client.h"
#include "pegasus/error.h"
#include "utils/defer.h"

namespace pegasus {

class IncrTest : public AtomicWriteTest
{
protected:
    IncrTest() : AtomicWriteTest("incr_test") {}

    void test_incr_on_del(const std::string &hash_key, int64_t increment)
    {
        del(hash_key);

        const auto cleanup = dsn::defer([this, &hash_key]() { del(hash_key); });

        test_incr(hash_key, increment, increment);
    }

    template <typename TBaseValue, typename... Args>
    void test_incr_on_set(const std::string &hash_key,
                          const TBaseValue &base_value,
                          int64_t increment,
                          int64_t expected_new_value,
                          Args &&...args)
    {
        set(hash_key, base_value);

        const auto cleanup = dsn::defer([this, &hash_key]() { del(hash_key); });

        test_incr(hash_key, increment, expected_new_value, std::forward<Args>(args)...);
    }

    template <typename TBaseValue, typename TReadValue>
    void test_incr_detail_on_set(const std::string &hash_key,
                                 const TBaseValue &base_value,
                                 int64_t increment,
                                 int expected_incr_err,
                                 int64_t expected_resp_value,
                                 int expected_read_err,
                                 const TReadValue &expected_read_value)
    {
        set(hash_key, base_value);

        const auto cleanup = dsn::defer([this, &hash_key]() { del(hash_key); });

        test_incr_detail(hash_key,
                         increment,
                         expected_incr_err,
                         expected_resp_value,
                         expected_read_err,
                         expected_read_value);
    }

private:
    void del(const std::string &hash_key)
    {
        ASSERT_EQ(PERR_OK, _client->del(hash_key, kSortKey));
        ASSERT_EQ(PERR_NOT_FOUND, _client->exist(hash_key, kSortKey));
    }

    template <typename TValue>
    void set(const std::string &hash_key, const TValue &value)
    {
        ASSERT_EQ(PERR_OK, _client->set(hash_key, kSortKey, fmt::format("{}", value)));
        ASSERT_EQ(PERR_OK, _client->exist(hash_key, kSortKey));
    }

    template <typename... Args>
    void test_incr(const std::string &hash_key,
                   int64_t increment,
                   int64_t expected_new_value,
                   Args &&...args)
    {
        test_incr_detail(
            hash_key, increment, PERR_OK, expected_new_value, PERR_OK, expected_new_value);

        test_incr(hash_key, std::forward<Args>(args)...);
    }

    void test_incr(const std::string &hash_key) {}

    template <typename TReadValue>
    void test_incr_detail(const std::string &hash_key,
                          int64_t increment,
                          int expected_incr_err,
                          int64_t expected_resp_value,
                          int expected_read_err,
                          const TReadValue &expected_read_value)
    {
        int64_t actual_new_value{0};
        ASSERT_EQ(expected_incr_err,
                  _client->incr(hash_key, kSortKey, increment, actual_new_value));
        ASSERT_EQ(expected_resp_value, actual_new_value);

        std::string actual_new_str;
        ASSERT_EQ(expected_read_err, _client->get(hash_key, kSortKey, actual_new_str));
        ASSERT_EQ(fmt::format("{}", expected_read_value), actual_new_str);
    }

    static const std::string kSortKey;
};

const std::string IncrTest::kSortKey("sort_key");

TEST_P(IncrTest, OnNonExistingKey) { test_incr_on_del("incr_on_non_existing_key", 100); }

TEST_P(IncrTest, OnEmptyValue) { test_incr_on_set("incr_on_empty_value", "", 100, 100); }

TEST_P(IncrTest, OnZeroValue) { test_incr_on_set("incr_on_zero_value", 0, 1, 1); }

TEST_P(IncrTest, OnPositiveValue) { test_incr_on_set("incr_on_positive_value", 100, 1, 101); }

TEST_P(IncrTest, OnNegativeValue) { test_incr_on_set("incr_on_negative_value", -100, -1, -101); }

TEST_P(IncrTest, ByZero) { test_incr_on_set("incr_by_zero", 100, 0, 100); }

TEST_P(IncrTest, MultiTimes) { test_incr_on_set("incr_multi_times", 100, 1, 101, 2, 103); }

TEST_P(IncrTest, OnInvalidValue)
{
    test_incr_detail_on_set(
        "incr_on_invalid_value", "aaa", 1, PERR_INVALID_ARGUMENT, 0, PERR_OK, "aaa");
}

TEST_P(IncrTest, OnTooLargeValue)
{
    test_incr_detail_on_set("incr_on_too_large_value",
                            "10000000000000000000000000000000000000",
                            1,
                            PERR_INVALID_ARGUMENT,
                            0,
                            PERR_OK,
                            "10000000000000000000000000000000000000");
}

TEST_P(IncrTest, Overflow)
{
    test_incr_detail_on_set("incr_overflow",
                            1,
                            std::numeric_limits<int64_t>::max(),
                            PERR_INVALID_ARGUMENT,
                            1,
                            PERR_OK,
                            1);
}

TEST_P(IncrTest, Underflow)
{
    test_incr_detail_on_set("incr_underflow",
                            -1,
                            std::numeric_limits<int64_t>::min(),
                            PERR_INVALID_ARGUMENT,
                            -1,
                            PERR_OK,
                            -1);
}

TEST_P(IncrTest, PreserveTtl)
{
    ASSERT_EQ(PERR_OK, _client->set("incr_test_preserve_ttl", "", "100", 5000, 3));

    int64_t new_value_int;
    ASSERT_EQ(PERR_OK, _client->incr("incr_test_preserve_ttl", "", 1, new_value_int));
    ASSERT_EQ(101, new_value_int);

    int ttl_seconds;
    ASSERT_EQ(PERR_OK, _client->ttl("incr_test_preserve_ttl", "", ttl_seconds));
    ASSERT_GE(3, ttl_seconds);

    sleep(4);

    std::string new_value_str;
    ASSERT_EQ(PERR_NOT_FOUND, _client->get("incr_test_preserve_ttl", "", new_value_str));

    ASSERT_EQ(PERR_OK, _client->del("incr_test_preserve_ttl", ""));
}

TEST_P(IncrTest, ResetTtl)
{
    /// reset after old value ttl timeout
    ASSERT_EQ(PERR_OK, _client->set("incr_test_reset_ttl", "", "100", 5000, 3));

    sleep(4);

    int64_t new_value_int;
    ASSERT_EQ(PERR_OK, _client->incr("incr_test_reset_ttl", "", 1, new_value_int));
    ASSERT_EQ(1, new_value_int);

    int ttl_seconds;
    ASSERT_EQ(PERR_OK, _client->ttl("incr_test_reset_ttl", "", ttl_seconds));
    ASSERT_GE(-1, ttl_seconds);

    ASSERT_EQ(PERR_OK, _client->del("incr_test_reset_ttl", ""));

    /// reset with new ttl
    ASSERT_EQ(PERR_OK, _client->set("incr_test_reset_ttl", "", "100"));

    ASSERT_EQ(PERR_OK, _client->ttl("incr_test_reset_ttl", "", ttl_seconds));
    ASSERT_GE(-1, ttl_seconds);

    ASSERT_EQ(PERR_OK, _client->incr("incr_test_reset_ttl", "", 1, new_value_int, 5000, 10));
    ASSERT_EQ(101, new_value_int);

    ASSERT_EQ(PERR_OK, _client->ttl("incr_test_reset_ttl", "", ttl_seconds));
    ASSERT_LT(0, ttl_seconds);
    ASSERT_GE(10, ttl_seconds);

    ASSERT_EQ(PERR_OK, _client->del("incr_test_reset_ttl", ""));

    /// reset with no ttl
    ASSERT_EQ(PERR_OK, _client->set("incr_test_reset_ttl", "", "200", 5000, 10));

    ASSERT_EQ(PERR_OK, _client->ttl("incr_test_reset_ttl", "", ttl_seconds));
    ASSERT_LT(0, ttl_seconds);
    ASSERT_GE(10, ttl_seconds);

    ASSERT_EQ(PERR_OK, _client->incr("incr_test_reset_ttl", "", 1, new_value_int, 5000, -1));
    ASSERT_EQ(201, new_value_int);

    ASSERT_EQ(PERR_OK, _client->ttl("incr_test_reset_ttl", "", ttl_seconds));
    ASSERT_GE(-1, ttl_seconds);

    ASSERT_EQ(PERR_OK, _client->del("incr_test_reset_ttl", ""));
}

INSTANTIATE_TEST_SUITE_P(AtomicWriteTest,
                         IncrTest,
                         testing::ValuesIn(generate_atomic_write_cases()));

} // namespace pegasus
