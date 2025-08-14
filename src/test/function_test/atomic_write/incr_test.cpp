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
#include <string>
#include <utility>
#include <vector>

#include "atomic_write_test.h"
#include "gtest/gtest.h"
#include "include/pegasus/client.h"
#include "pegasus/error.h"

namespace pegasus {

class IncrTest : public AtomicWriteTest
{
protected:
    IncrTest() : AtomicWriteTest("incr_test") {}

    void TearDown() override
    {
        del();
        AtomicWriteTest::TearDown();
    }

    void test_incr_on_del(int64_t increment)
    {
        del();

        test_incr(5000, 0, increment, increment);
    }

    template <typename TBaseValue, typename... Args>
    void test_incr_on_set(const TBaseValue &base_value,
                          int64_t increment,
                          int64_t expected_new_value,
                          Args &&...args)
    {
        set(base_value);

        test_incr(5000, 0, increment, expected_new_value, std::forward<Args>(args)...);
    }

    template <typename TBaseValue, typename TReadValue>
    void test_incr_detail_on_set(const TBaseValue &base_value,
                                 int64_t increment,
                                 int expected_incr_err,
                                 int64_t expected_resp_value,
                                 int expected_read_err,
                                 const TReadValue &expected_read_value)
    {
        set(base_value);

        test_incr_detail(5000,
                         0,
                         increment,
                         expected_incr_err,
                         expected_resp_value,
                         expected_read_err,
                         expected_read_value);
    }

    template <typename... Args>
    void test_incr(int timeout_milliseconds,
                   int ttl_seconds,
                   int64_t increment,
                   int64_t expected_new_value,
                   Args &&...args)
    {
        test_incr_detail(timeout_milliseconds,
                         ttl_seconds,
                         increment,
                         PERR_OK,
                         expected_new_value,
                         PERR_OK,
                         expected_new_value);

        test_incr(timeout_milliseconds, ttl_seconds, std::forward<Args>(args)...);
    }

    template <typename TValue>
    void set_with_ttl(const TValue &value, int timeout_milliseconds, int ttl_seconds)
    {
        ASSERT_EQ(
            PERR_OK,
            _client->set(
                _hash_key, kSortKey, fmt::format("{}", value), timeout_milliseconds, ttl_seconds));
        ASSERT_EQ(PERR_OK, _client->exist(_hash_key, kSortKey));
    }

    void get_ttl(int &ttl_seconds)
    {
        ASSERT_EQ(PERR_OK, _client->ttl(_hash_key, kSortKey, ttl_seconds));
    }

    void should_no_ttl()
    {
        int ttl_seconds{0};
        get_ttl(ttl_seconds);
        ASSERT_EQ(-1, ttl_seconds);
    }

    void should_not_found()
    {
        std::string value;
        ASSERT_EQ(PERR_NOT_FOUND, _client->get(_hash_key, kSortKey, value));
    }

private:
    void del()
    {
        ASSERT_EQ(PERR_OK, _client->del(_hash_key, kSortKey));
        ASSERT_EQ(PERR_NOT_FOUND, _client->exist(_hash_key, kSortKey));
    }

    template <typename TValue>
    void set(const TValue &value)
    {
        // Disable TTL with 0.
        set_with_ttl(value, 5000, 0);
    }

    // The end of recursion.
    void test_incr(int timeout_milliseconds, int ttl_seconds) {}

    template <typename TReadValue>
    void test_incr_detail(int timeout_milliseconds,
                          int ttl_seconds,
                          int64_t increment,
                          int expected_incr_err,
                          int64_t expected_resp_value,
                          int expected_read_err,
                          const TReadValue &expected_read_value)
    {
        int64_t actual_new_value{0};
        ASSERT_EQ(expected_incr_err,
                  _client->incr(_hash_key,
                                kSortKey,
                                increment,
                                actual_new_value,
                                timeout_milliseconds,
                                ttl_seconds));
        ASSERT_EQ(expected_resp_value, actual_new_value);

        std::string actual_new_str;
        ASSERT_EQ(expected_read_err, _client->get(_hash_key, kSortKey, actual_new_str));
        ASSERT_EQ(fmt::format("{}", expected_read_value), actual_new_str);
    }

    static const std::string kSortKey;
};

const std::string IncrTest::kSortKey("sort_key");

TEST_P(IncrTest, OnNonExistingKey) { test_incr_on_del(100); }

TEST_P(IncrTest, OnEmptyValue) { test_incr_on_set("", 100, 100); }

TEST_P(IncrTest, OnZeroValue) { test_incr_on_set(0, 1, 1); }

TEST_P(IncrTest, OnPositiveValue) { test_incr_on_set(100, 1, 101); }

TEST_P(IncrTest, OnNegativeValue) { test_incr_on_set(-100, -1, -101); }

TEST_P(IncrTest, ByZero) { test_incr_on_set(100, 0, 100); }

TEST_P(IncrTest, MultiTimes) { test_incr_on_set(100, 1, 101, 2, 103); }

TEST_P(IncrTest, OnInvalidValue)
{
    test_incr_detail_on_set("aaa", 1, PERR_INVALID_ARGUMENT, 0, PERR_OK, "aaa");
}

TEST_P(IncrTest, OnTooLargeValue)
{
    test_incr_detail_on_set("10000000000000000000000000000000000000",
                            1,
                            PERR_INVALID_ARGUMENT,
                            0,
                            PERR_OK,
                            "10000000000000000000000000000000000000");
}

TEST_P(IncrTest, Overflow)
{
    test_incr_detail_on_set(
        1, std::numeric_limits<int64_t>::max(), PERR_INVALID_ARGUMENT, 1, PERR_OK, 1);
}

TEST_P(IncrTest, Underflow)
{
    test_incr_detail_on_set(
        -1, std::numeric_limits<int64_t>::min(), PERR_INVALID_ARGUMENT, -1, PERR_OK, -1);
}

TEST_P(IncrTest, WithZeroTTL)
{
    // Set record with TTL 3 seconds.
    set_with_ttl(100, 5000, 5);

    for (int i = 0; i < 3; ++i) {
        test_incr(5000, 0, 1, 101 + i);

        int ttl_seconds{0};
        get_ttl(ttl_seconds);
        ASSERT_GE(5, ttl_seconds);
        ASSERT_LT(0, ttl_seconds);
    }

    // Sleep long enough so that the record expires due to exceeding the TTL.
    sleep(6);

    // Should not be found since the record has expired.
    should_not_found();

    test_incr(5000, 0, 1, 1);
    should_no_ttl();
}

TEST_P(IncrTest, WithPositiveTTL)
{
    set_with_ttl(100, 5000, 5);

    int ttl_seconds{0};

    get_ttl(ttl_seconds);
    ASSERT_GE(5, ttl_seconds);
    ASSERT_LT(0, ttl_seconds);

    test_incr(5000, 15, 1, 101);

    get_ttl(ttl_seconds);
    ASSERT_GE(15, ttl_seconds);
    ASSERT_LT(10, ttl_seconds);

    test_incr(5000, 25, 1, 102);

    get_ttl(ttl_seconds);
    ASSERT_GE(25, ttl_seconds);
    ASSERT_LT(20, ttl_seconds);

    // Preserve
    test_incr(5000, 0, 1, 103);

    get_ttl(ttl_seconds);
    ASSERT_GE(25, ttl_seconds);
    ASSERT_LT(20, ttl_seconds);
}

/* TEST_P(IncrTest, WithInvalidTTL)
{
PERR_INVALID_ARGUMENT
} */

TEST_P(IncrTest, ResetTTL)
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
