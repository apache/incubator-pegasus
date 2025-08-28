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

    void test_incr_on_del(int64_t increment) { test_incr_on_del(0, increment); }

    void test_incr_on_del(int ttl_seconds, int64_t increment)
    {
        del();

        test_incr(ttl_seconds, increment, increment);
    }

    template <typename TBaseValue, typename... Args>
    void test_incr_on_set(const TBaseValue &base_value,
                          int64_t increment,
                          int64_t expected_new_value,
                          Args &&...args)
    {
        set(base_value);

        test_incr(0, increment, expected_new_value, std::forward<Args>(args)...);
    }

    template <typename TBaseValue, typename TReadValue>
    void test_incr_detail_on_set(const TBaseValue &base_value,
                                 int64_t increment,
                                 int expected_incr_err,
                                 int64_t expected_resp_value,
                                 int expected_read_err,
                                 const TReadValue &expected_read_value)
    {
        test_incr_detail_on_set(base_value,
                                0,
                                increment,
                                expected_incr_err,
                                expected_resp_value,
                                expected_read_err,
                                expected_read_value);
    }

    template <typename TBaseValue, typename TReadValue>
    void test_incr_detail_on_set(const TBaseValue &base_value,
                                 int ttl_seconds,
                                 int64_t increment,
                                 int expected_incr_err,
                                 int64_t expected_resp_value,
                                 int expected_read_err,
                                 const TReadValue &expected_read_value)
    {
        set(base_value);

        test_incr_detail(5000,
                         ttl_seconds,
                         increment,
                         expected_incr_err,
                         expected_resp_value,
                         expected_read_err,
                         expected_read_value);
    }

    template <typename... Args>
    void test_incr(int ttl_seconds, int64_t increment, int64_t expected_new_value, Args &&...args)
    {
        test_incr_detail(
            5000, ttl_seconds, increment, PERR_OK, expected_new_value, PERR_OK, expected_new_value);

        test_incr(ttl_seconds, std::forward<Args>(args)...);
    }

    void get_ttl(int &ttl_seconds)
    {
        ASSERT_EQ(PERR_OK, _client->ttl(_hash_key, kSortKey, ttl_seconds));
    }

    // Due to factors such as the cost of execution itself, possible timeouts and potential
    // inaccuracies, the retrieved TTL can only be guaranteed to be an approximate value.
    // As such, `expected_max_ttl_seconds` is required to be at least 4.
    void check_appro_ttl(int expected_max_ttl_seconds)
    {
        int actual_ttl_seconds{0};
        get_ttl(actual_ttl_seconds);
        ASSERT_GE(expected_max_ttl_seconds, actual_ttl_seconds);

        // Allow at most 3 seconds timeout.
        ASSERT_LT(3, expected_max_ttl_seconds);
        ASSERT_LE(expected_max_ttl_seconds - 3, actual_ttl_seconds);
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
        ASSERT_EQ(PERR_OK, _client->set(_hash_key, kSortKey, fmt::format("{}", value)));

        std::string read_value;
        ASSERT_EQ(PERR_OK, _client->get(_hash_key, kSortKey, read_value));
        ASSERT_EQ(fmt::format("{}", value), read_value);
    }

    // The end of recursion.
    void test_incr(int ttl_seconds) {}

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
        if (expected_read_err != PERR_OK) {
            return;
        }

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
    // New value in response will be kept as 0 by default while base value is invalid.
    test_incr_detail_on_set("aaa", 1, PERR_INVALID_ARGUMENT, 0, PERR_OK, "aaa");
}

TEST_P(IncrTest, OnTooLargeValue)
{
    // New value in response will be kept as 0 by default while base value too large.
    test_incr_detail_on_set("10000000000000000000000000000000000000",
                            1,
                            PERR_INVALID_ARGUMENT,
                            0,
                            PERR_OK,
                            "10000000000000000000000000000000000000");
}

TEST_P(IncrTest, Overflow)
{
    // New value in response will be set to base value if overflows.
    test_incr_detail_on_set(
        1, std::numeric_limits<int64_t>::max(), PERR_INVALID_ARGUMENT, 1, PERR_OK, 1);
}

TEST_P(IncrTest, Underflow)
{
    // New value in response will be set to base value if underflows.
    test_incr_detail_on_set(
        -1, std::numeric_limits<int64_t>::min(), PERR_INVALID_ARGUMENT, -1, PERR_OK, -1);
}

TEST_P(IncrTest, PreserveTTL)
{
    // Set the record with specified TTL.
    test_incr_on_del(6, 100);
    check_appro_ttl(6);

    for (int i = 0; i < 3; ++i) {
        // TTL for the record will be kept unchanged.
        test_incr(0, 1, 101 + i);
        check_appro_ttl(6 - i);

        sleep(1);
    }

    // Sleep long enough so that the record expires due to exceeding the TTL.
    sleep(4);

    // Should not be found since the record has expired.
    should_not_found();

    // If the record has been expired, its base value will be reset to 0 without TTL.
    test_incr(0, 1, 1);
    should_no_ttl();
}

TEST_P(IncrTest, UpdateTTL)
{
    // Set the record with specified TTL.
    test_incr_on_del(10, 100);
    check_appro_ttl(10);

    // Change TTL to make it larger.
    test_incr(15, 1, 101);
    check_appro_ttl(15);

    // Change TTL to make it smaller.
    test_incr(5, 1, 102);
    check_appro_ttl(5);

    // Sleep long enough so that the record expires due to exceeding the TTL.
    sleep(6);

    // Should not be found since the record has expired.
    should_not_found();

    // Update TTL on the record that was expired.
    test_incr(5, 1, 1);
    check_appro_ttl(5);
}

TEST_P(IncrTest, WithInvalidTTL)
{
    // New value in response will be set to 0 if TTL < -1.
    test_incr_detail_on_set(100, -2, 1, PERR_INVALID_ARGUMENT, 0, PERR_OK, 100);
}

TEST_P(IncrTest, DisableTTL)
{
    // Set the record without TTL.
    test_incr_on_del(0, 100);
    should_no_ttl();

    // The record will keep non-TTL with -1.
    test_incr(-1, 1, 101);
    should_no_ttl();

    // Enable TTL with 5 seconds.
    test_incr(5, 1, 102);
    check_appro_ttl(5);

    // Disable TTL with -1.
    test_incr(-1, 1, 103);
    should_no_ttl();

    // Enable TTL with 5 seconds.
    test_incr(5, 1, 104);
    check_appro_ttl(5);

    // Sleep long enough so that the record expires due to exceeding the TTL.
    sleep(6);

    // Should not be found since the record has expired.
    should_not_found();

    // The record that was expired will keep non-TTL with -1.
    test_incr(-1, 1, 1);
    should_no_ttl();
}

INSTANTIATE_TEST_SUITE_P(AtomicWriteTest,
                         IncrTest,
                         testing::ValuesIn(generate_atomic_write_cases()));

} // namespace pegasus
