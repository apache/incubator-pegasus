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
#include <stdio.h>
#include <chrono>
#include <functional>
#include <ratio>
#include <thread>
#include <vector>

#include "gtest/gtest.h"
#include "runtime/api_layer1.h"
#include "utils/long_adder.h"

namespace dsn {

template <typename T>
struct type_parse_traits;

#define REGISTER_PARSE_TYPE(X)                                                                     \
    template <>                                                                                    \
    struct type_parse_traits<X>                                                                    \
    {                                                                                              \
        static const char *name;                                                                   \
    };                                                                                             \
    const char *type_parse_traits<X>::name = #X

REGISTER_PARSE_TYPE(striped_long_adder);
REGISTER_PARSE_TYPE(concurrent_long_adder);

template <typename Adder>
class long_adder_test
{
public:
    long_adder_test() = default;

    void run_increment_by(int64_t base_value,
                          int64_t delta,
                          int64_t num_operations,
                          int64_t num_threads,
                          int64_t &result)
    {
        execute(num_threads,
                [this, delta, num_operations]() { this->increment_by(delta, num_operations); });
        result = base_value + delta * num_operations * num_threads;
        ASSERT_EQ(result, _adder.value());
    }

    void
    run_increment(int64_t base_value, int64_t num_operations, int64_t num_threads, int64_t &result)
    {
        execute(num_threads, [this, num_operations]() { this->increment(num_operations); });
        result = base_value + num_operations * num_threads;
        ASSERT_EQ(result, _adder.value());
    }

    void
    run_decrement(int64_t base_value, int64_t num_operations, int64_t num_threads, int64_t &result)
    {
        execute(num_threads, [this, num_operations]() { this->decrement(num_operations); });
        result = base_value - num_operations * num_threads;
        ASSERT_EQ(result, _adder.value());
    }

    void run_basic_cases(int64_t num_threads)
    {
        fmt::print(stdout,
                   "Ready to run basic cases for {} with {} threads.\n",
                   type_parse_traits<Adder>::name,
                   num_threads);

        // Initially should be zero
        int64_t base_value = 0;
        ASSERT_EQ(base_value, _adder.value());

        // Do basic test with custom number of threads
        auto do_increment_by = std::bind(&long_adder_test::run_increment_by,
                                         this,
                                         std::placeholders::_1,
                                         std::placeholders::_2,
                                         std::placeholders::_3,
                                         num_threads,
                                         std::placeholders::_4);
        auto do_increment = std::bind(&long_adder_test::run_increment,
                                      this,
                                      std::placeholders::_1,
                                      std::placeholders::_2,
                                      num_threads,
                                      std::placeholders::_3);
        auto do_decrement = std::bind(&long_adder_test::run_decrement,
                                      this,
                                      std::placeholders::_1,
                                      std::placeholders::_2,
                                      num_threads,
                                      std::placeholders::_3);

        // Test increment_by
        do_increment_by(base_value, 1, 1, base_value);
        do_increment_by(base_value, 100, 1, base_value);
        do_increment_by(base_value, 10, 10, base_value);
        do_increment_by(base_value, -10, 10, base_value);
        do_increment_by(base_value, -100, 1, base_value);
        do_increment_by(base_value, -1, 1, base_value);
        ASSERT_EQ(0, _adder.value());
        ASSERT_EQ(0, base_value);

        // Test increment
        do_increment(base_value, 1, base_value);
        do_increment(base_value, 100, base_value);

        // Fetch and reset
        ASSERT_EQ(base_value, _adder.fetch_and_reset());
        base_value = 0;
        ASSERT_EQ(base_value, _adder.value());

        // Test decrement
        do_decrement(base_value, 100, base_value);
        do_decrement(base_value, 1, base_value);

        // Reset at last
        _adder.reset();
        base_value = 0;
        ASSERT_EQ(base_value, _adder.value());
    }

    void run_concurrent_cases(int64_t num_operations, int64_t num_threads)
    {
        fmt::print(
            stdout, "Ready to run concurrent cases for {}:\n", type_parse_traits<Adder>::name);

        // Initially adder should be zero
        int64_t base_value = 0;
        ASSERT_EQ(base_value, _adder.value());

        // Define runner to time each case
        auto runner = [num_operations, num_threads](
            const char *name, std::function<void(int64_t &)> func, int64_t &result) {
            uint64_t start = dsn_now_ns();
            func(result);
            uint64_t end = dsn_now_ns();

            auto duration_ns = static_cast<int64_t>(end - start);
            std::chrono::nanoseconds nano(duration_ns);
            auto duration_ms =
                std::chrono::duration_cast<std::chrono::duration<double, std::milli>>(nano).count();

            fmt::print(stdout,
                       "Running {} operations of {} with {} threads took {} ms.\n",
                       num_operations,
                       name,
                       num_threads,
                       duration_ms);
        };

        // Test increment
        auto do_increment = std::bind(&long_adder_test::run_increment,
                                      this,
                                      base_value,
                                      num_operations,
                                      num_threads,
                                      std::placeholders::_1);
        runner("Increment", do_increment, base_value);

        // Test decrement
        auto do_decrement = std::bind(&long_adder_test::run_decrement,
                                      this,
                                      base_value,
                                      num_operations,
                                      num_threads,
                                      std::placeholders::_1);
        runner("Decrement", do_decrement, base_value);

        // At last adder should also be zero
        ASSERT_EQ(0, _adder.value());
        ASSERT_EQ(0, base_value);
    }

private:
    void increment_by(int64_t delta, int64_t n)
    {
        for (int64_t i = 0; i < n; ++i) {
            _adder.increment_by(delta);
        }
    }

    void increment(int64_t num)
    {
        for (int64_t i = 0; i < num; ++i) {
            _adder.increment();
        }
    }

    void decrement(int64_t num)
    {
        for (int64_t i = 0; i < num; ++i) {
            _adder.decrement();
        }
    }

    void execute(int64_t num_threads, std::function<void()> runner)
    {
        std::vector<std::thread> threads;
        for (int64_t i = 0; i < num_threads; i++) {
            threads.emplace_back(runner);
        }
        for (auto &t : threads) {
            t.join();
        }
    }

    long_adder_wrapper<Adder> _adder;
};

template <typename Adder>
void run_basic_cases()
{
    long_adder_test<Adder> test;
    test.run_basic_cases(1);
    test.run_basic_cases(4);
}

template <typename Adder0, typename Adder1, typename... Others>
void run_basic_cases()
{
    run_basic_cases<Adder0>();
    run_basic_cases<Adder1, Others...>();
}

template <typename Adder>
void run_concurrent_cases()
{
    long_adder_test<Adder> test;
    test.run_concurrent_cases(10000000, 1);
    test.run_concurrent_cases(10000000, 4);
}

template <typename Adder0, typename Adder1, typename... Others>
void run_concurrent_cases()
{
    run_concurrent_cases<Adder0>();
    run_concurrent_cases<Adder1, Others...>();
}

TEST(long_adder_test, basic_cases) { run_basic_cases<striped_long_adder, concurrent_long_adder>(); }

TEST(long_adder_test, concurrent_cases)
{
    run_concurrent_cases<striped_long_adder, concurrent_long_adder>();
}

} // namespace dsn
