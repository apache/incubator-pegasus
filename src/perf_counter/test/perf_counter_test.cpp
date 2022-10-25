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
 *     Unit-test for perf counter.
 *
 * Revision history:
 *     Nov., 2015, @shengofsun (Weijie Sun), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "runtime/tool_api.h"
#include <gtest/gtest.h>
#include <thread>
#include <cmath>
#include <vector>

#include "perf_counter/perf_counter_atomic.h"

using namespace dsn;
using namespace dsn::tools;

const int count_times = 10000;

static void adder_function(perf_counter_ptr pc, int id, const std::vector<int> &vec)
{
    for (int i = id; i < 10000; i += 10)
        pc->add(vec[i]);
}

static void perf_counter_inc_dec(perf_counter_ptr pc)
{
    std::thread inc_thread(
        [](perf_counter_ptr counter) {
            for (int i = 0; i < count_times; ++i)
                counter->increment();
        },
        pc);
    std::thread dec_thread(
        [](perf_counter_ptr counter) {
            for (int i = 0; i < count_times; ++i)
                counter->decrement();
        },
        pc);

    inc_thread.join();
    dec_thread.join();
}

typedef std::shared_ptr<std::thread> thread_ptr;
static void perf_counter_add(perf_counter_ptr pc, const std::vector<int> &vec)
{
    std::vector<thread_ptr> add_threads;
    for (int i = 0; i < 10; ++i) {
        thread_ptr t(new std::thread(adder_function, pc, i, std::ref(vec)));
        add_threads.push_back(t);
    }
    for (unsigned int i = 0; i != add_threads.size(); ++i)
        add_threads[i]->join();
}

TEST(perf_counter, perf_counter_atomic)
{
    std::vector<int> vec(10000, 0);
    for (int i = 0; i < vec.size(); ++i) {
        vec[i] = rand() % 100;
    }
    std::vector<int> gen_numbers{1, 5, 1043};
    int sleep_interval = (int)dsn_config_get_value_uint64(
        "components.simple_perf_counter", "counter_computation_interval_seconds", 3, "period");

    perf_counter_ptr counter = new perf_counter_number_atomic(
        "", "", "", dsn_perf_counter_type_t::COUNTER_TYPE_NUMBER, "");
    perf_counter_inc_dec(counter);
    perf_counter_add(counter, vec);
    LOG_INFO("%lf", counter->get_value());

    counter = new perf_counter_volatile_number_atomic(
        "", "", "", dsn_perf_counter_type_t::COUNTER_TYPE_VOLATILE_NUMBER, "");
    perf_counter_inc_dec(counter);
    perf_counter_add(counter, vec);
    LOG_INFO("%lf", counter->get_value());

    counter =
        new perf_counter_rate_atomic("", "", "", dsn_perf_counter_type_t::COUNTER_TYPE_RATE, "");
    perf_counter_inc_dec(counter);
    perf_counter_add(counter, vec);
    LOG_INFO("%lf", counter->get_value());

    counter = new perf_counter_number_percentile_atomic(
        "", "", "", dsn_perf_counter_type_t::COUNTER_TYPE_NUMBER_PERCENTILES, "");
    std::this_thread::sleep_for(std::chrono::seconds(sleep_interval));
    for (auto &count : gen_numbers) {
        for (unsigned int i = 0; i != count; ++i)
            counter->set(rand() % 10000);
        // std::this_thread::sleep_for(std::chrono::seconds(sleep_interval));
        for (int i = 0; i != COUNTER_PERCENTILE_COUNT; ++i)
            LOG_INFO("%lf", counter->get_percentile((dsn_perf_counter_percentile_type_t)i));
    }
}

TEST(perf_counter, print_type)
{
    ASSERT_STREQ("NUMBER", dsn_counter_type_to_string(COUNTER_TYPE_NUMBER));
    ASSERT_STREQ("VOLATILE_NUMBER", dsn_counter_type_to_string(COUNTER_TYPE_VOLATILE_NUMBER));
    ASSERT_STREQ("RATE", dsn_counter_type_to_string(COUNTER_TYPE_RATE));
    ASSERT_STREQ("PERCENTILE", dsn_counter_type_to_string(COUNTER_TYPE_NUMBER_PERCENTILES));
    ASSERT_STREQ("INVALID_COUNTER", dsn_counter_type_to_string(COUNTER_TYPE_INVALID));

    ASSERT_EQ(COUNTER_TYPE_NUMBER,
              dsn_counter_type_from_string(dsn_counter_type_to_string(COUNTER_TYPE_NUMBER)));
    ASSERT_EQ(
        COUNTER_TYPE_VOLATILE_NUMBER,
        dsn_counter_type_from_string(dsn_counter_type_to_string(COUNTER_TYPE_VOLATILE_NUMBER)));
    ASSERT_EQ(COUNTER_TYPE_RATE,
              dsn_counter_type_from_string(dsn_counter_type_to_string(COUNTER_TYPE_RATE)));
    ASSERT_EQ(
        COUNTER_TYPE_NUMBER_PERCENTILES,
        dsn_counter_type_from_string(dsn_counter_type_to_string(COUNTER_TYPE_NUMBER_PERCENTILES)));
    ASSERT_EQ(COUNTER_TYPE_INVALID, dsn_counter_type_from_string("xxxx"));

    ASSERT_STREQ("P50", dsn_percentile_type_to_string(COUNTER_PERCENTILE_50));
    ASSERT_STREQ("P90", dsn_percentile_type_to_string(COUNTER_PERCENTILE_90));
    ASSERT_STREQ("P95", dsn_percentile_type_to_string(COUNTER_PERCENTILE_95));
    ASSERT_STREQ("P99", dsn_percentile_type_to_string(COUNTER_PERCENTILE_99));
    ASSERT_STREQ("P999", dsn_percentile_type_to_string(COUNTER_PERCENTILE_999));
    ASSERT_STREQ("INVALID_PERCENTILE", dsn_percentile_type_to_string(COUNTER_PERCENTILE_INVALID));

    ASSERT_EQ(
        COUNTER_PERCENTILE_50,
        dsn_percentile_type_from_string(dsn_percentile_type_to_string(COUNTER_PERCENTILE_50)));
    ASSERT_EQ(
        COUNTER_PERCENTILE_90,
        dsn_percentile_type_from_string(dsn_percentile_type_to_string(COUNTER_PERCENTILE_90)));
    ASSERT_EQ(
        COUNTER_PERCENTILE_95,
        dsn_percentile_type_from_string(dsn_percentile_type_to_string(COUNTER_PERCENTILE_95)));
    ASSERT_EQ(
        COUNTER_PERCENTILE_99,
        dsn_percentile_type_from_string(dsn_percentile_type_to_string(COUNTER_PERCENTILE_99)));
    ASSERT_EQ(
        COUNTER_PERCENTILE_999,
        dsn_percentile_type_from_string(dsn_percentile_type_to_string(COUNTER_PERCENTILE_999)));
    ASSERT_EQ(COUNTER_PERCENTILE_INVALID, dsn_percentile_type_from_string("afafda"));
}
