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

#include "../tools/common/simple_perf_counter.h"
#include "../tools/common/simple_perf_counter_v2_atomic.h"
#include "../tools/common/simple_perf_counter_v2_fast.h"

#include <dsn/tool_api.h>
#include <gtest/gtest.h>
#include <thread>
#include <cmath>
#include <vector>

using namespace dsn;
using namespace dsn::tools;

const int count_times = 10000;

static void adder_function(perf_counter_ptr pc, int id, const std::vector<int>& vec) {
    for (int i=id; i<10000; i+=10)
        pc->add(vec[i]);
}

static void perf_counter_inc_dec(perf_counter_ptr pc)
{
    std::thread inc_thread([](perf_counter_ptr counter){
        for (int i=0; i<count_times; ++i)
            counter->increment();
    }, pc);
    std::thread dec_thread([](perf_counter_ptr counter){
        for (int i=0; i<count_times; ++i)
            counter->decrement();
    }, pc);

    inc_thread.join();
    dec_thread.join();
}

typedef std::shared_ptr<std::thread> thread_ptr;
static void perf_counter_add(perf_counter_ptr pc, const std::vector<int>& vec)
{
    std::vector< thread_ptr > add_threads;
    for (int i=0; i<10; ++i) {
        thread_ptr t( new std::thread(adder_function, pc, i, std::ref(vec)) );
        add_threads.push_back(t);
    }
    for (unsigned int i=0; i!=add_threads.size(); ++i)
        add_threads[i]->join();
}

static void test_perf_counter(perf_counter::factory f)
{
    int ans=0;
    std::vector<int> vec(10000, 0);
    for (int i=0; i<vec.size(); ++i) {
        vec[i] = rand()%100;
        ans+=vec[i];
    }
    std::vector<int> gen_numbers{1, 5, 1043};
    int sleep_interval = config()->get_value<int>("components.simple_perf_counter", "counter_computation_interval_seconds", 3, "period");

    perf_counter_ptr counter = f("", "", "", dsn_perf_counter_type_t::COUNTER_TYPE_NUMBER, "");
    perf_counter_inc_dec(counter);
    perf_counter_add(counter, vec);
    ddebug("%lf", counter->get_value());

    counter = f("", "", "", dsn_perf_counter_type_t::COUNTER_TYPE_RATE, "");
    perf_counter_inc_dec(counter);
    perf_counter_add(counter, vec);
    ddebug("%lf", counter->get_value());

    counter = f("", "", "", dsn_perf_counter_type_t::COUNTER_TYPE_NUMBER_PERCENTILES, "");
    std::this_thread::sleep_for(std::chrono::seconds(sleep_interval));
    for (auto& count: gen_numbers) {
        for (unsigned int i=0; i!=count; ++i)
            counter->set(rand()%10000);
        std::this_thread::sleep_for(std::chrono::seconds(sleep_interval));
        for (int i=0; i!=COUNTER_PERCENTILE_COUNT; ++i)
            ddebug("%lf", counter->get_percentile((dsn_perf_counter_percentile_type_t)i));
    }
}

TEST(tools_common, simple_perf_counter)
{
    test_perf_counter(simple_perf_counter_factory);
}

TEST(tools_common, simple_perf_counter_v2_atomic)
{
    test_perf_counter(simple_perf_counter_v2_atomic_factory);
}

TEST(tools_common, simple_perf_counter_v2_fast)
{
    test_perf_counter(simple_perf_counter_v2_fast_factory);
}

