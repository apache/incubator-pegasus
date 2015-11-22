#include "../../tools/common/simple_perf_counter.h"
#include "../../tools/common/simple_perf_counter_v2_atomic.h"
#include "../../tools/common/simple_perf_counter_v2_fast.h"

#include <dsn/tool_api.h>
#include <gtest/gtest.h>
#include <thread>
#include <cmath>
#include <vector>

using namespace dsn;
using namespace dsn::tools;

const int count_times = 10000;

static void adder_function(perf_counter* pc, int id, const std::vector<int>& vec) {
    for (int i=id; i<10000; i+=10)
        pc->add(vec[i]);
}

static void perf_counter_inc_dec(perf_counter* pc)
{
    std::thread inc_thread([](perf_counter* counter){
        for (int i=0; i<count_times; ++i)
            counter->increment();
    }, pc);
    std::thread dec_thread([](perf_counter* counter){
        for (int i=0; i<count_times; ++i)
            counter->decrement();
    }, pc);

    inc_thread.join();
    dec_thread.join();
}

typedef std::shared_ptr<std::thread> thread_ptr;
static void perf_counter_add(perf_counter* pc, const std::vector<int>& vec)
{
    std::vector< thread_ptr > add_threads;
    for (int i=0; i<10; ++i) {
        thread_ptr t( new std::thread(adder_function, pc, i, std::ref(vec)) );
        add_threads.push_back(t);
    }
    for (unsigned int i=0; i!=add_threads.size(); ++i)
        add_threads[i]->join();
}

template<class perf_counter_impl>
static void test_perf_counter()
{
    int ans=0;
    std::vector<int> vec(10000, 0);
    for (int i=0; i<vec.size(); ++i) {
        vec[i] = rand()%100;
        ans+=vec[i];
    }
    std::vector<int> gen_numbers{1, 5, 1043};
    int sleep_interval = config()->get_value<int>("components.simple_perf_counter", "counter_computation_interval_seconds", 3, "period");

    perf_counter_impl* counter = new perf_counter_impl("", "", perf_counter_type::COUNTER_TYPE_NUMBER);
    perf_counter_inc_dec(counter);
    perf_counter_add(counter, vec);
    ddebug("%lf", counter->get_value());

    //don't delete the counter as it is shared by timer callback
    //delete counter;

    counter = new perf_counter_impl("", "", perf_counter_type::COUNTER_TYPE_RATE);
    perf_counter_inc_dec(counter);
    perf_counter_add(counter, vec);
    ddebug("%lf", counter->get_value());
    //don't delete the counter as it is shared by timer callback
    //delete counter;

    counter = new perf_counter_impl("", "", perf_counter_type::COUNTER_TYPE_NUMBER_PERCENTILES);
    std::this_thread::sleep_for(std::chrono::seconds(sleep_interval));
    for (auto& count: gen_numbers) {
        for (unsigned int i=0; i!=count; ++i)
            counter->set(rand()%10000);
        std::this_thread::sleep_for(std::chrono::seconds(sleep_interval));
        for (int i=0; i!=COUNTER_PERCENTILE_COUNT; ++i)
            ddebug("%lf", counter->get_percentile((counter_percentile_type)i));
    }
    //don't delete the counter as it is shared by timer callback
    //delete counter;
}

TEST(tools_common, simple_perf_counter)
{
    test_perf_counter<simple_perf_counter>();
}

TEST(tools_common, simple_perf_counter_v2_atomic)
{
    test_perf_counter<simple_perf_counter_v2_atomic>();
}

TEST(tools_common, simple_perf_counter_v2_fast)
{
    test_perf_counter<simple_perf_counter_v2_fast>();
}

