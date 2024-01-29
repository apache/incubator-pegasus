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
#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <thread>
#include <vector>

#include "test_util/test_util.h"
#include "utils/long_adder.h"
#include "utils/ports.h"
#include "utils/process_utils.h"
#include "utils/string_conv.h"
#include "utils/strings.h"

// The simplest implementation of long adder: just wrap std::atomic<int64_t>.
class simple_long_adder
{
public:
    simple_long_adder() = default;

    ~simple_long_adder() = default;

    inline void increment_by(int64_t x) { _value.fetch_add(x, std::memory_order_relaxed); }

    inline int64_t value() const { return _value.load(std::memory_order_relaxed); }

    inline void reset() { set(0); }

    inline int64_t fetch_and_reset() { return _value.exchange(0, std::memory_order_relaxed); }

private:
    inline void set(int64_t val) { _value.store(val, std::memory_order_relaxed); }

    std::atomic<int64_t> _value{0};

    DISALLOW_COPY_AND_ASSIGN(simple_long_adder);
};

// A modification of perf_counter_number_atomic from perf_counter.
// This modification has removed virtual functions from original version, where main interfaces
// has been implemented as virtual functions, however, which will slow down the execution.
#define DIVIDE_CONTAINER 107
class divided_long_adder
{
public:
    divided_long_adder()
    {
        for (int i = 0; i < DIVIDE_CONTAINER; ++i) {
            _value[i].store(0);
        }
    }

    ~divided_long_adder() = default;

    inline void increment_by(int64_t x)
    {
        auto task_id = static_cast<uint32_t>(dsn::utils::get_current_tid());
        _value[task_id % DIVIDE_CONTAINER].fetch_add(x, std::memory_order_relaxed);
    }

    int64_t value() const
    {
        int64_t sum = 0;
        for (int i = 0; i < DIVIDE_CONTAINER; ++i) {
            sum += _value[i].load(std::memory_order_relaxed);
        }
        return sum;
    }

    inline void reset() { set(0); }

    int64_t fetch_and_reset()
    {
        int64_t sum = 0;
        for (int i = 0; i < DIVIDE_CONTAINER; ++i) {
            sum += _value[i].exchange(0, std::memory_order_relaxed);
        }
        return sum;
    }

private:
    void set(int64_t val)
    {
        for (int i = 0; i < DIVIDE_CONTAINER; ++i) {
            _value[i].store(0, std::memory_order_relaxed);
        }
        _value[0].store(val, std::memory_order_relaxed);
    }

    std::atomic<int64_t> _value[DIVIDE_CONTAINER];

    DISALLOW_COPY_AND_ASSIGN(divided_long_adder);
};

void print_usage(const char *cmd)
{
    fmt::print(stderr, "USAGE: {} <num_operations> <num_threads> <long_adder_type>\n", cmd);
    fmt::print(stderr, "Run a simple benchmark that executes each sort of long adder.\n\n");

    fmt::print(
        stderr,
        "    <num_operations>       the number of increment operations executed by each thread\n");
    fmt::print(stderr, "    <num_threads>          the number of threads\n");
    fmt::print(stderr,
               "    <long_adder_type>      the type of long adder: simple_long_adder, "
               "divided_long_adder, striped_long_adder, concurrent_long_adder\n");
}

template <typename Adder>
void run_bench(int64_t num_operations, int64_t num_threads, const char *name)
{
    dsn::long_adder_wrapper<Adder> adder;

    std::vector<std::thread> threads;

    pegasus::stop_watch sw;
    for (int64_t i = 0; i < num_threads; i++) {
        threads.emplace_back([num_operations, &adder]() {
            for (int64_t i = 0; i < num_operations; ++i) {
                adder.increment();
            }
        });
    }
    for (auto &t : threads) {
        t.join();
    }
    sw.stop_and_output(fmt::format("Running {} operations of {} with {} threads, result = {}",
                                   num_operations,
                                   name,
                                   num_threads,
                                   adder.value()));
}

int main(int argc, char **argv)
{
    if (argc < 4) {
        print_usage(argv[0]);
        ::exit(-1);
    }

    int64_t num_operations;
    if (!dsn::buf2int64(argv[1], num_operations)) {
        fmt::print(stderr, "Invalid num_operations: {}\n\n", argv[1]);

        print_usage(argv[0]);
        ::exit(-1);
    }

    int64_t num_threads;
    if (!dsn::buf2int64(argv[2], num_threads)) {
        fmt::print(stderr, "Invalid num_threads: {}\n\n", argv[2]);

        print_usage(argv[0]);
        ::exit(-1);
    }

    const char *long_adder_type = argv[3];
    if (dsn::utils::equals(long_adder_type, "simple_long_adder")) {
        run_bench<simple_long_adder>(num_operations, num_threads, long_adder_type);
    } else if (dsn::utils::equals(long_adder_type, "divided_long_adder")) {
        run_bench<divided_long_adder>(num_operations, num_threads, long_adder_type);
    } else if (dsn::utils::equals(long_adder_type, "striped_long_adder")) {
        run_bench<dsn::striped_long_adder>(num_operations, num_threads, long_adder_type);
    } else if (dsn::utils::equals(long_adder_type, "concurrent_long_adder")) {
        run_bench<dsn::concurrent_long_adder>(num_operations, num_threads, long_adder_type);
    } else {
        fmt::print(stderr, "Invalid long_adder_type: {}\n\n", long_adder_type);

        print_usage(argv[0]);
        ::exit(-1);
    }

    return 0;
}
