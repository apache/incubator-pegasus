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

#pragma once
#include <iomanip>
#include "perf_counter/perf_counter_wrapper.h"

namespace dsn {
namespace tools {

enum perf_counter_ptr_type
{
    TASK_QUEUEING_TIME_NS,
    TASK_EXEC_TIME_NS,
    TASK_THROUGHPUT,
    TASK_CANCELLED,
    AIO_LATENCY_NS,
    RPC_SERVER_LATENCY_NS,
    RPC_SERVER_SIZE_PER_REQUEST_IN_BYTES,
    RPC_SERVER_SIZE_PER_RESPONSE_IN_BYTES,
    RPC_CLIENT_NON_TIMEOUT_LATENCY_NS,
    RPC_CLIENT_TIMEOUT_THROUGHPUT,
    TASK_IN_QUEUE,
    RPC_DROPPED_IF_TIMEOUT,

    PERF_COUNTER_COUNT,
    PERF_COUNTER_INVALID
};

class counter_info
{
public:
    counter_info(const std::vector<std::string> &command_keys,
                 perf_counter_ptr_type ptr_type,
                 dsn_perf_counter_type_t counter_type,
                 const std::string &title,
                 const std::string &unit)
        : keys(command_keys),
          counter_ptr_type(ptr_type),
          type(counter_type),
          title(title),
          unit_name(unit)
    {
    }

    std::vector<std::string> keys;
    perf_counter_ptr_type counter_ptr_type;
    dsn_perf_counter_type_t type;
    std::string title;
    std::string unit_name;
};

struct task_spec_profiler
{
    perf_counter_wrapper ptr[PERF_COUNTER_COUNT];
    bool collect_call_count;
    bool is_profile;
    std::atomic<int64_t> *call_counts;

    task_spec_profiler()
    {
        collect_call_count = false;
        is_profile = false;
        call_counts = nullptr;
        memset((void *)ptr, 0, sizeof(ptr));
    }
};
} // namespace tools
} // namespace dsn
