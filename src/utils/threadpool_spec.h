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

#include <string>
#include <list>

#include "utils/enum_helper.h"
#include "utils/config_helper.h"
#include "utils/threadpool_code.h"

namespace dsn {

enum worker_priority_t
{
    THREAD_xPRIORITY_LOWEST,
    THREAD_xPRIORITY_BELOW_NORMAL,
    THREAD_xPRIORITY_NORMAL,
    THREAD_xPRIORITY_ABOVE_NORMAL,
    THREAD_xPRIORITY_HIGHEST,
    THREAD_xPRIORITY_COUNT,
    THREAD_xPRIORITY_INVALID
};

ENUM_BEGIN(worker_priority_t, THREAD_xPRIORITY_INVALID)
ENUM_REG(THREAD_xPRIORITY_LOWEST)
ENUM_REG(THREAD_xPRIORITY_BELOW_NORMAL)
ENUM_REG(THREAD_xPRIORITY_NORMAL)
ENUM_REG(THREAD_xPRIORITY_ABOVE_NORMAL)
ENUM_REG(THREAD_xPRIORITY_HIGHEST)
ENUM_END(worker_priority_t)

struct threadpool_spec
{
    std::string name;
    dsn::threadpool_code pool_code;
    int worker_count;
    worker_priority_t worker_priority;
    bool worker_share_core;
    uint64_t worker_affinity_mask;
    int dequeue_batch_size;
    bool partitioned; // false by default
    std::string queue_factory_name;
    std::string worker_factory_name;
    std::list<std::string> queue_aspects;
    std::list<std::string> worker_aspects;
    int queue_length_throttling_threshold;
    bool enable_virtual_queue_throttling;

    threadpool_spec(const dsn::threadpool_code &code) : name(code.to_string()), pool_code(code) {}
    threadpool_spec(const threadpool_spec &source) = default;
    threadpool_spec &operator=(const threadpool_spec &source) = default;

    static bool init(/*out*/ std::vector<threadpool_spec> &specs);
};

CONFIG_BEGIN(threadpool_spec)
CONFIG_FLD_STRING(name, "", "Thread pool name")
CONFIG_FLD(int, uint64, worker_count, 4, "The number of threads in the thread pool")
CONFIG_FLD(int,
           uint64,
           dequeue_batch_size,
           5,
           "how many tasks (if available) should be returned "
           "for one dequeue call for best batching performance")
CONFIG_FLD_ENUM(worker_priority_t,
                worker_priority,
                THREAD_xPRIORITY_NORMAL,
                THREAD_xPRIORITY_INVALID,
                false,
                "The scheduling priority of threads in OS")
CONFIG_FLD(bool, bool, worker_share_core, true, "whether the threads share all assigned cores")
CONFIG_FLD(uint64_t,
           uint64,
           worker_affinity_mask,
           0,
           "what CPU cores are assigned to this pool, 0 for all")
CONFIG_FLD(bool,
           bool,
           partitioned,
           false,
           "Whether each thread has its own task queue, and tasks are assigned to a specific "
           "thread for execution based on a hash rule to reduce lock contention. Otherwise, "
           "the threads share a single queue")
CONFIG_FLD_STRING(queue_factory_name, "", "task queue provider name")
CONFIG_FLD_STRING(worker_factory_name, "", "task worker provider name")
CONFIG_FLD_STRING_LIST(queue_aspects, "task queue aspects names, usually for tooling purpose")
CONFIG_FLD_STRING_LIST(worker_aspects, "task aspects names, usually for tooling purpose")
CONFIG_FLD(int,
           uint64,
           queue_length_throttling_threshold,
           1000000,
           "throttling: throttling threshold above which rpc requests will be dropped")
CONFIG_FLD(bool,
           bool,
           enable_virtual_queue_throttling,
           false,
           "throttling: whether to enable throttling with virtual queues")
CONFIG_END
} // namespace dsn
