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

#if defined(__linux__)
#include <sys/prctl.h>
#endif // defined(__linux__)

#include <errno.h>
#include <pthread.h>
#include <sched.h>
#include <stdio.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <functional>
#include <list>

#include "runtime/service_engine.h"
#include "runtime/task/task.h"
#include "runtime/task/task_engine.h"
#include "runtime/task/task_queue.h"
#include "runtime/task/task_worker.h"
#include "utils/fmt_logging.h"
#include "utils/join_point.h"
#include "utils/ports.h"
#include "utils/process_utils.h"
#include "utils/safe_strerror_posix.h"

namespace dsn {

join_point<void, task_worker *> task_worker::on_start("task_worker::on_start");
join_point<void, task_worker *> task_worker::on_create("task_worker::on_create");

task_worker::task_worker(task_worker_pool *pool,
                         task_queue *q,
                         int index,
                         task_worker *inner_provider)
{
    _owner_pool = pool;
    _input_queue = q;
    _index = index;
    _native_tid = ::dsn::utils::INVALID_TID;

    char name[256];
    sprintf(name, "%5s.%s.%u", pool->node()->full_name(), pool->spec().name.c_str(), index);
    _name = std::string(name);
    _is_running = false;

    _thread = nullptr;
    _processed_task_count = 0;
}

task_worker::~task_worker()
{
    if (!_is_running)
        return;

    // TODO(wutao1): use join, detach is not work with valgrind
    _thread->detach();
}

void task_worker::start()
{
    if (_is_running)
        return;

    _is_running = true;

    _thread = std::make_unique<std::thread>(std::bind(&task_worker::run_internal, this));

    _started.wait();
}

void task_worker::stop()
{
    if (!_is_running)
        return;

    _is_running = false;

    _thread->join();
}

void task_worker::set_name(const char *name)
{
#if defined(__linux__)
    // http://0pointer.de/blog/projects/name-your-threads.html
    // Set the name for the LWP (which gets truncated to 15 characters).
    // Note that glibc also has a 'pthread_setname_np' api, but it may not be
    // available everywhere and it's only benefit over using prctl directly is
    // that it can set the name of threads other than the current thread.
    int err = prctl(PR_SET_NAME, name);
#else
    int err = pthread_setname_np(name);
#endif // defined(__linux__)
    // We expect EPERM failures in sandboxed processes, just ignore those.
    if (err < 0 && errno != EPERM) {
        LOG_WARNING(
            "Fail to set pthread name: err = {}, msg = {}", err, utils::safe_strerror(errno));
    }
}

void task_worker::set_priority(worker_priority_t pri)
{
    static int prio_max = -20;
    static int prio_min = 19;
    static int prio_middle = ((prio_min + prio_max + 1) / 2);

    static int g_thread_priority_map[] = {prio_min,
                                          (prio_min + prio_middle) / 2,
                                          prio_middle,
                                          (prio_middle + prio_max) / 2,
                                          prio_max};

    static_assert(ARRAYSIZE(g_thread_priority_map) == THREAD_xPRIORITY_COUNT,
                  "ARRAYSIZE(g_thread_priority_map) != THREAD_xPRIORITY_COUNT");

    int prio = g_thread_priority_map[static_cast<int>(pri)];
    bool succ = true;
    if ((nice(prio) == -1) && (errno != 0)) {
        succ = false;
    }
    if (!succ) {
        LOG_WARNING("You may need priviledge to set thread priority: errno = {}, msg = {}",
                    errno,
                    utils::safe_strerror(errno));
    }
}

void task_worker::set_affinity(uint64_t affinity)
{
#if defined(__linux__)
    CHECK_GT(affinity, 0);

    int nr_cpu = static_cast<int>(std::thread::hardware_concurrency());
    if (nr_cpu < 64) {
        CHECK_LE_MSG(
            affinity,
            (((uint64_t)1 << nr_cpu) - 1),
            "There are {} cpus in total, while setting thread affinity to a nonexistent one.",
            nr_cpu);
    }

    int err = 0;
    cpu_set_t cpuset;
    int nr_bits = std::min(nr_cpu, static_cast<int>(sizeof(affinity) * 8));

    CPU_ZERO(&cpuset);
    for (int i = 0; i < nr_bits; i++) {
        if ((affinity & ((uint64_t)1 << i)) != 0) {
            CPU_SET(i, &cpuset);
        }
    }
    err = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);

    if (err != 0) {
        LOG_WARNING(
            "Fail to set thread affinity: err = {}, msg = {}", err, utils::safe_strerror(errno));
    }
#endif // defined(__linux__)
}

void task_worker::run_internal()
{
    while (_thread == nullptr) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    task::set_tls_dsn_context(pool()->node(), this);

    _native_tid = ::dsn::utils::get_current_tid();
    set_name(name().c_str());
    set_priority(pool_spec().worker_priority);

    if (true == pool_spec().worker_share_core) {
        if (pool_spec().worker_affinity_mask > 0) {
            set_affinity(pool_spec().worker_affinity_mask);
        }
    } else {
        uint64_t current_mask = pool_spec().worker_affinity_mask;
        if (0 == current_mask) {
            LOG_ERROR("mask for {} is set to 0x0, mostly due to that #core > 64, set to 64 now",
                      pool_spec().name);

            current_mask = ~((uint64_t)0);
        }
        for (int i = 0; i < _index; ++i) {
            current_mask &= (current_mask - 1);
            if (0 == current_mask) {
                current_mask = pool_spec().worker_affinity_mask;
            }
        }
        current_mask -= (current_mask & (current_mask - 1));

        set_affinity(current_mask);
    }

    _started.notify();

    on_start.execute(this);

    loop();
}

void task_worker::loop()
{
    task_queue *q = queue();
    int best_batch_size = pool_spec().dequeue_batch_size;

    while (_is_running) {
        int batch_size = best_batch_size;
        task *task = q->dequeue(batch_size), *next;

        q->decrease_count(batch_size);

#ifndef NDEBUG
        int count = 0;
#endif
        while (task != nullptr) {
            next = task->next;
            task->next = nullptr;
            task->exec_internal();
            task = next;
#ifndef NDEBUG
            count++;
#endif
        }

#ifndef NDEBUG
        CHECK_EQ_MSG(count, batch_size, "returned task count and batch size do not match");
#endif

        _processed_task_count += batch_size;
    }
}

const threadpool_spec &task_worker::pool_spec() const { return pool()->spec(); }

} // end namespace
