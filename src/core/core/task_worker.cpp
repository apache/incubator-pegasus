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
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/tool-api/task_worker.h>
#include "task_engine.h"
#include <sstream>
#include <errno.h>

#ifdef _WIN32

#else
#include <pthread.h>

#ifdef __FreeBSD__
#include <pthread_np.h>
#endif

#ifdef __APPLE__
#include <mach/thread_policy.h>
#endif

#endif

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
    _native_tid = ::dsn::utils::get_invalid_tid();

    char name[256];
    sprintf(name, "%5s.%s.%u", pool->node()->full_name(), pool->spec().name.c_str(), index);
    _name = std::string(name);
    _is_running = false;

    _thread = nullptr;
    _processed_task_count = 0;
}

task_worker::~task_worker() { stop(); }

void task_worker::start()
{
    if (_is_running)
        return;

    _is_running = true;

    _thread = new std::thread(std::bind(&task_worker::run_internal, this));

    _started.wait();
}

void task_worker::stop()
{
    if (!_is_running)
        return;

    _is_running = false;

    _thread->join();
    delete _thread;
    _thread = nullptr;

    _is_running = false;
}

void task_worker::set_name(const char *name)
{
#ifdef _WIN32

#ifndef MS_VC_EXCEPTION
#define MS_VC_EXCEPTION 0x406D1388
#endif

    typedef struct tagTHREADNAME_INFO
    {
        uint32_t dwType;     // Must be 0x1000.
        LPCSTR szName;       // Pointer to name (in user addr space).
        uint32_t dwThreadID; // Thread ID (-1=caller thread).
        uint32_t dwFlags;    // Reserved for future use, must be zero.
    } THREADNAME_INFO;

    THREADNAME_INFO info;
    info.dwType = 0x1000;
    info.szName = name;
    info.dwThreadID = (uint32_t)-1;
    info.dwFlags = 0;

    __try {
        ::RaiseException(MS_VC_EXCEPTION, 0, sizeof(info) / sizeof(uint32_t), (ULONG_PTR *)&info);
    } __except (EXCEPTION_CONTINUE_EXECUTION) {
    }

#else
    std::string sname(name);
    auto thread_name = sname
#ifdef __linux__
                           .substr(0, (16 - 1))
#endif
        ;
    auto tid = pthread_self();
    int err = 0;
#ifdef __FreeBSD__
    pthread_set_name_np(tid, thread_name.c_str());
#elif defined(__linux__)
    err = pthread_setname_np(tid, thread_name.c_str());
#elif defined(__APPLE__)
    err = pthread_setname_np(thread_name.c_str());
#endif
    if (err != 0) {
        dwarn("Fail to set pthread name. err = %d", err);
    }
#endif
}

void task_worker::set_priority(worker_priority_t pri)
{
#ifndef _WIN32
#ifndef __linux__
    static int policy = SCHED_OTHER;
#endif
    static int prio_max =
#ifdef __linux__
        -20;
#else
        sched_get_priority_max(policy);
#endif
    static int prio_min =
#ifdef __linux__
        19;
#else
        sched_get_priority_min(policy);
#endif
    static int prio_middle = ((prio_min + prio_max + 1) / 2);
#endif

    static int g_thread_priority_map[] = {
#ifdef _WIN32
        THREAD_PRIORITY_LOWEST,
        THREAD_PRIORITY_BELOW_NORMAL,
        THREAD_PRIORITY_NORMAL,
        THREAD_PRIORITY_ABOVE_NORMAL,
        THREAD_PRIORITY_HIGHEST
#else
        prio_min, (prio_min + prio_middle) / 2, prio_middle, (prio_middle + prio_max) / 2, prio_max
#endif
    };

    static_assert(ARRAYSIZE(g_thread_priority_map) == THREAD_xPRIORITY_COUNT,
                  "ARRAYSIZE(g_thread_priority_map) != THREAD_xPRIORITY_COUNT");

    int prio = g_thread_priority_map[static_cast<int>(pri)];
    bool succ = true;
#if !defined(_WIN32) && !defined(__linux__)
    struct sched_param param;
    memset(&param, 0, sizeof(struct sched_param));
    param.sched_priority = prio;
#endif

#ifdef _WIN32
    succ = (::SetThreadPriority(::GetCurrentThread(), prio) == TRUE);
#elif defined(__linux__)
    if ((nice(prio) == -1) && (errno != 0)) {
        succ = false;
    }
#else
    succ = (pthread_setschedparam(pthread_self(), policy, &param) == 0);
//# error "not implemented"
#endif

    if (!succ) {
        dwarn("You may need priviledge to set thread priority. errno = %d", errno);
    }
}

void task_worker::set_affinity(uint64_t affinity)
{
    dassert(affinity > 0, "affinity cannot be 0.");

    int nr_cpu = static_cast<int>(std::thread::hardware_concurrency());
    if (nr_cpu < 64) {
        dassert(affinity <= (((uint64_t)1 << nr_cpu) - 1),
                "There are %d cpus in total, while setting thread affinity to a nonexistent one.",
                nr_cpu);
    }

    int err = 0;
#ifdef _WIN32
    if (::SetThreadAffinityMask(::GetCurrentThread(), static_cast<DWORD_PTR>(affinity)) == 0) {
        err = static_cast<int>(::GetLastError());
    }
#elif defined(__APPLE__)
    thread_affinity_policy_data_t policy;
    policy.affinity_tag = static_cast<integer_t>(affinity);
    err = static_cast<int>(thread_policy_set(static_cast<thread_t>(::dsn::utils::get_current_tid()),
                                             THREAD_AFFINITY_POLICY,
                                             (thread_policy_t)&policy,
                                             THREAD_AFFINITY_POLICY_COUNT));
#else
#ifdef __FreeBSD__
#ifndef cpu_set_t
#define cpu_set_t cpuset_t
#endif
#endif
    cpu_set_t cpuset;
    int nr_bits = std::min(nr_cpu, static_cast<int>(sizeof(affinity) * 8));

    CPU_ZERO(&cpuset);
    for (int i = 0; i < nr_bits; i++) {
        if ((affinity & ((uint64_t)1 << i)) != 0) {
            CPU_SET(i, &cpuset);
        }
    }
    err = pthread_setaffinity_np(pthread_self(), sizeof(cpuset), &cpuset);
#endif

    if (err != 0) {
        dwarn("Fail to set thread affinity. err = %d", err);
    }
}

void task_worker::run_internal()
{
    while (_thread == nullptr) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    task::set_tls_dsn_context(pool()->node(), this, queue());

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
            derror("mask for %s is set to 0x0, mostly due to that #core > 64, set to 64 now",
                   pool_spec().name.c_str());

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

    // try {
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
        dassert(count == batch_size,
                "returned task count and batch size do not match: %d vs %d",
                count,
                batch_size);
#endif

        _processed_task_count += batch_size;
    }
    /*}
    catch (std::exception& ex)
    {
        dassert (false, "%s: unhandled exception '%s'", name().c_str(), ex.what());
    }*/
}

const threadpool_spec &task_worker::pool_spec() const { return pool()->spec(); }

} // end namespace
