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

#include <cstdint>
#include <utility>

#include "runtime/scheduler.h"
#include "runtime/task/task.h"
#include "runtime/task/task_queue.h"
#include "task_engine.sim.h"
#include "utils/fmt_logging.h"
#include "utils/process_utils.h"
#include "utils/rand.h"
#include "utils/utils.h"

namespace dsn {
class task_worker_pool;

namespace tools {

void sim_timer_service::add_timer(task *task) { scheduler::instance().add_task(task, nullptr); }

sim_task_queue::sim_task_queue(task_worker_pool *pool, int index, task_queue *inner_provider)
    : task_queue(pool, index, inner_provider)
{
}

void sim_task_queue::enqueue(task *t)
{
    CHECK_EQ_MSG(0, t->delay_milliseconds(), "delay time must be zero");
    if (_tasks.size() > 0) {
        do {
            int random_pos = rand::next_u32(0, 1000000);
            auto pr = _tasks.insert(std::map<uint32_t, task *>::value_type(random_pos, t));
            if (pr.second)
                break;
        } while (true);
    } else {
        int random_pos = rand::next_u32(0, 1000000);
        _tasks.insert(std::map<uint32_t, task *>::value_type(random_pos, t));
    }

    // for scheduling
    if (task::get_current_worker()) {
        scheduler::instance().wait_schedule(true, true);
    }
}

// we always return 1 or 0 task in simulator
task *sim_task_queue::dequeue(/*inout*/ int &batch_size)
{
    scheduler::instance().wait_schedule(false);

    if (_tasks.size() > 0) {
        auto t = _tasks.begin()->second;
        _tasks.erase(_tasks.begin());
        batch_size = 1;
        return t;
    } else {
        batch_size = 0;
        return nullptr;
    }
}

void sim_semaphore_provider::signal(int count)
{
    _count += count;

    while (!_wait_threads.empty() && _count > 0) {
        --_count;

        sim_worker_state *thread = _wait_threads.front();
        _wait_threads.pop_front();
        thread->is_continuation_ready = true;
    }
}

bool sim_semaphore_provider::wait(int timeout_milliseconds)
{
    if (_count > 0) {
        --_count;
        scheduler::instance().wait_schedule(true, true);
        return true;
    } else if (0 == timeout_milliseconds) {
        scheduler::instance().wait_schedule(true, true);
        return false;
    } else {
        // wait success
        if (static_cast<unsigned int>(timeout_milliseconds) == TIME_MS_MAX ||
            rand::next_double01() <= 0.5) {
            _wait_threads.push_back(scheduler::task_worker_ext::get(task::get_current_worker()));
            scheduler::instance().wait_schedule(true, false);
            return true;
        }

        // timeout
        else {
            scheduler::instance().wait_schedule(true, true);
            return false;
        }
    }
}

sim_lock_provider::sim_lock_provider(lock_provider *inner_provider)
    : lock_provider(inner_provider), _sema(1, nullptr)
{
    _current_holder = -1;
    _lock_depth = 0;
}

sim_lock_provider::~sim_lock_provider() {}

void sim_lock_provider::lock()
{
    // ignore locks inside schedulers
    if (scheduler::is_scheduling())
        return;

    int ctid = ::dsn::utils::get_current_tid();
    if (ctid == _current_holder) {
        ++_lock_depth;
        return;
    }

    _sema.wait(TIME_MS_MAX);

    CHECK(-1 == _current_holder && _lock_depth == 0, "must be unlocked state");
    _current_holder = ctid;
    ++_lock_depth;
}

bool sim_lock_provider::try_lock()
{
    // ignore locks inside schedulers
    if (scheduler::is_scheduling())
        return true;

    int ctid = ::dsn::utils::get_current_tid();
    if (ctid == _current_holder) {
        ++_lock_depth;
        return true;
    }

    bool r = _sema.wait(0);
    if (r) {
        CHECK(-1 == _current_holder && _lock_depth == 0, "must be unlocked state");
        _current_holder = ctid;
        ++_lock_depth;
    }
    return r;
}

void sim_lock_provider::unlock()
{
    // ignore locks inside schedulers
    if (scheduler::is_scheduling())
        return;

    CHECK_EQ_MSG(::dsn::utils::get_current_tid(),
                 _current_holder,
                 "lock must be locked must current holder");

    if (0 == --_lock_depth) {
        _current_holder = -1;
        _sema.signal(1);
    } else {
    }
}

sim_lock_nr_provider::sim_lock_nr_provider(lock_nr_provider *inner_provider)
    : lock_nr_provider(inner_provider), _sema(1, nullptr)
{
    _current_holder = -1;
    _lock_depth = 0;
}

sim_lock_nr_provider::~sim_lock_nr_provider() {}

void sim_lock_nr_provider::lock()
{
    // ignore locks inside schedulers
    if (scheduler::is_scheduling())
        return;

    int ctid = ::dsn::utils::get_current_tid();
    CHECK_NE_MSG(ctid, _current_holder, "non-recursive lock, error or use recursive locks instead");

    _sema.wait(TIME_MS_MAX);

    CHECK(-1 == _current_holder && _lock_depth == 0, "must be unlocked state");
    _current_holder = ctid;
    ++_lock_depth;
}

bool sim_lock_nr_provider::try_lock()
{
    // ignore locks inside schedulers
    if (scheduler::is_scheduling())
        return true;

    int ctid = ::dsn::utils::get_current_tid();
    CHECK_NE_MSG(ctid, _current_holder, "non-recursive lock, error or use recursive locks instead");

    bool r = _sema.wait(0);
    if (r) {
        CHECK(-1 == _current_holder && _lock_depth == 0, "must be unlocked state");
        _current_holder = ctid;
        ++_lock_depth;
    }
    return r;
}

void sim_lock_nr_provider::unlock()
{
    // ignore locks inside schedulers
    if (scheduler::is_scheduling())
        return;

    CHECK_EQ_MSG(::dsn::utils::get_current_tid(),
                 _current_holder,
                 "lock must be locked must current holder");

    CHECK_EQ_MSG(0, --_lock_depth, "non-recursive lock, error or use recursive locks instead");
    _current_holder = -1;
    _sema.signal(1);
}
}
} // end namespace
