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

#include <cstdint>
#include <functional>
#include <list>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "runtime/simulator.h"
#include "task/task.h"
#include "task/task_worker.h"
#include "utils/extensible_object.h"
#include "utils/singleton.h"
#include "utils/synchronize.h"

namespace dsn {
class task_queue;

namespace tools {

struct event_entry
{
    task *app_task;
    std::function<void()> system_task;
};

class event_wheel
{
public:
    ~event_wheel() { clear(); }

    void add_event(uint64_t ts, task *t);
    void add_system_event(uint64_t ts, std::function<void()> t);
    std::vector<event_entry> *pop_next_events(/*out*/ uint64_t &ts);
    void clear();
    bool has_more_events() const
    {
        utils::auto_lock<::dsn::utils::ex_lock> l(_lock);
        return _events.size() > 0;
    }

private:
    typedef std::map<uint64_t, std::vector<event_entry> *> Events;
    Events _events;
    mutable ::dsn::utils::ex_lock _lock;
};

struct sim_worker_state
{
    utils::semaphore runnable;
    int index;
    task_worker *worker;
    bool first_time_schedule;
    bool in_continuation;
    bool is_continuation_ready;

    static void deletor(void *p) { delete (sim_worker_state *)p; }
};

class scheduler : public utils::singleton<scheduler>
{
public:
    void start();
    uint64_t now_ns() const
    {
        utils::auto_lock<::dsn::utils::ex_lock> l(_lock);
        return _time_ns;
    }

    void reset();
    void add_task(task *task, task_queue *q);
    void add_system_event(uint64_t ts_ns, std::function<void()> t);

    // TODO: time delay for true, true
    void wait_schedule(bool in_continue, bool is_continue_ready = false);
    void add_checker(const std::string &name, checker::factory f);
    static bool is_scheduling() { return _is_scheduling; }

public:
    struct task_state_ext
    {
        task_queue *queue;
        std::list<sim_worker_state *> wait_threads;

        static void deletor(void *p) { delete (task_state_ext *)p; }

        task_state_ext() { queue = nullptr; };
    };
    typedef object_extension_helper<sim_worker_state, task_worker> task_worker_ext;
    typedef object_extension_helper<task_state_ext, task> task_ext;

private:
    event_wheel _wheel;
    mutable ::dsn::utils::ex_lock _lock;
    uint64_t _time_ns;
    bool _running;
    std::vector<sim_worker_state *> _threads;
    sim_worker_state *_running_thread;
    static __thread bool _is_scheduling;

    struct checker_info
    {
        std::string name;
        checker::factory creator;
        std::unique_ptr<checker> instance;
    };
    std::vector<checker_info> _checkers;

private:
    scheduler(void);
    ~scheduler(void);

    void schedule();
    void check();

    static void on_task_worker_create(task_worker *worker);
    static void on_task_worker_start(task_worker *worker);
    static void on_task_wait(task *waitor, task *waitee, uint32_t timeout_milliseconds);
    static void on_task_wait_notified(task *task);

    friend class utils::singleton<scheduler>;
};

// ------------------  inline implementation ----------------------------

inline void scheduler::reset() { _wheel.clear(); }
} // namespace tools
} // namespace dsn
