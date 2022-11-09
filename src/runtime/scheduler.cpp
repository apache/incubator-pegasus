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

#include "utils/rand.h"
#include "runtime/simulator.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "runtime/node_scoper.h"
#include "scheduler.h"
#include "env.sim.h"
#include <set>

namespace dsn {
namespace tools {

void event_wheel::add_event(uint64_t ts, task *t)
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);

    std::vector<event_entry> *evts;
    auto itr = _events.find(ts);
    if (itr != _events.end())
        evts = itr->second;
    else {
        evts = new std::vector<event_entry>();
        _events.insert(std::make_pair(ts, evts));
    }

    event_entry entry;
    entry.app_task = t;
    evts->push_back(entry);
}

void event_wheel::add_system_event(uint64_t ts, std::function<void()> t)
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);

    std::vector<event_entry> *evts;
    auto itr = _events.find(ts);
    if (itr != _events.end())
        evts = itr->second;
    else {
        evts = new std::vector<event_entry>();
        _events.insert(std::make_pair(ts, evts));
    }

    event_entry entry;
    entry.system_task = std::move(t);
    entry.app_task = nullptr;
    evts->push_back(entry);
}

std::vector<event_entry> *event_wheel::pop_next_events(/*out*/ uint64_t &ts)
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);

    std::vector<event_entry> *evts = NULL;
    auto itr = _events.begin();
    if (itr != _events.end()) {
        evts = itr->second;
        ts = itr->first;
        _events.erase(itr);
    }
    return evts;
}

void event_wheel::clear()
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);
    _events.clear();
}

//////////////////////////////////////////////////////////////////////////////////////////////

__thread bool scheduler::_is_scheduling = false;

scheduler::scheduler(void)
{
    _time_ns = 0;
    _running = false;
    _running_thread = nullptr;
    task_worker::on_create.put_back(on_task_worker_create, "simulation.on_task_worker_create");
    task_worker::on_start.put_back(on_task_worker_start, "simulation.on_task_worker_start");

    for (int i = 0; i <= dsn::task_code::max(); i++) {
        task_spec::get(i)->on_task_wait_pre.put_back(scheduler::on_task_wait,
                                                     "simulation.on_task_wait");
        task_spec::get(i)->on_task_wait_notified.put_back(scheduler::on_task_wait_notified,
                                                          "simulation.on_task_wait_notified");
    }

    task_ext::register_ext(task_state_ext::deletor);
    task_worker_ext::register_ext(sim_worker_state::deletor);
}

scheduler::~scheduler(void) {}

/*static*/ void scheduler::on_task_worker_start(task_worker *worker)
{
    while (!scheduler::instance()._running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

/*static*/ void scheduler::on_task_worker_create(task_worker *worker)
{
    auto s = task_worker_ext::get_inited(worker);
    s->worker = worker;
    s->first_time_schedule = true;
    s->in_continuation = false;
    s->index = static_cast<int>(scheduler::instance()._threads.size());
    scheduler::instance()._threads.push_back(s);
}

/*static*/ void scheduler::on_task_wait(task *waitor, task *waitee, uint32_t timeout_milliseconds)
{
    if (waitor == nullptr)
        return;

    if (waitee->state() < task_state::TASK_STATE_FINISHED) {
        auto ts = task_ext::get_inited(waitee);
        auto wks = task_worker_ext::get(task::get_current_worker());
        ts->wait_threads.push_back(wks);

        scheduler::instance().wait_schedule(true, false);
    } else {
        scheduler::instance().wait_schedule(true, true);
    }
}

/*static*/ void scheduler::on_task_wait_notified(task *task)
{
    auto ts = task_ext::get(task);
    if (ts != nullptr) {
        for (auto &w : ts->wait_threads) {
            w->is_continuation_ready = true;
        }
    }
}

void scheduler::add_task(task *tsk, task_queue *q)
{
    auto ts = task_ext::get_inited(tsk);
    ts->queue = q;

    auto delay = (uint64_t)tsk->delay_milliseconds() * 1000000;
    tsk->set_delay(0);
    _wheel.add_event(now_ns() + delay, tsk);
}

void scheduler::add_system_event(uint64_t ts_ns, std::function<void()> t)
{
    _wheel.add_system_event(ts_ns, t);
}

void scheduler::start()
{
    // init all checkers
    std::vector<service_app *> apps;
    service_app::get_all_service_apps(&apps);
    for (checker_info &c : _checkers) {
        checker *a_checker = c.creator();
        a_checker->initialize(c.name, apps);
        c.instance.reset(a_checker);
    }

    // set flag
    _running = true;
}

void scheduler::add_checker(const std::string &name, checker::factory f)
{
    checker_info info;
    info.name = name;
    info.creator = f;

    _checkers.emplace_back(std::move(info));
}

void scheduler::check()
{
    for (checker_info &c : _checkers) {
        if (c.instance != nullptr)
            c.instance->check();
    }
}

void scheduler::wait_schedule(bool in_continue, bool is_continue_ready /*= false*/)
{
    auto s = task_worker_ext::get(task::get_current_worker());
    s->in_continuation = in_continue;
    s->is_continuation_ready = is_continue_ready;

    if (s->first_time_schedule) {
        s->first_time_schedule = false;
        if (s->index == 0)
            schedule();
    } else {
        schedule();
    }
    s->runnable.wait();
}

void scheduler::schedule()
{
    _is_scheduling = true;

    check(); // check before schedule

    while (true) {
        // run ready workers whenever possible
        std::vector<int> ready_workers;
        for (auto &s : _threads) {
            if ((s->in_continuation && s->is_continuation_ready) ||
                (!s->in_continuation && s->worker->queue()->count() > 0)) {
                ready_workers.push_back(s->index);
            }
        }

        if (ready_workers.size() > 0) {
            int i = rand::next_u32(0, (uint32_t)ready_workers.size() - 1);
            _running_thread = _threads[ready_workers[i]];
            _running_thread->runnable.release();

            _is_scheduling = false;
            return;
        }

        // otherwise, run the timed tasks
        uint64_t ts = 0;
        auto events = _wheel.pop_next_events(ts);
        if (events) {
            {
                utils::auto_lock<::dsn::utils::ex_lock> l(_lock);
                _time_ns = ts;
            }

            // randomize the events, and see
            std::random_shuffle(
                events->begin(), events->end(), [](int n) { return rand::next_u32(0, n - 1); });

            for (auto e : *events) {
                if (e.app_task != nullptr) {
                    task *t = e.app_task;

                    {
                        node_scoper ns(t->node());
                        t->enqueue();
                    }

                    t->release_ref(); // added by previous t->enqueue from app
                } else {
                    CHECK(e.system_task, "app and system tasks cannot be both empty");
                    e.system_task();
                }
            }

            delete events;
            continue;
        }

        // wait a moment
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }

    _is_scheduling = false;
}
}
} // end namespace
