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
# include <dsn/tool/simulator.h>
# include "scheduler.h"
# include "env.sim.h"
# include <dsn/service_api.h>
# include <set>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "simulator"

namespace dsn { namespace tools {

void event_wheel::add_event(uint64_t ts, task* t)
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);

    std::vector<task*>* evts;
    auto itr = _events.find(ts);
    if (itr != _events.end())
        evts = itr->second;
    else
    {
        evts = new std::vector<task*>();
        _events.insert(std::make_pair(ts, evts));
    }
    
    evts->push_back(t);

}

std::vector<task*>* event_wheel::pop_next_events(__out_param uint64_t& ts)
{
    utils::auto_lock<::dsn::utils::ex_lock> l(_lock);

    std::vector<task*>* evts = NULL;
    auto itr = _events.begin();
    if (itr != _events.end()){
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

scheduler::scheduler(void)
{
    _time_ns = 0;
    _running = false;
    task_worker::on_create.put_back(on_task_worker_create, "simulation.on_task_worker_create");
    task_worker::on_start.put_back(on_task_worker_start, "simulation.on_task_worker_start");
        
    for (int i = 0; i <= task_code::max_value(); i++)
    {
        task_spec::get(i)->on_task_wait_pre.put_back(scheduler::on_task_wait, "simulation.on_task_wait");
        task_spec::get(i)->on_task_end.put_back(scheduler::on_task_end, "simulation.on_task_end");
    }

    task_ext::register_ext(task_state_ext::deletor);
    task_worker_ext::register_ext(sim_worker_state::deletor);
}

scheduler::~scheduler(void)
{
}


/*static*/ void scheduler::on_task_worker_start(task_worker* worker)
{
    while (!scheduler::instance()._running)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
    }
}

/*static*/ void scheduler::on_task_worker_create(task_worker* worker)
{
    auto s = task_worker_ext::get_inited(worker);    
    s->worker = worker;
    s->first_time_schedule = true;
    s->in_continuation = false;
    s->index = static_cast<int>(scheduler::instance()._threads.size());    
    scheduler::instance()._threads.push_back(s);
}

/*static*/ void scheduler::on_task_wait(task* waitor, task* waitee, uint32_t timeout_milliseconds)
{
    if (waitor == nullptr)
        return;

    if (waitee->state() < task_state::TASK_STATE_FINISHED)
    {
        auto ts = task_ext::get_inited(waitee);
        ts->wait_threads.push_back(task_worker_ext::get(task::get_current_worker()));

        scheduler::instance().wait_schedule(true, false);
    }
    else
    {
        scheduler::instance().wait_schedule(true, true);
    }
}

/*static*/ void scheduler::on_task_end(task* task)
{
    auto ts = task_ext::get(task);
    if (ts != nullptr)
    {
        for (auto& w : ts->wait_threads)
        {
            w->is_continuation_ready = true;
        }
    }
}

void scheduler::add_task(task* tsk, task_queue* q)
{
    auto ts = task_ext::get_inited(tsk);
    ts->queue = q;

    auto delay = (uint64_t)tsk->delay_milliseconds() * 1000000;
    tsk->set_delay(0);
    _wheel.add_event(now_ns() + delay, tsk);
}

checker::checker(const char* name)
    : _name(name), _apps(::dsn::service::system::get_all_apps())
{
}

void scheduler::add_checker(checker* chker)
{
    _checkers.push_back(chker);
}

void scheduler::check()
{
    for (auto& c : _checkers)
    {
        c->check();
    }
}

void scheduler::wait_schedule(bool in_continue, bool is_continue_ready /*= false*/)
{
    auto s = task_worker_ext::get(task::get_current_worker());
    s->in_continuation = in_continue;
    s->is_continuation_ready = is_continue_ready;

    if (s->first_time_schedule)
    {
        s->first_time_schedule = false;
        if (s->index == 0)
            schedule();
    }
    else
    {
        schedule();
    }
    s->runnable.wait();
}

void scheduler::schedule()
{
    check(); // check before schedule

    while (true)
    {
        // run ready workers whenever possible
        std::vector<int> ready_workers;
        for (auto& s : _threads)
        {
            if ((s->in_continuation && s->is_continuation_ready)
                || (!s->in_continuation && s->worker->queue()->count() > 0)
                )
            {
                ready_workers.push_back(s->index);
            }
        }

        if (ready_workers.size() > 0)
        {
            int i = dsn::service::env::random32(0, (uint32_t)ready_workers.size() - 1);
            _threads[ready_workers[i]]->runnable.release();
            return;
        }

        // otherwise, run the timed tasks
        uint64_t ts = 0;
        auto events = _wheel.pop_next_events(ts);
        if (events)
        {
            {
                utils::auto_lock<::dsn::utils::ex_lock> l(_lock);
                _time_ns = ts;
            }

            // randomize the events, and see
            std::random_shuffle(events->begin(), events->end(), [](int n) { return dsn::service::env::random32(0, n - 1); });

            for (auto it = events->begin(); it != events->end(); it++)
            {
                task_ptr t = *it;
                ::dsn::service::tasking::enqueue(t);
                t->release_ref();
            }

            delete events;
            continue;
        }

        // wait a moment
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}


}} // end namespace
