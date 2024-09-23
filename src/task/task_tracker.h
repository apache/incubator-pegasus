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

#include <atomic>

#include "utils/fmt_logging.h"
#include "utils/link.h"
#include "utils/process_utils.h"
#include "utils/synchronize.h"

namespace dsn {
//
// many task requires a certain context to be executed
// trackable_task helps manaing the context automatically
// for tasks so that when the context is gone, the tasks are
// automatically cancelled to avoid invalid context access
//
class task;
class task_tracker;

class trackable_task
{
public:
    trackable_task() : _task(nullptr), _owner(nullptr), _dl_bucket_id(0) {}
    virtual ~trackable_task() {}

    void set_tracker(task_tracker *owner, task *tsk);
    void unset_tracker();
    task_tracker *tracker() const { return _owner; }

private:
    friend class task_tracker;

    enum owner_delete_state
    {
        OWNER_DELETE_NOT_LOCKED = 0,
        OWNER_DELETE_LOCKED = 1,
        OWNER_DELETE_FINISHED = 2
    };

    task *_task;
    task_tracker *_owner;
    std::atomic<owner_delete_state> _deleting_owner;

    // double-linked list for put into _owner
    dlink _dl;
    int _dl_bucket_id;

private:
    owner_delete_state owner_delete_prepare();
    void owner_delete_commit();
};

//
// task_tracker is used to track one or more unfinished tasks, you may use it to
// wait or cancel the tasks tracked by the tracker.
//
// A classical situation is use tracker to prevent a task from visiting a deleted object.
//
// for example:
// class A
// {
//   void func() {
//     tasking::enqueue(task_code, [this](){ std::cout << this->value; }, seconds_10);
//   }
// private:
//   int value;
// };
//
// if a object of class A is deleted before the enqueued task executed,
// memory corruption may occur.
//
// with task tracker, you may avoid this by declaring a variable of task_tracker in A
// and use it to track the created task:
//
// class A
// {
// public:
//   ~A() { _tracker.cancel_outstanding_tasks(); }
//   void func()
//   {
//     tasking::enqueue(task_code,
//                      &_tracker,
//                      [this]() { std::cout << this->value; },
//                      seconds_10);
//   }
//
// private:
//   int value;
//   task_tracker _tracker;
// };
//
// in the example above, calling "_tracker.wait_outstanding_tasks()" still works.
// the main difference is that:
//    1. "wait" will wait all tasks to finish
//    2. as for "cancel", only running tasks will be waited.
//       a not-started task will be cancelled directly.
//
// you may choose either one as you need.
//
// Some notices:
//
// 1. you may want to call "cancel" or "wait" in the beginning of
//    of your destructor, so as to ensure all tasks are
//    canceled/executed before destruction of any objects
// 2. please ensure the "wait"("cancel") and the created task are running
//    IN DIFFERENT THREAD, otherwise deadlock may occur
// 3. when wait_outstanding_task is called, please make sure that no timer tasks are running.
//    if timer and non-timer are both tracked, you may want to cancel the timer first.
//
//    For example:
//
//    task_tracker t;
//    auto tsk1 = tasking::enqueue(task_code, &t, [](){}, seconds_10);
//    auto tsk2 = tasking::enqueue_timer(task_code, &t, [](){}, delay_10s, period_10s);
//
//    t.wait_outstanding_tasks();  <-- wrong, coz tsk2 is a timer
//    t.cancel_outstanding_tasks(); <-- right, cancel can apply to any tasks.
//    tsk2.cancel(true); t.wait_out_standing_tasks(); <-- right, first cancel timer, then wait.
//
class task_tracker
{
public:
    explicit task_tracker(int task_bucket_count = 1);
    virtual ~task_tracker();

    // wait all outstanding tasks to finish
    void wait_outstanding_tasks();

    // cancel and wait all outstanding tasks to finish
    void cancel_outstanding_tasks();

    // cancel but not wait outstanding tasks to finish
    // return not finished task count
    int cancel_but_not_wait_outstanding_tasks();

    void set_tasks_success() { _all_tasks_success = true; }

    void clear_tasks_state() { _all_tasks_success = false; }

    bool all_tasks_success() const { return _all_tasks_success; }

private:
    friend class trackable_task;
    const int _task_bucket_count;
    ::dsn::utils::ex_lock_nr_spin *_outstanding_tasks_lock;
    dlink *_outstanding_tasks;
    bool _all_tasks_success{false};
};

// ------- inlined implementation ----------
inline void trackable_task::set_tracker(task_tracker *owner, task *tsk)
{
    CHECK(_owner == nullptr, "task tracker is already set");
    _owner = owner;
    _task = tsk;
    _deleting_owner.store(OWNER_DELETE_NOT_LOCKED, std::memory_order_release);

    if (nullptr != _owner) {
        _dl_bucket_id =
            static_cast<int>(::dsn::utils::get_current_tid() % _owner->_task_bucket_count);
        {
            utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(
                _owner->_outstanding_tasks_lock[_dl_bucket_id]);
            _dl.insert_after(&_owner->_outstanding_tasks[_dl_bucket_id]);
        }
    }
}

inline void trackable_task::unset_tracker()
{
    if (nullptr != _owner) {
        auto s = owner_delete_prepare();
        switch (s) {
        case OWNER_DELETE_NOT_LOCKED:
            owner_delete_commit();
            break;
        case OWNER_DELETE_LOCKED:
            while (OWNER_DELETE_LOCKED == _deleting_owner.load(std::memory_order_consume)) {
            }
            break;
        case OWNER_DELETE_FINISHED:
            break;
        }
        _owner = nullptr;
    }
}

inline trackable_task::owner_delete_state trackable_task::owner_delete_prepare()
{
    return _deleting_owner.exchange(OWNER_DELETE_LOCKED, std::memory_order_acquire);
}

inline void trackable_task::owner_delete_commit()
{
    {
        utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(
            _owner->_outstanding_tasks_lock[_dl_bucket_id]);
        _dl.remove();
    }

    _deleting_owner.store(OWNER_DELETE_FINISHED, std::memory_order_relaxed);
}
} // namespace dsn
