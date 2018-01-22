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
 *     tracker abstraction for tasks, to ensure the tasks are cancelled
 *     appropriately when the context is gone
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/service_api_c.h>
#include <dsn/utility/link.h>
#include <dsn/utility/synchronize.h>
#include <atomic>

namespace dsn {
//
// many task requires a certain context to be executed
// trackable_task helps manaing the context automatically
// for tasks so that when the context is gone, the tasks are
// automatically cancelled to avoid invalid context access
//
class task_tracker;
class trackable_task
{
public:
    trackable_task() : _task(nullptr), _owner(nullptr), _dl_bucket_id(0) {}
    virtual ~trackable_task() {}

    void set_tracker(task_tracker *owner, dsn_task_t task);
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

    dsn_task_t _task;
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
// task_tracker is the base class for RPC service and client
// there can be multiple task_tracker in the system, mostly
// defined during set_tracker in main
//
class task_tracker
{
public:
    explicit task_tracker(int task_bucket_count = 1);
    virtual ~task_tracker();

    void cancel_outstanding_tasks();
    void wait_outstanding_tasks();

private:
    friend class trackable_task;
    const int _task_bucket_count;
    ::dsn::utils::ex_lock_nr_spin *_outstanding_tasks_lock;
    dlink *_outstanding_tasks;
};

// ------- inlined implementation ----------
inline void trackable_task::set_tracker(task_tracker *owner, dsn_task_t task)
{
    dassert(_owner == nullptr, "task tracker is already set");
    _owner = owner;
    _task = task;
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
}
