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

#include "task_tracker.h"

#include <string>

#include "runtime/tool_api.h"
#include "task.h"
#include "utils/ports.h"

namespace dsn {

task_tracker::task_tracker(int task_bucket_count) : _task_bucket_count(task_bucket_count)
{
    _outstanding_tasks_lock = new ::dsn::utils::ex_lock_nr_spin[_task_bucket_count];
    _outstanding_tasks = new dlink[_task_bucket_count];
}

task_tracker::~task_tracker()
{
    cancel_outstanding_tasks();

    delete[] _outstanding_tasks;
    delete[] _outstanding_tasks_lock;
}

// TODO:
// hack for wait/cancel inside spin locks
struct tls_tracker_hack
{
    unsigned int magic;
    bool is_simulator;

    bool under_simulation()
    {
        if (magic != 0xdeadbeef) {
            is_simulator = (dsn::tools::get_current_tool()->name() == "simulator");
            magic = 0xdeadbeef;
        }
        return is_simulator;
    }
};

static __thread tls_tracker_hack s_hack;

void task_tracker::wait_outstanding_tasks()
{
    for (int i = 0; i < _task_bucket_count; i++) {
        while (true) {
            trackable_task::owner_delete_state prepare_state;
            trackable_task *tcm;

            {
                utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_outstanding_tasks_lock[i]);
                auto n = _outstanding_tasks[i].next();
                if (n != &_outstanding_tasks[i]) {
                    tcm = CONTAINING_RECORD(n, trackable_task, _dl);

                    // try to get the lock
                    prepare_state = tcm->owner_delete_prepare();
                } else
                    break; // assuming nobody is putting tasks into it anymore
            }

            switch (prepare_state) {
            // tracker get the lock
            case trackable_task::OWNER_DELETE_NOT_LOCKED:
                if (s_hack.under_simulation()) {
                    task *tsk = tcm->_task;
                    tsk->add_ref(); // released after delete commit
                    tcm->owner_delete_commit();

                    tsk->wait();        // wait outside the delete spin lock
                    tsk->release_ref(); // added before delete commit
                } else {
                    tcm->_task->wait();
                    tcm->owner_delete_commit();
                }
                break;
            case trackable_task::OWNER_DELETE_LOCKED:
            case trackable_task::OWNER_DELETE_FINISHED:
                break;
            }
        }
    }
}

void task_tracker::cancel_outstanding_tasks()
{
    for (int i = 0; i < _task_bucket_count; i++) {
        while (true) {
            trackable_task::owner_delete_state prepare_state;
            trackable_task *tcm;

            {
                utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_outstanding_tasks_lock[i]);
                auto n = _outstanding_tasks[i].next();
                if (n != &_outstanding_tasks[i]) {
                    tcm = CONTAINING_RECORD(n, trackable_task, _dl);
                    prepare_state = tcm->owner_delete_prepare();
                } else
                    break; // assuming nobody is putting tasks into it anymore
            }

            switch (prepare_state) {
            case trackable_task::OWNER_DELETE_NOT_LOCKED:
                if (s_hack.under_simulation()) {
                    task *tsk = tcm->_task;
                    tsk->add_ref(); // released after delete commit
                    tcm->owner_delete_commit();

                    tsk->cancel(true);  // cancel outside the delete spin lock
                    tsk->release_ref(); // added before delete commit
                } else {
                    tcm->_task->cancel(true);
                    tcm->owner_delete_commit();
                }
                break;
            case trackable_task::OWNER_DELETE_LOCKED:
            case trackable_task::OWNER_DELETE_FINISHED:
                break;
            }
        }
    }
}

int task_tracker::cancel_but_not_wait_outstanding_tasks()
{
    int not_finished = 0;
    for (int i = 0; i < _task_bucket_count; i++) {
        utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_outstanding_tasks_lock[i]);
        auto n = _outstanding_tasks[i].next();
        if (n != &_outstanding_tasks[i]) {
            trackable_task *tcm = CONTAINING_RECORD(n, trackable_task, _dl);
            if (tcm->_task != task::get_current_task()) {
                bool finished;
                tcm->_task->cancel(false, &finished);
                if (!finished)
                    not_finished++;
            }
        }
    }
    return not_finished;
}
}
