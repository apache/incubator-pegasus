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

# include <dsn/internal/servicelet.h>
# include <dsn/internal/singleton.h>
# include <iostream>

namespace dsn {
namespace service {

class service_objects : public ::dsn::utils::singleton<service_objects>
{
public:
    void add(servicelet* obj)
    {
        std::lock_guard<std::mutex> l(_lock);
        _services.insert(obj);
    }

    void remove(servicelet* obj)
    {
        std::lock_guard<std::mutex> l(_lock);
        _services.erase(obj);
    }

private:
    std::mutex            _lock;
    std::set<servicelet*> _services;
};

static service_objects* s_services = &(service_objects::instance());

servicelet::servicelet(int task_bucket_count)
: _access_thread_task_code(TASK_CODE_INVALID), _task_bucket_count(task_bucket_count)
{
    _outstanding_tasks_lock = new ::dsn::utils::ex_lock_nr_spin[_task_bucket_count];
    _outstanding_tasks = new dlink[_task_bucket_count];

    _access_thread_id_inited = false;
    _last_id = 0;
    service_objects::instance().add(this);
}

servicelet::~servicelet()
{
    clear_outstanding_tasks();
    service_objects::instance().remove(this);

    delete[] _outstanding_tasks;
    delete[] _outstanding_tasks_lock;
}

void servicelet::clear_outstanding_tasks()
{
    for (int i = 0; i < _task_bucket_count; i++)
    {
        while (true)
        {
            task_context_manager::owner_delete_state prepare_state;
            task_context_manager *tcm;

            {
                utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_outstanding_tasks_lock[i]);
                auto n = _outstanding_tasks[i].next();
                if (n != &_outstanding_tasks[i])
                {
                    tcm = CONTAINING_RECORD(n, task_context_manager, _dl);
                    prepare_state = tcm->owner_delete_prepare();
                }
                else
                    break; // assuming nobody is putting tasks into it anymore
            }

            switch (prepare_state)
            {
            case task_context_manager::OWNER_DELETE_NOT_LOCKED:
                tcm->_task->cancel(true);
                tcm->owner_delete_commit();
                break;
            case task_context_manager::OWNER_DELETE_LOCKED:
            case task_context_manager::OWNER_DELETE_FINISHED:
                break;
            }
        }
    }
}

void servicelet::check_hashed_access()
{
    if (_access_thread_id_inited)
    {
        dassert(::dsn::utils::get_current_tid() == _access_thread_id, "the service is assumed to be accessed by one thread only!");
    }
    else
    {
        _access_thread_id = ::dsn::utils::get_current_tid();
        _access_thread_id_inited = true;
        _access_thread_task_code.reset(task::get_current_task()->spec().code);
    }
}

}} // end namespace dsn::service
