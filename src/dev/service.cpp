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

servicelet::servicelet()
: _access_thread_task_code(TASK_CODE_INVALID)
{
    _access_thread_id_inited = false;
    _last_id = 0;
    service_objects::instance().add(this);
}

servicelet::~servicelet()
{
    clear_outstanding_tasks();
    service_objects::instance().remove(this);
}

void servicelet::clear_outstanding_tasks()
{
    utils::auto_lock l(_outstanding_tasks_lock);
    while (true)
    {
        auto n = _outstanding_tasks.next();
        if (n != &_outstanding_tasks)
        {
            auto tcm = CONTAINING_RECORD(n, task_context_manager, _dl);
            tcm->_task->cancel(true);
            tcm->delete_owner(false);
        }
        else
            break;
    }
}

void servicelet::check_hashed_access()
{
    if (_access_thread_id_inited)
    {
        dassert (std::this_thread::get_id() == _access_thread_id, "the service is assumed to be accessed by one thread only!");
    }
    else
    {
        _access_thread_id = std::this_thread::get_id();
        _access_thread_id_inited = true;
        _access_thread_task_code.reset(task::get_current_task()->spec().code);
    }
}

}} // end namespace dsn::service
