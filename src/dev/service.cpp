/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation, Robust Distributed System Nucleus(rDSN)

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

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

int servicelet::add_outstanding_task(task* tsk)
{
    std::lock_guard<std::mutex> l(_outstanding_tasks_lock);
    int id = ++_last_id;
    _outstanding_tasks.insert(std::map<int, task*>::value_type(id, tsk));
    return id;
}

void servicelet::remove_outstanding_task(int id)
{
    std::lock_guard<std::mutex> l(_outstanding_tasks_lock);
    auto pr = _outstanding_tasks.erase(id);
    dassert (pr == 1, "task with local id %d is not found in the hash table", id);
}

void servicelet::clear_outstanding_tasks()
{
    std::lock_guard<std::mutex> l(_outstanding_tasks_lock);
    for (auto it = _outstanding_tasks.begin(); it != _outstanding_tasks.end(); it++)
    {
        it->second->cancel(true);

        auto sc = dynamic_cast<service_context_manager*>(it->second);
        if (nullptr != sc)
        {
            sc->clear_context();
        }
    }
    _outstanding_tasks.clear();
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
    }
}

}} // end namespace dsn::service
