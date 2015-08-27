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

# include <dsn/cpp/servicelet.h>
# include <dsn/cpp/service_app.h>
# include <dsn/internal/singleton.h>
# include <iostream>
# include <map>

namespace dsn 
{
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

        void add_app(service_app* obj)
        {
            std::lock_guard<std::mutex> l(_lock);
            _apps[obj->name()] = obj;
        }

    private:
        std::mutex            _lock;
        std::set<servicelet*> _services;
        std::map<std::string, service_app*> _apps;
    };

    static service_objects* dsn_apps = &(service_objects::instance());

    void service_app::register_for_debugging()
    {
        service_objects::instance().add_app(this);
    }

    servicelet::servicelet(int task_bucket_count)
    {
        _tracker = dsn_task_tracker_create(task_bucket_count);
        _access_thread_id_inited = false;
        service_objects::instance().add(this);
    }

    servicelet::~servicelet()
    {
        dsn_task_tracker_destroy(_tracker);
        service_objects::instance().remove(this);
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
        }
    }
} // end namespace dsn::service
