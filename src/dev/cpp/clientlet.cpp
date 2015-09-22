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

# include <dsn/cpp/clientlet.h>
# include <dsn/cpp/service_app.h>
# include <dsn/internal/singleton.h>
# include <iostream>
# include <map>

namespace dsn 
{
    class service_objects : public ::dsn::utils::singleton<service_objects>
    {
    public:
        void add(clientlet* obj)
        {
            std::lock_guard<std::mutex> l(_lock);
            _services.insert(obj);
        }

        void remove(clientlet* obj)
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
        std::set<clientlet*> _services;
        std::map<std::string, service_app*> _apps;
    };

    static service_objects* dsn_apps = &(service_objects::instance());

    void service_app::register_for_debugging()
    {
        service_objects::instance().add_app(this);
    }

    clientlet::clientlet(int task_bucket_count)
    {
        _tracker = dsn_task_tracker_create(task_bucket_count);
        _access_thread_id_inited = false;
        service_objects::instance().add(this);
    }
    
    clientlet::~clientlet()
    {
        dsn_task_tracker_destroy(_tracker);
        service_objects::instance().remove(this);
    }


    void clientlet::check_hashed_access()
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

    namespace tasking 
    {
        task_ptr enqueue(
            dsn_task_code_t evt,
            clientlet* svc,
            task_handler callback,
            int hash /*= 0*/,
            int delay_milliseconds /*= 0*/,
            int timer_interval_milliseconds /*= 0*/
            )
        {
            dsn_task_t t;
            task_ptr tsk = new safe_task<task_handler>(callback, timer_interval_milliseconds != 0);

            tsk->add_ref(); // released in exec callback
            if (timer_interval_milliseconds != 0)
            {
                t = dsn_task_create_timer(evt, safe_task<task_handler>::exec, 
                    tsk, hash, timer_interval_milliseconds);
            }
            else
            {
                t = dsn_task_create(evt, safe_task<task_handler>::exec, tsk, hash);
            }

            tsk->set_task_info(t);

            dsn_task_call(t, svc ? svc->tracker() : nullptr, delay_milliseconds);
            return tsk;
        }
    }
    
    namespace rpc
    {
        task_ptr call(
            const ::dsn::rpc_address& server,
            dsn_message_t request,
            clientlet* svc,
            rpc_reply_handler callback,
            int reply_hash
            )
        {
            task_ptr tsk = new safe_task<rpc_reply_handler >(callback);

            if (callback != nullptr)
                tsk->add_ref(); // released in exec_rpc_response

            auto t = dsn_rpc_create_response_task(
                request,
                callback != nullptr ? safe_task<rpc_reply_handler >::exec_rpc_response : nullptr,
                (void*)tsk,
                reply_hash
                );
            tsk->set_task_info(t);
            dsn_rpc_call(&server.c_addr(), t, svc ? svc->tracker() : nullptr);

            return tsk;
        }
    }
    
    namespace file
    {
        task_ptr read(
            dsn_handle_t hFile,
            char* buffer,
            int count,
            uint64_t offset,
            dsn_task_code_t callback_code,
            clientlet* svc,
            aio_handler callback,
            int hash /*= 0*/
            )
        {
            task_ptr tsk = new safe_task<aio_handler>(callback);

            if (callback != nullptr)
                tsk->add_ref(); // released in exec_aio

            dsn_task_t t = dsn_file_create_aio_task(callback_code,
                callback != nullptr ? safe_task<aio_handler>::exec_aio : nullptr,
                tsk, hash
                );

            tsk->set_task_info(t);

            dsn_file_read(hFile, buffer, count, offset, t, svc ? svc->tracker() : nullptr);
            return tsk;
        }

        task_ptr write(
            dsn_handle_t hFile,
            const char* buffer,
            int count,
            uint64_t offset,
            dsn_task_code_t callback_code,
            clientlet* svc,
            aio_handler callback,
            int hash /*= 0*/
            )
        {
            task_ptr tsk = new safe_task<aio_handler>(callback);

            if (callback != nullptr)
                tsk->add_ref(); // released in exec_aio

            dsn_task_t t = dsn_file_create_aio_task(callback_code,
                callback != nullptr ? safe_task<aio_handler>::exec_aio : nullptr,
                tsk, hash
                );

            tsk->set_task_info(t);

            dsn_file_write(hFile, buffer, count, offset, t, svc ? svc->tracker() : nullptr);
            return tsk;
        }

        task_ptr copy_remote_files(
            const ::dsn::rpc_address& remote,
            const std::string& source_dir,
            std::vector<std::string>& files,  // empty for all
            const std::string& dest_dir,
            bool overwrite,
            dsn_task_code_t callback_code,
            clientlet* svc,
            aio_handler callback,
            int hash /*= 0*/
            )
        {
            task_ptr tsk = new safe_task<aio_handler>(callback);

            if (callback != nullptr)
                tsk->add_ref(); // released in exec_aio

            dsn_task_t t = dsn_file_create_aio_task(callback_code,
                callback != nullptr ? safe_task<aio_handler>::exec_aio : nullptr,
                tsk, hash
                );

            tsk->set_task_info(t);

            if (files.empty())
            {
                dsn_file_copy_remote_directory(&remote.c_addr(), source_dir.c_str(), dest_dir.c_str(),
                    overwrite, t, svc ? svc->tracker() : nullptr);
            }
            else
            {
                const char** ptr = (const char**)alloca(sizeof(const char*) * (files.size() + 1));
                const char** ptr_base = ptr;
                for (auto& f : files)
                {
                    *ptr++ = f.c_str();
                }
                *ptr = nullptr;

                dsn_file_copy_remote_files(
                    &remote.c_addr(), source_dir.c_str(), ptr_base,
                    dest_dir.c_str(), overwrite, t, svc ? svc->tracker() : nullptr
                    );
            }
            return tsk;
        }
    }
    
} // end namespace dsn::service
