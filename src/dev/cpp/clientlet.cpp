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

    namespace rpc
    {
        task_ptr create_rpc_empty_response_task(dsn_message_t request, clientlet* svc, int reply_hash)
        {
            auto tsk = new safe_task_handle;
            //do not add_ref here
            auto t = dsn_rpc_create_response_task(
                request,
                nullptr,
                nullptr,
                reply_hash,
                svc ? svc->tracker() : nullptr
                );
            tsk->set_task_info(t);
            return tsk;
        }

        task_ptr call(::dsn::rpc_address server, dsn_message_t request, clientlet* svc, int reply_hash)
        {
            auto t = create_rpc_empty_response_task(request, svc, reply_hash);
            dsn_rpc_call(server.c_addr(), t->native_handle());
            return t;
        }
    }


    namespace file
    {
        task_ptr create_empty_aio_task(dsn_task_code_t callback_code, clientlet* svc, int hash)
        {
            auto tsk = new safe_task_handle;
            //do not add_ref here
            dsn_task_t t = dsn_file_create_aio_task(callback_code,
                nullptr,
                nullptr, hash, svc ? svc->tracker() : nullptr
                );
            tsk->set_task_info(t);
            return tsk;
        }

        task_ptr read(dsn_handle_t fh, char* buffer, int count, uint64_t offset, dsn_task_code_t callback_code, clientlet* svc, int hash)
        {
            auto tsk = create_empty_aio_task(callback_code, svc, hash);
            dsn_file_read(fh, buffer, count, offset, tsk->native_handle());
            return tsk;
        }

        task_ptr write(dsn_handle_t fh, const char* buffer, int count, uint64_t offset, dsn_task_code_t callback_code, clientlet* svc, int hash)
        {
            auto tsk = create_empty_aio_task(callback_code, svc, hash);
            dsn_file_write(fh, buffer, count, offset, tsk->native_handle());
            return tsk;
        }

        task_ptr write_vector(dsn_handle_t fh, const dsn_file_buffer_t* buffers, int buffer_count, uint64_t offset, dsn_task_code_t callback_code, clientlet* svc, int hash)
        {
            auto tsk = create_empty_aio_task(callback_code, svc, hash);
            dsn_file_write_vector(fh, buffers, buffer_count, offset, tsk->native_handle());
            return tsk;
        }

        void copy_remote_files_impl(
            ::dsn::rpc_address remote,
            const std::string& source_dir,
            std::vector<std::string>& files,  // empty for all
            const std::string& dest_dir,
            bool overwrite,
            dsn_task_t native_task
            )
        {
            if (files.empty())
            {
                dsn_file_copy_remote_directory(remote.c_addr(), source_dir.c_str(), dest_dir.c_str(),
                    overwrite, native_task);
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
                    remote.c_addr(), source_dir.c_str(), ptr_base,
                    dest_dir.c_str(), overwrite, native_task
                    );
            }
        }

        task_ptr copy_remote_files(::dsn::rpc_address remote, const std::string& source_dir, std::vector<std::string>& files, const std::string& dest_dir, bool overwrite, dsn_task_code_t callback_code, clientlet* svc, int hash)
        {
            auto tsk = create_empty_aio_task(callback_code, svc, hash);
            copy_remote_files_impl(remote, source_dir, files, dest_dir, overwrite, tsk->native_handle());
            return tsk;
        }

        task_ptr copy_remote_directory(::dsn::rpc_address remote, const std::string& source_dir, const std::string& dest_dir, bool overwrite, dsn_task_code_t callback_code, clientlet* svc, int hash)
        {
            std::vector<std::string> files;
            return copy_remote_files(
                remote, source_dir, files, dest_dir, overwrite,
                callback_code, svc, hash
            );
        }
    }
    
} // end namespace dsn::service
