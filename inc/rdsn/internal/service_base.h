/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

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
# pragma once

# include <rdsn/service_api.h>
# include <set>
# include <map>
# include <mutex>
# include <thread>

namespace rdsn {
    typedef std::function<void()> task_handler;
    typedef std::function<void(message_ptr&)> rpc_handler;
    typedef std::function<void(error_code, message_ptr&, message_ptr&)> rpc_reply_handler;
    typedef std::function<void(error_code, uint32_t)> aio_handler;

    namespace service {       
        
        //
        // service_base is the base class for RPC service and client
        // there can be multiple service_bases in the system, mostly
        // defined during initialization in main
        //
        class service_base
        {
        public:
            service_base(const char* name);
            virtual ~service_base();

            const std::string& name() const { return _name; }
            static end_point address() { return rpc::get_local_address(); }
            static uint32_t random32(uint32_t min, uint32_t max) { return env::random32(min, max); }
            static uint64_t random64(uint64_t min, uint64_t max) { return env::random64(min, max); }
            static uint64_t now_ns() { return env::now_ns(); }
            static uint64_t now_us() { return env::now_us(); }
            static uint64_t now_ms() { return env::now_ms(); }
            static error_code file_close(handle_t hFile) { return file::close(hFile); }
            static handle_t file_open(const char* file_name, int flag, int pmode);

            static task_ptr enqueue_task(
                task_code evt,
                service_base* svc,
                task_handler callback,
                int hash = 0,
                int delay_milliseconds = 0,
                int timer_interval_milliseconds = 0
                );

            static rpc_response_task_ptr rpc_call(
                const end_point& server_addr,
                message_ptr& request
                )
            {
                return service::rpc::call(server_addr, request, nullptr);
            }
            
            static rpc_response_task_ptr rpc_call(
                const end_point& server_addr,
                message_ptr& request,
                service_base* svc,
                rpc_reply_handler callback,
                int reply_hash = 0
                );

            static void rpc_response(message_ptr& response);

            static aio_task_ptr file_read(
                handle_t hFile,
                char* buffer,
                int count,
                uint64_t offset,
                task_code callback_code,
                service_base* svc,
                aio_handler callback,
                int hash = 0
                );

            static aio_task_ptr file_write(
                handle_t hFile,
                const char* buffer,
                int count,
                uint64_t offset,
                task_code callback_code,
                service_base* svc,
                aio_handler callback,
                int hash = 0
                );

            static void copy_remote_files(
                const end_point& remote, 
                std::string& source_dir, 
                std::vector<std::string>& files,  // empty for all
                std::string& dest_dir, 
                bool overwrite, 
                task_code callback_code,
                service_base* svc,
                aio_handler callback,
                int hash = 0
                );

            static void copy_remote_directory(
                const end_point& remote,
                std::string& source_dir,
                std::string& dest_dir,
                bool overwrite,
                task_code callback_code,
                service_base* svc,
                aio_handler callback,
                int hash = 0
                )
            {
                std::vector<std::string> files;
                return copy_remote_files(
                    remote, source_dir, files, dest_dir, overwrite, 
                    callback_code, svc, callback, hash
                    );
            }

        public:
            task_ptr enqueue_task(
                task_code evt,
                task_handler callback,
                int hash = 0,
                int delay_milliseconds = 0,
                int timer_interval_milliseconds = 0
                )
            {
                return service_base::enqueue_task(evt, this, callback, hash, delay_milliseconds, timer_interval_milliseconds);
            }

            void register_rpc_handler(task_code rpc_code, const char* name, rpc_handler handler);

            bool unregister_rpc_handler(task_code rpc_code);

            rpc_response_task_ptr rpc_call(
                const end_point& server_addr,
                message_ptr& request,
                rpc_reply_handler callback,
                int reply_hash = 0
                )
            {
                return service_base::rpc_call(server_addr, request, this, callback, reply_hash);
            }

            aio_task_ptr file_read(
                handle_t hFile,
                char* buffer,
                int count,
                uint64_t offset,
                task_code callback_code,
                aio_handler callback,
                int hash = 0
                )
            {
                return service_base::file_read(hFile, buffer, count, offset, callback_code, this, callback, hash);
            }

            aio_task_ptr file_write(
                handle_t hFile,
                const char* buffer,
                int count,
                uint64_t offset,
                task_code callback_code,
                aio_handler callback,
                int hash = 0
                )
            {
                return service_base::file_write(hFile, buffer, count, offset, callback_code, this, callback, hash);
            }

        protected:
            friend class service_task;
            friend class service_timer_task;
            friend class service_rpc_request_task;
            friend class service_rpc_response_task;
            friend class service_aio_task;

            void add_outstanding_task(task* tsk);
            void remove_outstanding_task(task* tsk);
            void clear_outstanding_tasks();
            void check_hashed_access();

        private:
            std::string                    _name;
            std::map<uint64_t, task*> _outstanding_tasks;
            std::mutex                     _outstanding_tasks_lock;
            std::set<task_code>            _events;
            std::thread::id                _access_thread_id;
            bool                           _access_thread_id_inited;
        };



        class service_task : public task
        {
        public:
            service_task(task_code code, service_base* svc, task_handler& handler, int hash = 0)
                : task(code, hash)
            {
                _handler = handler;
                _svc = svc;
                if (nullptr != _svc) _svc->add_outstanding_task(this);
            }

            virtual ~service_task()
            {
                if (nullptr != _svc) _svc->remove_outstanding_task(this);
            }

            virtual void exec()
            {
                _handler();
                _handler = nullptr;
            }

            //task_handler& handler() { return _handler; }

        private:
            task_handler _handler;
            service_base* _svc;
        };

        class service_timer_task : public timer_task
        {
        public:
            service_timer_task(task_code code, service_base* svc, task_handler& handler, uint32_t intervalMilliseconds, int hash = 0)
                : timer_task(code, intervalMilliseconds, hash)
            {
                _handler = handler;
                _svc = svc;
                if (nullptr != _svc) _svc->add_outstanding_task(this);
            }

            virtual ~service_timer_task()
            {
                if (nullptr != _svc) _svc->remove_outstanding_task(this);
            }

            virtual bool on_timer() { _handler(); return true; }

        private:
            task_handler _handler;
            service_base* _svc;
        };

        //----------------- rpc task -------------------------------------------------------

        class service_rpc_request_task : public rpc_request_task
        {
        public:
            service_rpc_request_task(message_ptr& request, service_base* svc, rpc_handler& callback)
                : rpc_request_task(request)
            {
                _handler = callback;
                _svc = svc;
                if (nullptr != _svc) _svc->add_outstanding_task(this);
            }

            virtual ~service_rpc_request_task()
            {
                if (nullptr != _svc) _svc->remove_outstanding_task(this);
            }

            void exec()
            {
                _handler(_request);
                _handler = nullptr;
            }

        private:
            rpc_handler  _handler;
            service_base* _svc;
        };

        class service_rpc_server_handler : public rpc_server_handler
        {
        public:
            service_rpc_server_handler(service_base* svc, rpc_handler& handler)
            {
                _handler = handler;
                _svc = svc;
            }

            virtual rpc_request_task_ptr new_request_task(message_ptr& request)
            {
                return new service_rpc_request_task(request, _svc, _handler);
            }

        private:
            rpc_handler _handler;
            service_base* _svc;
        };

        class service_rpc_response_task : public rpc_response_task
        {
        public:
            service_rpc_response_task(message_ptr& request, service_base* svc, rpc_reply_handler& callback, int hash = 0)
                : rpc_response_task(request, hash)
            {
                _handler = callback;
                _svc = svc;
                if (nullptr != _svc) _svc->add_outstanding_task(this);
            }

            virtual ~service_rpc_response_task()
            {
                if (nullptr != _svc) _svc->remove_outstanding_task(this);
            }

            void on_response(error_code err, message_ptr& request, message_ptr& response)
            {
                _handler(err, request, response);
                _handler = nullptr;
            }

        private:
            rpc_reply_handler  _handler;
            service_base* _svc;
        };

        //------------------------- disk AIO task ---------------------------------------------------
        class service_aio_task : public aio_task
        {
        public:
            service_aio_task(task_code code, service_base* svc, aio_handler& handler, int hash = 0)
                : aio_task(code, hash)
            {
                _handler = handler;
                _svc = svc;
                if (nullptr != _svc) _svc->add_outstanding_task(this);
            }

            virtual ~service_aio_task()
            {
                if (nullptr != _svc) _svc->remove_outstanding_task(this);
            }

            virtual void on_completed(error_code err, uint32_t transferred_size)
            {
                if (_handler != nullptr)
                {
                    _handler(err, transferred_size);
                    _handler = nullptr;
                }
            }

        private:
            aio_handler _handler;
            service_base* _svc;
        };

    }
}
