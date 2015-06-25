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
# pragma once

# include <dsn/service_api.h>
# include <set>
# include <map>
# include <mutex>
# include <thread>

namespace dsn {
    typedef std::function<void()> task_handler;
    typedef std::function<void(error_code, uint32_t)> aio_handler;
    typedef std::function<void(error_code, message_ptr&, message_ptr&)> rpc_reply_handler;

    namespace service {
        
        //
        // servicelet is the base class for RPC service and client
        // there can be multiple servicelet in the system, mostly
        // defined during initialization in main
        //
        class servicelet
        {
        public:
            servicelet();
            virtual ~servicelet();

            static end_point primary_address() { return rpc::primary_address(); }
            static uint32_t random32(uint32_t min, uint32_t max) { return env::random32(min, max); }
            static uint64_t random64(uint64_t min, uint64_t max) { return env::random64(min, max); }
            static uint64_t now_ns() { return env::now_ns(); }
            static uint64_t now_us() { return env::now_us(); }
            static uint64_t now_ms() { return env::now_ms(); }
            
        protected:
            friend class service_context_manager;

            int  add_outstanding_task(task* tsk);
            void remove_outstanding_task(int id);
            void clear_outstanding_tasks();
            void check_hashed_access();

        private:
            int                            _last_id;
            std::map<int, task*>           _outstanding_tasks;
            std::mutex                     _outstanding_tasks_lock;

            std::set<task_code>            _events;
            std::thread::id                _access_thread_id;
            bool                           _access_thread_id_inited;
        };

        class service_context_manager
        {
        public:
            service_context_manager(servicelet* owner, task* task)
            {
                _owner = owner;
                if (nullptr != _owner)
                {
                    _id = owner->add_outstanding_task(task);
                }
            }

            virtual ~service_context_manager()
            {
                if (nullptr != _owner)
                {
                    _owner->remove_outstanding_task(_id);
                }
            }

            void clear_context() { _owner = nullptr; }

        private:
            int _id;
            servicelet *_owner;
        };
    }
}
