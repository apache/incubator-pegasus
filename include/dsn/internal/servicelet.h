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
# include <thread>
# include <dsn/internal/synchronize.h>
# include <dsn/internal/utils.h>

namespace dsn {
    typedef std::function<void()> task_handler;
    typedef std::function<void(error_code, uint32_t)> aio_handler;
    typedef std::function<void(error_code, message_ptr&, message_ptr&)> rpc_reply_handler;

    namespace service {

        // 
        // many task requires a certain context to be executed
        // task_context_manager helps manaing the context automatically
        // for tasks so that when the context is gone, the tasks are
        // automatically cancelled to avoid invalid context access
        //
        class servicelet;
        class task_context_manager
        {
        public:
            task_context_manager(servicelet* owner, task* task);
            virtual ~task_context_manager();

        private:
            friend class servicelet;

            enum owner_delete_state
            {
                OWNER_DELETE_NOT_LOCKED = 0,
                OWNER_DELETE_LOCKED = 1,
                OWNER_DELETE_FINISHED = 2
            };
            
            task       *_task;
            servicelet *_owner;
            std::atomic<owner_delete_state> _deleting_owner;
            
            // double-linked list for put into _owner
            dlink      _dl;
            int        _dl_bucket_id;
            
        private:
            owner_delete_state owner_delete_prepare();
            void               owner_delete_commit();
        };

        //
        // servicelet is the base class for RPC service and client
        // there can be multiple servicelet in the system, mostly
        // defined during initialization in main
        //
        class servicelet
        {
        public:
            servicelet(int task_bucket_count = 8);
            virtual ~servicelet();

            static end_point primary_address() { return rpc::primary_address(); }
            static uint32_t random32(uint32_t min, uint32_t max) { return env::random32(min, max); }
            static uint64_t random64(uint64_t min, uint64_t max) { return env::random64(min, max); }
            static uint64_t now_ns() { return env::now_ns(); }
            static uint64_t now_us() { return env::now_us(); }
            static uint64_t now_ms() { return env::now_ms(); }
            
        protected:
            void clear_outstanding_tasks();
            void check_hashed_access();

        private:
            int                            _last_id;
            std::set<task_code>            _events;
            int                            _access_thread_id;
            bool                           _access_thread_id_inited;
            task_code                      _access_thread_task_code;

            friend class task_context_manager;
            const int                      _task_bucket_count;
            ::dsn::utils::ex_lock_nr_spin  *_outstanding_tasks_lock;
            dlink                          *_outstanding_tasks;
        };

        // ------- inlined implementation ----------
        inline task_context_manager::task_context_manager(servicelet* owner, task* task)
            : _owner(owner), _task(task), _deleting_owner(OWNER_DELETE_NOT_LOCKED)
        {
            if (nullptr != _owner)
            {
                auto idx = task::get_current_worker_index();
                if (-1 != idx)
                    _dl_bucket_id = static_cast<int>(idx % _owner->_task_bucket_count);
                else
                    _dl_bucket_id = static_cast<int>(::dsn::utils::get_current_tid() % _owner->_task_bucket_count);

                {
                    utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_owner->_outstanding_tasks_lock[_dl_bucket_id]);
                    _dl.insert_after(&_owner->_outstanding_tasks[_dl_bucket_id]);
                }
            }
        }

        inline task_context_manager::owner_delete_state task_context_manager::owner_delete_prepare()
        {
            return _deleting_owner.exchange(OWNER_DELETE_LOCKED, std::memory_order_acquire);
        }

        inline void task_context_manager::owner_delete_commit()
        {
            {
                utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_owner->_outstanding_tasks_lock[_dl_bucket_id]);
                _dl.remove();
            }

            _deleting_owner.store(OWNER_DELETE_FINISHED, std::memory_order_relaxed);
        }

        inline task_context_manager::~task_context_manager()
        {
            if (nullptr != _owner)
            {
                auto s = owner_delete_prepare();
                switch (s)
                {
                case OWNER_DELETE_NOT_LOCKED:
                    owner_delete_commit();
                    break;
                case OWNER_DELETE_LOCKED:
                    while (OWNER_DELETE_LOCKED == _deleting_owner.load(std::memory_order_consume))
                    {
                    }
                    break;
                case OWNER_DELETE_FINISHED:
                    break;
                }
            }
        }
    }
}
