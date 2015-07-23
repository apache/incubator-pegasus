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

# include <dsn/service_api_c.h>
# include <dsn/internal/dsn_types.h>
# include <dsn/internal/rpc_message.h>
# include <set>
# include <map>
# include <thread>
# include <dsn/internal/synchronize.h>
# include <dsn/internal/utils.h>
# include <dsn/internal/link.h>

namespace dsn 
{
    typedef std::function<void()> task_handler;
    typedef std::function<void(error_code, size_t)> aio_handler;
    typedef std::function<void(error_code, message_ptr&, message_ptr&)> rpc_reply_handler;
    typedef std::function<void(message_ptr&)> rpc_request_handler;

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
            task_context_manager() : _owner(nullptr) {}
            virtual ~task_context_manager(); 
            void init(servicelet* owner, dsn_task_t task);      

        private:
            friend class servicelet;

            enum owner_delete_state
            {
                OWNER_DELETE_NOT_LOCKED = 0,
                OWNER_DELETE_LOCKED = 1,
                OWNER_DELETE_FINISHED = 2
            };
            
            dsn_task_t  _task;
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
            servicelet(int task_bucket_count = 13);
            virtual ~servicelet();

            static dsn_address_t primary_address() { return dsn_rpc_primary_address(); }
            static uint32_t random32(uint32_t min, uint32_t max) { return dsn_env_random32(min, max); }
            static uint64_t random64(uint64_t min, uint64_t max) { return dsn_env_random64(min, max); }
            static uint64_t now_ns() { return dsn_env_now_ns(); }
            static uint64_t now_us() { return dsn_env_now_us(); }
            static uint64_t now_ms() { return dsn_env_now_ms(); }
            
        protected:
            void clear_outstanding_tasks();
            void check_hashed_access();

        private:
            int                            _last_id;
            std::set<dsn_task_code_t>      _events;
            int                            _access_thread_id;
            bool                           _access_thread_id_inited;

            friend class task_context_manager;
            const int                      _task_bucket_count;
            ::dsn::utils::ex_lock_nr_spin  *_outstanding_tasks_lock;
            dlink                          *_outstanding_tasks;
        };

        //
        // basic cpp task wrapper
        // which manages the task handle
        // and the interaction with task context manager, servicelet
        //
        
        class cpp_dev_task_base : public ::dsn::ref_object
        {
        public:
            cpp_dev_task_base()
            {
                _task = 0;
                add_ref();
            }

            virtual ~cpp_dev_task_base()
            {
                dsn_task_close(_task); 
            }

            void set_task_info(dsn_task_t t, servicelet* svc)
            {
                _task = t;
                _manager.init(svc, t);
            }

            dsn_task_t native_handle() { return _task; }
                        
            bool cancel(bool wait_until_finished, bool* finished = nullptr)
            {
                bool r = dsn_task_cancel2(_task, wait_until_finished, finished);
                if (r)
                {
                    release_ref();
                }
                return r;
            }

            bool wait()
            {
                return dsn_task_wait(_task);
            }

            bool wait(int timeout_millieseconds)
            {
                return dsn_task_wait_timeout(_task, timeout_millieseconds);
            }

            ::dsn::error_code error()
            {
                ::dsn::error_code err2;
                err2.set(dsn_task_error(_task));
                return err2;
            }
            
            size_t io_size()
            {
                return dsn_file_get_io_size(_task);
            }
            
            void enqueue_aio(error_code err, size_t size)
            {
                dsn_file_task_enqueue(_task, err.get(), size);
            }

            ::dsn::message_ptr response()
            {
                auto msg = dsn_rpc_get_response(_task);
                return msg ? ::dsn::message::from_c_msg(msg) : nullptr;
            }

            void enqueue_rpc_response(error_code err, ::dsn::message_ptr resp)
            {
                dsn_rpc_enqueue_response(_task, err.get(), resp ? resp->c_msg() : nullptr);
            }

        private:
            dsn_task_t           _task;
            task_context_manager _manager;
        };

        DEFINE_REF_OBJECT(cpp_dev_task_base)
        typedef ::boost::intrusive_ptr<cpp_dev_task_base> cpp_task_ptr;

        template<typename THandler>
        class cpp_dev_task : public cpp_dev_task_base
        {
        public:
            cpp_dev_task(THandler& h, bool is_timer) : _handler(h), _is_timer(is_timer)
            {
            }

            cpp_dev_task(THandler& h) : _handler(h)
            {
            }

            static void exec(void* task)
            {
                cpp_dev_task* t = (cpp_dev_task*)task;
                t->_handler();
                if (!t->_is_timer)
                {
                    t->release_ref();
                }
            }

            static void exec_rcp_response(dsn_error_t err, dsn_message_t* req, dsn_message_t* resp, void* task)
            {
                cpp_dev_task* t = (cpp_dev_task*)task;
                ::dsn::message_ptr req1 = ::dsn::message::from_c_msg(req);
                ::dsn::message_ptr resp1 = ::dsn::message::from_c_msg(resp);
                error_code err2;
                err2.set(err);
                t->_handler(err2, req1, resp1);
                t->release_ref();
            }

            static void exec_aio(dsn_error_t err, size_t sz, void* task)
            {
                cpp_dev_task* t = (cpp_dev_task*)task;
                error_code err2;
                err2.set(err);
                t->_handler(err2, sz);
                t->release_ref();
            }
            
        private:
            bool                 _is_timer;
            THandler             _handler;
        };


        // ------- inlined implementation ----------
        inline void task_context_manager::init(servicelet* owner, dsn_task_t task)
        {
            _owner = owner;
            _task = task;
            _deleting_owner = OWNER_DELETE_NOT_LOCKED;

            if (nullptr != _owner)
            {
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
