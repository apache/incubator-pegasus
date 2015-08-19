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
 *
 * History:
 *      Aug., 2015, Zhenyu Guo (zhenyu.guo@microsoft.com)
 */

# include "mix_all_io_looper.h"
# include <dsn/internal/per_node_state.h>

namespace dsn
{
    namespace tools
    {
        struct io_loop_config
        {
            io_loop_type type;
            int          worker_count;
        };

        CONFIG_BEGIN(io_loop_config)
            CONFIG_FLD_ENUM(io_loop_type, type, IOLOOP_GLOBAL, IOLOOP_INVALID, false, "io loop mode: IOLOOP_GLOBAL, IOLOOP_PER_NODE, or IOLOOP_PER_QUEUE")
            CONFIG_FLD(int, uint64, worker_count, 4, "io loop thread count, not used for IOLOOP_PER_QUEUE")
        CONFIG_END

        static io_loop_config s_config;
        static io_looper *s_global_looper;

        io_loop_type get_io_looper_type()
        {
            std::once_flag flag;
            std::call_once(flag, []()
            {
                if (!read_config("io_loop", s_config, nullptr))
                {
                    dassert(false, "invalid io loop configuration");
                }

                if (s_config.type == IOLOOP_GLOBAL)
                {
                    s_global_looper = new io_looper();
                    s_global_looper->start(s_config.worker_count);
                }
            });

            return s_config.type;
        }

        io_looper* get_io_looper(service_node* node)
        {   
            switch (get_io_looper_type())
            {
            case IOLOOP_GLOBAL:
                return s_global_looper;
            case IOLOOP_PER_NODE:
                {
                    auto looper = (io_looper*)get_per_service_node_state(node, "io_looper");
                    if (looper == nullptr)
                    {
                        looper = new io_looper();
                        looper->start(s_config.worker_count);
                        put_per_service_node_state(node, "io_looper", looper);
                    }
                    return looper;
                }
            case IOLOOP_PER_QUEUE:
                return nullptr;
            default:
                dassert(false, "invalid io loop type");
                return nullptr;
            }
        }

        //--------------------------------------------
        //
        // when s_config.type == IOLOOP_PER_QUEUE
        //
        io_looper_task_queue::io_looper_task_queue(task_worker_pool* pool, int index, task_queue* inner_provider)
            : task_queue(pool, index, inner_provider)
        {
            _is_shared = is_shared();
        }

        io_looper_task_queue::~io_looper_task_queue()
        {
        
        }

        void io_looper_task_queue::start(int worker_count)
        {
            create_completion_queue();
        }

        void io_looper_task_queue::stop()
        {
            io_looper::stop();
        }

        void io_looper_task_queue::handle_local_queues()
        {
            // execute shared queue
            while (true)
            {
                utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);

                dlink *t;
                {
                    utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);
                    t = _remote_tasks.next();
                    if (t == &_remote_tasks)
                        break;
                    t->remove();
                }

                _remote_count.fetch_sub(1, std::memory_order_release);
                task* ts = CONTAINING_RECORD(t, task, _task_queue_dl);
                ts->exec_internal();
            }

            // execute local queue
            while (true)
            {
                dlink *t = _local_tasks.next();
                if (t == &_local_tasks)
                    break;

                t->remove();
                task* ts = CONTAINING_RECORD(t, task, _task_queue_dl);
                ts->exec_internal();
            }

            // TODO: execute timers

        }

        void io_looper_task_queue::enqueue(task* task)
        {
            // put into locked queue when it is shared or from remote threads
            if (_is_shared || task::get_current_worker_index() != index())
            {
                {
                    utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);

                    task->_task_queue_dl.insert_before(&_remote_tasks);
                }

                int old = _remote_count.fetch_add(1, std::memory_order_release);
                if (old == 0)
                {
                    notify_local_execution();
                }
            }

            // put into local queue
            else
            {
                task->_task_queue_dl.insert_before(&_local_tasks);
            }
        }

        task* io_looper_task_queue::dequeue()
        {
            dassert(false, "never execute here ...");
            return nullptr;
        }
               
        io_looper_task_worker::io_looper_task_worker(task_worker_pool* pool, task_queue* q, int index, task_worker* inner_provider)
            : task_worker(pool, q, index, inner_provider)
        {
        
        }

        void io_looper_task_worker::loop()
        {
            io_looper_task_queue* looper = (io_looper_task_queue*)queue();
            looper->loop_ios();
        }
    }
}
