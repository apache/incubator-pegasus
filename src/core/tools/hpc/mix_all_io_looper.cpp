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

namespace dsn
{
    namespace tools
    {
        class io_looper_holder : public utils::singleton < io_looper_holder >
        {
        public:
            std::unordered_map<service_node*, io_looper*> per_node_loopers;
            std::unordered_map<task_queue*, io_looper*> per_queue_loopers;
        };

        io_looper* get_io_looper(service_node* node, task_queue* q)
        {   
            switch (spec().io_mode)
            {
            case IOE_PER_NODE:
            {
                dassert(node, "node is not given");
                auto it = io_looper_holder::instance().per_node_loopers.find(node);
                if (it == io_looper_holder::instance().per_node_loopers.end())
                {
                    auto looper = new io_looper();
                    looper->start(node, spec().io_worker_count);
                    io_looper_holder::instance().per_node_loopers[node] = looper;
                    return looper;
                }
                else
                    return it->second;
            }
            case IOE_PER_QUEUE:
            {
                dassert(q, "task queue is not given");
                auto p = dynamic_cast<io_looper*>(q);
                dassert(p, "task queue must also be the io looper");
                return p;
            }
            default:
                dassert(false, "invalid io loop type");
                return nullptr;
            }
        }

        //--------------------------------------------
        //
        // when s_config.type == IOE_PER_QUEUE
        //
        io_looper_task_queue::io_looper_task_queue(task_worker_pool* pool, int index, task_queue* inner_provider)
            : task_queue(pool, index, inner_provider)
        {
            _is_shared = is_shared();
            _remote_count = 0;
        }

        io_looper_task_queue::~io_looper_task_queue()
        {
        
        }

        void io_looper_task_queue::start(service_node* node, int worker_count)
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
            if (_is_shared || task::get_current_worker() != this->owner_worker())
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
            io_looper_task_queue* looper = dynamic_cast<io_looper_task_queue*>(queue());
            looper->start(nullptr, 0);
        }

        void io_looper_task_worker::loop()
        {
            io_looper_task_queue* looper = dynamic_cast<io_looper_task_queue*>(queue());
            looper->loop_ios();
        }
    }
}
