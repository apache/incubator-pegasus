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

        io_looper* get_io_looper(service_node* node, task_queue* q, ioe_mode mode)
        {   
            switch (mode)
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
            // execute timers in current thread
            exec_timer_tasks(true);

            // execute local queue
            task *t = _local_tasks.pop_all(), *next;
            while (t)
            {
                next = t->_next;
                t->exec_internal();
                t = next;
            }

            // execute shared queue
            {
                utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);
                t = _remote_tasks.pop_all();
            }
            
            while (t)
            {
                next = t->_next;
                t->exec_internal();
                t = next;
            }
        }

        void io_looper::exec_timer_tasks(bool local_exec)
        {
            // execute local timers
            uint64_t nts = ::dsn::task::get_current_env()->now_ns() / 1000000;
            while (_local_timer_tasks.size() > 0)
            {
                auto it = _local_timer_tasks.begin();
                if (it->first <= nts)
                {
                    task* t = it->second.pop_all(), *next;
                    _local_timer_tasks.erase(it);

                    while (t)
                    {
                        next = t->_next;
                        if (local_exec)
                            t->exec_internal();
                        else
                        {
                            t->enqueue();
                            t->release_ref(); // added by first t->enqueue()
                        }

                        t = next;
                    }
                }
                else
                    break;
            }

            // execute shared timers
            while (_remote_timer_tasks_count.load(std::memory_order_relaxed) > 0)
            {
                task* t = nullptr, *next;
                {
                    utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_remote_timer_tasks_lock);
                    if (_remote_timer_tasks.size() == 0)
                        break;

                    auto it = _remote_timer_tasks.begin();
                    if (it->first <= nts)
                    {
                        t = it->second.pop_all();
                        _remote_timer_tasks.erase(it);
                    }
                    else
                        break;
                }

                while (t)
                {
                    _remote_timer_tasks_count--;
                    
                    next = t->_next;

                    if (local_exec)
                        t->exec_internal();
                    else
                    {
                        t->enqueue();
                        t->release_ref(); // added by first t->enqueue()
                    }

                    t = next;
                }
            }
        }

        void io_looper::add_timer(task* timer)
        {
            uint64_t ts_ms = dsn_now_ms() + timer->delay_milliseconds();
            timer->set_delay(0);

            // put into locked queue when it is shared or from remote threads
            if (is_shared_timer_queue())
            {
                {
                    utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_remote_timer_tasks_lock);
                    auto pr = _remote_timer_tasks.insert(
                        std::map<uint64_t, slist<task>>::value_type(ts_ms, slist<task>()));
                    pr.first->second.add(timer);
                }

                _remote_timer_tasks_count++;
            }

            // put into local queue
            else
            {
                auto pr = _local_timer_tasks.insert(
                    std::map<uint64_t, slist<task>>::value_type(ts_ms, slist<task>()));
                pr.first->second.add(timer);
            }
        }

        void io_looper_task_queue::enqueue(task* task)
        {
            // put into locked queue when it is shared or from remote threads
            if (is_shared() || task::get_current_worker() != this->owner_worker())
            {
                {
                    utils::auto_lock<::dsn::utils::ex_lock_nr_spin> l(_lock);
                    _remote_tasks.add(task);
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
                _local_tasks.add(task);
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
            looper->loop_worker();
        }
    }
}
