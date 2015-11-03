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
/*
 * History:
 *      Aug., 2015, Zhenyu Guo created (zhenyu.guo@microsoft.com)
 */

# include "io_looper.h"

namespace dsn
{
    namespace tools
    {
        extern io_looper* get_io_looper(service_node* node, task_queue* q, ioe_mode mode);

        class io_looper_task_queue : public task_queue, public io_looper
        {
        public:
            io_looper_task_queue(task_worker_pool* pool, int index, task_queue* inner_provider);
            virtual ~io_looper_task_queue();
                        
            virtual void  start(service_node* node, int worker_count) override;
            virtual void  stop() override;
            virtual void  handle_local_queues() override;

            virtual void  enqueue(task* task) override;
            virtual task* dequeue()override;
        
        protected:
            virtual bool is_shared_timer_queue() override
            {
                return is_shared() || task::get_current_worker() != owner_worker();
            }

        private:
            std::atomic<int>              _remote_count;

            // tasks from remote threads
            ::dsn::utils::ex_lock_nr_spin _lock;
            slist<task>                   _remote_tasks;

            // tasks from local thread
            slist<task>                   _local_tasks;
        };

        class io_looper_task_worker : public task_worker
        {
        public:
            io_looper_task_worker(task_worker_pool* pool, task_queue* q, int index, task_worker* inner_provider);
            virtual void loop();
        };

        class io_looper_timer_service : public timer_service
        {
        public:
            io_looper_timer_service(service_node* node, timer_service* inner_provider)
                : timer_service(node, inner_provider)
            {
                _looper = nullptr;
            }

            virtual void start(io_modifer& ctx)
            {
                _looper = get_io_looper(node(), ctx.queue, ctx.mode);
                dassert(_looper != nullptr, "correspondent looper is empty");
            }

            // after milliseconds, the provider should call task->enqueue()        
            virtual void add_timer(task* task)
            {
                _looper->add_timer(task);
            }

        private:
            io_looper *_looper;
        };

        // ------------------ inline implementation --------------------
        

    }
}
