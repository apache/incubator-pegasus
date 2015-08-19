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
 *      Aug., 2015, Zhenyu Guo created (zhenyu.guo@microsoft.com)
 */

# include "io_looper.h"

namespace dsn
{
    namespace tools
    {
        class mix_all_io_looper : public task_queue, public io_looper
        {
        public:
            mix_all_io_looper(task_worker_pool* pool, int index, task_queue* inner_provider);
            virtual ~mix_all_io_looper();

            virtual void start(int worker_count);

            virtual void stop();

            virtual void handle_local_queues();

        public:
            virtual void     enqueue(task* task);
            virtual task*    dequeue();
            virtual int      count() const { return _remote_count.load(); }

            void execute(int qutota_milliseconds);

        private:
            std::atomic<int>              _remote_count;

            // tasks from remote threads
            ::dsn::utils::ex_lock_nr_spin _lock;
            dlink                         _remote_tasks;

            // tasks from local thread
            dlink                         _local_tasks;

            // TODO: timer tasks
        };

        class io_looper_task_worker : public task_worker
        {
        public:
            io_looper_task_worker(task_worker_pool* pool, task_queue* q, int index, task_worker* inner_provider);
            virtual void loop();
        };
    }
}