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
 *      Aug., 2015, Zhenyu Guo, the first version (zhenyu.guo@microsoft.com)
 */
#pragma once

# include <dsn/ports.h>
# include <dsn/tool_api.h>

#ifndef _WIN32
# include <sys/epoll.h>
#endif

namespace dsn
{
    namespace tools
    {
        //
        // this structure is per io handle, and registered when bind_io_handle to completion queue
        // the callback will be executed per io completion or ready
        //
        // windows (on completion):
        //   using lolp to differentiate multiple ops
        // linux (on ready):
        //   using evens to differentiate differnt types of ops
        //   for the same type of ops, it seems Linux doesn't support op differentiation
        //
        struct io_loop_callback
        {
            virtual void handle_event(int native_error, uint32_t io_size, uintptr_t lolp_or_events) = 0;
        };

        //
        // io looper on completion queue
        // it is possible there are multiple loopers on the same io_queue
        //
        class io_looper
        {
        public:
            io_looper();
            virtual ~io_looper(void);

            void create_completion_queue();

            error_code bind_io_handle(dsn_handle_t handle, io_loop_callback* cb, unsigned int events = 0);
            
            error_code unbind_io_handle(dsn_handle_t handle);

            void notify_local_execution();
            
            void exit_loops(bool wait);

            dsn_handle_t native_handle() { return (dsn_handle_t)(uintptr_t)(_io_queue); }

            void loop_ios();

            virtual void start(int worker_count);

            virtual void stop();

            virtual void handle_local_queues() {}

        private:
# ifdef _WIN32
            HANDLE _io_queue;
# else
            int    _io_queue;
            struct epoll_event _events[100];
            int    _local_notification_fd;
            int    _disk_fd;
# endif
            std::vector<std::thread*> _workers;
        };
    }
}
