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
 *     define the base cross-platform asynchonous io looper interface 
 *
 * Revision history:
 *     Aug., 2015, @imzhenyu (Zhenyu Guo), the first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */
#pragma once

# include <dsn/internal/ports.h>
# include <dsn/tool_api.h>

# ifndef _WIN32

# ifdef __linux__
# include <sys/epoll.h>
# endif

# if defined(__APPLE__) || defined(__FreeBSD__)
# include <sys/types.h>
# include <sys/event.h>
# include <sys/time.h>
# include <unordered_set>
# ifndef EVFILT_NONE
# define EVFILT_NONE (-EVFILT_SYSCOUNT - 10)
# endif
# ifndef EVFILT_READ_WRITE
# define EVFILT_READ_WRITE (EVFILT_NONE - 1)
# endif
# endif

# endif

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
        // void handle_event(int native_error, uint32_t io_size, uintptr_t lolp_or_events) 
        typedef std::function<void(int, uint32_t, uintptr_t)> io_loop_callback;

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

            void close_completion_queue();

            error_code bind_io_handle(
                dsn_handle_t handle, 
                io_loop_callback* cb, 
                unsigned int events = 0,
                ref_counter* ctx = nullptr
                );
            
            error_code unbind_io_handle(dsn_handle_t handle, io_loop_callback* cb = nullptr);

            void notify_local_execution();
            
            void exit_loops(bool wait);

            dsn_handle_t native_handle() { return (dsn_handle_t)(uintptr_t)(_io_queue); }

            void loop_worker();

            virtual void start(service_node* node, int worker_count);

            virtual void stop();

            virtual void handle_local_queues() { exec_timer_tasks(false); }

            void add_timer(task* timer); // return next firing delay ms

        protected:
            virtual bool is_shared_timer_queue() { return true; }
            void exec_timer_tasks(bool local_exec);

        private:
            std::vector<std::thread*> _workers;
# ifdef _WIN32
            HANDLE                    _io_queue;
# else
            int                       _io_queue;

# define IO_LOOPER_MAX_EVENT_COUNT 128
# ifdef __linux__
            struct epoll_event        _events[IO_LOOPER_MAX_EVENT_COUNT];
# elif defined(__APPLE__) || defined(__FreeBSD__)
            struct kevent             _events[IO_LOOPER_MAX_EVENT_COUNT];
            typedef std::unordered_set<short> kqueue_filters;
            kqueue_filters            _filters;
# endif

            int                       _local_notification_fd;
            io_loop_callback          _local_notification_callback;

            //
            // epoll notifications are not per-op, so we have to
            // use a look-up layer to ensure the callback context
            // is correctly referenced when the callback is executed.
            //
            typedef std::unordered_map<io_loop_callback*, ref_counter*> io_sessions;
            ::dsn::utils::ex_lock_nr_spin _io_sessions_lock;
            io_sessions                   _io_sessions;
# endif
            // timers
            std::atomic<uint64_t>           _remote_timer_tasks_count;
            ::dsn::utils::ex_lock_nr_spin   _remote_timer_tasks_lock;
            std::map<uint64_t, slist<task>> _remote_timer_tasks; // ts (ms) => task
            std::map<uint64_t, slist<task>> _local_timer_tasks;
        };

        // --------------- inline implementation -------------------------
    }
}
