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

# include "io_looper.h"
# include <dsn/internal/per_node_state.h>

# if defined(__linux__)

# include <sys/eventfd.h>

namespace dsn
{
    namespace tools
    {
        io_looper::io_looper()
        {
            _io_queue = 0;
            _local_notification_fd = eventfd(0, EFD_NONBLOCK | EFD_SEMAPHORE);
        }

        io_looper::~io_looper(void)
        {
            stop();
            close(_local_notification_fd);
        }

        error_code io_looper::bind_io_handle(dsn_handle_t handle, io_loop_callback* cb, unsigned int events)
        {
            int fd = (int)(intptr_t)(handle);

            int flags = fcntl(fd, F_GETFL, 0);
            dassert (flags != -1, "fcntl failed, err = %s", strerror(errno));
            flags |= O_NONBLOCK;
            flags = fcntl(fd, F_SETFL, flags);
            dassert (flags != -1, "fcntl failed, err = %s", strerror(errno));

            struct epoll_event e;
            e.data.ptr = cb;
            e.events = events;
            
            if (epoll_ctl(_io_queue, EPOLL_CTL_ADD, fd, &e) < 0)
            {
                derror("bind io handler to completion port failed, err = %s", strerror(errno));
                return ERR_BIND_IOCP_FAILED;
            }
            else
                return ERR_OK;
        }

        error_code io_looper::unbind_io_handle(dsn_handle_t handle)
        {
            int fd = (int)(intptr_t)handle;

            if (epoll_ctl(_io_queue, EPOLL_CTL_DEL, fd, NULL) < 0)
            {
                derror("unbind io handler to completion port failed, err = %s", strerror(errno));
                return ERR_BIND_IOCP_FAILED;
            }
            else
                return ERR_OK;
        }

        void io_looper::notify_local_execution()
        {
            int64_t c = 1;
            ::write(_local_notification_fd, &c, sizeof(c));
        }

        void io_looper::create_completion_queue()
        {
            const int max_event_count = sizeof(_events) / sizeof(struct epoll_event);

            _io_queue = epoll_create(max_event_count);
        }

        void io_looper::start(service_node* node, int worker_count)
        {
            create_completion_queue();

            _local_notification_callback =[this](
                int native_error,
                uint32_t io_size,
                uintptr_t lolp_or_events
                )
            {
                uint32_t events = (uint32_t)lolp_or_events;
                int notify_count = 0;

                if (read(_local_notification_fd, &notify_count, sizeof(notify_count)) != sizeof(notify_count))
                {
                    dassert(false, "read number of aio completion from eventfd failed, err = %s",
                        strerror(errno)
                        );
                }

                this->handle_local_queues();
            };

            bind_io_handle((dsn_handle_t)_local_notification_fd, &_local_notification_callback, EPOLLIN | EPOLLET);

            for (int i = 0; i < worker_count; i++)
            {
                std::thread* thr = new std::thread([this, node]()
                {
                    const char* name = node ? ::dsn::tools::get_service_node_name(node) : "glb";
                    char buffer[128];
                    sprintf(buffer, "%s.io-loop.%d", name, i);
                    task_worker::set_name(buffer);

                    if (node)
                    {
                        task::set_current_worker(nullptr, node);
                    }

                    this->loop_ios(); 
                });
                _workers.push_back(thr);
            }
        }

        void io_looper::stop()
        {
            if (0 == _io_queue)
                return;

            close(_io_queue);
            _io_queue = 0;
            for (auto thr : _workers)
            {
                thr->join();
                delete thr;
            }
            _workers.clear();
        }

        void io_looper::loop_ios()
        {
            const int max_event_count = sizeof(_events) / sizeof(struct epoll_event);

            while (true)
            {
                int nfds = epoll_wait(_io_queue, _events, max_event_count, -1);
                if (-1 == nfds)
                {
                    if (errno == EINTR)
                        continue;
                    else
                    {
                        derror("epoll_wait loop exits, err = %s", strerror(errno));
                        break;
                    }
                }

                for (int i = 0; i < nfds; i++)
                {
                    auto cb = (io_loop_callback*)_events[i].data.ptr;
                    dinfo("epoll_wait get events %x, cb = %p", _events[i].events, cb);
                    (*cb)(0, 0, (uintptr_t)_events[i].events);
                }
            }
        }
    }
}

# endif
