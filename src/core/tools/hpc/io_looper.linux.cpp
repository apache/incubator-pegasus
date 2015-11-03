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


# if defined(__linux__)

# include "io_looper.h"
# include <sys/eventfd.h>

namespace dsn
{
    namespace tools
    {
        io_looper::io_looper()
        {
            _io_queue = 0;
            _local_notification_fd = eventfd(0, EFD_NONBLOCK);
        }

        io_looper::~io_looper(void)
        {
            stop();
            close(_local_notification_fd);
        }

        error_code io_looper::bind_io_handle(
            dsn_handle_t handle,
            io_loop_callback* cb,
            unsigned int events,
            ref_counter* ctx
            )
        {
            int fd = (int)(intptr_t)(handle);

            int flags = fcntl(fd, F_GETFL, 0);
            dassert (flags != -1, "fcntl failed, err = %s, fd = %d", strerror(errno), fd);

            if (!(flags & O_NONBLOCK))
            {
                flags |= O_NONBLOCK;
                flags = fcntl(fd, F_SETFL, flags);
                dassert(flags != -1, "fcntl failed, err = %s, fd = %d", strerror(errno), fd);
            }
            
            uintptr_t cb0 = (uintptr_t)cb;
            dassert((cb0 & 0x1) == 0, "the least one bit must be zero for the callback address");

            if (ctx)
            {
                cb0 |= 0x1; // has ref_counter

                utils::auto_lock<utils::ex_lock_nr_spin> l(_io_sessions_lock);
                auto pr = _io_sessions.insert(io_sessions::value_type(cb, ctx));
                dassert(pr.second, "the callback must not be registered before");
            }
            
            struct epoll_event e;
            e.data.ptr = (void*)cb0;
            e.events = events;
            
            if (epoll_ctl(_io_queue, EPOLL_CTL_ADD, fd, &e) < 0)
            {
                derror("bind io handler to epoll_wait failed, err = %s, fd = %d", strerror(errno), fd);

                if (ctx)
                {
                    utils::auto_lock<utils::ex_lock_nr_spin> l(_io_sessions_lock);
                    auto r = _io_sessions.erase(cb);
                    dassert(r > 0, "the callback must be present");
                }
                return ERR_BIND_IOCP_FAILED;
            }
            else
                return ERR_OK;
        }
        
        error_code io_looper::unbind_io_handle(dsn_handle_t handle, io_loop_callback* cb)
        {
            int fd = (int)(intptr_t)handle;
            
            if (epoll_ctl(_io_queue, EPOLL_CTL_DEL, fd, NULL) < 0)
            {
                derror("unbind io handler to epoll_wait failed, err = %s, fd = %d", strerror(errno), fd);

                // in case the fd is already invalid
                if (cb)
                {
                    utils::auto_lock<utils::ex_lock_nr_spin> l(_io_sessions_lock);
                    _io_sessions.erase(cb);
                }
                return ERR_BIND_IOCP_FAILED;
            }
            else
            {
                if (cb)
                {
                    utils::auto_lock<utils::ex_lock_nr_spin> l(_io_sessions_lock);
                    auto r = _io_sessions.erase(cb);
                    dassert(r > 0, "the callback must be present");
                }
                return ERR_OK;
            }                
        }

        void io_looper::notify_local_execution()
        {
            int64_t c = 1;
            if (::write(_local_notification_fd, &c, sizeof(c)) < 0)
            {
                dassert(false, "post local notification via eventfd failed, err = %s", strerror(errno));
            }
            dinfo("notify local");
        }

        void io_looper::create_completion_queue()
        {
            const int max_event_count = sizeof(_events) / sizeof(struct epoll_event);

            _io_queue = epoll_create(max_event_count);

            _local_notification_callback = [this](
                int native_error,
                uint32_t io_size,
                uintptr_t lolp_or_events
                )
            {
                uint32_t events = (uint32_t)lolp_or_events;
                int64_t notify_count = 0;

                if (read(_local_notification_fd, &notify_count, sizeof(notify_count)) != sizeof(notify_count))
                {
                    // possibly consumed already by others
                    // e.g., two contiguous write with two read, 
                    // the second read will read nothing
                    return;
                }

                this->handle_local_queues();
            };

            bind_io_handle((dsn_handle_t)(intptr_t)_local_notification_fd, &_local_notification_callback, 
                EPOLLIN | EPOLLET);
        }

        void io_looper::close_completion_queue()
        {
            if (_io_queue != 0)
            {
                ::close(_io_queue);
                _io_queue = 0;
            }
        }

        void io_looper::start(service_node* node, int worker_count)
        {
            create_completion_queue();
            
            for (int i = 0; i < worker_count; i++)
            {
                std::thread* thr = new std::thread([this, node, i]()
                {
                    task::set_tls_dsn_context(node, nullptr, nullptr);

                    const char* name = node ? ::dsn::tools::get_service_node_name(node) : "glb";
                    char buffer[128];
                    sprintf(buffer, "%s.io-loop.%d", name, i);
                    task_worker::set_name(buffer);
                    
                    this->loop_worker(); 
                });
                _workers.push_back(thr);
            }
        }

        void io_looper::stop()
        {
            close_completion_queue();

            if (_workers.size() > 0)
            {
                for (auto thr : _workers)
                {
                    thr->join();
                    delete thr;
                }
                _workers.clear();
            }
        }

        void io_looper::loop_worker()
        {
            const int max_event_count = sizeof(_events) / sizeof(struct epoll_event);

            while (true)
            {
                int nfds = epoll_wait(_io_queue, _events, max_event_count, 1); // 1ms for timers
                if (nfds == 0) // timeout
                {
                    handle_local_queues();
                }
                else if (-1 == nfds)
                {
                    if (errno == EINTR)
                    {
                        continue;
                    }                        
                    else
                    {
                        derror("epoll_wait loop exits, err = %s", strerror(errno));
                        break;
                    }
                }

                for (int i = 0; i < nfds; i++)
                {
                    auto cb = (io_loop_callback*)_events[i].data.ptr;
                    dinfo("epoll_wait get events 0x%x, cb = %p", _events[i].events, cb);

                    uintptr_t cb0 = (uintptr_t)cb;

                    // for those with ref_counter register entries
                    if (cb0 & 0x1)
                    {
                        cb = (io_loop_callback*)(cb0 - 1);

                        ref_counter* robj;
                        {

                            utils::auto_lock<utils::ex_lock_nr_spin> l(_io_sessions_lock);
                            auto it = _io_sessions.find(cb);
                            if (it != _io_sessions.end())
                            {
                                robj = it->second;
                                // make sure callback is protected by ref counting
                                robj->add_ref();
                            }
                            else
                            {
                                robj = nullptr;
                            }
                        }

                        if (robj)
                        {
                            (*cb)(0, 0, (uintptr_t)_events[i].events);
                            robj->release_ref();
                        }
                        else
                        {
                            // context is gone (unregistered), let's skip
                            dwarn("epoll_wait event 0x%x skipped as session is gone, cb = %p",
                                _events[i].events,
                                cb
                                );
                        }
                    }
                    else
                    {
                        (*cb)(0, 0, (uintptr_t)_events[i].events);
                    }
                }
            }
        }
    }
}

# endif
