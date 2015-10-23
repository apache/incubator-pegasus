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

# if defined(__APPLE__) || defined(__FreeBSD__)

# include "io_looper.h"

# define IO_LOOPER_USER_NOTIFICATION_FD (-10)

namespace dsn
{
    namespace tools
    {
        io_looper::io_looper()
        {
            _io_queue = -1;
            _local_notification_fd = IO_LOOPER_USER_NOTIFICATION_FD;
            _filters.insert(EVFILT_READ);
            _filters.insert(EVFILT_WRITE);
            //EVFILT_AIO is automatically registered.
            //_filters.insert(EVFILT_AIO);
            _filters.insert(EVFILT_READ_WRITE);
            //Internal use
            _filters.insert(EVFILT_USER);
        }

        io_looper::~io_looper(void)
        {
            stop();
        }

        error_code io_looper::bind_io_handle(
            dsn_handle_t handle,
            io_loop_callback* cb,
            unsigned int events,
            ref_counter* ctx
            )
        {
            int fd;
            short filter;
            int nr_filters;
            struct kevent e[2];

            if (cb == nullptr)
            {
                derror("cb == nullptr");
                return ERR_INVALID_PARAMETERS;
            }

            filter = (short)events;
            if (_filters.find(filter) == _filters.end())
            {
                derror("The filter %hd is unsupported.", filter);
                return ERR_INVALID_PARAMETERS;
            }

            fd = (int)(intptr_t)(handle);
            if (fd != IO_LOOPER_USER_NOTIFICATION_FD)
            {
                if (fd < 0)
                {
                    derror("bind_io_handle: the fd %d is less than 0.", fd);
                    return ERR_INVALID_PARAMETERS;
                }

                if (filter == EVFILT_USER)
                {
                    derror("EVFILT_USER is internally used.");
                    return ERR_INVALID_PARAMETERS;
                }
            }

            if (fd > 0)
            {
                int flags = fcntl(fd, F_GETFL, 0);
                dassert (flags != -1, "fcntl failed, err = %s, fd = %d", strerror(errno), fd);

                if (!(flags & O_NONBLOCK))
                {
                    flags |= O_NONBLOCK;
                    flags = fcntl(fd, F_SETFL, flags);
                    dassert(flags != -1, "fcntl failed, err = %s, fd = %d", strerror(errno), fd);
                }
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
            
            if (filter == EVFILT_READ_WRITE)
            {
                e[0].filter = EVFILT_READ;
                e[1].filter = EVFILT_WRITE;
                nr_filters = 2;
            }
            else
            {
                e[0].filter = filter;
                nr_filters = 1;
            }

            for (int i = 0; i < nr_filters; i++)
            {
                EV_SET(&e[i], fd, e[i].filter, (EV_ADD | EV_CLEAR), 0, 0, (void*)cb0);
            }

            if (kevent(_io_queue, e, nr_filters, nullptr, 0, nullptr) == -1)
            {
                derror("bind io handler to kqueue failed, err = %s, fd = %d", strerror(errno), fd);
                unbind_io_handle(handle, cb);
                return ERR_BIND_IOCP_FAILED;
            }

            return ERR_OK;
        }
        
        error_code io_looper::unbind_io_handle(dsn_handle_t handle, io_loop_callback* cb)
        {
            int fd = (int)(intptr_t)handle;
            int nr_filters;
            struct kevent e[2];
            int cnt = 0;
            bool succ = true;

            if (fd != IO_LOOPER_USER_NOTIFICATION_FD)
            {
                if (fd < 0)
                {
                    derror("unbind_io_handle: the fd %d is less than 0.", fd);
                    return ERR_INVALID_PARAMETERS;
                }

                e[0].filter = EVFILT_READ;
                e[1].filter = EVFILT_WRITE;
                nr_filters = 2;
            }
            else
            {
                e[0].filter = EVFILT_USER;
                nr_filters = 1;
            }

            for (int i = 0; i < nr_filters; i++)
            {
                EV_SET(&e[i], fd, e[i].filter, EV_DELETE, 0, 0, nullptr);

                if (kevent(_io_queue, &e[i], 1, nullptr, 0, nullptr) == -1)
                {
                    if (errno != ENOENT)
                    {
                        derror("unbind io handler to kqueue failed, err = %s, fd = %d", strerror(errno), fd);
                        succ = false;
                    }
                }
                else
                {
                    cnt++;
                }
            }

            if ((cnt == 0) && succ)
            {
                dwarn("fd = %d has not been bound yet.", fd);
            }


            // in case the fd is already invalid
            if (cb)
            {
                utils::auto_lock<utils::ex_lock_nr_spin> l(_io_sessions_lock);
                _io_sessions.erase(cb);
            }

            return (succ ? ERR_OK : ERR_BIND_IOCP_FAILED );
        }

        void io_looper::notify_local_execution()
        {
            struct kevent e;
            EV_SET(&e, _local_notification_fd, EVFILT_USER, 0, (NOTE_FFCOPY | NOTE_TRIGGER), 0, &_local_notification_callback);

            if (kevent(_io_queue, &e, 1, nullptr, 0, nullptr) == -1)
            {
                dassert(false, "post local notification via eventfd failed, err = %s", strerror(errno));
            }
            dinfo("notify local");
        }

        void io_looper::create_completion_queue()
        {
            _io_queue = ::kqueue();
            dassert(_io_queue != -1, "Fail to create kqueue");

            _local_notification_callback = [this](
                int native_error,
                uint32_t io_size,
                uintptr_t lolp_or_events
                )
            {
                this->handle_local_queues();
            };

            bind_io_handle((dsn_handle_t)(intptr_t)_local_notification_fd, &_local_notification_callback, 
                EVFILT_USER);
        }

        void io_looper::close_completion_queue()
        {
            if (_io_queue != -1)
            {
		unbind_io_handle((dsn_handle_t)(intptr_t)_local_notification_fd, &_local_notification_callback);
                ::close(_io_queue);
                _io_queue = -1;
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
            struct timespec ts = { 0, 1000000 };

            while (true)
            {
                int nfds = kevent(_io_queue, nullptr, 0, _events, IO_LOOPER_MAX_EVENT_COUNT, &ts); // 1ms for timers
                if (nfds == 0) // timeout
                {
                    handle_local_queues();
                    continue;
                }
                else if (-1 == nfds)
                {
                    if (errno == EINTR)
                    {
                        continue;
                    }                        
                    else
                    {
                        derror("kevent loop exits, err = %s", strerror(errno));
                        break;
                    }
                }

                for (int i = 0; i < nfds; i++)
                {
                    auto cb = (io_loop_callback*)_events[i].udata;
                    dinfo("kevent get events 0x%x, cb = %p", _events[i].filter, cb);

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
                            (*cb)(0, 0, (uintptr_t)&_events[i]);
                            robj->release_ref();
                        }
                        else
                        {
                            // context is gone (unregistered), let's skip
                            dwarn("kevent event 0x%x skipped as session is gone, cb = %p",
                                _events[i].filter,
                                cb
                                );
                        }
                    }
                    else
                    {
                        (*cb)(0, 0, (uintptr_t)&_events[i]);
                    }
                }
            }
        }
    }
}

# endif
