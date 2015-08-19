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

# ifdef _WIN32

namespace dsn
{
    namespace tools
    {        
        io_looper::io_looper()
        {
            _io_queue = 0;
        }

        io_looper::~io_looper(void)
        {
            stop();
        }

        error_code io_looper::bind_io_handle(dsn_handle_t handle, io_loop_callback* cb, unsigned int events)
        {
            events; // not used on windows
            if (NULL == ::CreateIoCompletionPort((HANDLE)handle, _io_queue, (ULONG_PTR)cb, 0))
            {
                derror("bind io handler to completion port failed, err = %d", ::GetLastError());
                return ERR_BIND_IOCP_FAILED;
            }
            else
                return ERR_OK;
        }

        error_code io_looper::unbind_io_handle(dsn_handle_t handle)
        {
            // nothing to do
            return ERR_OK;
        }

        void io_looper::start(int worker_count)
        {
            _io_queue = ::CreateIoCompletionPort(INVALID_HANDLE_VALUE, NULL, 0, 0);
            for (int i = 0; i < worker_count; i++)
            {
                std::thread* thr = new std::thread([this](){ this->loop_ios(); });
                _workers.push_back(thr);
            }
        }

        void io_looper::stop()
        {
            if (0 == _io_queue)
                return;

            ::CloseHandle(_io_queue);
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
            DWORD io_size;
            uintptr_t completion_key;
            LPOVERLAPPED lolp;
            DWORD error;

            while (true)
            {
                BOOL r = ::GetQueuedCompletionStatus(_io_queue, &io_size, &completion_key, &lolp, INFINITE);

                // everything goes fine
                if (r)
                {
                    error = ERROR_SUCCESS;
                }

                // failed or timeout
                else
                {
                    error = ::GetLastError();
                    if (error == ERROR_ABANDONED_WAIT_0)
                    {
                        derror("completion port loop exits");
                        break;
                    }

                    /*
                    If *lpOverlapped is NULL, the function did not dequeue a completion packet from
                    the completion port. In this case, the function does not store information in the 
                    variables pointed to by the lpNumberOfBytes and lpCompletionKey parameters, 
                    and their values are indeterminate.
                    */
                    if (NULL == lolp)
                    {
                        dassert(false, "unhandled case!!!");
                        continue;
                    }
                }

                if (NON_IO_TASK_NOTIFICATION_KEY == completion_key)
                {
                    handle_local_queues();
                }
                else
                {
                    io_loop_callback* cb = (io_loop_callback*)completion_key;
                    cb->handle_event((int)error, io_size, (uintptr_t)lolp);
                }
            }
        }
    }
}

# endif
