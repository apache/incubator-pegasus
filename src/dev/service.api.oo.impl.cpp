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

# include <dsn/internal/service.api.oo.h>

namespace dsn {
    namespace service
    {
        namespace tasking
        {
            class service_task : public task, public service_context_manager
            {
            public:
                service_task(task_code code, servicelet* svc, task_handler& handler, int hash = 0)
                    : task(code, hash), service_context_manager(svc, this)
                {
                    _handler = handler;
                }

                virtual void exec()
                {
                    if (nullptr != _handler)
                    {
                        _handler();
                        _handler = nullptr;
                    }
                }

                //task_handler& handler() { return _handler; }

            private:
                task_handler _handler;
            };

            class service_timer_task : public timer_task, public service_context_manager
            {
            public:
                service_timer_task(task_code code, servicelet* svc, task_handler& handler, uint32_t intervalMilliseconds, int hash = 0)
                    : timer_task(code, intervalMilliseconds, hash), service_context_manager(svc, this)
                {
                    _handler = handler;
                }

                virtual bool on_timer() { _handler(); return true; }

            private:
                task_handler _handler;
            };

            task_ptr enqueue(
                task_code evt,
                servicelet *context,
                task_handler callback,
                int hash /*= 0*/,
                int delay_milliseconds /*= 0*/,
                int timer_interval_milliseconds /*= 0*/
                )
            {
                task_ptr tsk;
                if (timer_interval_milliseconds != 0)
                    tsk.reset(new service_timer_task(evt, context, callback, timer_interval_milliseconds, hash));                    
                else
                    tsk.reset(new service_task(evt, context, callback, hash));

                enqueue(tsk, delay_milliseconds);
                return tsk;
            }
        }

        namespace rpc
        {        

            rpc_response_task_ptr call(
                const end_point& server,
                message_ptr& request,
                servicelet* context,
                std::function<void(error_code, message_ptr&, message_ptr&)> callback,
                int reply_hash /*= 0*/
                )
            {
                rpc_response_task_ptr resp_task(new internal_use_only::service_rpc_response_task4(
                    context,                    
                    callback,
                    request,
                    reply_hash
                    ));

                return rpc::call(server, request, resp_task);
            }
        }

        namespace file
        {
            aio_task_ptr read(
                handle_t hFile,
                char* buffer,
                int count,
                uint64_t offset,
                task_code callback_code,
                servicelet* context,
                aio_handler callback,
                int hash /*= 0*/
                )
            {
                aio_task_ptr tsk(new internal_use_only::service_aio_task(callback_code, context, callback, hash));
                read(hFile, buffer, count, offset, tsk);
                return tsk;
            }

            aio_task_ptr write(
                handle_t hFile,
                const char* buffer,
                int count,
                uint64_t offset,
                task_code callback_code,
                servicelet* context,
                aio_handler callback,
                int hash /*= 0*/
                )
            {
                aio_task_ptr tsk(new internal_use_only::service_aio_task(callback_code, context, callback, hash));
                write(hFile, buffer, count, offset, tsk);
                return tsk;
            }


            aio_task_ptr copy_remote_files(
                const end_point& remote,
                std::string& source_dir,
                std::vector<std::string>& files,  // empty for all
                std::string& dest_dir,
                bool overwrite,
                task_code callback_code,
                servicelet* context,
                aio_handler callback,
                int hash /*= 0*/
                )
            {
                aio_task_ptr tsk(new internal_use_only::service_aio_task(callback_code, context, callback, hash));
                copy_remote_files(remote, source_dir, files, dest_dir, overwrite, tsk);
                return tsk;
            }
        }

    } // end namespace service
} // end namespace



