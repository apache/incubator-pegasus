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
            cpp_task_ptr enqueue(
                dsn_task_code_t evt,
                servicelet *context,
                task_handler callback,
                int hash /*= 0*/,
                int delay_milliseconds /*= 0*/,
                int timer_interval_milliseconds /*= 0*/
                )
            {                
                dsn_task_t t;
                auto tsk = new cpp_dev_task<task_handler>(callback, timer_interval_milliseconds != 0);
                
                if (timer_interval_milliseconds != 0)
                { 
                    t = dsn_task_create_timer(evt, cpp_dev_task<task_handler>::exec, tsk, hash, timer_interval_milliseconds);
                }   
                else
                {
                    t = dsn_task_create(evt, cpp_dev_task<task_handler>::exec, tsk, hash);
                }

                tsk->set_task_info(t, context);

                dsn_task_call(t, delay_milliseconds);
                return tsk;
            }
        }

        namespace rpc
        {   
            cpp_task_ptr call(
                const dsn_address_t& server,
                message_ptr& request,
                servicelet* owner,
                rpc_reply_handler callback,
                int reply_hash
                )
            {
                auto tsk = new cpp_dev_task<rpc_reply_handler >(callback);
                auto t = dsn_rpc_create(
                    request->c_msg(),
                    cpp_dev_task<rpc_reply_handler >::exec_rcp_response,
                    (void*)tsk,
                    reply_hash
                    );
                tsk->set_task_info(t, (servicelet*)owner);
                dsn_rpc_call(server, t);

                return tsk;
            }
        }

        namespace file
        {
            cpp_task_ptr read(
                dsn_handle_t hFile,
                char* buffer,
                int count,
                uint64_t offset,
                dsn_task_code_t callback_code,
                servicelet* owner,
                aio_handler callback,
                int hash /*= 0*/
                )
            {
                auto tsk = new cpp_dev_task<aio_handler>(callback);
                dsn_task_t t = dsn_file_create_callback_task(callback_code, cpp_dev_task<aio_handler>::exec_aio, tsk, hash);
                tsk->set_task_info(t, owner);

                dsn_file_read(hFile, buffer, count, offset, t);
                return tsk;
            }

            cpp_task_ptr write(
                dsn_handle_t hFile,
                const char* buffer,
                int count,
                uint64_t offset,
                dsn_task_code_t callback_code,
                servicelet* owner,
                aio_handler callback,
                int hash /*= 0*/
                )
            {
                auto tsk = new cpp_dev_task<aio_handler>(callback);
                dsn_task_t t = dsn_file_create_callback_task(callback_code, cpp_dev_task<aio_handler>::exec_aio, tsk, hash);
                tsk->set_task_info(t, owner);

                dsn_file_write(hFile, buffer, count, offset, t);
                return tsk;
            }


            cpp_task_ptr copy_remote_files(
                const dsn_address_t& remote,
                const std::string& source_dir,
                std::vector<std::string>& files,  // empty for all
                const std::string& dest_dir,
                bool overwrite,
                dsn_task_code_t callback_code,
                servicelet* owner,
                aio_handler callback,
                int hash /*= 0*/
                )
            {
                auto tsk = new cpp_dev_task<aio_handler>(callback);
                dsn_task_t t = dsn_file_create_callback_task(callback_code, cpp_dev_task<aio_handler>::exec_aio, tsk, hash);
                tsk->set_task_info(t, owner);

                if (files.empty())
                {
                    dsn_file_copy_remote_directory(remote, source_dir.c_str(), dest_dir.c_str(), overwrite, t);
                }
                else
                {
                    const char** ptr = (const char**)alloca(sizeof(const char*) * (files.size() + 1));
                    for (auto& f : files)
                    {
                        *ptr++ = f.c_str();
                    }
                    *ptr = nullptr;

                    dsn_file_copy_remote_files(remote, source_dir.c_str(), ptr, dest_dir.c_str(), overwrite, t);
                }
                return tsk;
            }
        }

    } // end namespace service
} // end namespace



