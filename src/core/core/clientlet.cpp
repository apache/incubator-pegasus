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

#include <dsn/cpp/clientlet.h>
#include <dsn/cpp/service_app.h>
#include <dsn/utility/singleton.h>
#include <iostream>
#include <map>

namespace dsn {

clientlet::clientlet(int task_bucket_count)
{
    _tracker = dsn_task_tracker_create(task_bucket_count);
    _access_thread_id_inited = false;
}

clientlet::~clientlet() { dsn_task_tracker_destroy(_tracker); }

void clientlet::check_hashed_access()
{
    if (_access_thread_id_inited) {
        dassert(::dsn::utils::get_current_tid() == _access_thread_id,
                "the service is assumed to be accessed by one thread only!");
    } else {
        _access_thread_id = ::dsn::utils::get_current_tid();
        _access_thread_id_inited = true;
    }
}

task_ptr rpc::create_rpc_response_task(dsn_message_t request,
                                       clientlet *svc,
                                       empty_callback_t,
                                       int reply_thread_hash)
{
    task_ptr tsk = new safe_task_handle;
    // do not add_ref here
    auto t = dsn_rpc_create_response_task(
        request, nullptr, nullptr, reply_thread_hash, svc ? svc->tracker() : nullptr);
    tsk->set_task_info(t);
    return tsk;
}

namespace file {
task_ptr create_aio_task(dsn_task_code_t callback_code, clientlet *svc, empty_callback_t, int hash)
{
    task_ptr tsk = new safe_task_handle;
    // do not add_ref here
    dsn_task_t t = dsn_file_create_aio_task(
        callback_code, nullptr, nullptr, hash, svc ? svc->tracker() : nullptr);
    tsk->set_task_info(t);
    return tsk;
}

void copy_remote_files_impl(::dsn::rpc_address remote,
                            const std::string &source_dir,
                            const std::vector<std::string> &files, // empty for all
                            const std::string &dest_dir,
                            bool overwrite,
                            bool high_priority,
                            dsn_task_t native_task)
{
    if (files.empty()) {
        dsn_file_copy_remote_directory(remote.c_addr(),
                                       source_dir.c_str(),
                                       dest_dir.c_str(),
                                       overwrite,
                                       high_priority,
                                       native_task);
    } else {
        const char **ptr = (const char **)alloca(sizeof(const char *) * (files.size() + 1));
        const char **ptr_base = ptr;
        for (auto &f : files) {
            *ptr++ = f.c_str();
        }
        *ptr = nullptr;

        dsn_file_copy_remote_files(remote.c_addr(),
                                   source_dir.c_str(),
                                   ptr_base,
                                   dest_dir.c_str(),
                                   overwrite,
                                   high_priority,
                                   native_task);
    }
}
}

} // end namespace dsn::service
