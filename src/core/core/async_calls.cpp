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

#include <dsn/cpp/service_app.h>
#include <dsn/utility/singleton.h>
#include <dsn/tool-api/task.h>
#include <dsn/tool-api/async_calls.h>
#include <iostream>
#include <map>

namespace dsn {
namespace file {

void copy_remote_files_impl(::dsn::rpc_address remote,
                            const std::string &source_dir,
                            const std::vector<std::string> &files, // empty for all
                            const std::string &dest_dir,
                            bool overwrite,
                            bool high_priority,
                            aio_task *tsk)
{
    if (files.empty()) {
        dsn_file_copy_remote_directory(
            remote, source_dir.c_str(), dest_dir.c_str(), overwrite, high_priority, tsk);
    } else {
        const char **ptr = (const char **)alloca(sizeof(const char *) * (files.size() + 1));
        const char **ptr_base = ptr;
        for (auto &f : files) {
            *ptr++ = f.c_str();
        }
        *ptr = nullptr;

        dsn_file_copy_remote_files(
            remote, source_dir.c_str(), ptr_base, dest_dir.c_str(), overwrite, high_priority, tsk);
    }
}
}
}
