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

#include <algorithm>
#include <utility>

#include "aio/file_io.h"
#include "nfs/nfs_node.h"
#include "nfs_node_simple.h"
#include "utils/autoref_ptr.h"

namespace dsn {
class task_tracker;

std::unique_ptr<nfs_node> nfs_node::create()
{
    return std::make_unique<dsn::service::nfs_node_simple>();
}

aio_task_ptr nfs_node::copy_remote_directory(const host_port &remote,
                                             const std::string &source_disk_tag,
                                             const std::string &source_dir,
                                             const std::string &dest_disk_tag,
                                             const std::string &dest_dir,
                                             const dsn::gpid &pid,
                                             bool overwrite,
                                             bool high_priority,
                                             task_code callback_code,
                                             task_tracker *tracker,
                                             aio_handler &&callback,
                                             int hash)
{
    return copy_remote_files(remote,
                             source_disk_tag,
                             source_dir,
                             {},
                             dest_disk_tag,
                             dest_dir,
                             pid,
                             overwrite,
                             high_priority,
                             callback_code,
                             tracker,
                             std::move(callback),
                             hash);
}

aio_task_ptr nfs_node::copy_remote_files(const host_port &remote,
                                         const std::string &source_disk_tag,
                                         const std::string &source_dir,
                                         const std::vector<std::string> &files,
                                         const std::string &dest_disk_tag,
                                         const std::string &dest_dir,
                                         const dsn::gpid &pid,
                                         bool overwrite,
                                         bool high_priority,
                                         task_code callback_code,
                                         task_tracker *tracker,
                                         aio_handler &&callback,
                                         int hash)
{
    auto cb = dsn::file::create_aio_task(callback_code, tracker, std::move(callback), hash);

    std::shared_ptr<remote_copy_request> rci = std::make_shared<remote_copy_request>();
    rci->source = remote;
    rci->source_disk_tag = source_disk_tag;
    rci->source_dir = source_dir;
    rci->files = files;
    rci->dest_disk_tag = dest_disk_tag;
    rci->dest_dir = dest_dir;
    rci->pid = pid;
    rci->overwrite = overwrite;
    rci->high_priority = high_priority;
    call(rci, cb);

    return cb;
}

aio_task_ptr nfs_node::copy_remote_files(std::shared_ptr<remote_copy_request> &request,
                                         task_code callback_code,
                                         task_tracker *tracker,
                                         aio_handler &&callback,
                                         int hash)
{
    auto cb = dsn::file::create_aio_task(callback_code, tracker, std::move(callback), hash);
    call(request, cb);
    return cb;
}
} // namespace dsn
