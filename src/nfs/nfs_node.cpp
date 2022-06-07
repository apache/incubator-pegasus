/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <dsn/utility/smart_pointers.h>
#include <dsn/tool-api/async_calls.h>
#include <dsn/dist/nfs_node.h>

#include "nfs_node_simple.h"

namespace dsn {

std::unique_ptr<nfs_node> nfs_node::create()
{
    return dsn::make_unique<dsn::service::nfs_node_simple>();
}

aio_task_ptr nfs_node::copy_remote_directory(const rpc_address &remote,
                                             const std::string &source_disk_tag,
                                             const std::string &source_dir,
                                             const std::string &dest_disk_tag,
                                             const std::string &dest_dir,
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
                             overwrite,
                             high_priority,
                             callback_code,
                             tracker,
                             std::move(callback),
                             hash);
}

aio_task_ptr nfs_node::copy_remote_files(const rpc_address &remote,
                                         const std::string &source_disk_tag,
                                         const std::string &source_dir,
                                         const std::vector<std::string> &files,
                                         const std::string &dest_disk_tag,
                                         const std::string &dest_dir,
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
}
