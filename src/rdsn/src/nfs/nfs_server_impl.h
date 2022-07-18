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

#pragma once
#include <dsn/tool-api/task_tracker.h>
#include <dsn/perf_counter/perf_counter_wrapper.h>
#include <iostream>
#include <dsn/cpp/serverlet.h>
#include <dsn/utility/flags.h>
#include <dsn/tool-api/command_manager.h>
#include <dsn/utils/token_buckets.h>

#include "nfs_code_definition.h"
#include "nfs_types.h"
#include "nfs_client_impl.h"

namespace dsn {
namespace service {
class nfs_service_impl : public ::dsn::serverlet<nfs_service_impl>
{
public:
    nfs_service_impl();
    virtual ~nfs_service_impl() { _tracker.cancel_outstanding_tasks(); }

    void open_service()
    {
        register_async_rpc_handler(RPC_NFS_COPY, "copy", &nfs_service_impl::on_copy);
        register_async_rpc_handler(
            RPC_NFS_GET_FILE_SIZE, "get_file_size", &nfs_service_impl::on_get_file_size);
    }

    void register_cli_commands();

    void close_service()
    {
        unregister_rpc_handler(RPC_NFS_COPY);
        unregister_rpc_handler(RPC_NFS_GET_FILE_SIZE);
        UNREGISTER_VALID_HANDLER(_nfs_max_send_rate_megabytes_cmd);
    }

protected:
    // RPC_NFS_V2_NFS_COPY
    virtual void on_copy(const copy_request &request, ::dsn::rpc_replier<copy_response> &reply);
    // RPC_NFS_V2_NFS_GET_FILE_SIZE
    virtual void on_get_file_size(const get_file_size_request &request,
                                  ::dsn::rpc_replier<get_file_size_response> &reply);

private:
    struct callback_para
    {
        dsn_handle_t hfile;
        std::string source_disk_tag;
        std::string file_path;
        std::string dst_dir;
        blob bb;
        uint64_t offset;
        uint32_t size;
        rpc_replier<copy_response> replier;

        callback_para(rpc_replier<copy_response> &&r)
            : hfile(nullptr), offset(0), size(0), replier(std::move(r))
        {
        }
        callback_para(callback_para &&r)
            : hfile(r.hfile),
              file_path(std::move(r.file_path)),
              dst_dir(std::move(r.dst_dir)),
              bb(std::move(r.bb)),
              offset(r.offset),
              size(r.size),
              replier(std::move(r.replier))
        {
            r.hfile = nullptr;
            r.offset = 0;
            r.size = 0;
        }
    };

    struct file_handle_info_on_server
    {
        disk_file *file_handle;
        int32_t file_access_count; // concurrent r/w count
        uint64_t last_access_time; // last touch time

        file_handle_info_on_server()
            : file_handle(nullptr), file_access_count(0), last_access_time(0)
        {
        }

        ~file_handle_info_on_server()
        {
            error_code err = file::close(file_handle);
            dassert(err == ERR_OK, "file::close failed, err = %s", err.to_string());
        }
    };

    void internal_read_callback(error_code err, size_t sz, callback_para &cp);

    void close_file();

private:
    zlock _handles_map_lock;
    std::unordered_map<std::string, std::shared_ptr<file_handle_info_on_server>>
        _handles_map; // cache file handles

    ::dsn::task_ptr _file_close_timer;

    std::unique_ptr<dsn::utils::token_buckets>
        _send_token_buckets; // rate limiter of send to remote

    perf_counter_wrapper _recent_copy_data_size;
    perf_counter_wrapper _recent_copy_fail_count;

    dsn_handle_t _nfs_max_send_rate_megabytes_cmd;

    dsn::task_tracker _tracker;
};
} // namespace service
} // namespace dsn
