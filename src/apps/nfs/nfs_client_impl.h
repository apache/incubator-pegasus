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
#pragma once
#include "nfs_client.h"
#include <queue>
#include <dsn/tool-api/nfs.h>
#include <dsn/cpp/perf_counter_wrapper.h>

namespace dsn {
namespace service {

struct nfs_opts
{
    uint32_t nfs_copy_block_bytes;
    int max_concurrent_remote_copy_requests;
    int max_concurrent_local_writes;
    int max_buffered_local_writes;
    int high_priority_speed_rate;

    int file_close_expire_time_ms;
    int file_close_timer_interval_ms_on_server;
    int max_file_copy_request_count_per_file;

    void init()
    {
        nfs_copy_block_bytes =
            (uint32_t)dsn_config_get_value_uint64("nfs",
                                                  "nfs_copy_block_bytes",
                                                  4 * 1024 * 1024,
                                                  "max block size (bytes) for each network copy");
        max_concurrent_remote_copy_requests = (int)dsn_config_get_value_uint64(
            "nfs",
            "max_concurrent_remote_copy_requests",
            50,
            "max concurrent remote copy to the same server on nfs client");
        max_concurrent_local_writes = (int)dsn_config_get_value_uint64(
            "nfs", "max_concurrent_local_writes", 5, "max local file writes on nfs client");
        max_buffered_local_writes = (int)dsn_config_get_value_uint64(
            "nfs", "max_buffered_local_writes", 500, "max buffered file writes on nfs client");
        high_priority_speed_rate = (int)dsn_config_get_value_uint64(
            "nfs",
            "high_priority_speed_rate",
            2,
            "the copy speed rate of high priority comparing with low priority on nfs client");
        file_close_expire_time_ms =
            (int)dsn_config_get_value_uint64("nfs",
                                             "file_close_expire_time_ms",
                                             60 * 1000,
                                             "max idle time for an opening file on nfs server");
        file_close_timer_interval_ms_on_server = (int)dsn_config_get_value_uint64(
            "nfs",
            "file_close_timer_interval_ms_on_server",
            30 * 1000,
            "time interval for checking whether cached file handles need to be closed");
        max_file_copy_request_count_per_file = (int)dsn_config_get_value_uint64(
            "nfs",
            "max_file_copy_request_count_per_file",
            10,
            "maximum concurrent remote copy requests for the same file on nfs client"
            "to limit each file copy speed");
    }
};

class nfs_client_impl : public ::dsn::service::nfs_client
{
public:
    struct user_request;
    struct file_context;
    struct copy_request_ex : public ::dsn::ref_counter
    {
        file_context *file_ctx;
        int index;
        uint64_t offset;
        uint32_t size;
        bool is_last;
        copy_response response;
        ::dsn::task_ptr remote_copy_task;
        ::dsn::task_ptr local_write_task;
        bool is_ready_for_write;
        bool is_valid;
        zlock lock;

        copy_request_ex(file_context *file, int idx)
        {
            file_ctx = file;
            index = idx;
            offset = 0;
            size = 0;
            is_last = false;
            remote_copy_task = nullptr;
            local_write_task = nullptr;
            is_ready_for_write = false;
            is_valid = true;
        }
    };

    struct file_context
    {
        user_request *user_req;

        std::string file_name;
        uint64_t file_size;

        std::atomic<dsn_handle_t> file;
        int current_write_index;
        int finished_segments;
        std::vector<::dsn::ref_ptr<copy_request_ex>> copy_requests;

        file_context(user_request *req, const std::string &file_nm, uint64_t sz)
        {
            user_req = req;
            file_name = file_nm;
            file_size = sz;
            file = nullptr;

            current_write_index = -1;
            finished_segments = 0;
        }
    };

    struct user_request
    {
        zlock user_req_lock;

        bool high_priority;
        get_file_size_request file_size_req;
        ::dsn::ref_ptr<aio_task> nfs_task;
        std::atomic<int> finished_files;
        bool is_finished;

        std::vector<file_context *> file_context_vec;

        user_request()
        {
            high_priority = false;
            finished_files = 0;
            is_finished = false;
        }
    };

public:
    nfs_client_impl(nfs_opts &opts);
    virtual ~nfs_client_impl() {}

    // copy file request entry
    void begin_remote_copy(std::shared_ptr<remote_copy_request> &rci, aio_task *nfs_task);

    // write file callback
    void
    local_write_callback(error_code err, size_t sz, const ::dsn::ref_ptr<copy_request_ex> &reqc);

private:
    // rewrite end_copy function
    void end_copy(::dsn::error_code err, const copy_response &resp, void *context);

    // rewrite end_get_file_size function
    void end_get_file_size(::dsn::error_code err,
                           const ::dsn::service::get_file_size_response &resp,
                           void *context);

    void continue_copy(int done_count);

    void write_copy(::dsn::ref_ptr<copy_request_ex> reqc);

    void continue_write();

    void handle_completion(user_request *req, error_code err);

private:
    nfs_opts &_opts;

    std::atomic<int> _concurrent_copy_request_count; // record concurrent request count, limited
                                                     // by max_concurrent_remote_copy_requests.
    std::atomic<int> _concurrent_local_write_count;  // record concurrent write count, limited
                                                     // by max_concurrent_local_writes.
    std::atomic<int> _buffered_local_write_count;    // record current buffered write count, limited
                                                     // by max_buffered_local_writes.

    zlock _copy_requests_lock;
    std::queue<::dsn::ref_ptr<copy_request_ex>> _copy_requests_high;
    std::queue<::dsn::ref_ptr<copy_request_ex>> _copy_requests_low;
    int _high_priority_remaining_time;

    zlock _local_writes_lock;
    std::queue<::dsn::ref_ptr<copy_request_ex>> _local_writes;

    perf_counter_wrapper _recent_copy_data_size;
    perf_counter_wrapper _recent_copy_fail_count;
    perf_counter_wrapper _recent_write_data_size;
    perf_counter_wrapper _recent_write_fail_count;
};
}
}
