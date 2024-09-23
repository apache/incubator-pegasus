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

#include <stddef.h>
#include <stdint.h>
#include <atomic>
#include <chrono>
#include <deque>
#include <list>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "aio/aio_task.h"
#include "aio/file_io.h"
#include "nfs_code_definition.h"
#include "nfs_types.h"
#include "rpc/rpc_address.h"
#include "task/async_calls.h"
#include "task/task.h"
#include "task/task_tracker.h"
#include "utils/TokenBucket.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "utils/metrics.h"
#include "utils/zlocks.h"

namespace dsn {
class command_deregister;
class disk_file;

namespace utils {
class token_buckets;
} // namespace utils
struct remote_copy_request;

namespace service {

using TokenBucket = folly::BasicTokenBucket<std::chrono::steady_clock>;

template <typename TCallback>
task_ptr async_nfs_get_file_size(const get_file_size_request &request,
                                 TCallback &&callback,
                                 std::chrono::milliseconds timeout,
                                 rpc_address server_addr)
{
    return rpc::call(server_addr,
                     RPC_NFS_GET_FILE_SIZE,
                     request,
                     nullptr,
                     std::forward<TCallback>(callback),
                     timeout);
}

template <typename TCallback>
task_ptr async_nfs_copy(const copy_request &request,
                        TCallback &&callback,
                        std::chrono::milliseconds timeout,
                        rpc_address server_addr)
{
    return rpc::call(
        server_addr, RPC_NFS_COPY, request, nullptr, std::forward<TCallback>(callback), timeout);
}

class nfs_client_impl
{
public:
    struct copy_request_ex;
    struct file_context;
    struct file_wrapper;
    struct user_request;

    typedef ::dsn::ref_ptr<user_request> user_request_ptr;
    typedef ::dsn::ref_ptr<file_context> file_context_ptr;
    typedef ::dsn::ref_ptr<copy_request_ex> copy_request_ex_ptr;
    typedef ::dsn::ref_ptr<file_wrapper> file_wrapper_ptr;

    struct file_wrapper : public ::dsn::ref_counter
    {
        disk_file *file_handle;

        file_wrapper() { file_handle = nullptr; }
        ~file_wrapper()
        {
            if (file_handle != nullptr) {
                auto err = file::close(file_handle);
                CHECK_EQ_MSG(err, ERR_OK, "file::close failed");
            }
        }
    };

    struct copy_request_ex : public ::dsn::ref_counter
    {
        file_context_ptr file_ctx; // reference to the owner
        int index;
        uint64_t offset;
        uint32_t size;
        bool is_last;
        copy_response response;
        ::dsn::task_ptr remote_copy_task;
        ::dsn::task_ptr local_write_task;
        bool is_ready_for_write;
        bool is_valid;
        int retry_count;
        zlock lock; // to protect is_valid

        copy_request_ex(const file_context_ptr &file, int idx, int try_count)
        {
            file_ctx = file;
            index = idx;
            offset = 0;
            size = 0;
            is_last = false;
            is_ready_for_write = false;
            is_valid = true;
            retry_count = try_count;
        }
    };

    struct file_context : public ::dsn::ref_counter
    {
        user_request_ptr user_req; // reference to the owner

        std::string file_name;
        uint64_t file_size;

        file_wrapper_ptr file_holder;
        int current_write_index;
        int finished_segments;
        std::vector<copy_request_ex_ptr> copy_requests;

        file_context(const user_request_ptr &req, const std::string &file_nm, uint64_t sz)
        {
            user_req = req;
            file_name = file_nm;
            file_size = sz;
            file_holder = new file_wrapper();
            current_write_index = -1;
            finished_segments = 0;
        }
    };

    struct user_request : public ::dsn::ref_counter
    {
        zlock user_req_lock;

        bool high_priority;
        int low_queue_index;
        get_file_size_request file_size_req;
        ::dsn::ref_ptr<aio_task> nfs_task;
        std::atomic<int> finished_files;
        std::atomic<int> concurrent_copy_count;
        bool is_finished;

        std::vector<file_context_ptr> file_contexts;

        user_request()
        {
            high_priority = false;
            low_queue_index = -1;
            finished_files = 0;
            concurrent_copy_count = 0;
            is_finished = false;
        }
    };

    struct random_robin_queue
    {
        int max_concurrent_copy_count_per_queue;
        size_t total_count;
        // each queue represents all requests for one user_request
        std::list<std::deque<copy_request_ex_ptr>> queue_list;
        // the next queue to pop request
        std::list<std::deque<copy_request_ex_ptr>>::iterator pop_it;

        random_robin_queue(int max_concurrent_copy_count_per_queue_)
        {
            max_concurrent_copy_count_per_queue = max_concurrent_copy_count_per_queue_;
            total_count = 0;
            pop_it = queue_list.end();
        }

        // push request queue as an unique sub-queue.
        void push(std::deque<copy_request_ex_ptr> &&q)
        {
            total_count += q.size();
            queue_list.emplace_back(std::move(q));
        }

        // push retry request to this queue.
        // if the original sub-queue is exist, push to front of it,
        // else push to a new sub-queue.
        void push_retry(const copy_request_ex_ptr &p)
        {
            total_count++;
            for (auto it = queue_list.begin(); it != queue_list.end(); ++it) {
                if (it->front()->file_ctx->user_req.get() == p->file_ctx->user_req.get()) {
                    // belong the the same user_request
                    it->push_front(p);
                    return;
                }
            }
            queue_list.emplace_back(std::deque<copy_request_ex_ptr>({p}));
        }

        // pop one request from this queue.
        // return nullptr if no valid request found.
        copy_request_ex_ptr pop()
        {
            copy_request_ex_ptr p;
            if (total_count == 0)
                return p;
            if (pop_it == queue_list.end())
                pop_it = queue_list.begin();
            auto start_it = pop_it;
            while (true) {
                if (pop_it->front()->file_ctx->user_req->concurrent_copy_count <
                    max_concurrent_copy_count_per_queue) {
                    // ok, find one, pop from queue, and forward pop_it
                    p = pop_it->front();
                    pop_it->pop_front();
                    if (pop_it->empty()) {
                        pop_it = queue_list.erase(pop_it);
                    } else {
                        pop_it++;
                    }
                    total_count--;
                    break;
                }
                // forward pop_it
                pop_it++;
                if (pop_it == queue_list.end())
                    pop_it = queue_list.begin();
                // iterate for a round
                if (pop_it == start_it)
                    break;
            }
            return p;
        }

        bool empty() { return total_count == 0; }
    };

public:
    nfs_client_impl();
    virtual ~nfs_client_impl();

    // copy file request entry
    void begin_remote_copy(std::shared_ptr<remote_copy_request> &rci, aio_task *nfs_task);

private:
    void end_get_file_size(::dsn::error_code err,
                           const ::dsn::service::get_file_size_response &resp,
                           const user_request_ptr &ureq);

    void continue_copy();

    void
    end_copy(::dsn::error_code err, const copy_response &resp, const copy_request_ex_ptr &reqc);

    void continue_write();

    void end_write(error_code err, size_t sz, const copy_request_ex_ptr &reqc);

    void handle_completion(const user_request_ptr &req, error_code err);

    void register_cli_commands();

private:
    std::unique_ptr<dsn::utils::token_buckets>
        _copy_token_buckets; // rate limiter of copy from remote

    std::atomic<int> _concurrent_copy_request_count; // record concurrent request count, limited
                                                     // by max_concurrent_remote_copy_requests.
    std::atomic<int> _concurrent_local_write_count;  // record concurrent write count, limited
                                                     // by max_concurrent_local_writes.
    std::atomic<int> _buffered_local_write_count;    // record current buffered write count, limited
                                                     // by max_buffered_local_writes.

    zlock _copy_requests_lock;
    std::deque<copy_request_ex_ptr> _copy_requests_high;
    random_robin_queue _copy_requests_low;
    int _high_priority_remaining_time;

    zlock _local_writes_lock;
    std::deque<copy_request_ex_ptr> _local_writes;

    METRIC_VAR_DECLARE_counter(nfs_client_copy_bytes);
    METRIC_VAR_DECLARE_counter(nfs_client_copy_failed_requests);
    METRIC_VAR_DECLARE_counter(nfs_client_write_bytes);
    METRIC_VAR_DECLARE_counter(nfs_client_failed_writes);

    std::unique_ptr<command_deregister> _nfs_max_copy_rate_megabytes_cmd;

    dsn::task_tracker _tracker;
};
} // namespace service
} // namespace dsn
