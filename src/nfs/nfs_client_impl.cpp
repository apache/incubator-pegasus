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

#include "nfs_client_impl.h"

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <mutex>

#include "nfs/nfs_code_definition.h"
#include "nfs/nfs_node.h"
#include "perf_counter/perf_counter.h"
#include "utils/blob.h"
#include "utils/command_manager.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/string_conv.h"
#include "utils/token_buckets.h"

namespace dsn {
namespace service {
static uint32_t current_max_copy_rate_megabytes = 0;

DSN_DEFINE_uint32(nfs,
                  nfs_copy_block_bytes,
                  4 * 1024 * 1024,
                  "max block size (bytes) for each network copy");
DSN_DEFINE_uint32(
    nfs,
    max_copy_rate_megabytes_per_disk,
    0,
    "max rate per disk of copying from remote node(MB/s), zero means disable rate limiter");
DSN_TAG_VARIABLE(max_copy_rate_megabytes_per_disk, FT_MUTABLE);
// max_copy_rate_bytes should be zero or greater than nfs_copy_block_bytes which is the max
// batch copy size once
DSN_DEFINE_group_validator(max_copy_rate_megabytes_per_disk, [](std::string &message) -> bool {
    return FLAGS_max_copy_rate_megabytes_per_disk == 0 ||
           (FLAGS_max_copy_rate_megabytes_per_disk << 20) > FLAGS_nfs_copy_block_bytes;
});

DSN_DEFINE_int32(nfs,
                 max_concurrent_remote_copy_requests,
                 50,
                 "max concurrent remote copy to the same server on nfs client");
DSN_DEFINE_int32(nfs, max_concurrent_local_writes, 50, "max local file writes on nfs client");
DSN_DEFINE_int32(nfs, max_buffered_local_writes, 500, "max buffered file writes on nfs client");
DSN_DEFINE_int32(nfs,
                 high_priority_speed_rate,
                 2,
                 "the copy speed rate of high priority comparing with low priority on nfs client");
DSN_DEFINE_int32(nfs,
                 file_close_expire_time_ms,
                 60 * 1000,
                 "max idle time for an opening file on nfs server");
DSN_DEFINE_int32(nfs,
                 file_close_timer_interval_ms_on_server,
                 30 * 1000,
                 "time interval for checking whether cached file handles need to be closed");
DSN_DEFINE_int32(nfs,
                 max_file_copy_request_count_per_file,
                 2,
                 "maximum concurrent remote copy requests for the same file on nfs client"
                 "to limit each file copy speed");
DSN_DEFINE_int32(nfs, max_retry_count_per_copy_request, 2, "maximum retry count when copy failed");
DSN_DEFINE_int32(nfs,
                 rpc_timeout_ms,
                 1e5, // 100s
                 "rpc timeout in milliseconds for nfs copy, "
                 "0 means use default timeout of rpc engine");

nfs_client_impl::nfs_client_impl()
    : _concurrent_copy_request_count(0),
      _concurrent_local_write_count(0),
      _buffered_local_write_count(0),
      _copy_requests_low(FLAGS_max_file_copy_request_count_per_file),
      _high_priority_remaining_time(FLAGS_high_priority_speed_rate)
{
    _recent_copy_data_size.init_app_counter("eon.nfs_client",
                                            "recent_copy_data_size",
                                            COUNTER_TYPE_VOLATILE_NUMBER,
                                            "nfs client copy data size in the recent period");
    _recent_copy_fail_count.init_app_counter(
        "eon.nfs_client",
        "recent_copy_fail_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "nfs client copy fail count count in the recent period");
    _recent_write_data_size.init_app_counter("eon.nfs_client",
                                             "recent_write_data_size",
                                             COUNTER_TYPE_VOLATILE_NUMBER,
                                             "nfs client write data size in the recent period");
    _recent_write_fail_count.init_app_counter(
        "eon.nfs_client",
        "recent_write_fail_count",
        COUNTER_TYPE_VOLATILE_NUMBER,
        "nfs client write fail count count in the recent period");

    _copy_token_buckets = std::make_unique<utils::token_buckets>();

    register_cli_commands();
}

nfs_client_impl::~nfs_client_impl() { _tracker.cancel_outstanding_tasks(); }

void nfs_client_impl::begin_remote_copy(std::shared_ptr<remote_copy_request> &rci,
                                        aio_task *nfs_task)
{
    user_request_ptr req(new user_request());
    req->high_priority = rci->high_priority;
    req->file_size_req.source = rci->source;
    req->file_size_req.dst_dir = rci->dest_dir;
    req->file_size_req.file_list = rci->files;
    req->file_size_req.source_dir = rci->source_dir;
    req->file_size_req.overwrite = rci->overwrite;
    req->file_size_req.__set_source_disk_tag(rci->source_disk_tag);
    req->file_size_req.__set_dest_disk_tag(rci->dest_disk_tag);
    req->file_size_req.__set_pid(rci->pid);
    req->nfs_task = nfs_task;
    req->is_finished = false;

    async_nfs_get_file_size(req->file_size_req,
                            [=](error_code err, get_file_size_response &&resp) {
                                end_get_file_size(err, std::move(resp), req);
                            },
                            std::chrono::milliseconds(FLAGS_rpc_timeout_ms),
                            req->file_size_req.source);
}

void nfs_client_impl::end_get_file_size(::dsn::error_code err,
                                        const ::dsn::service::get_file_size_response &resp,
                                        const user_request_ptr &ureq)
{
    if (err != ::dsn::ERR_OK) {
        LOG_ERROR("[nfs_service] remote get file size failed, source = {}, dir = {}, err = {}",
                  ureq->file_size_req.source,
                  ureq->file_size_req.source_dir,
                  err);
        ureq->nfs_task->enqueue(err, 0);
        return;
    }

    err = dsn::error_code(resp.error);
    if (err != ::dsn::ERR_OK) {
        LOG_ERROR("[nfs_service] remote get file size failed, source = {}, dir = {}, err = {}",
                  ureq->file_size_req.source,
                  ureq->file_size_req.source_dir,
                  err);
        ureq->nfs_task->enqueue(err, 0);
        return;
    }

    std::deque<copy_request_ex_ptr> copy_requests;
    ureq->file_contexts.resize(resp.size_list.size());
    for (size_t i = 0; i < resp.size_list.size(); i++) // file list
    {
        file_context_ptr filec(new file_context(ureq, resp.file_list[i], resp.size_list[i]));
        ureq->file_contexts[i] = filec;

        // init copy requests
        uint64_t size = resp.size_list[i];
        uint64_t req_offset = 0;
        uint32_t req_size = size > FLAGS_nfs_copy_block_bytes ? FLAGS_nfs_copy_block_bytes
                                                              : static_cast<uint32_t>(size);

        filec->copy_requests.reserve(size / FLAGS_nfs_copy_block_bytes + 1);
        int idx = 0;
        for (;;) // send one file with multi-round rpc
        {
            copy_request_ex_ptr req(
                new copy_request_ex(filec, idx++, FLAGS_max_retry_count_per_copy_request));
            req->offset = req_offset;
            req->size = req_size;
            req->is_last = (size <= req_size);

            filec->copy_requests.push_back(req);
            copy_requests.push_back(req);

            req_offset += req_size;
            size -= req_size;
            if (size <= 0) {
                CHECK_EQ_MSG(
                    size, 0, "last request must read exactly the remaing size of the file");
                break;
            }

            req_size = size > FLAGS_nfs_copy_block_bytes ? FLAGS_nfs_copy_block_bytes
                                                         : static_cast<uint32_t>(size);
        }
    }

    if (!copy_requests.empty()) {
        zauto_lock l(_copy_requests_lock);
        if (ureq->high_priority)
            _copy_requests_high.insert(
                _copy_requests_high.end(), copy_requests.begin(), copy_requests.end());
        else
            _copy_requests_low.push(std::move(copy_requests));
    }

    tasking::enqueue(LPC_NFS_COPY_FILE, nullptr, [this]() { continue_copy(); }, 0);
}

void nfs_client_impl::continue_copy()
{
    if (_buffered_local_write_count >= FLAGS_max_buffered_local_writes) {
        // exceed max_buffered_local_writes limit, pause.
        // the copy task will be triggered by continue_copy() invoked in local_write_callback().
        return;
    }

    if (++_concurrent_copy_request_count > FLAGS_max_concurrent_remote_copy_requests) {
        // exceed max_concurrent_remote_copy_requests limit, pause.
        // the copy task will be triggered by continue_copy() invoked in end_copy().
        --_concurrent_copy_request_count;
        return;
    }

    copy_request_ex_ptr req = nullptr;
    while (true) {
        {
            zauto_lock l(_copy_requests_lock);

            if (_high_priority_remaining_time > 0 && !_copy_requests_high.empty()) {
                // pop from high queue
                req = _copy_requests_high.front();
                _copy_requests_high.pop_front();
                --_high_priority_remaining_time;
            } else {
                // try to pop from low queue
                req = _copy_requests_low.pop();
                if (req) {
                    _high_priority_remaining_time = FLAGS_high_priority_speed_rate;
                }
            }

            if (!req && !_copy_requests_high.empty()) {
                // pop from low queue failed, then pop from high priority,
                // but not change the _high_priority_remaining_time
                req = _copy_requests_high.front();
                _copy_requests_high.pop_front();
            }

            if (req) {
                ++req->file_ctx->user_req->concurrent_copy_count;
            } else {
                // no copy request
                --_concurrent_copy_request_count;
                break;
            }
        }

        {
            zauto_lock l(req->lock);
            const user_request_ptr &ureq = req->file_ctx->user_req;
            if (req->is_valid) {
                if (FLAGS_max_copy_rate_megabytes_per_disk > 0) {
                    _copy_token_buckets->get_token_bucket(ureq->file_size_req.dest_disk_tag)
                        ->consumeWithBorrowAndWait(
                            req->size,
                            FLAGS_max_copy_rate_megabytes_per_disk << 20,
                            1.5 * (FLAGS_max_copy_rate_megabytes_per_disk << 20));
                }

                copy_request copy_req;
                copy_req.source = ureq->file_size_req.source;
                copy_req.file_name = req->file_ctx->file_name;
                copy_req.offset = req->offset;
                copy_req.size = req->size;
                copy_req.dst_dir = ureq->file_size_req.dst_dir;
                copy_req.source_dir = ureq->file_size_req.source_dir;
                copy_req.overwrite = ureq->file_size_req.overwrite;
                copy_req.is_last = req->is_last;
                copy_req.__set_source_disk_tag(ureq->file_size_req.source_disk_tag);
                copy_req.__set_pid(ureq->file_size_req.pid);
                req->remote_copy_task =
                    async_nfs_copy(copy_req,
                                   [=](error_code err, copy_response &&resp) {
                                       end_copy(err, std::move(resp), req);
                                       // reset task to release memory quickly.
                                       // should do this after end_copy() done.
                                       if (req->is_ready_for_write) {
                                           ::dsn::task_ptr tsk;
                                           zauto_lock l(req->lock);
                                           tsk = std::move(req->remote_copy_task);
                                       }
                                   },
                                   std::chrono::milliseconds(FLAGS_rpc_timeout_ms),
                                   req->file_ctx->user_req->file_size_req.source);
            } else {
                --ureq->concurrent_copy_count;
                --_concurrent_copy_request_count;
            }
        }

        if (++_concurrent_copy_request_count > FLAGS_max_concurrent_remote_copy_requests) {
            // exceed max_concurrent_remote_copy_requests limit, pause.
            // the copy task will be triggered by continue_copy() invoked in end_copy().
            --_concurrent_copy_request_count;
            break;
        }
    }
}

void nfs_client_impl::end_copy(::dsn::error_code err,
                               const copy_response &resp,
                               const copy_request_ex_ptr &reqc)
{
    --_concurrent_copy_request_count;
    --reqc->file_ctx->user_req->concurrent_copy_count;

    const file_context_ptr &fc = reqc->file_ctx;

    if (err == ERR_OK) {
        err = resp.error;
    }

    if (err != ::dsn::ERR_OK) {
        _recent_copy_fail_count->increment();

        if (!fc->user_req->is_finished) {
            if (reqc->retry_count > 0) {
                LOG_WARNING("[nfs_service] remote copy failed, source = {}, dir = {}, file = {}, "
                            "err = {}, retry_count = {}",
                            fc->user_req->file_size_req.source,
                            fc->user_req->file_size_req.source_dir,
                            fc->file_name,
                            err,
                            reqc->retry_count);

                // retry copy
                reqc->retry_count--;

                // put back into copy request queue
                zauto_lock l(_copy_requests_lock);
                if (fc->user_req->high_priority)
                    _copy_requests_high.push_front(reqc);
                else
                    _copy_requests_low.push_retry(reqc);
            } else {
                LOG_ERROR("[nfs_service] remote copy failed, source = {}, dir = {}, file = {}, "
                          "err = {}, retry_count = {}",
                          fc->user_req->file_size_req.source,
                          fc->user_req->file_size_req.source_dir,
                          fc->file_name,
                          err,
                          reqc->retry_count);

                handle_completion(fc->user_req, err);
            }
        }
    }

    else {
        _recent_copy_data_size->add(resp.size);

        reqc->response = resp;
        reqc->is_ready_for_write = true;

        // prepare write requests
        std::deque<copy_request_ex_ptr> new_writes;
        {
            zauto_lock l(fc->user_req->user_req_lock);
            if (!fc->user_req->is_finished && fc->current_write_index == reqc->index - 1) {
                for (int i = reqc->index; i < (int)(fc->copy_requests.size()); i++) {
                    if (fc->copy_requests[i]->is_ready_for_write) {
                        fc->current_write_index++;
                        new_writes.push_back(fc->copy_requests[i]);
                    } else {
                        break;
                    }
                }
            }
        }

        // put write requests into queue
        if (!new_writes.empty()) {
            zauto_lock l(_local_writes_lock);
            _local_writes.insert(_local_writes.end(), new_writes.begin(), new_writes.end());
            _buffered_local_write_count += new_writes.size();
        }
    }

    continue_copy();
    continue_write();
}

void nfs_client_impl::continue_write()
{
    // check write quota
    if (++_concurrent_local_write_count > FLAGS_max_concurrent_local_writes) {
        // exceed max_concurrent_local_writes limit, pause.
        // the copy task will be triggered by continue_write() invoked in
        // local_write_callback().
        --_concurrent_local_write_count;
        return;
    }

    // get write data
    copy_request_ex_ptr reqc;
    while (true) {
        {
            zauto_lock l(_local_writes_lock);
            if (!_local_writes.empty()) {
                reqc = _local_writes.front();
                _local_writes.pop_front();
                --_buffered_local_write_count;
            } else {
                // no write data
                reqc = nullptr;
                break;
            }
        }

        {
            // only process valid request, and discard invalid request
            zauto_lock l(reqc->lock);
            if (reqc->is_valid) {
                break;
            }
        }
    }

    if (nullptr == reqc) {
        --_concurrent_local_write_count;
        return;
    }

    // real write
    const file_context_ptr &fc = reqc->file_ctx;
    std::string file_path =
        dsn::utils::filesystem::path_combine(fc->user_req->file_size_req.dst_dir, fc->file_name);
    std::string path = dsn::utils::filesystem::remove_file_name(file_path.c_str());
    CHECK(dsn::utils::filesystem::create_directory(path), "create directory {} failed", path);

    if (!fc->file_holder->file_handle) {
        // double check
        zauto_lock l(fc->user_req->user_req_lock);
        if (!fc->file_holder->file_handle) {
            fc->file_holder->file_handle = file::open(file_path, file::FileOpenType::kWriteOnly);
        }
    }

    if (!fc->file_holder->file_handle) {
        --_concurrent_local_write_count;
        LOG_ERROR("open file {} failed", file_path);
        handle_completion(fc->user_req, ERR_FILE_OPERATION_FAILED);
    } else {
        LOG_DEBUG("nfs: copy to file {} [{}, {}]",
                  file_path,
                  reqc->response.offset,
                  reqc->response.offset + reqc->response.size);
        zauto_lock l(reqc->lock);
        if (reqc->is_valid) {
            reqc->local_write_task = file::write(fc->file_holder->file_handle,
                                                 reqc->response.file_content.data(),
                                                 reqc->response.size,
                                                 reqc->response.offset,
                                                 LPC_NFS_WRITE,
                                                 &_tracker,
                                                 [=](error_code err, int sz) {
                                                     end_write(err, sz, reqc);
                                                     // reset task to release memory quickly.
                                                     // should do this after local_write_callback()
                                                     // done.
                                                     {
                                                         ::dsn::task_ptr tsk;
                                                         zauto_lock l(reqc->lock);
                                                         tsk = std::move(reqc->local_write_task);
                                                     }
                                                 });
        } else {
            --_concurrent_local_write_count;
        }
    }
}

void nfs_client_impl::end_write(error_code err, size_t sz, const copy_request_ex_ptr &reqc)
{
    --_concurrent_local_write_count;

    // clear content to release memory quickly
    reqc->response.file_content = blob();

    const file_context_ptr &fc = reqc->file_ctx;

    bool completed = false;
    if (err != ERR_OK) {
        _recent_write_fail_count->increment();

        LOG_ERROR("[nfs_service] local write failed, dir = {}, file = {}, err = {}",
                  fc->user_req->file_size_req.dst_dir,
                  fc->file_name,
                  err);
        completed = true;
    } else {
        _recent_write_data_size->add(sz);

        file_wrapper_ptr temp_holder;
        zauto_lock l(fc->user_req->user_req_lock);
        if (!fc->user_req->is_finished &&
            ++fc->finished_segments == (int)fc->copy_requests.size()) {
            // release file to make it closed immediately after write done.
            // we use temp_holder to make file closing out of lock.
            temp_holder = std::move(fc->file_holder);

            if (++fc->user_req->finished_files == (int)fc->user_req->file_contexts.size()) {
                completed = true;
            }
        }
    }

    if (completed) {
        handle_completion(fc->user_req, err);
    }

    continue_write();
    continue_copy();
}

void nfs_client_impl::handle_completion(const user_request_ptr &req, error_code err)
{
    // ATTENTION: only here we may lock for two level (user_req_lock -> copy_request_ex.lock)
    zauto_lock l(req->user_req_lock);

    // make sure this function can only be executed for once
    if (req->is_finished)
        return;
    req->is_finished = true;

    size_t total_size = 0;
    for (file_context_ptr &fc : req->file_contexts) {
        total_size += fc->file_size;
        if (err != ERR_OK) {
            // mark all copy_requests to be invalid
            for (const copy_request_ex_ptr &rc : fc->copy_requests) {
                zauto_lock l(rc->lock);
                rc->is_valid = false;
            }
        }
        // clear copy_requests to break circle reference
        fc->copy_requests.clear();
    }

    // clear file_contexts to break circle reference
    req->file_contexts.clear();

    // notify aio_task
    req->nfs_task->enqueue(err, err == ERR_OK ? total_size : 0);
}

// todo(jiashuo1) just for compatibility with scripts, such as
// https://github.com/apache/incubator-pegasus/blob/v2.3/scripts/pegasus_offline_node_list.sh
void nfs_client_impl::register_cli_commands()
{
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        _nfs_max_copy_rate_megabytes_cmd = dsn::command_manager::instance().register_command(
            {"nfs.max_copy_rate_megabytes_per_disk"},
            "nfs.max_copy_rate_megabytes_per_disk [num]",
            "control the max rate(MB/s) for one disk to copy file from remote node",
            [](const std::vector<std::string> &args) {
                std::string result("OK");

                if (args.empty()) {
                    return std::to_string(FLAGS_max_copy_rate_megabytes_per_disk);
                }

                int32_t max_copy_rate_megabytes = 0;
                if (!dsn::buf2int32(args[0], max_copy_rate_megabytes) ||
                    max_copy_rate_megabytes <= 0) {
                    return std::string("ERR: invalid arguments");
                }

                uint32_t max_copy_rate_bytes = max_copy_rate_megabytes << 20;
                if (max_copy_rate_bytes <= FLAGS_nfs_copy_block_bytes) {
                    result = std::string("ERR: max_copy_rate_bytes(max_copy_rate_megabytes << 20) "
                                         "should be greater than nfs_copy_block_bytes:")
                                 .append(std::to_string(FLAGS_nfs_copy_block_bytes));
                    return result;
                }
                FLAGS_max_copy_rate_megabytes_per_disk = max_copy_rate_megabytes;
                return result;
            });
    });
}
} // namespace service
} // namespace dsn
