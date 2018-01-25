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
#include "nfs_client_impl.h"
#include <dsn/tool-api/nfs.h>
#include <dsn/utility/filesystem.h>
#include <queue>

namespace dsn {
namespace service {

nfs_client_impl::nfs_client_impl(nfs_opts &opts) : _opts(opts)
{
    _concurrent_copy_request_count = 0;
    _concurrent_local_write_count = 0;
    _buffered_local_write_count = 0;
    _high_priority_remaining_time = _opts.high_priority_speed_rate;

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
}

void nfs_client_impl::begin_remote_copy(std::shared_ptr<remote_copy_request> &rci,
                                        aio_task *nfs_task)
{
    user_request *req = new user_request();
    req->high_priority = rci->high_priority;
    req->file_size_req.source = rci->source;
    req->file_size_req.dst_dir = rci->dest_dir;
    req->file_size_req.file_list = rci->files;
    req->file_size_req.source_dir = rci->source_dir;
    req->file_size_req.overwrite = rci->overwrite;
    req->nfs_task = nfs_task;
    req->is_finished = false;

    get_file_size(req->file_size_req,
                  [=](error_code err, get_file_size_response &&resp) {
                      end_get_file_size(err, std::move(resp), req);
                  },
                  std::chrono::milliseconds(0),
                  0,
                  0,
                  0,
                  req->file_size_req.source);
}

void nfs_client_impl::end_get_file_size(::dsn::error_code err,
                                        const ::dsn::service::get_file_size_response &resp,
                                        void *context)
{
    user_request *ureq = (user_request *)context;

    if (err != ::dsn::ERR_OK) {
        derror("{nfs_service} remote get file size failed, source = %s, dir = %s, err = %s",
               ureq->file_size_req.source.to_string(),
               ureq->file_size_req.source_dir.c_str(),
               err.to_string());
        ureq->nfs_task->enqueue(err, 0);
        delete ureq;
        return;
    }

    err = resp.error;
    if (err != ::dsn::ERR_OK) {
        derror("{nfs_service} remote get file size failed, source = %s, dir = %s, err = %s",
               ureq->file_size_req.source.to_string(),
               ureq->file_size_req.source_dir.c_str(),
               err.to_string());
        ureq->nfs_task->enqueue(err, 0);
        delete ureq;
        return;
    }

    ureq->file_context_vec.resize(resp.size_list.size());
    for (size_t i = 0; i < resp.size_list.size(); i++) // file list
    {
        file_context *filec;
        uint64_t size = resp.size_list[i];

        filec = new file_context(ureq, resp.file_list[i], resp.size_list[i]);
        ureq->file_context_vec[i] = filec;

        // dinfo("this file size is %d, name is %s", size, resp.file_list[i].c_str());

        // new all the copy requests

        uint64_t req_offset = 0;
        uint32_t req_size;
        if (size > _opts.nfs_copy_block_bytes)
            req_size = _opts.nfs_copy_block_bytes;
        else
            req_size = static_cast<uint32_t>(size);

        filec->copy_requests.reserve(size / _opts.nfs_copy_block_bytes + 1);
        int idx = 0;
        for (;;) // send one file with multi-round rpc
        {
            auto req = dsn::ref_ptr<copy_request_ex>(new copy_request_ex(filec, idx++));
            filec->copy_requests.push_back(req);

            {
                zauto_lock l(_copy_requests_lock);
                if (ureq->high_priority)
                    _copy_requests_high.push(req);
                else
                    _copy_requests_low.push(req);
            }

            req->offset = req_offset;
            req->size = req_size;
            req->is_last = (size <= req_size);

            req_offset += req_size;
            size -= req_size;
            if (size <= 0) {
                dassert(size == 0, "last request must read exactly the remaing size of the file");
                break;
            }

            if (size > _opts.nfs_copy_block_bytes)
                req_size = _opts.nfs_copy_block_bytes;
            else
                req_size = static_cast<uint32_t>(size);
        }
    }

    continue_copy(0);
}

void nfs_client_impl::continue_copy(int done_count)
{
    if (done_count > 0) {
        _concurrent_copy_request_count -= done_count;
    }

    if (_buffered_local_write_count >= _opts.max_buffered_local_writes) {
        // exceed max_buffered_local_writes limit, pause.
        // the copy task will be triggered by continue_copy() invoked in local_write_callback().
        return;
    }

    if (++_concurrent_copy_request_count > _opts.max_concurrent_remote_copy_requests) {
        // exceed max_concurrent_remote_copy_requests limit, pause.
        // the copy task will be triggered by continue_copy() invoked in end_copy().
        --_concurrent_copy_request_count;
        return;
    }

    dsn::ref_ptr<copy_request_ex> req = nullptr;
    while (true) {
        {
            zauto_lock l(_copy_requests_lock);
            if (!_copy_requests_high.empty() &&
                (_high_priority_remaining_time > 0 || _copy_requests_low.empty())) {
                // pop from high priority
                req = _copy_requests_high.front();
                _copy_requests_high.pop();
                if (_high_priority_remaining_time > 0)
                    --_high_priority_remaining_time;
            } else if (!_copy_requests_low.empty()) {
                // pop from low priority
                req = _copy_requests_low.front();
                _copy_requests_low.pop();
                _high_priority_remaining_time = _opts.high_priority_speed_rate;
            } else {
                --_concurrent_copy_request_count;
                break;
            }
        }

        {
            zauto_lock l(req->lock);
            if (req->is_valid) {
                req->add_ref();
                user_request *ureq = req->file_ctx->user_req;
                copy_request copy_req;
                copy_req.source = ureq->file_size_req.source;
                copy_req.file_name = req->file_ctx->file_name;
                copy_req.offset = req->offset;
                copy_req.size = req->size;
                copy_req.dst_dir = ureq->file_size_req.dst_dir;
                copy_req.source_dir = ureq->file_size_req.source_dir;
                copy_req.overwrite = ureq->file_size_req.overwrite;
                copy_req.is_last = req->is_last;
                req->remote_copy_task = copy(copy_req,
                                             [=](error_code err, copy_response &&resp) {
                                                 end_copy(err, std::move(resp), req.get());
                                                 // reset task to release memory quickly.
                                                 // should do this after end_copy() done.
                                                 ::dsn::task_ptr tsk;
                                                 {
                                                     zauto_lock l(req->lock);
                                                     tsk = std::move(req->remote_copy_task);
                                                 }
                                             },
                                             std::chrono::milliseconds(0),
                                             0,
                                             0,
                                             0,
                                             req->file_ctx->user_req->file_size_req.source);

                if (++_concurrent_copy_request_count > _opts.max_concurrent_remote_copy_requests) {
                    --_concurrent_copy_request_count;
                    break;
                }
            }
        }
    }
}

void nfs_client_impl::end_copy(::dsn::error_code err, const copy_response &resp, void *context)
{
    // dinfo("*** call RPC_NFS_COPY end, return (%d, %d) with %s", resp.offset, resp.size,
    // err.to_string());

    dsn::ref_ptr<copy_request_ex> reqc;
    reqc = (copy_request_ex *)context;
    reqc->release_ref();

    continue_copy(1);

    if (err == ERR_OK) {
        err = resp.error;
    }

    if (err != ::dsn::ERR_OK) {
        derror("{nfs_service} remote copy failed, source = %s, dir = %s, file = %s, err = %s",
               reqc->file_ctx->user_req->file_size_req.source.to_string(),
               reqc->file_ctx->user_req->file_size_req.source_dir.c_str(),
               reqc->file_ctx->file_name.c_str(),
               err.to_string());
        _recent_copy_fail_count->increment();
        handle_completion(reqc->file_ctx->user_req, err);
        return;
    } else {
        _recent_copy_data_size->add(resp.size);
    }

    reqc->response = resp;
    reqc->response.error.end_tracking(); // always ERR_OK
    reqc->is_ready_for_write = true;

    auto &fc = reqc->file_ctx;

    // check write availability
    {
        zauto_lock l(fc->user_req->user_req_lock);
        if (fc->current_write_index != reqc->index - 1)
            return;
    }

    // check readies for local writes
    {
        zauto_lock l(fc->user_req->user_req_lock);
        for (int i = reqc->index; i < (int)(fc->copy_requests.size()); i++) {
            if (fc->copy_requests[i]->is_ready_for_write) {
                fc->current_write_index++;

                {
                    zauto_lock l(_local_writes_lock);
                    _local_writes.push(fc->copy_requests[i]);
                    ++_buffered_local_write_count;
                }
            } else
                break;
        }
    }

    continue_write();
}

void nfs_client_impl::continue_write()
{
    // check write quota
    if (++_concurrent_local_write_count > _opts.max_concurrent_local_writes) {
        --_concurrent_local_write_count;
        return;
    }

    // get write
    dsn::ref_ptr<copy_request_ex> reqc;
    while (true) {
        {
            zauto_lock l(_local_writes_lock);
            if (!_local_writes.empty()) {
                reqc = _local_writes.front();
                _local_writes.pop();
                --_buffered_local_write_count;
            } else {
                reqc = nullptr;
                break;
            }
        }

        {
            zauto_lock l(reqc->lock);
            if (reqc->is_valid)
                break;
        }
    }

    if (nullptr == reqc) {
        --_concurrent_local_write_count;
        return;
    }

    // real write
    std::string file_path = dsn::utils::filesystem::path_combine(
        reqc->file_ctx->user_req->file_size_req.dst_dir, reqc->file_ctx->file_name);
    std::string path = dsn::utils::filesystem::remove_file_name(file_path.c_str());
    if (!dsn::utils::filesystem::create_directory(path)) {
        dassert(false, "Fail to create directory %s.", path.c_str());
    }

    dsn_handle_t hfile = reqc->file_ctx->file.load();
    if (!hfile) {
        zauto_lock l(reqc->file_ctx->user_req->user_req_lock);
        hfile = reqc->file_ctx->file.load();
        if (!hfile) {
            hfile = dsn_file_open(file_path.c_str(), O_RDWR | O_CREAT | O_BINARY, 0666);
            reqc->file_ctx->file = hfile;
        }
    }

    if (!hfile) {
        derror("file open %s failed", file_path.c_str());
        error_code err = ERR_FILE_OPERATION_FAILED;
        handle_completion(reqc->file_ctx->user_req, err);
        --_concurrent_local_write_count;
        continue_write();
        return;
    }

    {
        zauto_lock l(reqc->lock);
        if (reqc->is_valid) {
            reqc->local_write_task = file::write(hfile,
                                                 reqc->response.file_content.data(),
                                                 reqc->response.size,
                                                 reqc->response.offset,
                                                 LPC_NFS_WRITE,
                                                 this,
                                                 [=](error_code err, int sz) {
                                                     local_write_callback(err, sz, reqc);
                                                     // reset task to release memory quickly.
                                                     // should do this after local_write_callback()
                                                     // done.
                                                     ::dsn::task_ptr tsk;
                                                     {
                                                         zauto_lock l(reqc->lock);
                                                         tsk = std::move(reqc->local_write_task);
                                                     }
                                                 });
        }
    }
}

void nfs_client_impl::local_write_callback(error_code err,
                                           size_t sz,
                                           const dsn::ref_ptr<copy_request_ex> &reqc)
{
    // dassert(reqc->local_write_task == task::get_current_task(), "");
    --_concurrent_local_write_count;

    // clear all content and reset task to release memory quickly
    reqc->response.file_content = blob();

    continue_write();
    continue_copy(0);

    bool completed = false;
    dsn_handle_t file_to_close = nullptr;
    if (err != ERR_OK) {
        derror("{nfs_service} local write failed, dir = %s, file = %s, err = %s",
               reqc->file_ctx->user_req->file_size_req.dst_dir.c_str(),
               reqc->file_ctx->file_name.c_str(),
               err.to_string());
        _recent_write_fail_count->increment();
        completed = true;
    } else {
        _recent_write_data_size->add(sz);
        zauto_lock l(reqc->file_ctx->user_req->user_req_lock);
        if (++reqc->file_ctx->finished_segments == (int)reqc->file_ctx->copy_requests.size()) {
            // close file immediately after write done to release resouces quickly
            file_to_close = reqc->file_ctx->file.exchange(nullptr);
            if (++reqc->file_ctx->user_req->finished_files ==
                (int)reqc->file_ctx->user_req->file_context_vec.size()) {
                completed = true;
            }
        }
    }

    if (file_to_close) {
        auto err = dsn_file_close(file_to_close);
        dassert(err == ERR_OK, "dsn_file_close failed, err = %s", dsn_error_to_string(err));
    }

    if (completed) {
        handle_completion(reqc->file_ctx->user_req, err);
    }
}

void nfs_client_impl::handle_completion(user_request *req, error_code err)
{
    {
        zauto_lock l(req->user_req_lock);
        if (req->is_finished)
            return;

        req->is_finished = true;
    }

    size_t total_size = 0;
    for (auto &fc : req->file_context_vec) {
        total_size += fc->file_size;

        for (auto &rc : fc->copy_requests) {
            ::dsn::task_ptr ctask, wtask;
            {
                zauto_lock l(rc->lock);
                rc->is_valid = false;
                ctask = std::move(rc->remote_copy_task);
                wtask = std::move(rc->local_write_task);
            }

            if (err != ERR_OK) {
                if (ctask != nullptr) {
                    if (ctask->cancel(true)) {
                        _concurrent_copy_request_count--;
                        rc->release_ref();
                    }
                }

                if (wtask != nullptr) {
                    if (wtask->cancel(true)) {
                        _concurrent_local_write_count--;
                    }
                }
            }
        }

        dsn_handle_t file = fc->file.exchange(nullptr);
        if (file) {
            auto err2 = dsn_file_close(file);
            dassert(err2 == ERR_OK, "dsn_file_close failed, err = %s", dsn_error_to_string(err2));

            if (fc->finished_segments != (int)fc->copy_requests.size()) {
                ::remove((fc->user_req->file_size_req.dst_dir + fc->file_name).c_str());
            }
        }

        fc->copy_requests.clear();

        delete fc;
    }

    req->file_context_vec.clear();
    req->nfs_task->enqueue(err, err == ERR_OK ? total_size : 0);

    delete req;

    // clear out all canceled requests
    if (err != ERR_OK) {
        continue_copy(0);
        continue_write();
    }
}
}
}
