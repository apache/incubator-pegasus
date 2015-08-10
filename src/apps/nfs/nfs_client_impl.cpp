# include "nfs_client_impl.h"
# include <dsn/internal/nfs.h>
# include <queue>

namespace dsn {
    namespace service {


        void nfs_client_impl::begin_remote_copy(std::shared_ptr<remote_copy_request>& rci, aio_task* nfs_task)
        {
            user_request* req = new user_request();
            req->file_size_req.source = rci->source;
            req->file_size_req.dst_dir = rci->dest_dir;
            req->file_size_req.file_list = rci->files;
            req->file_size_req.source_dir = rci->source_dir;
            req->file_size_req.overwrite = rci->overwrite;
            req->nfs_task = nfs_task;
            req->is_finished = false;

            begin_get_file_size(req->file_size_req, req, 0, 0, 0, &req->file_size_req.source); // async copy file
        }

        void nfs_client_impl::end_get_file_size(
            ::dsn::error_code err,
            const ::dsn::service::get_file_size_response& resp,
            void* context)
        {
            user_request* ureq = (user_request*)context;

            if (err != ::dsn::ERR_OK)
            {
                derror("remote copy request failed");
                ureq->nfs_task->enqueue(err, 0, ureq->nfs_task->node());
                delete ureq;
                return;
            }

            err = resp.error;
            if (err != ::dsn::ERR_OK)
            {
                derror("remote copy request failed");
                ureq->nfs_task->enqueue(err, 0, ureq->nfs_task->node());
                delete ureq;
                return;
            }

            for (size_t i = 0; i < resp.size_list.size(); i++) // file list
            {
                file_context *filec;
                uint64_t size = resp.size_list[i];

                filec = new file_context(ureq, resp.file_list[i], resp.size_list[i]);
                ureq->file_context_map.insert(std::pair<std::string, file_context*>(
                    ureq->file_size_req.dst_dir + resp.file_list[i], filec));

                //dinfo("this file size is %d, name is %s", size, resp.file_list[i].c_str());

                // new all the copy requests                

                uint64_t req_offset = 0;
                uint32_t req_size;
                if (size > _opts.nfs_copy_block_bytes)
                    req_size = _opts.nfs_copy_block_bytes;
                else
                    req_size = static_cast<uint32_t>(size);

                int idx = 0;
                for (;;) // send one file with multi-round rpc
                {
                    auto req = dsn::ref_ptr<copy_request_ex>(new copy_request_ex(filec, idx++));
                    filec->copy_requests.push_back(req);

                    {
                        zauto_lock l(_copy_requests_lock);
                        _copy_requests.push(req);
                    }

                    req->copy_req.source = ureq->file_size_req.source;
                    req->copy_req.file_name = resp.file_list[i];
                    req->copy_req.offset = req_offset;
                    req->copy_req.size = req_size;
                    req->copy_req.dst_dir = ureq->file_size_req.dst_dir;
                    req->copy_req.source_dir = ureq->file_size_req.source_dir;
                    req->copy_req.overwrite = ureq->file_size_req.overwrite;
                    req->copy_req.is_last = (size <= req_size);

                    req_offset += req_size;
                    size -= req_size;
                    if (size <= 0)
                    {
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
            if (done_count > 0)
            {
                _concurrent_copy_request_count -= done_count;
            }

            if (++_concurrent_copy_request_count > _opts.max_concurrent_remote_copy_requests)
            {
                --_concurrent_copy_request_count;
                return;
            }

            dsn::ref_ptr<copy_request_ex> req = nullptr;
            while (true)
            {
                {
                    zauto_lock l(_copy_requests_lock);
                    if (!_copy_requests.empty())
                    {
                        req = _copy_requests.front();
                        _copy_requests.pop();
                    }
                    else
                    {
                        --_concurrent_copy_request_count;
                        break;
                    }
                }

                {
                    zauto_lock l(req->lock);
                    if (req->is_valid)
                    {
                        req->add_ref();
                        req->remote_copy_task = begin_copy(req->copy_req, req.get(), 0, 0, 0, &req->file_ctx->user_req->file_size_req.source);

                        if (++_concurrent_copy_request_count > _opts.max_concurrent_remote_copy_requests)
                        {
                            --_concurrent_copy_request_count;
                            break;
                        }
                    }
                }
            }
        }

        void nfs_client_impl::end_copy(
            ::dsn::error_code err,
            const copy_response& resp,
            void* context)
        {
            //dinfo("*** call RPC_NFS_COPY end, return (%d, %d) with %s", resp.offset, resp.size, err.to_string());

            dsn::ref_ptr<copy_request_ex> reqc;
            reqc = (copy_request_ex*)context;
            reqc->release_ref();

            continue_copy(1);
            
            if (err == ERR_OK)
            {
                err = resp.error;
            }

            if (err != ::dsn::ERR_OK)
            {
                handle_completion(reqc->file_ctx->user_req, err);
                return;
            }
            
            reqc->response = resp;
            reqc->response.error.end_tracking(); // always ERR_OK
            reqc->is_ready_for_write = true;

            auto& fc = reqc->file_ctx;

            // check write availability
            {
                zauto_lock l(fc->user_req->user_req_lock);
                if (fc->current_write_index != reqc->index - 1)
                    return;
            }

            // check readies for local writes
            {
                zauto_lock l(fc->user_req->user_req_lock);
                for (int i = reqc->index; i < (int)(fc->copy_requests.size()); i++)
                {
                    if (fc->copy_requests[i]->is_ready_for_write)
                    {
                        fc->current_write_index++;

                        {
                            zauto_lock l(_local_writes_lock);
                            _local_writes.push(fc->copy_requests[i]);
                        }
                    }
                    else
                        break;
                }
            }

            continue_write();
        }

        void nfs_client_impl::continue_write()
        {
            // check write quota
            if (++_concurrent_local_write_count > _opts.max_concurrent_local_writes)
            {
                --_concurrent_local_write_count;
                return;
            }

            // get write
            dsn::ref_ptr<copy_request_ex> reqc;
            while (true)
            {
                {
                    zauto_lock l(_local_writes_lock);
                    if (!_local_writes.empty())
                    {
                        reqc = _local_writes.front();
                        _local_writes.pop();
                    }
                    else
                    {
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

            if (nullptr == reqc)
            {
                --_concurrent_local_write_count;
                return;
            }   

            // real write
            std::string file_path = reqc->copy_req.dst_dir + reqc->file_ctx->file_name;
            std::string path = ::dsn::utils::remove_file_name(file_path.c_str());
            if (!::dsn::utils::is_file_or_dir_exist(path.c_str()))
            {
                mkdir_(path.c_str());
            }

            dsn_handle_t hfile = reqc->file_ctx->file.load();
            if (!hfile)
            {
                zauto_lock l(reqc->file_ctx->user_req->user_req_lock);
                hfile = reqc->file_ctx->file.load();
                if (!hfile)
                {
                    hfile = dsn_file_open(file_path.c_str(), O_RDWR | O_CREAT | O_BINARY, 0666);
                    reqc->file_ctx->file = hfile;
                }
            }

            if (!hfile)
            {
                derror("file open %s failed", file_path.c_str());
                error_code err = ERR_FILE_OPERATION_FAILED;
                handle_completion(reqc->file_ctx->user_req, err);
                --_concurrent_local_write_count;
                continue_write();
                return;
            }

            {
                zauto_lock l(reqc->lock);

                reqc->local_write_task = file::write(
                    hfile,
                    reqc->response.file_content.data(),
                    reqc->response.size,
                    reqc->response.offset,
                    LPC_NFS_WRITE,
                    this,
                    std::bind(
                    &nfs_client_impl::local_write_callback,
                    this,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    reqc
                    ),
                    0);
            }
        }

        void nfs_client_impl::local_write_callback(error_code err, size_t sz, dsn::ref_ptr<copy_request_ex> reqc)
        {
            //dassert(reqc->local_write_task == task::get_current_task(), "");
            --_concurrent_local_write_count;

            // clear all content to release memory quickly
            reqc->response.file_content = blob();

            continue_write();

            bool completed = false;
            if (err != ERR_OK)
            {
                completed = true;
            }
            else
            {
                zauto_lock l(reqc->file_ctx->user_req->user_req_lock);
                if (++reqc->file_ctx->finished_segments == (int)reqc->file_ctx->copy_requests.size())
                {
                    auto err = dsn_file_close(reqc->file_ctx->file);
                    dassert(err == ERR_OK, "dsn_file_close failed, err = %s", dsn_error_to_string(err));
                    reqc->file_ctx->file = static_cast<dsn_handle_t>(0);
                    reqc->file_ctx->copy_requests.clear();

                    if (++reqc->file_ctx->user_req->finished_files == (int)reqc->file_ctx->user_req->file_context_map.size())
                    {
                        completed = true;
                    }
                }
            }

            if (completed)
            {   
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

            for (auto& f : req->file_context_map)
            {
                
                for (auto& rc : f.second->copy_requests)
                {
                    ::dsn::task_ptr ctask, wtask;
                    {
                        zauto_lock l(rc->lock);
                        rc->is_valid = false;
                        ctask = rc->remote_copy_task;
                        wtask = rc->local_write_task;

                        rc->remote_copy_task = nullptr;
                        rc->local_write_task = nullptr;
                    }

                    if (err != ERR_OK)
                    {
                        if (ctask != nullptr)
                        {
                            if (ctask->cancel(true))
                            {
                                _concurrent_copy_request_count--;
                                rc->release_ref();
                            }
                        }

                        if (wtask != nullptr)
                        {
                            if (wtask->cancel(true))
                            {
                                _concurrent_local_write_count--;
                            }
                        }
                    }
                }
                                
                if (f.second->file)
                {
                    auto err2 = dsn_file_close(f.second->file);
                    dassert(err2 == ERR_OK, "dsn_file_close failed, err = %s", dsn_error_to_string(err2)); 

                    f.second->file = static_cast<dsn_handle_t>(0);

                    if (f.second->finished_segments != (int)f.second->copy_requests.size())
                    {
                        ::remove((f.second->user_req->file_size_req.dst_dir 
                            + f.second->file_name).c_str());
                    }
                }

                delete f.second;
            }

            req->file_context_map.clear();
            req->nfs_task->enqueue(err, 0, req->nfs_task->node());

            delete req;

            // clear out all canceled requests
            if (err != ERR_OK)
            {
                continue_copy(0);
                continue_write();
            }
        }

    }
}
