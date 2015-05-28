# pragma once
# include "nfs_client_impl.h"
# include <dsn\internal\nfs.h>
# include <queue>
# include <io.h>
# include <direct.h>

namespace dsn { 
	namespace service {

		void nfs_client_impl::end_copy(
			::dsn::error_code err,
			const copy_response& resp,
			void* context)
		{	
            std::cout << "*** call RPC_NFS_COPY end, return " << "(" << resp.offset << ", " << resp.size << ")" << " with err " << err.to_string() << std::endl;

            copy_request_ex* reqc = (copy_request_ex*)context;
            if (err == ::dsn::ERR_SUCCESS)
            {
                error_code resp_err;
                resp_err.set(resp.error);
            }

            write_copy(err, reqc->user_req, resp);

            delete reqc;
		}

        void nfs_client_impl::write_copy(error_code err, user_request* req, const ::dsn::service::copy_response& resp)
        {
            // 
            // TODO: concurrent copy is much more complicated (e.g., out-of-order file content delivery)
            // the following logic is only right when concurrent request # == 1
            //
            dassert(_opts.max_concurrent_remote_copy_requests == 1, "");

            if (err == ::dsn::ERR_SUCCESS)
            {
                std::string file_path = req->file_size_req.dst_dir + resp.file_name;

                // TODO: !overwrite means failure when file already exists
                //if (!reqc->copy_req.overwrite) // not overwrite
                //{
                //    file_path += ".conflict";
                //}

                for (size_t i = 0; i < file_path.length(); i++) // create file folder if not existed
                {
                    if (file_path[i] == '/')
                    {
                        if (access(file_path.substr(0, i).c_str(), 6) == -1)
                        {
                            mkdir(file_path.substr(0, i).c_str());
                        }
                    }
                }

                handle_t hfile = file::open(file_path.c_str(), O_RDWR | O_CREAT, 0);

                auto task = file::write(
                    hfile,
                    resp.file_content.data(),
                    resp.size,
                    resp.offset,
                    LPC_NFS_WRITE,
                    nullptr,
                    nullptr,
                    0);

                task->wait();
                file::close(hfile);

                err = task->error();
            }

            auto left_reqs = --req->copy_request_count;
            if (0 == left_reqs || err != ERR_SUCCESS)
            {
                req->finished = true;
                req->nfs_task->enqueue(err, 0, req->nfs_task->node());
            }

            if (0 == left_reqs)
            {
                delete req;
            }

            continue_copy(1);
        }

        void nfs_client_impl::continue_copy(int done_count)
        {
            if (done_count > 0)
            {
                zauto_lock l(_lock);
                dassert(_concurrent_request_count >= done_count, "");

                _concurrent_request_count -= done_count;
            }

            while (true)
            {
                copy_request_ex* req = nullptr;
                {
                    zauto_lock l(_lock);
                    if (_req_copy_file_queue.empty())
                        return;

                    if (_concurrent_request_count >= _opts.max_concurrent_remote_copy_requests)
                        return;
                    
                    req = _req_copy_file_queue.front();
                    _req_copy_file_queue.pop();
                    ++_concurrent_request_count;
                }

                if (req->user_req->finished)
                {
                    auto left_reqs = --req->user_req->copy_request_count;
                    if (0 == left_reqs) delete req->user_req;
                    delete req;

                    zauto_lock l(_lock);
                    dassert(_concurrent_request_count >= 1, "");
                    _concurrent_request_count -= 1;
                }
                else
                    begin_copy(req->copy_req, req);
            }
        }

		void nfs_client_impl::end_get_file_size(
			::dsn::error_code err,
			const ::dsn::service::get_file_size_response& resp,
			void* context)
		{
            user_request* reqc = (user_request*)context;

			if (err != ::dsn::ERR_SUCCESS)
			{
				reqc->nfs_task->enqueue(err, 0, reqc->nfs_task->node());
                delete reqc;
                return;
			}

			if (resp.error != ::dsn::ERR_SUCCESS)
			{
				error_code resp_err;
				resp_err.set(resp.error);
				reqc->nfs_task->enqueue(resp_err, 0, reqc->nfs_task->node());
                delete reqc;
				return;
			}

            std::cout << "get file size ok" << std::endl;
			for (size_t i = 0; i < resp.size_list.size(); i++) // file list
			{
				int32_t size = resp.size_list[i];
				std::cout << "this file size is " << size << ", name is " << resp.file_list[i] << std::endl;

				int32_t req_offset = 0;
				int32_t req_size;
				if (size > _opts.max_buf_size)
					req_size = _opts.max_buf_size;
				else
					req_size = size;

				for (;;) // send one file with multi-round rpc
				{
                    copy_request_ex* req = new copy_request_ex;
					req->copy_req.source = reqc->file_size_req.source;
                    req->copy_req.file_name = resp.file_list[i];
                    req->copy_req.offset = req_offset;
                    req->copy_req.size = req_size;
                    req->copy_req.dst_dir = reqc->file_size_req.dst_dir;
                    req->copy_req.source_dir = reqc->file_size_req.source_dir;
                    req->copy_req.overwrite = reqc->file_size_req.overwrite;
                    req->copy_req.is_last = (size <= req_size);
                    req->user_req = reqc;
                    req->user_req->copy_request_count++;

					{
						zauto_lock l(_lock);
						_req_copy_file_queue.push(req); // enqueue instead of sending it
					}

					req_offset += req_size;
					size -= req_size;
                    if (size <= 0)
                    {
                        dassert(size == 0, "last request must read exactly the remaing size of the file");
                        break;
                    }	

					if (size > _opts.max_buf_size)
						req_size = _opts.max_buf_size;
					else
						req_size = size;
				}
			}

            continue_copy();
		}
        		
		void nfs_client_impl::begin_remote_copy(std::shared_ptr<remote_copy_request>& rci, aio_task_ptr nfs_task)
		{
            dassert(_server == rci->source, "");

            user_request* req = new user_request;
            req->file_size_req.source = rci->source;
            req->file_size_req.dst_dir = rci->dest_dir;
            req->file_size_req.file_list = rci->files;
            req->file_size_req.source_dir = rci->source_dir;
            req->file_size_req.overwrite = rci->overwrite;
            req->nfs_task = nfs_task;
            req->finished = false;
            req->copy_request_count = 0;

			begin_get_file_size(req->file_size_req, req); // async copy file
		}
	} 
} 