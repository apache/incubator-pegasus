# include "nfs_client_impl.h"
# include <dsn/internal/nfs.h>
# include <queue>
# include <boost/filesystem.hpp>

namespace dsn { 
	namespace service {

		void nfs_client_impl::end_copy(
			::dsn::error_code err,
			const copy_response& resp,
			void* context)
		{	
			dinfo("*** call RPC_NFS_COPY end, return (%d, %d) with %s", resp.offset, resp.size, err.to_string());

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
            // concurrent copy is much more complicated (e.g., out-of-order file content delivery)
            // the following logic is only right when concurrent request # == 1
			//
			// test out-of-order file write is ok, lsl.
            //
            // dassert(_opts.max_concurrent_remote_copy_requests == 1, "");

			if (err == ::dsn::ERR_SUCCESS)
			{
				std::string file_path = req->file_size_req.dst_dir + resp.file_name;

				// create directory recursively if necessary
				boost::filesystem::path path(file_path);
				path = path.remove_filename();
				if (!boost::filesystem::exists(path))
				{
					boost::filesystem::create_directory(path);
				}

				handle_t hfile;

				{
					zauto_lock l(_handles_map_lock);
					auto it = _handles_map.find(file_path); // find file handle cache first

					if (it == _handles_map.end()) // not found
					{
						hfile = file::open(file_path.c_str(), O_RDWR | O_CREAT | O_BINARY, 0666);
						if (hfile == 0)
						{
							derror("file open failed");
							err = ERR_OBJECT_NOT_FOUND;
							req->nfs_task->enqueue(err, 0, req->nfs_task->node());
							return;
						}
						_handles_map.insert(std::pair<std::string, handle_t>(file_path.c_str(), hfile));

						{
							zauto_lock l(_copy_request_count_map_lock);
							auto it_copy = _copy_request_count_map.find(resp.file_name);
							if (it_copy == _copy_request_count_map.end())
							{
								derror("%s should be existed in _copy_request_count_map.", file_path);
							}
							else
							{
								it_copy->second->file_handle = hfile;
							}
						}
					}
					else // found
					{
						hfile = it->second;
					}
				}

				auto task = file::write(
					hfile,
					resp.file_content.data(),
					resp.size,
					resp.offset,
					LPC_NFS_WRITE,
					nullptr,
					std::bind(
					&nfs_client_impl::internal_write_callback,
					this,
					std::placeholders::_1,
					std::placeholders::_2,
					resp.file_name,
					req
					),
					0);
			}
        }

		void nfs_client_impl::internal_write_callback(error_code err, uint32_t sz, ::std::string file_name, user_request* req)
		{
			if (err != 0)
			{
				derror("file operation failed, err = %s", err.to_string());
			}

			{
				zauto_lock l(_copy_request_count_map_lock);
				auto it_copy = _copy_request_count_map.find(file_name);
				if (it_copy == _copy_request_count_map.end())
				{
					derror("%s should be existed in _copy_request_count_map.", file_name);
				}
				else
				{
					it_copy->second->copy_request_count--;
					if (it_copy->second->copy_request_count == 0)
					{
						file::close(it_copy->second->file_handle);
						_copy_request_count_map.erase(it_copy);
					}
				}
			}

			auto left_reqs = --req->copy_request_count;
			if (0 == left_reqs || err != ERR_SUCCESS)
			{
				req->finished = true;
				req->nfs_task->enqueue(err, 0, req->nfs_task->node());

				{
					zauto_lock l(_handles_map_lock);
					for (auto it = _handles_map.begin(); it != _handles_map.end();)
					{
						_handles_map.erase(it++);
					}
				}
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
				dassert(_concurrent_copy_request_count >= done_count, "");

				_concurrent_copy_request_count -= done_count;
            }

            while (true)
            {
                copy_request_ex* req = nullptr;
                {
                    zauto_lock l(_lock);
                    if (_req_copy_file_queue.empty())
                        return;

					if (_concurrent_copy_request_count >= _opts.max_concurrent_remote_copy_requests)
                        return;
                    
                    req = _req_copy_file_queue.front();
                    _req_copy_file_queue.pop();
					++_concurrent_copy_request_count;
                }

                if (req->user_req->finished)
                {
                    auto left_reqs = --req->user_req->copy_request_count;
                    if (0 == left_reqs) delete req->user_req;
                    delete req;

                    zauto_lock l(_lock);
					dassert(_concurrent_copy_request_count >= 1, "");
					_concurrent_copy_request_count -= 1;
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

			for (size_t i = 0; i < resp.size_list.size(); i++) // file list
			{
				uint64_t size = resp.size_list[i];
				dinfo("this file size is %d, name is %s", size, resp.file_list[i].c_str());

				{
					zauto_lock l(_copy_request_count_map_lock);
					auto it = _copy_request_count_map.find(resp.file_list[i]);
					if (it == _copy_request_count_map.end())
					{
						file_handle_info_on_client *fh = new file_handle_info_on_client;
						fh->copy_request_count = 0;
						_copy_request_count_map.insert(std::pair<std::string, file_handle_info_on_client*>(resp.file_list[i], fh));
					}
					else
					{
						// deplicated copy request, skip this file
						continue;
					}
				}

				uint64_t req_offset = 0;
				uint32_t req_size;
				if (size > _opts.max_buf_size)
					req_size = _opts.max_buf_size;
				else
					req_size = size;

				for (;;) // send one file with multi-round rpc
				{
                    copy_request_ex* req = new copy_request_ex;
					req->copy_req.source = reqc->file_size_req.source;
					req->copy_req.file_name = resp.file_list[i];

					{
						zauto_lock l(_copy_request_count_map_lock);
						auto it = _copy_request_count_map.find(resp.file_list[i]);
						if (it != _copy_request_count_map.end())
						{
							it->second->copy_request_count++;
						}
					}

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
