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
			//dinfo("*** call RPC_NFS_COPY end, return (%d, %d) with %s", resp.offset, resp.size, err.to_string());
			copy_request_ex* reqc = (copy_request_ex*)context;

			if (reqc->user_req->finished)
				return;

			continue_copy(1, reqc->user_req);

			if (err == ::dsn::ERR_SUCCESS)
			{
				{
					zauto_lock l(reqc->user_req->user_req_lock);
					auto it_file = reqc->user_req->file_context_map.find(reqc->copy_req.dst_dir + reqc->copy_req.file_name);
					dassert(it_file != reqc->user_req->file_context_map.end(), "it_file should be existed");

					if (it_file->second->err != 0)
						return;
				}
			}

			if (err != ::dsn::ERR_SUCCESS)
			{
				handle_fault(reqc->copy_req.dst_dir + reqc->copy_req.file_name, reqc->user_req, err);
				return;
			}

			if (resp.error != ::dsn::ERR_SUCCESS)
			{
				handle_fault(reqc->copy_req.dst_dir + reqc->copy_req.file_name, reqc->user_req, resp.error);
				return;
			}

			write_copy(reqc->user_req, resp);
			delete reqc;
		}

        void nfs_client_impl::write_copy(user_request* req, const ::dsn::service::copy_response& resp)
        {
            // 
            // concurrent copy is much more complicated (e.g., out-of-order file content delivery)
            // the following logic is only right when concurrent request # == 1
			//
			// TODO: handle concurrent out-of-order file writes, lsl. done
            //
            // dassert(_opts.max_concurrent_remote_copy_requests == 1, "");

			std::string file_path = resp.dst_dir + resp.file_name;

			boost::filesystem::path path(file_path); // create directory recursively if necessary
			path = path.remove_filename();
			if (!boost::filesystem::exists(path))
			{
				boost::filesystem::create_directory(path);
			}

			handle_t hfile;

			{
				zauto_lock l(req->user_req_lock);
				auto it_handle = _handles_map.find(file_path); // find file handle cache first

				if (it_handle == _handles_map.end()) // not found
				{
					hfile = file::open(file_path.c_str(), O_RDWR | O_CREAT | O_BINARY, 0666);
					if (hfile == 0)
					{
						derror("file open failed");
						error_code err = ERR_OBJECT_NOT_FOUND;
						req->nfs_task->enqueue(err, 0, req->nfs_task->node());
						return;
					}
					_handles_map.insert(std::pair<std::string, handle_t>(file_path, hfile));
				}
				else // found
				{
					hfile = it_handle->second;
				}
			
				auto it_file = req->file_context_map.find(file_path);
				dassert(it_file != req->file_context_map.end(), "it_file should be existed");

				if (it_file->second->resp_info == NULL)
				{
					it_file->second->resp_info = new resp_copy_file_info;

					it_file->second->resp_info->copy_response_map.insert(std::pair<uint64_t, copy_response>(resp.offset, resp));
					it_file->second->resp_info->current_offset = 0;
					it_file->second->resp_info->finished_count = 0;
					it_file->second->resp_info->copy_count = it_file->second->file_size / _opts.max_buf_size + 1;
				}
				else
				{
					it_file->second->resp_info->copy_response_map.insert(std::pair<uint64_t, copy_response>(resp.offset, resp));
				}
			}

			if (req->finished)
				return;

			write_file(req);
        }

		void nfs_client_impl::handle_fault(std::string file_path, user_request *req, error_code err)
		{
			{
				zauto_lock l(req->user_req_lock);
				derror("%s copy error", file_path.c_str());

				auto it_file = req->file_context_map.find(file_path);
				if (it_file->second->err == 0)
				{
					it_file->second->err = err;
				}
				else
					return;

				if (it_file->second->resp_info != NULL)
				{
					req->copy_request_count -= it_file->second->resp_info->copy_count;
					req->copy_request_count += it_file->second->resp_info->finished_count;
				}
				else
				{
					req->copy_request_count -= it_file->second->file_size / _opts.max_buf_size + 1;
				}

				delete it_file->second->resp_info; // remove copy map
				it_file->second->resp_info = NULL;
			}

			if (0 == req->copy_request_count)
			{
				handle_finish(file_path, req, err);
			}
		}

		void nfs_client_impl::handle_success(std::string file_path, user_request *req, error_code err)
		{
			int left_reqs;
			{
				zauto_lock l(req->user_req_lock);

				auto it_file = req->file_context_map.find(file_path);
				if (it_file->second->resp_info == NULL)
					return;

				it_file->second->resp_info->finished_count++;

				if (it_file->second->resp_info->finished_count == it_file->second->resp_info->copy_count)
				{
					if (boost::filesystem::exists(file_path))
					{
						file::close(_handles_map[file_path]);
						delete it_file->second->resp_info;
						it_file->second->resp_info = NULL;

						if (it_file->second->err != 0)
						{
							boost::filesystem::remove(file_path);
						}
					}
				}
				left_reqs = --req->copy_request_count;
			}
			if (0 == left_reqs)
			{
				handle_finish(file_path, req, err);
			}
		}

		void nfs_client_impl::handle_finish(std::string file_path, user_request *req, error_code err)
		{
			req->finished = true;

			auto it_file = req->file_context_map.find(file_path);
			if (it_file->second->err == 0)
			{
				req->nfs_task->enqueue(ERR_SUCCESS, 0, req->nfs_task->node());
			}
			else
			{
				req->nfs_task->enqueue(it_file->second->err, 0, req->nfs_task->node());
			}

			for (auto it_file = req->file_context_map.begin(); it_file != req->file_context_map.end(); it_file++)
			{
				if (it_file->second->err != 0 && boost::filesystem::exists(it_file->first))
				{
					file::close(_handles_map[it_file->first]);
					boost::filesystem::remove(it_file->first);
				}
			}
			
			garbage_collect(req);
		}

		void nfs_client_impl::garbage_collect(user_request* req)
		{
			for (int i = 0; i < req->task_ptr_list.size(); i++)
			{
				if (req->task_ptr_list[i] != nullptr)
				{
					req->task_ptr_list[i]->cancel(true);
				}
			}

			//_handles_map.clear(); // globel variable, TODO

			for (auto it_file = req->file_context_map.begin(); it_file != req->file_context_map.end(); )
			{
				if (it_file->second->resp_info != NULL)
					delete it_file->second->resp_info;
				req->file_context_map.erase(it_file++);
			}
			for (int i = 0; i < req->req_copy_file_vector.size(); i++)
			{
				std::queue<copy_request*> empty;
				std::swap(req->req_copy_file_vector[i], empty);
			}
			delete req;
		}

		void nfs_client_impl::internal_write_callback(error_code err, uint32_t sz, ::std::string file_path, user_request* req)
		{
			if (err != ERR_SUCCESS)
			{
				handle_fault(file_path, req, err);
			}
			if (err == ERR_SUCCESS)
			{
				bool success_flag = false;
				{
					zauto_lock l(req->user_req_lock);

					auto it_file = req->file_context_map.find(file_path);
					if (it_file->second->err == 0)
					{
						success_flag = true;
					}
				}
				if (success_flag)
					handle_success(file_path, req, err);
			}

			if (req->finished)
				return;

			write_file(req);
		}

		void nfs_client_impl::continue_copy(int done_count, user_request* reqc)
        {
            if (done_count > 0)
            {
				zauto_lock l(reqc->user_req_lock);
				dassert(_concurrent_copy_request_count >= done_count, "");

				_concurrent_copy_request_count -= done_count;
            }

            copy_request* req = nullptr;
            {
				zauto_lock l(reqc->user_req_lock);

				if (!reqc->req_copy_file_vector.empty() && _concurrent_copy_request_count < _opts.max_concurrent_remote_copy_requests)
				{
					for (int i = 0; i < reqc->req_copy_file_vector.size(); i++)
					{
						if (reqc->req_copy_file_vector[i].empty())
							continue;
						
						req = reqc->req_copy_file_vector[i].front();

						auto it_file = reqc->file_context_map.find(req->dst_dir+req->file_name);
						dassert(it_file != reqc->file_context_map.end(), "it_file should be existed");
						if (it_file->second->err != 0) // no need to send a failed file request
						{
							std::queue<copy_request*> empty;
							std::swap(reqc->req_copy_file_vector[i], empty);
							continue;
						}
						if (it_file->second->resp_info != NULL)
						{
							if ((req->offset - it_file->second->resp_info->current_offset) / _opts.max_buf_size > _opts.max_request_step) // exceed request bound for one file
								continue;
						}

						reqc->req_copy_file_vector[i].pop();
						++_concurrent_copy_request_count;

						copy_request_ex *copy_req = new copy_request_ex;
						copy_req->user_req = reqc;
						copy_req->copy_req = *req;

						task_ptr task = begin_copy(*req, copy_req);

						reqc->task_ptr_list.push_back(task);
					}
				}
            }
        }

		void nfs_client_impl::write_file(user_request* req)
		{
			{
				zauto_lock l(req->user_req_lock);

				copy_response resp;

				for (auto it_file = req->file_context_map.begin(); it_file != req->file_context_map.end(); it_file++)
				{
					auto it = it_file->second->resp_info;

					if (it_file->second->err != 0 || it == NULL)
						continue;

					auto it_response = it->copy_response_map.find(it->current_offset);

					if (it_response != it->copy_response_map.end())
					{
						resp = it_response->second;
						std::string tmp = resp.file_name;
						it->current_offset += resp.size; // move to callback

						task_ptr task = file::write(
							_handles_map[resp.dst_dir+resp.file_name],
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
							resp.dst_dir + resp.file_name,
							req
							),
							0);
						req->task_ptr_list.push_back(task);
					}
				}
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
				derror("remote copy request failed");
				reqc->nfs_task->enqueue(err, 0, reqc->nfs_task->node());
                delete reqc;
                return;
			}

			if (resp.error != ::dsn::ERR_SUCCESS)
			{
				derror("remote copy request failed");
				error_code resp_err;
				resp_err.set(resp.error);
				reqc->nfs_task->enqueue(resp_err, 0, reqc->nfs_task->node());
                delete reqc;
				return;
			}

			for (size_t i = 0; i < resp.size_list.size(); i++) // file list
			{
				uint64_t size = resp.size_list[i];
				{
					zauto_lock l(reqc->user_req_lock);

					file_context *filec = new file_context;
					filec->file_size = size;
					filec->err = ERR_SUCCESS;
					filec->resp_info = NULL;
					reqc->file_context_map.insert(std::pair<std::string, file_context*>(reqc->file_size_req.dst_dir + resp.file_list[i], filec));
				}
				//dinfo("this file size is %d, name is %s", size, resp.file_list[i].c_str());

				uint64_t req_offset = 0;
				uint32_t req_size;
				if (size > _opts.max_buf_size)
					req_size = _opts.max_buf_size;
				else
					req_size = size;

				std::queue<copy_request*> copy_request_ex_queue;
				reqc->req_copy_file_vector.push_back(copy_request_ex_queue);
				for (;;) // send one file with multi-round rpc
				{
                    copy_request* req = new copy_request;
					req->source = reqc->file_size_req.source;
					req->file_name = resp.file_list[i];

                    req->offset = req_offset;
                    req->size = req_size;
                    req->dst_dir = reqc->file_size_req.dst_dir;
                    req->source_dir = reqc->file_size_req.source_dir;
                    req->overwrite = reqc->file_size_req.overwrite;
                    req->is_last = (size <= req_size);

                    reqc->copy_request_count++;

					{
						zauto_lock l(reqc->user_req_lock);
						reqc->req_copy_file_vector[i].push(req); // enqueue instead of sending it
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

			continue_copy(0, reqc);
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
