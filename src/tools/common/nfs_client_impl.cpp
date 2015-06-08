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
			continue_copy(1); // consume left copy request

            copy_request_ex* reqc = (copy_request_ex*)context;

			if (err == ::dsn::ERR_SUCCESS)
			{
				{
					zauto_lock l(_lock);
					auto it = _file_failure_map.find(_file_path_map[resp.file_name]);
					if (it != _file_failure_map.end())
						return;
				}
			}

			if (err != ::dsn::ERR_SUCCESS)
			{
				{
					zauto_lock l(_lock);
					handle_fault(_file_path_map[reqc->copy_req.file_name], reqc->user_req, err);
				}
				return;
			}

			if (resp.error != ::dsn::ERR_SUCCESS)
			{
				{
					zauto_lock l(_lock);
					handle_fault(_file_path_map[reqc->copy_req.file_name], reqc->user_req, err);
				}
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
				zauto_lock l(_lock);
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
			
				auto it_resp = _resp_copy_file_map.find(file_path);
				auto it_suc = _file_failure_map.find(file_path);
				if (it_resp == _resp_copy_file_map.end())
				{
					resp_copy_file_info _resp_copy_file_info;

					//_resp_copy_file_info.copy_response_vector.push_back(resp);
					_resp_copy_file_info.copy_response_map.insert(std::pair<uint64_t, copy_response>(resp.offset, resp));
					_resp_copy_file_info.current_offset = 0;
					_resp_copy_file_info.finished_count = 0;
					_resp_copy_file_info.copy_count = _file_size_map[file_path] / _opts.max_buf_size + 1;
					_resp_copy_file_map.insert(std::pair<std::string, resp_copy_file_info>(file_path, _resp_copy_file_info));
				}
				else
				{
					it_resp->second.copy_response_map.insert(std::pair<uint64_t, copy_response>(resp.offset, resp));
					//it_resp->second.copy_response_vector.push_back(resp);
				}

				::dsn::service::tasking::enqueue(
					WRITE_FILE,
					this,
					std::bind(
					&nfs_client_impl::write_file,
					this,
					req
					));
			}
        }

		void nfs_client_impl::handle_fault(std::string file_path, user_request *req, error_code err)
		{
			derror("%s copy error", file_path.c_str());

			auto it_failure = _file_failure_map.find(file_path); // add failure map
			if (it_failure == _file_failure_map.end())
			{
				_file_failure_map.insert(std::pair<std::string, error_code>(file_path, err));
			}
			else
				return;

			auto it_copy = _resp_copy_file_map.find(file_path); // remove copy map

			if (it_copy != _resp_copy_file_map.end())
			{
				req->copy_request_count -= it_copy->second.copy_count;
				req->copy_request_count += it_copy->second.finished_count;
			}
			else
			{
				req->copy_request_count -= _file_size_map[file_path] / _opts.max_buf_size + 1;
			}

			if (it_copy != _resp_copy_file_map.end())
			{
				_resp_copy_file_map.erase(it_copy);
			}

			if (0 == req->copy_request_count)
			{
				handle_finish(file_path, req, err);
			}
		}

		void nfs_client_impl::handle_success(std::string file_path, user_request *req, error_code err)
		{
			auto it = _resp_copy_file_map.find(file_path);
			it->second.finished_count++;

			if (it->second.finished_count == it->second.copy_count)
			{
				if (boost::filesystem::exists(file_path))
				{
					file::close(_handles_map[file_path]);
					_resp_copy_file_map.erase(it);

					auto it = _file_failure_map.find(file_path);
					if (it != _file_failure_map.end())
					{
						boost::filesystem::remove(file_path);
					}
				}
			}

			auto left_reqs = --req->copy_request_count;
			if (0 == left_reqs)
			{
				handle_finish(file_path, req, err);
			}
		}

		void nfs_client_impl::handle_finish(std::string file_path, user_request *req, error_code err)
		{
			req->finished = true;

			if (_file_failure_map.empty())
			{
				req->nfs_task->enqueue(ERR_SUCCESS, 0, req->nfs_task->node());
			}
			else
			{
				req->nfs_task->enqueue(_file_failure_map.begin()->second, 0, req->nfs_task->node());
			}

			for (auto it = _file_failure_map.begin(); it != _file_failure_map.end();)
			{
				if (boost::filesystem::exists(it->first))
				{
					file::close(_handles_map[it->first]);
					boost::filesystem::remove(it->first);
				}
				it++;
				//_file_failure_map.erase(it++); // TODO: clean failure_map
			}

			for (auto it = _handles_map.begin(); it != _handles_map.end();)
			{
				_handles_map.erase(it++);
			}

			for (auto it = _file_size_map.begin(); it != _file_size_map.end();)
			{
				_file_size_map.erase(it++);
			}

			delete(req);
			_file_size_map.clear();
			//_file_failure_map.clear();
			_resp_copy_file_map.clear();
		}

		void nfs_client_impl::internal_write_callback(error_code err, uint32_t sz, ::std::string file_path, user_request* req)
		{
			if (err != ERR_SUCCESS)
			{
				{
					zauto_lock l(_lock);
					handle_fault(file_path, req, err);
				}
			}
			if (err == ERR_SUCCESS)
			{
				{
					zauto_lock l(_lock);

					auto it = _file_failure_map.find(file_path);
					if (it == _file_failure_map.end())
					{
						handle_success(file_path, req, err);
					}
				}
			}
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
                        break;

					if (_concurrent_copy_request_count >= _opts.max_concurrent_remote_copy_requests)
						break;
                    
                    req = _req_copy_file_queue.front();
                    _req_copy_file_queue.pop();
					++_concurrent_copy_request_count;
					
					begin_copy(req->copy_req, req);
                }
            }
        }

		void nfs_client_impl::write_file(user_request* req)
		{
			{
				zauto_lock l(_lock);

				copy_response resp;

				for (auto it = _resp_copy_file_map.begin(); it != _resp_copy_file_map.end(); it++)
				{
					auto it_failure = _file_failure_map.find(it->first);
					if (it_failure != _file_failure_map.end())
						continue;

					auto it_response = it->second.copy_response_map.find(it->second.current_offset);

					if (it_response != it->second.copy_response_map.end())
					{
						resp = it_response->second;
						std::string tmp = resp.file_name;
						it->second.current_offset += resp.size; // move to callback

						file::write(
							_handles_map[_file_path_map[resp.file_name]],
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
							_file_path_map[resp.file_name],
							req
							),
							0);
					}

					/*
					for (int i = 0; i < it->second.copy_response_vector.size(); i++)
					{
						if (it->second.copy_response_vector[i].offset == it->second.current_offset)
						{
							resp = it->second.copy_response_vector[i];
							std::string tmp = resp.file_name;
							it->second.current_offset += resp.size; // move to callback

							file::write(
								_handles_map[_file_path_map[resp.file_name]],
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
								_file_path_map[resp.file_name],
								req
								),
								0);
							break;
						}
					}
					*/
				}
				if (_resp_copy_file_map.empty())
					return;

				::dsn::service::tasking::enqueue(
					WRITE_FILE,
					this,
					std::bind(
					&nfs_client_impl::write_file,
					this,
					req
					));
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
					zauto_lock l(_lock);
					auto it_size = _file_size_map.find(reqc->file_size_req.dst_dir + resp.file_list[i]); // find file handle cache first, TODO it should be a path

					if (it_size == _file_size_map.end()) // not found
					{
						_file_size_map.insert(std::pair<std::string, uint64_t>(reqc->file_size_req.dst_dir + resp.file_list[i], size));
					}

					auto it_path = _file_path_map.find(resp.file_list[i]); // find file handle cache first, TODO it should be a path

					if (it_path == _file_path_map.end()) // not found
					{
						_file_path_map.insert(std::pair<std::string, std::string>(resp.file_list[i], reqc->file_size_req.dst_dir + resp.file_list[i]));
					}
				}
				//dinfo("this file size is %d, name is %s", size, resp.file_list[i].c_str());

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
