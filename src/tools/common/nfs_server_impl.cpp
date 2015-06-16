# include "nfs_server_impl.h"
# include <cstdlib>
# include <boost/filesystem.hpp>
# include <sys/stat.h>

namespace dsn { 
	namespace service { 

		void nfs_service_impl::on_copy(const ::dsn::service::copy_request& request, ::dsn::service::rpc_replier<::dsn::service::copy_response>& reply)
		{
			std::cout << ">>> on call RPC_COPY end, exec RPC_NFS_COPY" << std::endl;

            std::string file_path = request.source_dir + request.file_name;
			std::shared_ptr<char> buf(new char[_opts.max_buf_size]);
			blob bb(buf, _opts.max_buf_size);
			handle_t hfile;

			{
				zauto_lock l(_handles_map_lock);
				auto it = _handles_map.find(request.file_name); // find file handle cache first

				if (it == _handles_map.end()) // not found
				{
					hfile = file::open(file_path.c_str(), O_RDONLY | O_BINARY, 0);
					if (hfile == 0)
					{
						derror("file operation failed");
						return;
					}
					map_value* mv = new map_value;
					mv->ht = hfile;
					mv->counter = 1;
					mv->stime_ms = dsn::service::env::now_ms();
					_handles_map.insert(std::pair<std::string, map_value*>(request.file_name, mv));
				}
				else // found
				{
					hfile = it->second->ht;
					it->second->counter++;
					it->second->stime_ms = dsn::service::env::now_ms();
				}
			}
			

			callback_para cp = { hfile, request.file_name, request.dst_dir, bb, request.offset, request.size};

			auto task = file::read(
				hfile,
				bb.buffer().get(),
				request.size,
				request.offset,
				LPC_NFS_READ,
				nullptr,
				std::bind(
				&nfs_service_impl::internal_read_callback,
				this,
				std::placeholders::_1,
				std::placeholders::_2,
				cp,
				reply
				)
				);
		}

		void nfs_service_impl::internal_read_callback(error_code err, int sz, callback_para cp, ::dsn::service::rpc_replier<::dsn::service::copy_response>& reply)
		{
			if (err != 0)
			{
				derror("file operation failed, err = %s", err.to_string());
			}

			{
				zauto_lock l(_handles_map_lock);
				auto it = _handles_map.find(cp.file_name);

				if (it != _handles_map.end())
				{
					it->second->counter--;
				}
			}

			::dsn::service::copy_response resp;
			resp.error = err;
			resp.file_name = cp.file_name;
			resp.dst_dir = cp.dst_dir;

            auto sptr = cp.bb.buffer();
			resp.file_content = blob(sptr, 0, sz);

			resp.offset = cp.offset;
			resp.size = cp.size;
			reply(resp);
		}

		// RPC_NFS_NEW_NFS_GET_FILE_SIZE 
		void nfs_service_impl::on_get_file_size(const ::dsn::service::get_file_size_request& request, ::dsn::service::rpc_replier<::dsn::service::get_file_size_response>& reply)
		{
			std::cout << ">>> on call RPC_NFS_GET_FILE_SIZE end, exec RPC_NFS_GET_FILE_SIZE" << std::endl;

			get_file_size_response resp;
			int err = ERR_SUCCESS;
			std::vector<std::string> file_list;
			std::string folder = request.source_dir;
			if (request.file_list.size() == 0) // return all file size in the destination file folder
			{
				get_file_names(folder, file_list);
				for (size_t i = 0; i < file_list.size(); i++)
				{
					std::cout << file_list[i] << std::endl;

                    struct stat st;
                    ::stat(file_list[i].c_str(), &st);

                    // TODO: using int64 instead as file ma
                    int size = st.st_size;

					resp.size_list.push_back(size);
                    resp.file_list.push_back(file_list[i].substr(request.source_dir.length(), file_list[i].length() - 1));
				}
			}
			else // return file size in the request file folder
			{
				for (size_t i = 0; i < request.file_list.size(); i++)
				{
					std::string file_path = folder + request.file_list[i];

                    struct stat st;
                    if (0 != ::stat(file_path.c_str(), &st))
                    {
                        derror("file open %s error!", file_path.c_str());
                        err = ERR_OBJECT_NOT_FOUND;
                        break;
                    }

                    // TODO: using int64 instead as file may exceed the size of 32bit
                    int size = st.st_size;

					resp.size_list.push_back(size);
                    resp.file_list.push_back((folder + request.file_list[i]).substr(request.source_dir.length(), (folder + request.file_list[i]).length() - 1));
				}
			}

			resp.error = err;
			reply(resp);
		}	

		void nfs_service_impl::close_file() // release out-of-date file handle
		{
			error_code err;
			{
				zauto_lock l(_handles_map_lock);

				if (_handles_map.size() == 0)
					return;

				for (auto it = _handles_map.begin(); it != _handles_map.end();)
				{
					if (it->second->counter == 0 && dsn::service::env::now_ms() - it->second->stime_ms > file_open_expire_time_ms) // not opened and expired
					{
						err = file::close(it->second->ht);
						_handles_map.erase(it++);
						if (err != 0)
						{
							derror("close file error: %s", err.to_string());
						}
					}
					else
						it++;
				}
			}
		}

		void nfs_service_impl::get_file_names(std::string dir, std::vector<std::string>& file_list)
		{
            boost::filesystem::recursive_directory_iterator it(dir), end;
            for (; it != end; ++it) 
            {
                if (!boost::filesystem::is_directory(*it))
                {
                    file_list.push_back(it->path().string());
                }
            }
		}

	} 
} 
