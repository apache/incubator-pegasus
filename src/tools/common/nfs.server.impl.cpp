# pragma once
# include <dsn/internal/nfs.server.impl.h>
# include <io.h>
#include "stdlib.h"
#include "direct.h"

namespace dsn { 
	namespace service { 

		void nfs_service_impl::on_copy(const ::dsn::service::copy_request& request, ::dsn::service::rpc_replier<::dsn::service::copy_response>& reply)
		{
			std::cout << ">>> on call RPC_COPY end, exec RPC_NFS_COPY" << std::endl;

			std::string file_path = request.dst_dir + request.file_name;
			std::shared_ptr<char> buf(new char[max_buf_size]);
			blob bb(buf, max_buf_size);
			handle_t hfile;

			{
				zauto_lock l(_handles_map_lock);
				auto it = _handles_map.find(request.file_name);

				if (it == _handles_map.end())
				{
					hfile = file::open(file_path.c_str(), O_RDONLY, 0);
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
				else
				{
					hfile = it->second->ht;
					it->second->counter++;
					it->second->stime_ms = dsn::service::env::now_ms();
				}
			}
			

			callback_para cp = { hfile, request.file_name, bb, request.offset, request.size };

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
				derror("file operation failed, err = %u", err);
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
			resp.file_content = blob(cp.bb.buffer(), 0, cp.size);

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
			std::string folder = request.dst_dir;
			if (request.file_list.size() == 0)
			{
				get_file_names(folder, file_list);
				for (int i = 0; i < file_list.size(); i++)
				{
					std::cout << file_list[i] << std::endl;
					FILE *file = fopen((file_list[i]).c_str(), "r");
					if (file == NULL)
					{
						derror("file open error!\n");
						err = ERR_OBJECT_NOT_FOUND;
						break;
					}
					int size = _filelength(fileno(file));
					fseek(file, 0, SEEK_SET);
					fclose(file);

					resp.size_list.push_back(size);
					resp.file_list.push_back(file_list[i].substr(request.dst_dir.length(), file_list[i].length()-1));
				}
			}
			else
			{
				for (int i = 0; i < request.file_list.size(); i++)
				{
					std::string file_path = folder + request.file_list[i];

					FILE *file = fopen(file_path.c_str(), "r");
					if (file == NULL)
					{
						derror("file open error!\n");
						err = ERR_OBJECT_NOT_FOUND;
						break;
					}
					int size = _filelength(fileno(file));
					fseek(file, 0, SEEK_SET);
					fclose(file);

					resp.size_list.push_back(size);
					resp.file_list.push_back((folder + request.file_list[i]).substr(request.dst_dir.length(), (folder + request.file_list[i]).length() - 1));
				}
			}

			resp.error = err;
			reply(resp);
		}	

		void nfs_service_impl::close_file()
		{
			error_code err;
			{
				zauto_lock l(_handles_map_lock);

				if (_handles_map.size() == 0)
					return;

				for (auto it = _handles_map.begin(); it != _handles_map.end();)
				{
					if (it->second->counter == 0 && dsn::service::env::now_ms() - it->second->stime_ms > out_of_date) // not opened and expired
					{
						err = file::close(it->second->ht);
						_handles_map.erase(it++);
						if (err != 0)
						{
							derror("close file error: %u", err);
						}
					}
					else
						it++;
				}
			}
		}

		void nfs_service_impl::get_file_names(std::string folderPath, std::vector<std::string>& file_list)
		{
			_finddata_t FileInfo;
			std::string strfind = folderPath + "*";
			intptr_t Handle = _findfirst(strfind.c_str(), &FileInfo);

			if (Handle == -1L)
			{
				std::cout << "can not match the folder path" << std::endl;
				return;
			}
			do{
				if (FileInfo.attrib & _A_SUBDIR)
				{
					if ((strcmp(FileInfo.name, ".") != 0) && (strcmp(FileInfo.name, "..") != 0))
					{
						std::string newPath = folderPath  + FileInfo.name + "/";
						get_file_names(newPath, file_list);
					}
				}
				else
				{
					file_list.push_back(folderPath + FileInfo.name);
				} 
			} while (_findnext(Handle, &FileInfo) == 0);

			_findclose(Handle);
			return;
		}

	} 
} 