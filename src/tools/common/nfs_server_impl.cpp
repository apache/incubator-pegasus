# include "nfs_server_impl.h"
# include <cstdlib>
# include <boost/filesystem.hpp>
# include <sys/stat.h>

namespace dsn {
    namespace service {

        void nfs_service_impl::on_copy(const ::dsn::service::copy_request& request, ::dsn::service::rpc_replier<::dsn::service::copy_response>& reply)
        {
            //dinfo(">>> on call RPC_COPY end, exec RPC_NFS_COPY");

            std::string file_path = request.source_dir + request.file_name;            
            handle_t hfile;

            {
                zauto_lock l(_handles_map_lock);
                auto it = _handles_map.find(request.file_name); // find file handle cache first

                if (it == _handles_map.end()) // not found
                {
                    hfile = file::open(file_path.c_str(), O_RDONLY | O_BINARY, 0);
                    if (hfile)
                    {
                    
                        file_handle_info_on_server* fh = new file_handle_info_on_server;
                        fh->file_handle = hfile;
                        fh->file_access_count = 1;
                        fh->last_access_time = dsn::service::env::now_ms();
                        _handles_map.insert(std::pair<std::string, file_handle_info_on_server*>(request.file_name, fh));
                    }
                }
                else // found
                {
                    hfile = it->second->file_handle;
                    it->second->file_access_count++;
                    it->second->last_access_time = dsn::service::env::now_ms();
                }
            }

            if (hfile == 0)
            {
                derror("file open failed");
                ::dsn::service::copy_response resp;
                resp.error = ERR_OBJECT_NOT_FOUND;
                resp.file_name = request.file_name;
                reply(resp);
                return;
            }

            std::shared_ptr<char> buf(new char[_opts.nfs_copy_block_bytes]);
            blob bb(buf, _opts.nfs_copy_block_bytes);

            std::shared_ptr<callback_para> cp(new callback_para(reply));
            cp->bb = bb;
            cp->dst_dir = request.dst_dir;
            cp->file_name = request.file_name;
            cp->hfile = hfile;
            cp->offset = request.offset;
            cp->size = request.size;

            auto task = file::read(
                hfile,
                bb.buffer().get(),
                request.size,
                request.offset,
                LPC_NFS_READ,
                this,
                std::bind(
                    &nfs_service_impl::internal_read_callback,
                    this,
                    std::placeholders::_1,
                    std::placeholders::_2,
                    cp
                )
                );
        }

        void nfs_service_impl::internal_read_callback(error_code err, uint32_t sz, std::shared_ptr<callback_para> cp)
        {
            {
                zauto_lock l(_handles_map_lock);
                auto it = _handles_map.find(cp->file_name);

                if (it != _handles_map.end())
                {
                    it->second->file_access_count--;
                }
            }

            ::dsn::service::copy_response resp;
            resp.error = err;
            resp.file_name = cp->file_name;
            resp.dst_dir = cp->dst_dir;
            resp.file_content = cp->bb;
            resp.offset = cp->offset;
            resp.size = cp->size;

            cp->replier(resp);
        }

        // RPC_NFS_NEW_NFS_GET_FILE_SIZE 
        void nfs_service_impl::on_get_file_size(const ::dsn::service::get_file_size_request& request, ::dsn::service::rpc_replier<::dsn::service::get_file_size_response>& reply)
        {
            //dinfo(">>> on call RPC_NFS_GET_FILE_SIZE end, exec RPC_NFS_GET_FILE_SIZE");

            get_file_size_response resp;
            int err = ERR_OK;
            std::vector<std::string> file_list;
            std::string folder = request.source_dir;
            if (request.file_list.size() == 0) // return all file size in the destination file folder
            {
                if (!::boost::filesystem::exists(folder))
                {
                    err = ERR_OBJECT_NOT_FOUND;
                }
                else
                {
                    get_file_names(folder, file_list);
                    for (size_t i = 0; i < file_list.size(); i++)
                    {
                        struct stat st;
                        ::stat(file_list[i].c_str(), &st);

                        // TODO: using uint64 instead as file ma
                        // Done
                        uint64_t size = st.st_size;

                        resp.size_list.push_back(size);
                        resp.file_list.push_back(file_list[i].substr(request.source_dir.length(), file_list[i].length() - 1));
                    }
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
                    // Done
                    uint64_t size = st.st_size;

                    resp.size_list.push_back(size);
                    resp.file_list.push_back((folder + request.file_list[i]).substr(request.source_dir.length(), (folder + request.file_list[i]).length() - 1));
                }
            }

            resp.error = err;
            reply(resp);
        }

        void nfs_service_impl::close_file() // release out-of-date file handle
        {
            zauto_lock l(_handles_map_lock);

            for (auto it = _handles_map.begin(); it != _handles_map.end();)
            {
                auto fptr = it->second;

                if (fptr->file_access_count == 0 
                    && dsn::service::env::now_ms() - fptr->last_access_time > _opts.file_close_expire_time_ms) // not opened and expired
                {
                    it = _handles_map.erase(it);

                    file::close(fptr->file_handle);

                    delete fptr;
                }
                else
                    it++;
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
