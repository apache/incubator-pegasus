# pragma once
# include "nfs_server.h"
# include "nfs_client_impl.h"
# include <dsn/service_api.h>

namespace dsn {
    namespace service {
        class nfs_service_impl
            : public ::dsn::service::nfs_service, public ::dsn::service::serverlet<nfs_service_impl>
        {
        public:
            nfs_service_impl(nfs_opts& opts) :
                ::dsn::service::serverlet<nfs_service_impl>("nfs"), _opts(opts)
            {
                _file_close_timer = ::dsn::service::tasking::enqueue(LPC_NFS_FILE_CLOSE_TIMER, 
                    this, &nfs_service_impl::close_file, 0, 0, opts.file_close_timer_interval_ms_on_server);
            }
            virtual ~nfs_service_impl() {}

        protected:
            // RPC_NFS_V2_NFS_COPY 
            virtual void on_copy(const copy_request& request, ::dsn::service::rpc_replier<copy_response>& reply);
            // RPC_NFS_V2_NFS_GET_FILE_SIZE 
            virtual void on_get_file_size(const get_file_size_request& request, ::dsn::service::rpc_replier<get_file_size_response>& reply);

        private:
            struct callback_para
            {
                handle_t hfile;
                std::string file_name;
                std::string dst_dir;
                blob bb;
                uint64_t offset;
                uint32_t size;
                rpc_replier<copy_response> replier;

                callback_para(rpc_replier<copy_response>& r) : replier(r){}
            };

            struct file_handle_info_on_server
            {
                handle_t file_handle;
                int32_t file_access_count; // concurrent r/w count
                uint64_t last_access_time; // last touch time
            };

            void internal_read_callback(error_code err, uint32_t sz, std::shared_ptr<callback_para> cp);

            void close_file();

            void get_file_names(std::string dir, std::vector<std::string>& file_list);

        private:
            nfs_opts  &_opts;

            zlock _handles_map_lock;
            std::map <std::string, file_handle_info_on_server*> _handles_map; // cache file handles

            ::dsn::task_ptr _file_close_timer;
        };

    }
}