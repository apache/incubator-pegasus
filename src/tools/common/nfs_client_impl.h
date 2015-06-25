# pragma once
# include "nfs_client.h"
# include <queue>
# include <dsn/service_api.h>
# include <dsn/internal/nfs.h>

namespace dsn {
    namespace service {

        struct nfs_opts
        {
            uint32_t nfs_copy_block_bytes;
            int max_concurrent_remote_copy_requests;
            int max_concurrent_local_writes;

            int file_close_expire_time_ms;
            int file_close_timer_interval_ms_on_server;
            int max_file_copy_request_count_per_file;

            void init(configuration_ptr config)
            {
                nfs_copy_block_bytes = config->get_value<uint32_t>("nfs", "nfs_copy_block_bytes", 4*1024*1024);
                max_concurrent_remote_copy_requests = config->get_value<uint32_t>("nfs", "max_concurrent_remote_copy_requests", 50);
                max_concurrent_local_writes = config->get_value<uint32_t>("nfs", "max_concurrent_local_writes", 5);
                file_close_expire_time_ms = config->get_value<uint32_t>("nfs", "file_close_expire_time_ms", 3*60*1000);
                file_close_timer_interval_ms_on_server = config->get_value<uint32_t>("nfs", "file_close_timer_interval_ms_on_server", 2*60*1000);
                max_file_copy_request_count_per_file = config->get_value<uint32_t>("nfs", "max_file_copy_request_count_per_file", 10); // limit each file copy speed
            }
        };

        class nfs_client_impl
            : public ::dsn::service::nfs_client
        {
        public:
            struct user_request;
            struct file_context;
            struct copy_request_ex : public ::dsn::ref_object
            {
                file_context *file_ctx;
                int           index;
                copy_request  copy_req;                             
                copy_response response;
                task_ptr      remote_copy_task;
                task_ptr      local_write_task;
                bool          is_ready_for_write;
                bool          is_valid;
                zlock         lock;

                copy_request_ex(file_context* file, int idx)
                {
                    file_ctx = file;
                    index = idx;
                    remote_copy_task = nullptr;
                    local_write_task = nullptr;
                    is_ready_for_write = false;
                    is_valid = true;
                }
            };

            struct file_context
            {
                user_request  *user_req;

                std::string file_name;
                uint64_t    file_size;
                error_code  err;

                std::atomic<handle_t> file;
                int         current_write_index;
                int         finished_segments;
                std::vector<boost::intrusive_ptr<copy_request_ex> > copy_requests;

                file_context(user_request* req, const std::string& file_nm, uint64_t sz)
                {
                    user_req = req;
                    file_name = file_nm;
                    file_size = sz;
                    err = ERR_IO_PENDING;
                    file = static_cast<handle_t>(0);

                    current_write_index = -1;
                    finished_segments = 0;
                }
            };

            struct user_request
            {
                zlock                   user_req_lock;

                get_file_size_request  file_size_req;
                aio_task_ptr           nfs_task;
                std::atomic<int>       finished_files;
                bool                   is_finished;

                std::map<std::string, file_context*> file_context_map; // map file name and file info

                user_request()
                {
                    nfs_task = nullptr;
                    finished_files = 0;
                }
            };
                        
        public:
            nfs_client_impl(nfs_opts& opts) : nfs_client(end_point::INVALID), _opts(opts)
            {
                _concurrent_copy_request_count = 0;
                _concurrent_local_write_count = 0;
            }

            virtual ~nfs_client_impl() {}

            void begin_remote_copy(std::shared_ptr<remote_copy_request>& rci, aio_task_ptr nfs_task); // copy file request entry

            void local_write_callback(error_code err, uint32_t sz, boost::intrusive_ptr<copy_request_ex> reqc); // write file callback

        private:
            void end_copy(
                ::dsn::error_code err,
                const copy_response& resp,
                void* context); // rewrite end_copy function

            void end_get_file_size(
                ::dsn::error_code err,
                const ::dsn::service::get_file_size_response& resp,
                void* context); // rewrite end_get_file_size function

            void continue_copy(int done_count);

            void write_copy(boost::intrusive_ptr<copy_request_ex> reqc);

            void continue_write();

            void handle_completion(user_request *req, error_code err);

        private:
            nfs_opts         &_opts;

            std::atomic<int> _concurrent_copy_request_count; // record concurrent request count, need be limitted above max_concurrent_remote_copy_requests
            std::atomic<int> _concurrent_local_write_count; // 

            zlock                            _copy_requests_lock;
            std::queue <boost::intrusive_ptr<copy_request_ex> >    _copy_requests;

            zlock                            _local_writes_lock;
            std::queue <boost::intrusive_ptr<copy_request_ex> >    _local_writes;
        };


        DEFINE_REF_OBJECT(nfs_client_impl::copy_request_ex);

    }
}
