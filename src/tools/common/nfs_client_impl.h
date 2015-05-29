# pragma once
# include "nfs_client.h"
# include <queue>
# include <dsn/service_api.h>
# include <dsn/internal/nfs.h>

namespace dsn {
	namespace service { 

        struct nfs_opts
        {
			uint32_t max_buf_size;
            int max_concurrent_remote_copy_requests;
            int file_open_expire_time_ms;

            void init(configuration_ptr config)
            {
                max_buf_size = config->get_value<uint32_t>("nfs", "max_buf_size", 32 * 1024 * 1024);
                max_concurrent_remote_copy_requests = config->get_value<uint32_t>("nfs", "max_concurrent_remote_copy_requests", 5);
                file_open_expire_time_ms = config->get_value<uint32_t>("nfs", "file_open_expire_time_ms", 10000);
            }
        };

        class nfs_client_impl
	        : public ::dsn::service::nfs_client
        {
			struct user_request
			{
				get_file_size_request  file_size_req;
				aio_task_ptr           nfs_task;
				std::atomic<int>       copy_request_count;
				std::atomic<bool>      finished;
			};
			struct file_handle_info_on_client
			{
				handle_t file_handle;
				uint32_t copy_request_count;
			};

			

        public:
            nfs_client_impl(const ::dsn::end_point& server, nfs_opts& opts) : nfs_client(server), _opts(opts)
	        { 
		        _server = server; 
				_concurrent_copy_request_count = 0;
	        }

            virtual ~nfs_client_impl() {}

	        void begin_remote_copy(std::shared_ptr<remote_copy_request>& rci, aio_task_ptr nfs_task); // copy file request entry

			void internal_write_callback(error_code err, uint32_t sz, ::std::string file_name, user_request* req);

			std::map <std::string, file_handle_info_on_client*> _copy_request_count_map; // close judgement for each file 
        private:
	        void end_copy(
		        ::dsn::error_code err,
		        const copy_response& resp,
		        void* context); // rewrite end_copy function

	        void end_get_file_size(
		        ::dsn::error_code err,
		        const ::dsn::service::get_file_size_response& resp,
		        void* context); // rewrite end_get_file_size function

            void continue_copy(int done_count = 0);

            struct user_request;
            void write_copy(error_code err, user_request* req, const ::dsn::service::copy_response& resp);
            
        private:

			struct copy_request_ex
            {
                copy_request           copy_req;
                user_request           *user_req;
            };

        private:
            nfs_opts         &_opts;
	        ::dsn::end_point _server;

	        zlock            _lock;
			zlock _handles_map_lock;
			zlock _copy_request_count_map_lock;

	        int              _concurrent_copy_request_count; // concurrent request count
            std::queue<copy_request_ex*> _req_copy_file_queue; // used to store the blocked requests
			std::map <std::string, handle_t> _handles_map; // cache file handles
        };

    } 
} 