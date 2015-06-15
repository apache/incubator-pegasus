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
			int file_close_time;
			int client_close_time;
			int max_request_step;

            void init(configuration_ptr config)
            {
				max_buf_size = config->get_value<uint32_t>("nfs", "max_buf_size", max_buf_size);
				max_concurrent_remote_copy_requests = config->get_value<uint32_t>("nfs", "max_concurrent_remote_copy_requests", max_concurrent_remote_copy_requests);
				file_open_expire_time_ms = config->get_value<uint32_t>("nfs", "file_open_expire_time_ms", file_open_expire_time_ms);
				file_close_time = config->get_value<uint32_t>("nfs", "file_close_time", file_close_time);
				client_close_time = config->get_value<uint32_t>("nfs", "client_close_time", client_close_time);
				max_request_step = config->get_value<uint32_t>("nfs", "max_request_step", max_request_step); // limit each file copy speed
            }
        };

        class nfs_client_impl
	        : public ::dsn::service::nfs_client
        {
			struct resp_copy_file_info
			{
				uint64_t current_offset;
				std::map<uint64_t, copy_response> copy_response_map; // map write offset and copy response
				int finished_count; // when finished_count == copy_count, file copy success
				int copy_count;
			};

			struct file_context
			{
				uint64_t file_size;
				error_code err;
				resp_copy_file_info* resp_info; // store the response rpc
			};

			struct user_request
			{
				zlock				   user_req_lock;
				get_file_size_request  file_size_req;
				aio_task_ptr           nfs_task;
				std::atomic<int>       copy_request_count;
				std::atomic<bool>      finished;

				std::vector<task_ptr>	task_ptr_list;
				std::vector<std::queue<copy_request*>> req_copy_file_vector; // store the request rpc, one file one queue
				std::map<std::string, file_context*> file_context_map; // map file name and file info
			};

			struct copy_request_ex
			{
				copy_request copy_req;
				user_request *user_req;
			};
			
        public:
            nfs_client_impl(const ::dsn::end_point& server, nfs_opts& opts) : nfs_client(server), _opts(opts)
	        { 
		        _server = server; 
				_concurrent_copy_request_count = 0;
	        }

            virtual ~nfs_client_impl() {}

	        void begin_remote_copy(std::shared_ptr<remote_copy_request>& rci, aio_task_ptr nfs_task); // copy file request entry

			void internal_write_callback(error_code err, uint32_t sz, ::std::string file_name, user_request* req); // write file callback

        private:
	        void end_copy(
		        ::dsn::error_code err,
		        const copy_response& resp,
		        void* context); // rewrite end_copy function

	        void end_get_file_size(
		        ::dsn::error_code err,
		        const ::dsn::service::get_file_size_response& resp,
		        void* context); // rewrite end_get_file_size function

			void continue_copy(int done_count, user_request* req);

			void write_copy(user_request* req, const ::dsn::service::copy_response& resp);

			void write_file(user_request* req);

			void handle_fault(std::string file_path, user_request *req, error_code err);
            
			void handle_success(std::string file_path, user_request *req, error_code err);

			void handle_finish(std::string file_path, user_request *req, error_code err);
        
			void garbage_collect(user_request* req);

        private:
            nfs_opts         &_opts;
	        ::dsn::end_point _server;

	        int              _concurrent_copy_request_count; // record concurrent request count, need be limitted above max_concurrent_remote_copy_requests
			
			std::map <std::string, handle_t> _handles_map; // cache file handles for write op, TODO: multi user request write the same file conflict
        };
    } 
} 