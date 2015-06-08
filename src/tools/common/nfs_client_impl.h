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

            void init(configuration_ptr config)
            {
				max_buf_size = config->get_value<uint32_t>("nfs", "max_buf_size", max_buf_size);
				max_concurrent_remote_copy_requests = config->get_value<uint32_t>("nfs", "max_concurrent_remote_copy_requests", max_concurrent_remote_copy_requests);
				file_open_expire_time_ms = config->get_value<uint32_t>("nfs", "file_open_expire_time_ms", file_open_expire_time_ms);
				file_close_time = config->get_value<uint32_t>("nfs", "file_close_time", file_close_time);
				client_close_time = config->get_value<uint32_t>("nfs", "client_close_time", client_close_time);
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

			struct resp_copy_file_info
			{
				uint64_t current_offset;
				std::map<uint64_t, copy_response> copy_response_map; // map offset and response 
				//std::vector<copy_response> copy_response_vector;
				int finished_count;
				int copy_count;
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

			//std::map <std::string, file_handle_info_on_client*> _copy_request_count_map; // close judgement for each file 

		private:

			struct copy_request_ex
			{
				copy_request           copy_req;
				user_request           *user_req;
			};

        private:
	        void end_copy(
		        ::dsn::error_code err,
		        const copy_response& resp,
		        void* context); // rewrite end_copy function

	        void end_get_file_size(
		        ::dsn::error_code err,
		        const ::dsn::service::get_file_size_response& resp,
		        void* context); // rewrite end_get_file_size function

			//void continue_copy(int done_count, user_request* req);
			void continue_copy(int done_count = 0);

            struct user_request;
            //void write_copy(error_code err, user_request* req, const ::dsn::service::copy_response& resp);
			void write_copy(user_request* req, const ::dsn::service::copy_response& resp);

			void write_file(user_request* req);

			void handle_fault(std::string file_path, user_request *req, error_code err);
            
			void handle_success(std::string file_path, user_request *req, error_code err);

			void handle_finish(std::string file_path, user_request *req, error_code err);
        
        private:
            nfs_opts         &_opts;
	        ::dsn::end_point _server;

	        zlock            _lock;

	        int              _concurrent_copy_request_count; // concurrent request count

            std::queue<copy_request_ex*> _req_copy_file_queue; // store the client requests
			std::map <std::string, handle_t> _handles_map; // cache file handles
			std::map<std::string, resp_copy_file_info> _resp_copy_file_map; // store the returned rpc
			std::map<std::string, uint64_t> _file_size_map; // map file name and size
			std::map<std::string, error_code> _file_failure_map; // flag file failure info
			std::map<std::string, std::string> _file_path_map; // map file name and path
        };

    } 
} 