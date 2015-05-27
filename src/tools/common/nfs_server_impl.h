# pragma once
# include "nfs_server.h"

namespace dsn { namespace service { 
class nfs_service_impl
	: public ::dsn::service::nfs_service, public ::dsn::service::serverlet<nfs_service_impl>
{
public:
	nfs_service_impl(configuration_ptr config) : ::dsn::service::serverlet<nfs_service_impl>("nfs") // get para values from configuration file
	{
		out_of_date = config->get_value<uint32_t>("nfs", "out_of_date", out_of_date);
		max_buf_size = config->get_value<uint32_t>("nfs", "max_buf_size", max_buf_size);
	}
	virtual ~nfs_service_impl() {}

	struct map_value
	{
		handle_t ht;
		int32_t counter; // concurrent r/w count
		uint64_t stime_ms; // last touch time
	};

	struct callback_para
	{
		handle_t hfile;
		std::string file_name;
		blob bb;
		int32_t offset;
		int32_t size;
	};
protected:
	// RPC_NFS_V2_NFS_COPY 
	virtual void on_copy(const copy_request& request, ::dsn::service::rpc_replier<copy_response>& reply);
	// RPC_NFS_V2_NFS_GET_FILE_SIZE 
	virtual void on_get_file_size(const get_file_size_request& request, ::dsn::service::rpc_replier<get_file_size_response>& reply);
	
public:

	void internal_read_callback(error_code err, int sz, callback_para cp, ::dsn::service::rpc_replier<::dsn::service::copy_response>& reply);

	void close_file();

	void get_file_names(std::string folderPath, std::vector<std::string>& file_list);

private:
	zlock _handles_map_lock;
	uint32_t out_of_date; // file expiration time
	uint32_t max_buf_size; // max size of rpc message
	std::map <std::string, map_value*> _handles_map; // cache file handles
};

} } 