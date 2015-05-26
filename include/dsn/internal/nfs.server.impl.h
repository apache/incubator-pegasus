# pragma once
# include "nfs.server.h"

namespace dsn { namespace service { 
class nfs_service_impl
	: public ::dsn::service::nfs_service, public ::dsn::service::serverlet<nfs_service_impl>
{
public:
	nfs_service_impl(configuration_ptr config) : ::dsn::service::serverlet<nfs_service_impl>("nfs")
	{
		out_of_date = config->get_value<uint32_t>("nfs", "out_of_date", out_of_date);
		max_buf_size = config->get_value<uint32_t>("nfs", "max_buf_size", max_buf_size);
	}
	virtual ~nfs_service_impl() {}

	struct map_value
	{
		handle_t ht;
		int32_t counter;
		uint64_t stime_ms;
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
	// all service handlers to be implemented further
	// RPC_NFS_V2_NFS_COPY 
	virtual void on_copy(const copy_request& request, ::dsn::service::rpc_replier<copy_response>& reply);
	// RPC_NFS_V2_NFS_GET_FILE_SIZE 
	virtual void on_get_file_size(const get_file_size_request& request, ::dsn::service::rpc_replier<get_file_size_response>& reply);
	
public:

	void internal_read_callback(error_code err, int sz, callback_para cp, ::dsn::service::rpc_replier<::dsn::service::copy_response>& reply);

	//std::map <std::string, map_value*> get_handles_map();
	//void erase_handles_map(std::string file_name);

	void close_file();

	void get_file_names(std::string folderPath, std::vector<std::string>& file_list);

	std::map <std::string, map_value*> _handles_map;

private:
	zlock _handles_map_lock;
	uint32_t out_of_date;
	uint32_t max_buf_size;
};

} } 