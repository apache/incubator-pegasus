# pragma once
# include "nfs_server.h"
# include "nfs_client_impl.h"

namespace dsn { namespace service { 
class nfs_service_impl
	: public ::dsn::service::nfs_service, public ::dsn::service::serverlet<nfs_service_impl>
{
public:
	nfs_service_impl(nfs_opts& opts) : 
        ::dsn::service::serverlet<nfs_service_impl>("nfs"), _opts(opts)
	{
        _file_timer = ::dsn::service::tasking::enqueue(LPC_NFS_FILE_CLOSE_TIMER, this, &nfs_service_impl::close_file, 0, 0, 30000);
	}
	virtual ~nfs_service_impl() {}

protected:
	// RPC_NFS_V2_NFS_COPY 
	virtual void on_copy(const copy_request& request, ::dsn::service::rpc_replier<copy_response>& reply);
	// RPC_NFS_V2_NFS_GET_FILE_SIZE 
	virtual void on_get_file_size(const get_file_size_request& request, ::dsn::service::rpc_replier<get_file_size_response>& reply);
	
private:
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
		std::string dst_dir;
        blob bb;
        int32_t offset;
        int32_t size;
    };

	void internal_read_callback(error_code err, int sz, callback_para cp, ::dsn::service::rpc_replier<::dsn::service::copy_response>& reply);

	void close_file();

	void get_file_names(std::string dir, std::vector<std::string>& file_list);

private:
    nfs_opts  &_opts;

	zlock _handles_map_lock;
	uint32_t file_open_expire_time_ms; // file expiration time
	std::map <std::string, map_value*> _handles_map; // cache file handles

    ::dsn::task_ptr _file_timer;
};

} } 