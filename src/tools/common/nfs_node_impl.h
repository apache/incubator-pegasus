# pragma once
# include <dsn/tool_api.h>
# include "nfs_client_impl.h"
# include "nfs_server_impl.h"
# include <dsn/internal/nfs.h>


namespace dsn { namespace service {
class nfs_node_impl : public nfs_node
{
public:
	nfs_node_impl(service_node* node) : nfs_node(node) {}

	nfs_node_impl(configuration_ptr config, service_node* node) : nfs_node(node)
	{
		_nfs_service_impl = new nfs_service_impl(config); // new a nfs server
	}

	virtual ~nfs_node_impl(void) { }

	void call(std::shared_ptr<remote_copy_request> rci, aio_task_ptr& callback)
	{
		_nfs_client_impl = new nfs_client_impl(rci->source, ::dsn::service::system::config()); // bew a nfs client
		_nfs_client_impl->begin_remote_copy(rci, callback); // copy file request entry
	}

	nfs_client_impl* get_nfs_client_impl()
	{
		return _nfs_client_impl;
	}

	nfs_service_impl* get_nfs_service_impl()
	{
		return _nfs_service_impl;
	}

private:
	nfs_client_impl *_nfs_client_impl;
	nfs_service_impl *_nfs_service_impl;
};

} } 