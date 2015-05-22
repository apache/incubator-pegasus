# pragma once
# include "nfs.client.impl.h"
# include "nfs.server.impl.h"
namespace dsn { namespace service {
class nfs_node_impl
{
public:
	nfs_node_impl(const ::dsn::end_point& server)
	{
		_nfs_client_impl = new nfs_client_impl(server);
	}

	nfs_node_impl() 
	{
		_nfs_service_impl = new nfs_service_impl();
	}

	virtual ~nfs_node_impl() {}

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