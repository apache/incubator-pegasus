# pragma once
# include <dsn\internal\nfs.node.impl.h>

namespace dsn { namespace service {
	/*
	void nfs_node_impl::call(std::shared_ptr<remote_copy_request>& rci, aio_task_ptr& calback)
	{
		_nfs_client_impl->begin_remote_copy(rci);
	}

	void nfs_node_impl::start_nfs_copy(::dsn::end_point server)
	{
		std::string a = "mydir";
		std::string b = "";
		std::vector<std::string> c;
		file::copy_remote_files(server, a, c, b, true, LPC_NFS_COPY_FILE, nullptr, nullptr);
	}
	*/
} } 