# pragma once
# include <dsn/dist/replication.h>
# include "simple_kv.code.definition.h"
# include <iostream>

namespace dsn { namespace replication { namespace application { 
class simple_kv_service 
	: public ::dsn::replication::replication_app_base
{
public:
	simple_kv_service(::dsn::replication::replica* replica, ::dsn::configuration_ptr& config) 
		: ::dsn::replication::replication_app_base(replica, config)
	{
		open_service();
	}
	
	virtual ~simple_kv_service() 
	{
		close_service();
	}

protected:
	// all service handlers to be implemented further
	// RPC_SIMPLE_KV_SIMPLE_KV_READ 
	virtual void on_read(const std::string& key, ::dsn::service::rpc_replier<std::string>& reply)
	{
		std::cout << "... exec RPC_SIMPLE_KV_SIMPLE_KV_READ ... (not implemented) " << std::endl;
		std::string resp;
		reply(resp);
	}
	// RPC_SIMPLE_KV_SIMPLE_KV_WRITE 
	virtual void on_write(const ::dsn::replication::application::kv_pair& pr, ::dsn::service::rpc_replier<int32_t>& reply)
	{
		std::cout << "... exec RPC_SIMPLE_KV_SIMPLE_KV_WRITE ... (not implemented) " << std::endl;
		int32_t resp;
		reply(resp);
	}
	// RPC_SIMPLE_KV_SIMPLE_KV_APPEND 
	virtual void on_append(const ::dsn::replication::application::kv_pair& pr, ::dsn::service::rpc_replier<int32_t>& reply)
	{
		std::cout << "... exec RPC_SIMPLE_KV_SIMPLE_KV_APPEND ... (not implemented) " << std::endl;
		int32_t resp;
		reply(resp);
	}
	
public:
	void open_service()
	{
		this->register_async_rpc_handler(RPC_SIMPLE_KV_SIMPLE_KV_READ, "read", &simple_kv_service::on_read);
		this->register_async_rpc_handler(RPC_SIMPLE_KV_SIMPLE_KV_WRITE, "write", &simple_kv_service::on_write);
		this->register_async_rpc_handler(RPC_SIMPLE_KV_SIMPLE_KV_APPEND, "append", &simple_kv_service::on_append);
	}

	void close_service()
	{
		this->unregister_rpc_handler(RPC_SIMPLE_KV_SIMPLE_KV_READ);
		this->unregister_rpc_handler(RPC_SIMPLE_KV_SIMPLE_KV_WRITE);
		this->unregister_rpc_handler(RPC_SIMPLE_KV_SIMPLE_KV_APPEND);
	}
};

} } } 