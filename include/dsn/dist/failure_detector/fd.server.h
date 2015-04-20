# pragma once
# include <dsn/serverlet.h>
# include <dsn/dist/failure_detector/fd.code.definition.h>
# include <iostream>

namespace dsn { namespace fd { 
class failure_detector_service 
	: public ::dsn::service::serverlet<failure_detector_service>
{
public:
	failure_detector_service() : ::dsn::service::serverlet<failure_detector_service>("failure_detector") {}
	virtual ~failure_detector_service() {}

protected:
	// all service handlers to be implemented further
	// RPC_FD_FAILURE_DETECTOR_PING 
	virtual void on_ping(const ::dsn::fd::beacon_msg& beacon, ::dsn::service::rpc_replier<::dsn::fd::beacon_ack>& reply)
	{
		std::cout << "... exec RPC_FD_FAILURE_DETECTOR_PING ... (not implemented) " << std::endl;
		::dsn::fd::beacon_ack resp;
		reply(resp);
	}
	
public:
	void open_service()
	{
		this->register_async_rpc_handler(RPC_FD_FAILURE_DETECTOR_PING, "ping", &failure_detector_service::on_ping);
	}

	void close_service()
	{
		this->unregister_rpc_handler(RPC_FD_FAILURE_DETECTOR_PING);
	}
};

} } 