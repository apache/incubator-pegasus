# pragma once
# include <dsn/internal/service.api.oo.h>
# include <dsn/dist/failure_detector/fd.code.definition.h>
# include <iostream>


namespace dsn { namespace fd { 
class failure_detector_client 
	: public virtual ::dsn::service::servicelet
{
public:
	failure_detector_client(const ::dsn::end_point& server) { _server = server; }
	failure_detector_client() { _server = ::dsn::end_point::INVALID; }
	virtual ~failure_detector_client() {}


	// ---------- call RPC_FD_FAILURE_DETECTOR_PING ------------
	// - synchronous 
	::dsn::error_code ping(
		const ::dsn::fd::beacon_msg& beacon, 
		__out_param ::dsn::fd::beacon_ack& resp, 
		int timeout_milliseconds = 0, 
		int hash = 0,
		const ::dsn::end_point *p_server_addr = nullptr)
	{
		::dsn::message_ptr msg = ::dsn::message::create_request(RPC_FD_FAILURE_DETECTOR_PING, timeout_milliseconds, hash);
		marshall(msg->writer(), beacon);
		auto resp_task = ::dsn::service::rpc::call(p_server_addr ? *p_server_addr : _server, msg, nullptr);
		resp_task->wait();
		if (resp_task->error() == ::dsn::ERR_SUCCESS)
		{
			unmarshall(resp_task->get_response()->reader(), resp);
		}
		return resp_task->error();
	}
	
	// - asynchronous with on-stack ::dsn::fd::beacon_msg and ::dsn::fd::beacon_ack 
	::dsn::rpc_response_task_ptr begin_ping(
		const ::dsn::fd::beacon_msg& beacon, 		
		int timeout_milliseconds = 0, 
		int reply_hash = 0,
		int request_hash = 0,
		const ::dsn::end_point *p_server_addr = nullptr)
	{
		return ::dsn::service::rpc::call_typed(
					p_server_addr ? *p_server_addr : _server, 
					RPC_FD_FAILURE_DETECTOR_PING, 
					beacon, 
					this, 
					&failure_detector_client::end_ping, 
					request_hash, 
					timeout_milliseconds, 
					reply_hash
					);
	}

	virtual void end_ping(
		::dsn::error_code err, 
		const ::dsn::fd::beacon_ack& resp)
	{
		if (err != ::dsn::ERR_SUCCESS) std::cout << "reply RPC_FD_FAILURE_DETECTOR_PING err : " << err.to_string() << std::endl;
		else
		{
			std::cout << "reply RPC_FD_FAILURE_DETECTOR_PING ok" << std::endl;
		}
	}
	
	// - asynchronous with on-heap std::shared_ptr<::dsn::fd::beacon_msg> and std::shared_ptr<::dsn::fd::beacon_ack> 
	::dsn::rpc_response_task_ptr begin_ping2(
		std::shared_ptr<::dsn::fd::beacon_msg>& beacon, 		
		int timeout_milliseconds = 0, 
		int reply_hash = 0,
		int request_hash = 0,
		const ::dsn::end_point *p_server_addr = nullptr)
	{
		return ::dsn::service::rpc::call_typed(
					p_server_addr ? *p_server_addr : _server, 
					RPC_FD_FAILURE_DETECTOR_PING, 
					beacon, 
					this, 
					&failure_detector_client::end_ping2, 
					request_hash, 
					timeout_milliseconds, 
					reply_hash
					);
	}

	virtual void end_ping2(
		::dsn::error_code err, 
		std::shared_ptr<::dsn::fd::beacon_msg>& beacon, 
		std::shared_ptr<::dsn::fd::beacon_ack>& resp)
	{
		if (err != ::dsn::ERR_SUCCESS) std::cout << "reply RPC_FD_FAILURE_DETECTOR_PING err : " << err.to_string() << std::endl;
		else
		{
			std::cout << "reply RPC_FD_FAILURE_DETECTOR_PING ok" << std::endl;
		}
	}
	

private:
	::dsn::end_point _server;
};

} } 