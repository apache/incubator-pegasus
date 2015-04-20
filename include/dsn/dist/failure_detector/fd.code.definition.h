# pragma once
# include <dsn/service_api.h>
# include <dsn/dist/failure_detector/fd.types.h>

namespace dsn { namespace fd { 
	// define RPC task code for service 'failure_detector'
	DEFINE_TASK_CODE_RPC(RPC_FD_FAILURE_DETECTOR_PING, ::dsn::TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
	// test timer task code
	DEFINE_TASK_CODE(LPC_FD_TEST_TIMER, ::dsn::TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
} } 
