# pragma once
# include <dsn/service_api.h>
# include "simple_kv.types.h"

namespace dsn { namespace replication { namespace application { 
	// define RPC task code for service 'simple_kv'
	DEFINE_TASK_CODE_RPC(RPC_SIMPLE_KV_SIMPLE_KV_READ, ::dsn::TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
	DEFINE_TASK_CODE_RPC(RPC_SIMPLE_KV_SIMPLE_KV_WRITE, ::dsn::TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
	DEFINE_TASK_CODE_RPC(RPC_SIMPLE_KV_SIMPLE_KV_APPEND, ::dsn::TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
	// test timer task code
	DEFINE_TASK_CODE(LPC_SIMPLE_KV_TEST_TIMER, ::dsn::TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
} } } 
