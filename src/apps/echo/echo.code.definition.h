# pragma once
# include <dsn/service_api_cpp.h>
# include "echo.types.h"

namespace dsn { namespace example { 
    // define your own thread pool using DEFINE_THREAD_POOL_CODE(xxx)
    // define RPC task code for service 'echo'
    DEFINE_TASK_CODE_RPC(RPC_ECHO_ECHO_PING, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
    // test timer task code
    DEFINE_TASK_CODE(LPC_ECHO_TEST_TIMER, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
} } 
