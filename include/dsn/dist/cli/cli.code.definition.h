#pragma once
#include <dsn/service_api_cpp.h>

namespace dsn {
// define your own thread pool using DEFINE_THREAD_POOL_CODE(xxx)
// define RPC task code for service 'cli'
DEFINE_TASK_CODE_RPC(RPC_CLI_CLI_CALL, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
}
