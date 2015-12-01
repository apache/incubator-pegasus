#pragma once

#include <dsn/cpp/autoref_ptr.h>
#include <dsn/cpp/clientlet.h>
#include <dsn/dist/distributed_lock_service.h>
#include <dsn/cpp/auto_codes.h>

namespace dsn { namespace dist{
    
DEFINE_THREAD_POOL_CODE(THREAD_POOL_DLOCK)
DEFINE_TASK_CODE(TASK_CODE_DLOCK, TASK_PRIORITY_HIGH, THREAD_POOL_DLOCK)

class distributed_lock_service_zookeeper;
class lock_struct;
typedef ref_ptr<distributed_lock_service_zookeeper> lock_srv_ptr;
typedef ref_ptr<lock_struct> lock_struct_ptr;

typedef safe_late_task<distributed_lock_service::lock_callback>* lock_task_t;
typedef safe_late_task<distributed_lock_service::err_callback>* unlock_task_t;

}}
