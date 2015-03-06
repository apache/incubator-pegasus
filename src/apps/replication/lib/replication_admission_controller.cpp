#include "replication_admission_controller.h"

namespace rdsn { namespace replication {

replication_admission_controller::replication_admission_controller(task_queue* q, std::vector<std::string>& sargs)
    : admission_controller(q, sargs)
{
}

replication_admission_controller::~replication_admission_controller(void)
{
}

bool replication_admission_controller::is_task_accepted(task_ptr& task)
{
    if (task->code() != RPC_REPLICATION_CLIENT_WRITE && task->code() != RPC_REPLICATION_CLIENT_READ)
        return true;

    // read latency 
    
    return true;
}

int  replication_admission_controller::get_syste_utilization()
{
    return 0;
}

}} // end namespace
