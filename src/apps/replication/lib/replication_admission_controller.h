#pragma once

#include <rdsn/tool_api.h>
using namespace rdsn::service;
#include "codes.h"

namespace rdsn { namespace replication {

class replication_admission_controller :
    public admission_controller
{
public:
    replication_admission_controller(task_queue* q, std::vector<std::string>& sargs);
    ~replication_admission_controller(void);

private:
    virtual bool is_task_accepted(task_ptr& task);
    virtual int  get_syste_utilization();

private:

};

}} // end namespace
