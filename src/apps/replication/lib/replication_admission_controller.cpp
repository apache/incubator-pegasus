/*
 * The MIT License (MIT)

 * Copyright (c) 2015 Microsoft Corporation

 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:

 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.

 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
#include "replication_admission_controller.h"

namespace dsn { namespace replication {

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
