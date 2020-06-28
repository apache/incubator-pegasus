/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <dsn/tool-api/admission_controller.h>

namespace dsn {

//
////-------------------------- BoundedQueueAdmissionController
///--------------------------------------------------
//
//// arguments: MaxTaskQueueSize
// BoundedQueueAdmissionController::BoundedQueueAdmissionController(task_queue* q,
// std::vector<std::string>& sargs)
//    : admission_controller(q, sargs)
//{
//    if (sargs.size() > 0)
//    {
//        _maxTaskQueueSize = atoi(sargs[0].c_str());
//        if (_maxTaskQueueSize <= 0)
//        {
//            dassert (false, "Invalid arguments for BoundedQueueAdmissionController:
//            MaxTaskQueueSize = '%s'", sargs[0].c_str());
//        }
//    }
//    else
//    {
//        dassert (false, "arguments for BoundedQueueAdmissionController is missing:
//        MaxTaskQueueSize");
//    }
//}
//
// BoundedQueueAdmissionController::~BoundedQueueAdmissionController(void)
//{
//}
//
// bool BoundedQueueAdmissionController::is_task_accepted(task* task)
//{
//    if (InQueueTaskCount() < _maxTaskQueueSize ||
//    task->spec().pool->shared_same_worker_with_current_task(task))
//    {
//        return true;
//    }
//    else
//    {
//        return false;
//    }
//}
//
// int  BoundedQueueAdmissionController::get_system_utilization()
//{
//    return static_cast<int>(100.0 * static_cast<double>InQueueTaskCount() /
//    static_cast<double>_maxTaskQueueSize);
//}
//
////------------------------------ SingleRpcClassResponseTimeAdmissionController
///----------------------------------------------------------------
//
////      args: dsn::task_code PercentileType LatencyThreshold100ns(from task create to end in
/// local process)
////
//
// SingleRpcClassResponseTimeAdmissionController::SingleRpcClassResponseTimeAdmissionController(task_queue*
// q, std::vector<std::string>& sargs)
//    : admission_controller(q, sargs)
//{
//    if (sargs.size() >= 3)
//    {
//        _rpcCode = enum_from_string(sargs[0].c_str(), TASK_CODE_INVALID);
//        _percentile = atoi(sargs[1].c_str());
//        _latencyThreshold100ns = atoi(sargs[2].c_str());
//
//        if (TASK_CODE_INVALID == _rpcCode || task_spec::get(_rpcCode).type !=
//        TASK_TYPE_RPC_REQUEST
//            || _latencyThreshold100ns <= 0
//            || _percentile < 0
//            || _percentile >= 5
//            )
//        {
//            dassert (false, "Invalid arguments for SingleRpcClassResponseTimeAdmissionController:
//            RpcRequestEventCode PercentileType(0-4) LatencyThreshold100ns\n"
//                "\tcounter percentile type (0-4): 999,   99,  95,  90,  50\n");
//        }
//
//        _counter = task_spec::get(_rpcCode).rpc_server_latency_100ns;
//    }
//    else
//    {
//        dassert (false, "arguments for SingleRpcClassResponseTimeAdmissionController is missing:
//        RpcRequestEventCode PercentileType(0-4) LatencyThreshold100ns\n"
//            "\tcounter percentile type (0-4): 999,   99,  95,  90,  50\n");
//    }
//}
//
// SingleRpcClassResponseTimeAdmissionController::~SingleRpcClassResponseTimeAdmissionController(void)
//{
//}
//
// bool SingleRpcClassResponseTimeAdmissionController::is_task_accepted(task* task)
//{
//    if (task->spec().type != TASK_TYPE_RPC_REQUEST
//        //|| task->spec().code == _rpcCode
//        || _counter->get_percentile(_percentile) < _latencyThreshold100ns
//        )
//    {
//        return true;
//    }
//    else
//    {
//        return false;
//    }
//}
//
// int SingleRpcClassResponseTimeAdmissionController::get_system_utilization()
//{
//    return static_cast<int>(100.0 * static_cast<double>_counter->get_percentile(_percentile) /
//    static_cast<double>_latencyThreshold100ns);
//}

} // end namespace
