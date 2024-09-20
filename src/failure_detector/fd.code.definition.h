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

#pragma once

#include "task/task_code.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "task/task_code.h"
#include "common/gpid.h"
#include "rpc/serialization.h"
#include "rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "rpc/rpc_address.h"
#include "fd_types.h"

namespace dsn {
namespace fd {

DEFINE_THREAD_POOL_CODE(THREAD_POOL_DEFAULT)

// define RPC task code for service 'failure_detector'
DEFINE_TASK_CODE_RPC(RPC_FD_FAILURE_DETECTOR_PING, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
// test timer task code
DEFINE_TASK_CODE(LPC_FD_TEST_TIMER, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

inline bool is_failure_detector_message(dsn::task_code code)
{
    return code == RPC_FD_FAILURE_DETECTOR_PING || code == RPC_FD_FAILURE_DETECTOR_PING_ACK;
}
} // namespace fd
} // namespace dsn
