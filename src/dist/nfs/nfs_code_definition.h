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
#pragma once

namespace dsn {
namespace service {
// define RPC task code for service 'nfs'
DEFINE_TASK_CODE_RPC(RPC_NFS_COPY, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE_RPC(RPC_NFS_GET_FILE_SIZE, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
// test timer task code
DEFINE_TASK_CODE(LPC_NFS_REQUEST_TIMER, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

DEFINE_TASK_CODE_AIO(LPC_NFS_READ, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE(LPC_NFS_FILE_CLOSE_TIMER, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

DEFINE_TASK_CODE_AIO(LPC_NFS_WRITE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

DEFINE_TASK_CODE_AIO(LPC_NFS_COPY_FILE, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
}
}
