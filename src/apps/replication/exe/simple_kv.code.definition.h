/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus(rDSN) -=- 
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
