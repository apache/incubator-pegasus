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
# pragma once

# include <dsn/internal/utils.h>
# include <dsn/internal/threadpool_code.h>
# include <dsn/internal/enum_helper.h>
# include <dsn/internal/perf_counter.h>
# include <dsn/internal/customizable_id.h>
# include <dsn/internal/singleton_vector_store.h>
# include <dsn/internal/join_point.h>
# include <dsn/internal/extensible_object.h>

namespace dsn {

enum task_type
{    
    TASK_TYPE_RPC_REQUEST,
    TASK_TYPE_RPC_RESPONSE,
    TASK_TYPE_COMPUTE,
    TASK_TYPE_AIO,
    TASK_TYPE_CONTINUATION,
    TASK_TYPE_COUNT,
    TASK_TYPE_INVALID,
};

ENUM_BEGIN(task_type, TASK_TYPE_INVALID)    
    ENUM_REG(TASK_TYPE_RPC_REQUEST)
    ENUM_REG(TASK_TYPE_RPC_RESPONSE)
    ENUM_REG(TASK_TYPE_COMPUTE)
    ENUM_REG(TASK_TYPE_AIO)
    ENUM_REG(TASK_TYPE_CONTINUATION)
ENUM_END(task_type)

enum task_priority
{
    TASK_PRIORITY_LOW,
    TASK_PRIORITY_COMMON,
    TASK_PRIORITY_HIGH,
    TASK_PRIORITY_COUNT,
    TASK_PRIORITY_INVALID,
};

ENUM_BEGIN(task_priority, TASK_PRIORITY_INVALID)
    ENUM_REG(TASK_PRIORITY_LOW)
    ENUM_REG(TASK_PRIORITY_COMMON)
    ENUM_REG(TASK_PRIORITY_HIGH)
ENUM_END(task_priority)

enum task_state
{
    TASK_STATE_READY,
    TASK_STATE_RUNNING,
    TASK_STATE_FINISHED,
    TASK_STATE_CANCELLED,

    TASK_STATE_COUNT,
    TASK_STATE_INVALID
};

ENUM_BEGIN(task_state, TASK_STATE_INVALID)
    ENUM_REG(TASK_STATE_READY)
    ENUM_REG(TASK_STATE_RUNNING)
    ENUM_REG(TASK_STATE_FINISHED)
    ENUM_REG(TASK_STATE_CANCELLED)
ENUM_END(task_state)

#define MAX_TASK_CODE_NAME_LENGTH 47

// define network header format for RPC
DEFINE_CUSTOMIZED_ID_TYPE(network_header_format);
DEFINE_CUSTOMIZED_ID(network_header_format, NET_HDR_DSN);

// define network channel types for RPC
DEFINE_CUSTOMIZED_ID_TYPE(rpc_channel)
DEFINE_CUSTOMIZED_ID(rpc_channel, RPC_CHANNEL_TCP)
DEFINE_CUSTOMIZED_ID(rpc_channel, RPC_CHANNEL_UDP)

struct task_code : public dsn::utils::customized_id<task_code>
{
    task_code(const char* xxx, task_type type, threadpool_code pool, task_priority pri, int rpcPairedCode);

    task_code(const task_code& source) 
        : dsn::utils::customized_id<task_code>(source) 
    {
    }

    task_code(int code) : dsn::utils::customized_id<task_code>(code) {}

    static task_code from_string(const char* name, task_code invalid_value)
    {
        dsn::utils::customized_id<task_code> id = dsn::utils::customized_id<task_code>::from_string(name, invalid_value);
        return task_code(id);
    }

private:
    // no assignment operator
    task_code& operator=(const task_code& source);
};

// task code with explicit name
#define DEFINE_NAMED_TASK_CODE(x, name, priority, pool) __selectany const dsn::task_code x(#name, dsn::TASK_TYPE_COMPUTE, pool, priority, 0);
#define DEFINE_NAMED_TASK_CODE_AIO(x, name, priority, pool) __selectany const dsn::task_code x(#name, dsn::TASK_TYPE_AIO, pool, priority, 0);

// RPC between client and server, usually use different pools for server and client callbacks
#define DEFINE_NAMED_TASK_CODE_RPC(x, name, priority, pool) \
    __selectany const dsn::task_code x##_ACK(#name"_ACK", dsn::TASK_TYPE_RPC_RESPONSE, pool, priority, 0); \
    __selectany const dsn::task_code x(#name, dsn::TASK_TYPE_RPC_REQUEST, pool, priority, x##_ACK);

#define DEFINE_NAMED_TASK_CODE_RPC_PRIVATE(x, name, priority, pool) \
    static const dsn::task_code x##_ACK(#name"_ACK", dsn::TASK_TYPE_RPC_RESPONSE, pool, priority, 0); \
    static const dsn::task_code x(#name, dsn::TASK_TYPE_RPC_REQUEST, pool, priority, x##_ACK);

// auto name version
#define DEFINE_TASK_CODE(x, priority, pool) DEFINE_NAMED_TASK_CODE(x, x, priority, pool)
#define DEFINE_TASK_CODE_AIO(x, priority, pool) DEFINE_NAMED_TASK_CODE_AIO(x, x, priority, pool)
#define DEFINE_TASK_CODE_RPC(x, priority, pool) DEFINE_NAMED_TASK_CODE_RPC(x, x, priority, pool)
#define DEFINE_TASK_CODE_RPC_PRIVATE(x, priority, pool) DEFINE_NAMED_TASK_CODE_RPC_PRIVATE(x, x, priority, pool)

DEFINE_TASK_CODE(TASK_CODE_INVALID, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

class task;
class aio_task;
class rpc_request_task;
class rpc_response_task;
class message;
class admission_controller;
typedef void (*task_rejection_handler)(task*, admission_controller*);

class task_spec : public extensible_object<task_spec, 4>
{
public:
    static task_spec* get(int ec);

public:
    task_code              code;
    task_type              type;
    const char*            name;    
    task_code              rpc_paired_code;
    task_priority          priority;
    threadpool_code        pool_code; 
    bool                   allow_inline; // allow task executed in other thread pools or tasks
    bool                   fast_execution_in_network_thread;
    network_header_format  rpc_call_header_format;

    task_rejection_handler rejection_handler;
    rpc_channel            rpc_call_channel;
    int32_t                rpc_timeout_milliseconds;

    // COMPUTE
    join_point<void, task*, task*>               on_task_enqueue;    
    join_point<void, task*>                      on_task_begin; // TODO: parent task
    join_point<void, task*>                      on_task_end;
    join_point<void, task*>                      on_task_cancelled;

    join_point<bool, task*, task*, uint32_t>     on_task_wait_pre;
    join_point<void, task*, task*, bool>         on_task_wait_post; // wait succeeded or timedout
    join_point<void, task*, task*, bool>         on_task_cancel_post; // cancel succeeded or not
    

    // AIO
    join_point<bool, task*, aio_task*>           on_aio_call; // return true means continue, otherwise early terminate with task::set_error_code
    join_point<void, aio_task*>                  on_aio_enqueue; // aio done, enqueue callback

    // RPC_REQUEST
    join_point<bool, task*, message*, rpc_response_task*>  on_rpc_call; // return true means continue, otherwise dropped and (optionally) timedout
    join_point<void, rpc_request_task*>          on_rpc_request_enqueue;
    
    // RPC_RESPONSE
    join_point<bool, task*, message*>            on_rpc_reply;
    join_point<bool, rpc_response_task*>         on_rpc_response_enqueue; // response, task

    // message data flow
    join_point<void, message*, message*>         on_rpc_create_response;

public:    
    task_spec(int code, const char* name, task_type type, threadpool_code pool, int paired_code, task_priority pri);
    
public:
    static bool init(configuration_ptr config);
    void init_profiling(bool profile);
};

CONFIG_BEGIN(task_spec)
    CONFIG_FLD(bool, allow_inline, false)
    CONFIG_FLD(bool, fast_execution_in_network_thread, false)
    CONFIG_FLD_ID(network_header_format, rpc_call_header_format, NET_HDR_DSN)
    CONFIG_FLD_ID(rpc_channel, rpc_call_channel, RPC_CHANNEL_TCP)
    CONFIG_FLD(int32_t, rpc_timeout_milliseconds, 5000)
CONFIG_END

} // end namespace

