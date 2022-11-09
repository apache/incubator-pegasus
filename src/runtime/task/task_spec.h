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
 *     specification for the labeled tasks (task kinds)
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include "utils/utils.h"
#include "utils/config_helper.h"
#include "utils/enum_helper.h"
#include "utils/customizable_id.h"
#include "utils/join_point.h"
#include "utils/extensible_object.h"
#include "utils/exp_delay.h"
#include "perf_counter/perf_counter.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "utils/api_utilities.h"

ENUM_BEGIN(dsn_log_level_t, LOG_LEVEL_INVALID)
ENUM_REG(LOG_LEVEL_DEBUG)
ENUM_REG(LOG_LEVEL_INFO)
ENUM_REG(LOG_LEVEL_WARNING)
ENUM_REG(LOG_LEVEL_ERROR)
ENUM_REG(LOG_LEVEL_FATAL)
ENUM_END(dsn_log_level_t)

namespace dsn {

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

typedef enum grpc_mode_t {
    GRPC_TO_LEADER, // the rpc is sent to the leader (if exist)
    GRPC_TO_ALL,    // the rpc is sent to all
    GRPC_TO_ANY,    // the rpc is sent to one of the group member
    GRPC_COUNT,
    GRPC_INVALID
} grpc_mode_t;

ENUM_BEGIN(grpc_mode_t, GRPC_INVALID)
ENUM_REG(GRPC_TO_LEADER)
ENUM_REG(GRPC_TO_ALL)
ENUM_REG(GRPC_TO_ANY)
ENUM_END(grpc_mode_t)

typedef enum throttling_mode_t {
    TM_NONE,   // no throttling applied
    TM_REJECT, // reject the incoming request
    TM_DELAY,  // delay network receive ops to reducing incoming rate
    TM_COUNT,
    TM_INVALID
} throttling_mode_t;

ENUM_BEGIN(throttling_mode_t, TM_INVALID)
ENUM_REG(TM_NONE)
ENUM_REG(TM_REJECT)
ENUM_REG(TM_DELAY)
ENUM_END(throttling_mode_t)

typedef enum dsn_msg_serialize_format {
    DSF_INVALID = 0,
    DSF_THRIFT_BINARY = 1,
    DSF_THRIFT_COMPACT = 2,
    DSF_THRIFT_JSON = 3,
    DSF_PROTOC_BINARY = 4,
    DSF_PROTOC_JSON = 5,
    DSF_JSON = 6
} dsn_msg_serialize_format;

ENUM_BEGIN(dsn_msg_serialize_format, DSF_INVALID)
ENUM_REG(DSF_THRIFT_BINARY)
ENUM_REG(DSF_THRIFT_COMPACT)
ENUM_REG(DSF_THRIFT_JSON)
ENUM_REG(DSF_PROTOC_BINARY)
ENUM_REG(DSF_PROTOC_JSON)
ENUM_END(dsn_msg_serialize_format)

// define network header format for RPC
DEFINE_CUSTOMIZED_ID_TYPE(network_header_format)
DEFINE_CUSTOMIZED_ID(network_header_format, NET_HDR_INVALID)
DEFINE_CUSTOMIZED_ID(network_header_format, NET_HDR_DSN)

// define network channel types for RPC
DEFINE_CUSTOMIZED_ID_TYPE(rpc_channel)
DEFINE_CUSTOMIZED_ID(rpc_channel, RPC_CHANNEL_TCP)
DEFINE_CUSTOMIZED_ID(rpc_channel, RPC_CHANNEL_UDP)

class task;
class task_queue;
class aio_task;
class rpc_request_task;
class rpc_response_task;
class message_ex;

std::set<dsn::task_code> &get_storage_rpc_req_codes();

class task_spec : public extensible_object<task_spec, 4>
{
public:
    static task_spec *get(int ec);
    static void register_task_code(dsn::task_code code,
                                   dsn_task_type_t type,
                                   dsn_task_priority_t pri,
                                   dsn::threadpool_code pool);

    static void register_storage_task_code(dsn::task_code code,
                                           dsn_task_type_t type,
                                           dsn_task_priority_t pri,
                                           dsn::threadpool_code pool,
                                           bool is_write_operation,
                                           bool allow_batch,
                                           bool is_idempotent);

public:
    // not configurable [
    dsn::task_code code;
    dsn_task_type_t type;
    std::string name;
    dsn::task_code rpc_paired_code;
    shared_exp_delay rpc_request_delayer;

    bool rpc_request_for_storage;
    bool rpc_request_is_write_operation;   // need stateful replication
    bool rpc_request_is_write_allow_batch; // if write allow batch
    bool rpc_request_is_write_idempotent;  // if write operation is idempotent
    // ]

    // configurable [
    dsn_task_priority_t priority;
    grpc_mode_t grpc_mode; // used when a rpc request is sent to a group address
    dsn::threadpool_code pool_code;

    // allow task executed in other thread pools or tasks
    // for TASK_TYPE_COMPUTE - allow-inline allows a task being executed in its caller site
    // for other tasks - allow-inline allows a task being execution in io-thread
    bool allow_inline;
    bool randomize_timer_delay_if_zero; // to avoid many timers executing at the same time
    network_header_format rpc_call_header_format;
    dsn_msg_serialize_format rpc_msg_payload_serialize_default_format;
    rpc_channel rpc_call_channel;
    bool rpc_message_crc_required;

    int32_t rpc_timeout_milliseconds;
    int32_t rpc_request_resend_timeout_milliseconds;  // 0 for no auto-resend
    throttling_mode_t rpc_request_throttling_mode;    //
    std::vector<int> rpc_request_delays_milliseconds; // see exp_delay for delaying recving
    bool rpc_request_dropped_before_execution_when_timeout;

    // COMPUTE
    /*!
     @addtogroup tool-api-hooks
     @{
     */
    join_point<void, task *, task *> on_task_create;

    join_point<void, task *, task *> on_task_enqueue;
    join_point<void, task *> on_task_begin; // TODO: parent task
    join_point<void, task *> on_task_end;
    join_point<void, task *> on_task_cancelled;

    join_point<void, task *, task *, uint32_t> on_task_wait_pre; // waitor, waitee, timeout
    join_point<void, task *> on_task_wait_notified;
    join_point<void, task *, task *, bool> on_task_wait_post;   // wait succeeded or timedout
    join_point<void, task *, task *, bool> on_task_cancel_post; // cancel succeeded or not

    // AIO
    join_point<bool, task *, aio_task *> on_aio_call; // return true means continue, otherwise early
                                                      // terminate with task::set_error_code
    join_point<void, aio_task *> on_aio_enqueue;      // aio done, enqueue callback

    // RPC_REQUEST
    join_point<bool, task *, message_ex *, rpc_response_task *>
        on_rpc_call; // return true means continue, otherwise dropped and (optionally) timedout
    join_point<bool, rpc_request_task *> on_rpc_request_enqueue;
    join_point<void, rpc_request_task *> on_rpc_task_dropped; // rpc task dropped

    // RPC_RESPONSE
    join_point<bool, task *, message_ex *> on_rpc_reply;
    join_point<bool, rpc_response_task *> on_rpc_response_enqueue; // response, task

    // message data flow
    join_point<void, message_ex *, message_ex *> on_rpc_create_response;
    /*@}*/

public:
    task_spec(int code,
              const char *name,
              dsn_task_type_t type,
              dsn_task_priority_t pri,
              dsn::threadpool_code pool);

public:
    static bool init();
    void init_profiling(bool profile);
};

CONFIG_BEGIN(task_spec)
CONFIG_FLD_ENUM(dsn_task_priority_t,
                priority,
                TASK_PRIORITY_COMMON,
                TASK_PRIORITY_INVALID,
                true,
                "task priority")
CONFIG_FLD_ENUM(grpc_mode_t,
                grpc_mode,
                GRPC_TO_LEADER,
                GRPC_INVALID,
                false,
                "group rpc mode: GRPC_TO_LEADER, GRPC_TO_ALL, GRPC_TO_ANY")
CONFIG_FLD_ID(
    threadpool_code, pool_code, THREAD_POOL_DEFAULT, true, "thread pool to execute the task")
CONFIG_FLD(bool,
           bool,
           allow_inline,
           false,
           "allow task executed in other thread pools or tasks "
           "for TASK_TYPE_COMPUTE - allow-inline allows a task being executed in its caller site "
           "for other tasks - allow-inline allows a task being execution in io-thread ")
CONFIG_FLD(bool,
           bool,
           randomize_timer_delay_if_zero,
           false,
           "whether to randomize the timer delay "
           "to random(0, timer_interval), if the "
           "initial delay is zero, to avoid "
           "multiple timers executing at the "
           "same time (e.g., checkpointing)")
CONFIG_FLD_ID(network_header_format,
              rpc_call_header_format,
              NET_HDR_DSN,
              false,
              "what kind of header format for this kind of rpc calls")
CONFIG_FLD_ENUM(dsn_msg_serialize_format,
                rpc_msg_payload_serialize_default_format,
                DSF_THRIFT_BINARY,
                DSF_INVALID,
                false,
                "what kind of payload serialization format for this kind of msgs")
CONFIG_FLD_ID(rpc_channel,
              rpc_call_channel,
              RPC_CHANNEL_TCP,
              false,
              "what kind of network channel for this kind of rpc calls")
CONFIG_FLD(bool,
           bool,
           rpc_message_crc_required,
           false,
           "whether to calculate the crc checksum when send request/response")
CONFIG_FLD(int32_t,
           uint64,
           rpc_timeout_milliseconds,
           5000,
           "what is the default timeout (ms) for this kind of rpc calls")
CONFIG_FLD(int32_t,
           uint64,
           rpc_request_resend_timeout_milliseconds,
           0,
           "for how long (ms) the "
           "request will be resent if "
           "no response is received "
           "yet, 0 for disable this "
           "feature")
CONFIG_FLD_ENUM(throttling_mode_t,
                rpc_request_throttling_mode,
                TM_NONE,
                TM_INVALID,
                false,
                "throttling mode for rpc requets: TM_NONE, TM_REJECT, TM_DELAY when queue length > "
                "pool.queue_length_throttling_threshold")
CONFIG_FLD_INT_LIST(rpc_request_delays_milliseconds,
                    "how many milliseconds to delay recving rpc session for when queue length ~= "
                    "[1.0, 1.2, 1.4, 1.6, 1.8, >=2.0] x pool.queue_length_throttling_threshold, "
                    "e.g., 0, 0, 1, 2, 5, 10")
CONFIG_FLD(bool,
           bool,
           rpc_request_dropped_before_execution_when_timeout,
           false,
           "whether to drop a request right before execution when its queueing time is already "
           "greater than its timeout value")
CONFIG_END

} // end namespace
