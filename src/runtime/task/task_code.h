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

#include <ostream>

#include "utils/ports.h"
#include "utils/enum_helper.h"
#include "utils/threadpool_code.h"
#include <thrift/protocol/TProtocol.h>

typedef enum dsn_task_type_t {
    TASK_TYPE_RPC_REQUEST,  ///< task handling rpc request
    TASK_TYPE_RPC_RESPONSE, ///< task handling rpc response or timeout
    TASK_TYPE_COMPUTE,      ///< async calls or timers
    TASK_TYPE_AIO,          ///< callback for file read and write
    TASK_TYPE_CONTINUATION, ///< above tasks are seperated into several continuation
                            ///< tasks by thread-synchronization operations.
                            ///< so that each "task" is non-blocking
    TASK_TYPE_COUNT,
    TASK_TYPE_INVALID
} dsn_task_type_t;

ENUM_BEGIN(dsn_task_type_t, TASK_TYPE_INVALID)
ENUM_REG(TASK_TYPE_RPC_REQUEST)
ENUM_REG(TASK_TYPE_RPC_RESPONSE)
ENUM_REG(TASK_TYPE_COMPUTE)
ENUM_REG(TASK_TYPE_AIO)
ENUM_REG(TASK_TYPE_CONTINUATION)
ENUM_END(dsn_task_type_t)

typedef enum dsn_task_priority_t {
    TASK_PRIORITY_LOW,
    TASK_PRIORITY_COMMON,
    TASK_PRIORITY_HIGH,
    TASK_PRIORITY_COUNT,
    TASK_PRIORITY_INVALID
} dsn_task_priority_t;

ENUM_BEGIN(dsn_task_priority_t, TASK_PRIORITY_INVALID)
ENUM_REG(TASK_PRIORITY_LOW)
ENUM_REG(TASK_PRIORITY_COMMON)
ENUM_REG(TASK_PRIORITY_HIGH)
ENUM_END(dsn_task_priority_t)

namespace dsn {

/// task code is an index for a specific kind of task. with the index, you can
/// get properties of this kind of task: name, type, priority, etc. you may want to refer to
/// task_spec.h for the detailed task properties.
///
/// Like dsn::blob, task_code is a special thrift primitive type that's defined
/// by the rDSN framework. Internally as a C++ object, it's is represented as an integer,
/// but in thrift representation it's serialized as a string.
///
/// It should be noted that a task_code may have different code number in two different
/// clusters. So DO NOT use a integer as task_code.
///
///  **.thrift
///    x: 1: i32  task_code;
///    ✓: 1: dsn.task_code  task_code;
///
class task_code
{
public:
    constexpr task_code() = default;

    constexpr explicit task_code(int code) : _internal_code(code) {}

    task_code(const char *name,
              dsn_task_type_t tt,
              dsn_task_priority_t pri,
              dsn::threadpool_code pool);

    task_code(const char *name,
              dsn_task_type_t tt,
              dsn_task_priority_t pri,
              dsn::threadpool_code pool,
              bool is_storage_write,
              bool allow_batch,
              bool is_idempotent);

    const char *to_string() const;

    constexpr bool operator==(const task_code &r) { return _internal_code == r._internal_code; }

    constexpr bool operator!=(const task_code &r) { return !(*this == r); }

    constexpr operator int() const { return _internal_code; }

    constexpr int code() const { return _internal_code; }

    // for serialization in thrift format
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;

    static int max();
    static bool is_exist(const char *name);
    static task_code try_get(const char *name, task_code default_value);
    static task_code try_get(const std::string &name, task_code default_value);

    friend std::ostream &operator<<(std::ostream &os, const task_code &tc)
    {
        return os << tc.to_string();
    }

private:
    task_code(const char *name);
    int _internal_code{0};
};

// you can define task_code by the following macros
#define DEFINE_NAMED_TASK_CODE(x, name, pri, pool)                                                 \
    __selectany const ::dsn::task_code x(#name, TASK_TYPE_COMPUTE, pri, pool);

#define DEFINE_NAMED_TASK_CODE_AIO(x, name, pri, pool)                                             \
    __selectany const ::dsn::task_code x(#name, TASK_TYPE_AIO, pri, pool);

#define DEFINE_NAMED_TASK_CODE_RPC(x, name, pri, pool)                                             \
    __selectany const ::dsn::task_code x(#name, TASK_TYPE_RPC_REQUEST, pri, pool);                 \
    __selectany const ::dsn::task_code x##_ACK(#name "_ACK", TASK_TYPE_RPC_RESPONSE, pri, pool);

/*! define a new task code with TASK_TYPE_COMPUTATION */
#define DEFINE_TASK_CODE(x, pri, pool) DEFINE_NAMED_TASK_CODE(x, x, pri, pool)
#define DEFINE_TASK_CODE_AIO(x, pri, pool) DEFINE_NAMED_TASK_CODE_AIO(x, x, pri, pool)
#define DEFINE_TASK_CODE_RPC(x, pri, pool) DEFINE_NAMED_TASK_CODE_RPC(x, x, pri, pool)

// define a rpc code for storage engine
//
// storage engine's rpc code is special because
// 1. we need to find a proper replica to serve the rpc
//    then forward it to the storage engine atop of replica.
// 2. for a write rpc, a primary may also need to replicate it
//    to secondaries before forwarding to the storage engine.
// 3. some storage engine's rpc shouldn't be batched,
//    either for better performance or correctness.
// 4. some write rpc is idempotent, but some is not.
//    we should differentiate it.
// so we define some specical fields in task_spec to mark these features.
//
// please refer to rpc_engine::on_recv_request for the detailes on how storage_engine's rpc
// is handled
//
// Notice we dispatch storage rpc's response to THREAD_POOL_DEFAULT,
// the reason is that the storage rpc's response mainly runs at client side, which is not
// necessary to start so many threadpools
#define DEFINE_STORAGE_RPC_CODE(x, pri, pool, is_write, allow_batch, is_idempotent)                \
    __selectany const ::dsn::task_code x(                                                          \
        #x, TASK_TYPE_RPC_REQUEST, pri, pool, is_write, allow_batch, is_idempotent);               \
    __selectany const ::dsn::task_code x##_ACK(#x "_ACK",                                          \
                                               TASK_TYPE_RPC_RESPONSE,                             \
                                               pri,                                                \
                                               THREAD_POOL_DEFAULT,                                \
                                               is_write,                                           \
                                               allow_batch,                                        \
                                               is_idempotent);

#define ALLOW_BATCH true
#define NOT_ALLOW_BATCH false
#define IS_IDEMPOTENT true
#define NOT_IDEMPOTENT false

// define a default task code "task_code_invalid", it's mainly used for representing
// some error status when you want to return task_code in some functions.
DEFINE_TASK_CODE(TASK_CODE_INVALID, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)

// define a task_code "task_code_inlined", it's mainly used in situations when you want execute
// a task with "inline" mode.
DEFINE_TASK_CODE(TASK_CODE_EXEC_INLINED, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
}
