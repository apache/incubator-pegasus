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

#include <dsn/service_api_c.h>
#include <dsn/utility/ports.h>
#include <dsn/utility/autoref_ptr.h>
#include <dsn/tool-api/threadpool_code.h>
#include <memory>
#include <atomic>

#ifdef DSN_USE_THRIFT_SERIALIZATION
#include <thrift/protocol/TProtocol.h>
#endif

#define TRACK_ERROR_CODE 1

namespace dsn {
typedef void (*safe_handle_release)(void *);

template <safe_handle_release releaser>
class safe_handle : public ::dsn::ref_counter
{
public:
    safe_handle(void *handle, bool is_owner)
    {
        _handle = handle;
        _is_owner = is_owner;
    }

    safe_handle()
    {
        _handle = nullptr;
        _is_owner = false;
    }

    void assign(void *handle, bool is_owner)
    {
        clear();

        _handle = handle;
        _is_owner = is_owner;
    }

    void set_owner(bool owner = true) { _is_owner = owner; }

    ~safe_handle() { clear(); }

    void *native_handle() const { return _handle; }

private:
    void clear()
    {
        if (_is_owner && nullptr != _handle) {
            releaser(_handle);
            _handle = nullptr;
        }
    }

private:
    void *_handle;
    bool _is_owner;
};

class gpid
{
private:
    dsn_gpid _value;

public:
    gpid(int app_id, int pidx)
    {
        _value.u.app_id = app_id;
        _value.u.partition_index = pidx;
    }

    gpid(dsn_gpid gd) { _value = gd; }

    gpid(const gpid &gd) { _value.value = gd._value.value; }

    gpid() { _value.value = 0; }

    uint64_t value() const { return _value.value; }

    operator dsn_gpid() const { return _value; }

    bool operator<(const gpid &r) const
    {
        return _value.u.app_id < r._value.u.app_id ||
               (_value.u.app_id == r._value.u.app_id &&
                _value.u.partition_index < r._value.u.partition_index);
    }

    bool operator==(const gpid &r) const { return value() == r.value(); }

    bool operator!=(const gpid &r) const { return value() != r.value(); }

    int32_t get_app_id() const { return _value.u.app_id; }
    int32_t get_partition_index() const { return _value.u.partition_index; }
    void set_app_id(int32_t v) { _value.u.app_id = v; }
    void set_partition_index(int32_t v) { _value.u.partition_index = v; }
    dsn_gpid &raw() { return _value; }
    const dsn_gpid &raw() const { return _value; }

#ifdef DSN_USE_THRIFT_SERIALIZATION
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;
#endif
};

/*!
  @addtogroup exec-model
  @{
 */
class task_code
{
public:
    task_code(const char *name,
              dsn_task_type_t tt,
              dsn_task_priority_t pri,
              dsn::threadpool_code pool)
    {
        _internal_code = dsn_task_code_register(name, tt, pri, pool);
    }

    task_code() { _internal_code = 0; }

    explicit task_code(dsn_task_code_t code) { _internal_code = code; }

    task_code(const task_code &r) { _internal_code = r._internal_code; }

    const char *to_string() const { return dsn_task_code_to_string(_internal_code); }

    task_code &operator=(const task_code &source)
    {
        _internal_code = source._internal_code;
        return *this;
    }

    bool operator==(const task_code &r) { return _internal_code == r._internal_code; }

    bool operator!=(const task_code &r) { return !(*this == r); }

    operator dsn_task_code_t() const { return _internal_code; }

#ifdef DSN_USE_THRIFT_SERIALIZATION
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;
#endif
private:
    dsn_task_code_t _internal_code;
};

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

// define default task code
DEFINE_TASK_CODE(TASK_CODE_INVALID, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE(TASK_CODE_EXEC_INLINED, TASK_PRIORITY_COMMON, THREAD_POOL_DEFAULT)
/*@}*/

/*!
 @addtogroup error-t
 @{
 */
class error_code
{
public:
    error_code(const char *name)
    {
        _internal_code = dsn_error_register(name);

        dassert(name, "name for an error code cannot be empty");
#ifdef TRACK_ERROR_CODE
        _used = true;
#endif
    }

    error_code()
    {
        _internal_code = 0;

#ifdef TRACK_ERROR_CODE
        _used = true;
#endif
    }

    error_code(dsn_error_t err)
    {
        _internal_code = err;

#ifdef TRACK_ERROR_CODE
        _used = false;
#endif
    }

    error_code(const error_code &err)
    {
        _internal_code = err._internal_code;
#ifdef TRACK_ERROR_CODE
        _used = false;
        err._used = true;
#endif
    }

    const char *to_string() const
    {
#ifdef TRACK_ERROR_CODE
        _used = true;
#endif
        return dsn_error_to_string(_internal_code);
    }

    error_code &operator=(const error_code &source)
    {
        _internal_code = source._internal_code;
#ifdef TRACK_ERROR_CODE
        _used = false;
        source._used = true;
#endif
        return *this;
    }

    bool operator==(const error_code &r)
    {
#ifdef TRACK_ERROR_CODE
        _used = true;
        r._used = true;
#endif
        return _internal_code == r._internal_code;
    }

    bool operator!=(const error_code &r) { return !(*this == r); }

#ifdef TRACK_ERROR_CODE
    ~error_code()
    {
        // in cases where error code is std::bind-ed as task callbacks,
        // and when tasks are cancelled, it is difficult to end track them on cancel
        // therefore we change derror to dwarn
        if (!_used) {
            if (_internal_code != 0) {
                dlog(LOG_LEVEL_ERROR,
                     "error-code",
                     "error code is not handled, err = %s",
                     to_string());
            }
        }
    }
#endif

    dsn_error_t get() const
    {
#ifdef TRACK_ERROR_CODE
        _used = true;
#endif
        return _internal_code;
    }

    operator dsn_error_t() const
    {
#ifdef TRACK_ERROR_CODE
        _used = true;
#endif
        return _internal_code;
    }

    void end_tracking() const
    {
#ifdef TRACK_ERROR_CODE
        _used = true;
#endif
    }

#ifdef DSN_USE_THRIFT_SERIALIZATION
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;
#endif
private:
#ifdef TRACK_ERROR_CODE
    mutable bool _used;
#endif
    dsn_error_t _internal_code;
};

#define DEFINE_ERR_CODE(x) __selectany const dsn::error_code x(#x);

DEFINE_ERR_CODE(ERR_OK)

DEFINE_ERR_CODE(ERR_UNKNOWN)
DEFINE_ERR_CODE(ERR_SERVICE_NOT_FOUND)
DEFINE_ERR_CODE(ERR_SERVICE_ALREADY_RUNNING)
DEFINE_ERR_CODE(ERR_IO_PENDING)
DEFINE_ERR_CODE(ERR_TIMEOUT)
DEFINE_ERR_CODE(ERR_SERVICE_NOT_ACTIVE)
DEFINE_ERR_CODE(ERR_BUSY)
DEFINE_ERR_CODE(ERR_NETWORK_INIT_FAILED)
DEFINE_ERR_CODE(ERR_FORWARD_TO_OTHERS)
DEFINE_ERR_CODE(ERR_OBJECT_NOT_FOUND)

DEFINE_ERR_CODE(ERR_HANDLER_NOT_FOUND)
DEFINE_ERR_CODE(ERR_LEARN_FILE_FAILED)
DEFINE_ERR_CODE(ERR_GET_LEARN_STATE_FAILED)
DEFINE_ERR_CODE(ERR_INVALID_VERSION)
DEFINE_ERR_CODE(ERR_INVALID_PARAMETERS)
DEFINE_ERR_CODE(ERR_CAPACITY_EXCEEDED)
DEFINE_ERR_CODE(ERR_INVALID_STATE)
DEFINE_ERR_CODE(ERR_INACTIVE_STATE)
DEFINE_ERR_CODE(ERR_NOT_ENOUGH_MEMBER)
DEFINE_ERR_CODE(ERR_FILE_OPERATION_FAILED)

DEFINE_ERR_CODE(ERR_HANDLE_EOF)
DEFINE_ERR_CODE(ERR_WRONG_CHECKSUM)
DEFINE_ERR_CODE(ERR_INVALID_DATA)
DEFINE_ERR_CODE(ERR_INVALID_HANDLE)
DEFINE_ERR_CODE(ERR_INCOMPLETE_DATA)
DEFINE_ERR_CODE(ERR_VERSION_OUTDATED)
DEFINE_ERR_CODE(ERR_PATH_NOT_FOUND)
DEFINE_ERR_CODE(ERR_PATH_ALREADY_EXIST)
DEFINE_ERR_CODE(ERR_ADDRESS_ALREADY_USED)
DEFINE_ERR_CODE(ERR_STATE_FREEZED)

DEFINE_ERR_CODE(ERR_LOCAL_APP_FAILURE)
DEFINE_ERR_CODE(ERR_BIND_IOCP_FAILED)
DEFINE_ERR_CODE(ERR_NETWORK_START_FAILED)
DEFINE_ERR_CODE(ERR_NOT_IMPLEMENTED)
DEFINE_ERR_CODE(ERR_CHECKPOINT_FAILED)
DEFINE_ERR_CODE(ERR_WRONG_TIMING)
DEFINE_ERR_CODE(ERR_NO_NEED_OPERATE)
DEFINE_ERR_CODE(ERR_CORRUPTION)
DEFINE_ERR_CODE(ERR_TRY_AGAIN)
DEFINE_ERR_CODE(ERR_CLUSTER_NOT_FOUND)

DEFINE_ERR_CODE(ERR_CLUSTER_ALREADY_EXIST)
DEFINE_ERR_CODE(ERR_SERVICE_ALREADY_EXIST)
DEFINE_ERR_CODE(ERR_INJECTED)
DEFINE_ERR_CODE(ERR_REPLICATION_FAILURE)
DEFINE_ERR_CODE(ERR_APP_EXIST)
DEFINE_ERR_CODE(ERR_APP_NOT_EXIST)
DEFINE_ERR_CODE(ERR_BUSY_CREATING)
DEFINE_ERR_CODE(ERR_BUSY_DROPPING)
DEFINE_ERR_CODE(ERR_NETWORK_FAILURE)
DEFINE_ERR_CODE(ERR_UNDER_RECOVERY)

DEFINE_ERR_CODE(ERR_LEARNER_NOT_FOUND)
DEFINE_ERR_CODE(ERR_OPERATION_DISABLED)
/*@}*/
} // end namespace
