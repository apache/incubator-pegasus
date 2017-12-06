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
#include <dsn/utility/error_code.h>
#include <dsn/tool-api/threadpool_code.h>
#include <memory>
#include <atomic>

#ifdef DSN_USE_THRIFT_SERIALIZATION
#include <thrift/protocol/TProtocol.h>
#endif

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
} // end namespace
