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
 *     helpers for easier task programing atop C api
 *
 * Revision history:
 *     Sep., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#pragma once

#include <dsn/service_api_c.h>
#include <dsn/tool-api/auto_codes.h>
#include <dsn/cpp/rpc_stream.h>
#include <dsn/cpp/serialization.h>
#include <dsn/cpp/zlocks.h>
#include <dsn/cpp/callocator.h>
#include <dsn/utility/utils.h>
#include <dsn/utility/autoref_ptr.h>
#include <dsn/utility/synchronize.h>
#include <dsn/utility/link.h>
#include <dsn/utility/optional.h>
#include <set>
#include <map>
#include <thread>

namespace dsn {
typedef std::function<void(error_code, size_t)> aio_handler;
class safe_task_handle;
typedef ::dsn::ref_ptr<safe_task_handle> task_ptr;

//
// basic cpp task wrapper
// which manages the task handle
// and the interaction with task context manager, clientlet
//
class safe_task_handle : public ::dsn::ref_counter
{
public:
    safe_task_handle()
    {
        _task = nullptr;
        _rpc_response = nullptr;
    }

    virtual ~safe_task_handle()
    {
        dsn_task_release_ref(_task);

        if (_rpc_response != nullptr)
            dsn_msg_release_ref(_rpc_response);
    }

    void set_task_info(dsn_task_t t)
    {
        _task = t;
        dsn_task_add_ref(t);
    }

    dsn_task_t native_handle() const { return _task; }

    virtual bool cancel(bool wait_until_finished, bool *finished = nullptr)
    {
        return dsn_task_cancel2(_task, wait_until_finished, finished);
    }

    void set_delay(int delay_ms) { dsn_task_set_delay(_task, delay_ms); }

    void wait() const { dsn_task_wait(_task); }

    bool wait(int timeout_millieseconds) const
    {
        return dsn_task_wait_timeout(_task, timeout_millieseconds);
    }

    ::dsn::error_code error() const { return dsn_task_error(_task); }

    size_t io_size() const { return dsn_file_get_io_size(_task); }

    void enqueue(std::chrono::milliseconds delay = std::chrono::milliseconds(0)) const
    {
        dsn_task_call(_task, static_cast<int>(delay.count()));
    }

    void enqueue_aio(error_code err, size_t size) const { dsn_file_task_enqueue(_task, err, size); }

    dsn_message_t response()
    {
        if (_rpc_response == nullptr)
            _rpc_response = dsn_rpc_get_response(_task);
        return _rpc_response;
    }

    void enqueue_rpc_response(error_code err, dsn_message_t resp) const
    {
        dsn_rpc_enqueue_response(_task, err, resp);
    }

private:
    dsn_task_t _task;
    dsn_message_t _rpc_response;
};

template <typename THandler>
class transient_safe_task : public safe_task_handle, public transient_object
{
public:
    explicit transient_safe_task(THandler &&h) : _handler(std::move(h)) {}
    explicit transient_safe_task(const THandler &h) : _handler(h) {}
    virtual bool cancel(bool wait_until_finished, bool *finished = nullptr) override
    {
        return safe_task_handle::cancel(wait_until_finished, finished);
    }

    static void on_cancel(void *task)
    {
        auto t = static_cast<transient_safe_task *>(task);
        t->_handler.reset();
        t->release_ref(); // added upon callback exec registration
    }

    static void exec(void *task)
    {
        auto t = static_cast<transient_safe_task *>(task);
        dbg_dassert(t->_handler.is_some(), "_handler is missing");
        t->_handler.unwrap()();
        t->_handler.reset();
        t->release_ref(); // added upon callback exec registration
    }

    static void
    exec_rpc_response(dsn::error_code err, dsn_message_t req, dsn_message_t resp, void *task)
    {
        auto t = static_cast<transient_safe_task *>(task);
        dbg_dassert(t->_handler.is_some(), "_handler is missing");
        t->_handler.unwrap()(err, req, resp);
        t->_handler.reset();
        t->release_ref(); // added upon callback exec_rpc_response registration
    }

    static void exec_aio(dsn::error_code err, size_t sz, void *task)
    {
        auto t = static_cast<transient_safe_task *>(task);
        dbg_dassert(t->_handler.is_some(), "_handler is missing");
        t->_handler.unwrap()(err, sz);
        t->_handler.reset();
        t->release_ref(); // added upon callback exec_aio registration
    }

private:
    dsn::optional<THandler> _handler;
};

template <typename THandler>
class timer_safe_task : public safe_task_handle
{
public:
    explicit timer_safe_task(THandler &&h) : _handler(std::move(h)) {}
    explicit timer_safe_task(const THandler &h) : _handler(h) {}
    virtual bool cancel(bool wait_until_finished, bool *finished = nullptr) override
    {
        return safe_task_handle::cancel(wait_until_finished, finished);
    }

    static void on_cancel(void *task)
    {
        auto t = static_cast<timer_safe_task *>(task);
        t->_handler.reset();
        t->release_ref(); // added upon callback exec registration
    }

    static void exec_timer(void *task)
    {
        auto t = static_cast<timer_safe_task *>(task);
        dbg_dassert(t->_handler.is_some(), "_handler is missing");
        t->_handler.unwrap()();
    }

private:
    dsn::optional<THandler> _handler;
};

//
// two staged computation task
// this is used when a task handle is returned when a call is made,
// while the task, is however, enqueued later after other operations when
// certain parameters to the task is known (e.g., error code after logging)
// in thise case, we can use two staged computation task as this is.
//
//    task_ptr task = tasking::create_late_task(...);
//    ...
//    return task;
//
//    ... after logging ....
//    task->bind_and_enqueue([&](cb)=>{std::bind(cb, error)}, delay);
//
template <typename THandler>
class safe_late_task : public safe_task_handle
{
public:
    safe_late_task(THandler &h) : _bound_handler(nullptr), _handler(h) {}

    operator task_ptr() const { return task_ptr(this); }

    virtual bool cancel(bool wait_until_finished, bool *finished = nullptr) override
    {
        bool r = safe_task_handle::cancel(wait_until_finished, finished);
        if (r) {
            _bound_handler = nullptr;
        }
        return r;
    }

    void bind_and_enqueue(std::function<std::function<void()>(THandler &)> binder,
                          int delay_milliseconds = 0)
    {
        _bound_handler = _handler ? binder(_handler) : nullptr;
        _handler = nullptr;
        dsn_task_call(native_handle(), delay_milliseconds);
    }

    static void on_cancel(void *task)
    {
        auto t = (safe_late_task<THandler> *)task;
        t->release_ref(); // added upon callback exec registration
    }

    static void exec(void *task)
    {
        auto t = (safe_late_task<THandler> *)task;
        if (t->_bound_handler) {
            t->_bound_handler();
            t->_bound_handler = nullptr;
        }
        t->release_ref(); // added upon callback exec registration
    }

private:
    std::function<void()> _bound_handler;
    THandler _handler;
};

//
//  call a safe_late_task with specified response
//
template <typename TResponse>
void call_safe_late_task(const dsn::task_ptr &task, TResponse &&response)
{
    typedef std::function<void(const TResponse &)> TCallback;
    typedef dsn::safe_late_task<TCallback> task_type;
    task_type *t = reinterpret_cast<task_type *>(task.get());
    t->bind_and_enqueue([r = std::move(response)](TCallback & callback) {
        return std::bind(callback, std::move(r));
    });
}

// ------- inlined implementation ----------
}
