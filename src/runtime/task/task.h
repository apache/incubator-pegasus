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

#include <functional>
#include <tuple>
#include "utils/ports.h"
#include "utils/extensible_object.h"
#include "utils/utils.h"
#include "utils/apply.h"
#include "utils/binary_writer.h"
#include "task_spec.h"
#include "task_tracker.h"
#include "runtime/rpc/rpc_message.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"

namespace dsn {

class task_worker;
class task_worker_pool;
class service_node;
class task_engine;
class task_queue;
class rpc_engine;
class disk_engine;
class env_provider;
class timer_service;
class task;

struct __tls_dsn__
{
    uint32_t magic;
    task *current_task;

    task_worker *worker;
    int worker_index;
    service_node *node;
    int node_id;

    rpc_engine *rpc;
    env_provider *env;

    int last_worker_queue_size;
    uint64_t node_pool_thread_ids; // 8,8,16 bits
    uint32_t last_lower32_task_id; // 32bits
};

extern __thread struct __tls_dsn__ tls_dsn;

///
/// Task is a thread-like execution piece that is much lighter than a normal thread.
/// Huge number of tasks may be hosted by a small number of actual threads run within
/// a thread pool.
///
/// When creating the task, user must use 3 parameters to specify in which thread the
/// callback should run:
///
///     1. node: specifies the computation engine of the callback, i.e, the "pool" of "thread_pool"
///     2. task_code: a index to the "task_spec". task_spec specifies which thread pool of
///        the computation engine to run the callback. some other task information is also
///        recorded in task_spec. please refer to @task_code, @task_spec, @thread_pool_code
///        for more details.
///     3. hash: specifies which thread in the thread pool to execute the callback. (This is
///        somewhat not accurate, coz "hash" will work together with thread_pool's "partition"
///        option. Please refer to @task_worker_pool for more details).
///
/// So the running thread of callback will be determined hierarchically:
///
///         |<---determined by "node"
///         |        |<-----determined by "code"
///         |        |       |-------------determined by "hash"
///         |        |       |
///  |------V--------|-------|---------------------------|     |--------------|
///  | |-------------V-------|-----|     |-------------| |     |              |
///  | | |--------|     |----V---| |     |             | |     |              |
///  | | | thread | ... | thread | |     |             | |     |              |
///  | | |--------|     |--------| |     |             | |     |              |
///  | |       thread pool         | ... | thread pool | |     |              |
///  | |---------------------------|     |-------------| |     |              |
///  |                 service node                      | ... | service node |
///  |---------------------------------------------------|     |--------------|
///
/// A status value called "task_state" is kept in each task to indicate the task running state,
/// the transition among different states are:
///
///                      |
///              (new created task)
///                      |
///                      V
///      |-----------> ready------------|
///      |               |              |
/// (timer task)     (execute)       (cancel)
///      |               |              |
///      |               V              V
///      |------------ running      cancelled
///                      |
///                (one shot task)
///                      |
///                      V
///                   finished
///
/// As shown above, a new created task will be in "ready" state. After obtaining the data
/// (like a rpc gets the response, or a disk io succeeds), the data provider module will store
/// proper value into the task and dispatch it to some thread to execute the task with "enqueue".
///
/// After a task is "dequeued" from thread, it will be in "running" state.
///
/// For one shot tasks(in which callback can ONLY be executed ONCE),  the task will be in
/// "finished" state after running of callbacks.
///
/// But for timer tasks, the state will transit to "ready" again after "running"
/// as the callback need to be executed periodically.
///
/// The callers can cancel the execution of "ready" tasks with method "cancel". However,
/// if a task is in not in "ready" state, the cancel will fail (returning false).
///
/// So from the perspective of task user, a created task can only be controlled by "enqueue" or
/// "cancel". An "enqueue" operation will make the callback to execute at some time in the future,
/// and an "cancel" operation will prevent the callback from executing.
///
/// So please take care when you call cancel. Memory leak may occur if you don't pay attention.
/// For example:
///
/// int *a = new int(5);
/// raw_task t = new raw_task(code, [a](){ std::cout << *a << std::endl; delete a; }, hash, node);
/// t->enqueue(10_seconds_latey);
/// if (t->cancel()) {
///    std::cout << "cancel succeed, the callback will not execute and a won't be deleted"
///               << std::endl;
/// }
///
/// In order to prevent this, we recommend you to pass RAII objects to callback:
///
/// std::shared_ptr<int> a = std::make_shared<int>(5);
/// raw_task t = new raw_task(code, [a]() { std::cout << *a << std::endl; }, hash, node);
///
/// In this case, the callback object will be destructed if t is cancelled.
///
/// Another key design in rDSN is that we add some "hook points" when the state is in transition,
/// like "on_task_create", "on_task_enqueue", "on_task_dequeue", etc. We can execute different
/// functions for different purposes on these hook points, you may want to refer to
/// "tracer", "profiler" and "fault_injector" for details.
///
class task : public ref_counter, public extensible_object<task, 4>
{
public:
    task(task_code code, int hash = 0, service_node *node = nullptr);

    virtual ~task();
    virtual void enqueue();

    //
    // if we successfully change a task's state from ready to cancelled, then return true,
    // otherwise, false is returned.
    //
    // if wait_until_finished is true, the function will wait a running task to be finished
    //
    // if finished isn't nullptr, it will be used to indicate
    // whether the task is finished/cancelled, that is to say:
    //    *finished == true <- the task has been finished/cancelled when this method returns
    //    *finished == false <- the task may still be running or ready(timer tasks)
    //
    bool cancel(bool wait_until_finished, /*out*/ bool *finished = nullptr);

    // wait until a task to finished/cancelled or timeout
    bool wait(int timeout_milliseconds = TIME_MS_MAX);

    // TODO: modify delay from chrono to int, keep in consistency with other API
    void enqueue(std::chrono::milliseconds delay)
    {
        set_delay(static_cast<int>(delay.count()));
        enqueue();
    }

    // used for task_worker to execute the task
    void exec_internal();

    // only call this function when task is running
    //
    // @param enqueue_immediately
    //    whether we should enqueue right now
    // return:
    //    true : change task state from TASK_STATE_RUNNING to TASK_STATE_READY succeed
    //    false : change task state failed
    bool set_retry(bool enqueue_immediately = true);

    void set_error_code(error_code err) { _error = err; }
    void set_delay(int delay_milliseconds = 0) { _delay_milliseconds = delay_milliseconds; }
    void set_tracker(task_tracker *tracker) { _context_tracker.set_tracker(tracker, this); }

    uint64_t id() const { return _task_id; }
    task_state state() const { return _state.load(std::memory_order_acquire); }
    task_code code() const { return _spec->code; }
    task_spec &spec() const { return *_spec; }
    int hash() const { return _hash; }
    int delay_milliseconds() const { return _delay_milliseconds; }
    error_code error() const { return _error; }
    service_node *node() const { return _node; }
    task_tracker *tracker() const { return _context_tracker.tracker(); }
    bool is_empty() const { return _is_null; }

    // static helper utilities
    static task *get_current_task();
    static uint64_t get_current_task_id();
    static task_worker *get_current_worker();
    static task_worker *get_current_worker2();
    static service_node *get_current_node();
    static service_node *get_current_node2();
    static int get_current_node_id();
    static int get_current_worker_index();
    static const char *get_current_node_name();
    static rpc_engine *get_current_rpc();
    static env_provider *get_current_env();

    static void set_tls_dsn_context(
        service_node *node, // cannot be null
        task_worker *worker // null for io or timer threads if they are not worker threads
        );

protected:
    void enqueue(task_worker_pool *pool);
    void set_task_id(uint64_t tid) { _task_id = tid; }

    virtual void exec() = 0;
    //
    // this function is used for clearing the non-trivial objects assigned to this task, like
    // callback functors and some task-specific values.
    //
    // circular reference may occur if we don't clear them manually, for example:
    //
    // class A: public dsn::ref_counter {
    // public:
    //   int value;
    //   task_ptr my_task;
    // };
    //
    // dsn::ref_ptr<A> a_obj = new A();
    // a_obj->my_task = tasking::enqueue(task_code,
    //                                   [a_obj](){ std::cout << value << std::endl; });
    //
    // in the case above, a_obj holds a ref_counter for my_task,
    // my task holds a ref_counter for a_obj because it owns a functor
    // which captures a_obj by value
    //
    // in order to prevent this case, we let the task to clear these non-trival objects when
    // a task is finished or cancelled.
    //
    // we may call this function in "exec_internal" or "cancel". however, it's still subclass's
    // duty to define "how to clear the callback".
    //
    // don't declare this as pure virtual function, coz it is not necessary for every subclass
    // to have non trivial objects to clear.
    //
    virtual void clear_non_trivial_on_task_end() {}

    bool _is_null;
    error_code _error;

private:
    friend class task_test;

    task(const task &);
    bool wait_on_cancel();

    // return true if some waiters have been notified
    bool signal_waiters();

    static void check_tls_dsn();
    static void on_tls_dsn_not_set();

    mutable std::atomic<task_state> _state;
    uint64_t _task_id;
    std::atomic<void *> _wait_event;
    int _hash;
    int _delay_milliseconds;
    bool _wait_for_cancel;
    task_spec *_spec;
    service_node *_node;
    trackable_task _context_tracker; // when tracker is gone, the task is cancelled automatically

public:
    // used by task queue only
    task *next;
};
typedef dsn::ref_ptr<dsn::task> task_ptr;

class raw_task : public task
{
public:
    raw_task(task_code code, const task_handler &cb, int hash = 0, service_node *node = nullptr)
        : task(code, hash, node), _cb(cb)
    {
    }
    raw_task(task_code code, task_handler &&cb, int hash = 0, service_node *node = nullptr)
        : task(code, hash, node), _cb(std::move(cb))
    {
    }

    void exec() override
    {
        if (dsn_likely(_cb != nullptr)) {
            _cb();
        }
    }

protected:
    void clear_non_trivial_on_task_end() override { _cb = nullptr; }

protected:
    task_handler _cb;
};

//----------------- timer task -------------------------------------------------------

class timer_task : public task
{
public:
    timer_task(task_code code,
               const task_handler &cb,
               int interval_milliseconds,
               int hash = 0,
               service_node *node = nullptr);
    timer_task(task_code code,
               task_handler &&cb,
               int interval_milliseconds,
               int hash = 0,
               service_node *node = nullptr);

    // for timer task, we will reset its state to TASK_READY after exec
    void exec() override;
    void enqueue() override;

    void update_interval(int interval_ms);

protected:
    void clear_non_trivial_on_task_end() override { _cb = nullptr; }

private:
    // ATTENTION: if _interval_ms == 0, then timer task will just be executed once;
    // otherwise, timer task will be executed periodically(period = _interval_ms)
    int _interval_ms;
    task_handler _cb;
};
typedef dsn::ref_ptr<dsn::timer_task> timer_task_ptr;

template <typename First, typename... Remaining>
class future_task : public task
{
public:
    typedef std::function<void(const First, const Remaining &...)> TCallback;
    future_task(task_code code, const TCallback &cb, int hash, service_node *node = nullptr)
        : task(code, hash, node), _cb(cb)
    {
    }
    future_task(task_code code, TCallback &&cb, int hash, service_node *node = nullptr)
        : task(code, hash, node), _cb(std::move(cb))
    {
    }
    virtual void exec() override { dsn::apply(_cb, std::move(_values)); }

    void enqueue_with(const First &t, const Remaining &... r, int delay_ms = 0)
    {
        _values = std::make_tuple(t, r...);
        set_delay(delay_ms);
        enqueue();
    }
    void enqueue_with(First &&t, Remaining &&... r, int delay_ms = 0)
    {
        _values = std::make_tuple(std::move(t), std::forward<Remaining>(r)...);
        set_delay(delay_ms);
        enqueue();
    }

protected:
    void clear_non_trivial_on_task_end() override
    {
        _cb = nullptr;
        _values = {};
    }

private:
    TCallback _cb;
    std::tuple<First, Remaining...> _values;
};

class rpc_request_task : public task
{
public:
    rpc_request_task(message_ex *request, rpc_request_handler &&h, service_node *node);
    virtual ~rpc_request_task() override;

    message_ex *get_request() const { return _request; }

    void enqueue() override;

    void exec() override
    {
        if (0 == _enqueue_ts_ns ||
            dsn_now_ns() - _enqueue_ts_ns <
                static_cast<uint64_t>(_request->header->client.timeout_ms) * 1000000ULL) {
            if (dsn_likely(nullptr != _handler)) {
                _handler(_request);
            }
        } else {
            LOG_DEBUG("rpc_request_task({}) from({}) stop to execute due to timeout_ms({}) exceed",
                      spec().name,
                      _request->header->from_address,
                      _request->header->client.timeout_ms);
            spec().on_rpc_task_dropped.execute(this);
        }
    }

protected:
    void clear_non_trivial_on_task_end() override { _handler = nullptr; }

protected:
    message_ex *_request;
    rpc_request_handler _handler;
    uint64_t _enqueue_ts_ns;
};
typedef dsn::ref_ptr<rpc_request_task> rpc_request_task_ptr;

class rpc_response_task : public task
{
public:
    rpc_response_task(message_ex *request,
                      const rpc_response_handler &cb,
                      int hash = 0,
                      service_node *node = nullptr);
    rpc_response_task(message_ex *request,
                      rpc_response_handler &&cb,
                      int hash,
                      service_node *node = nullptr);
    virtual ~rpc_response_task() override;

    // return true for normal case, false for fault injection applied
    bool enqueue(error_code err, message_ex *reply);

    // re-enqueue after above enqueue, e.g., after delay
    void enqueue() override;

    void exec() override
    {
        if (dsn_likely(nullptr != _cb)) {
            _cb(_error, _request, _response);
        }
    }

    message_ex *get_request() const { return _request; }
    message_ex *get_response() const { return _response; }

    //
    // rpc_response_task is a special kind of task, because
    // we support the semantic of distributed service in sending rpc request, for example:
    // "querying meta-server and send message to proper replica-server based on gpid".
    //
    // in order to support this, we need to replace the original rpc response callback
    // to another one:
    //    1. which can proecss the distributed service semantics, AND
    //    2. which won't call the original callback until real results are got
    //
    // we supply 2 functions to meet this demand:
    //    1. current_handler, you can get the original handler if you want to replace it
    //    2. replace_callback, replace the callback to any one you like. there are two varieties
    //       for coping or move the new callback
    //
    // yo may want to refer to rpc_engine::call_uri for details
    //
    // TODO(sunweijie): totally elimite this feature
    //
    void fetch_current_handler(rpc_response_handler &cb) { cb = std::move(_cb); }
    void replace_callback(rpc_response_handler &&cb)
    {
        task_state cur_state = state();
        CHECK(cur_state == TASK_STATE_READY || cur_state == TASK_STATE_RUNNING,
              "invalid task_state: {}",
              enum_to_string(cur_state));
        _cb = std::move(cb);
    }
    void replace_callback(const rpc_response_handler &cb)
    {
        replace_callback(rpc_response_handler(cb));
    }

    task_worker_pool *caller_pool() const { return _caller_pool; }
    void set_caller_pool(task_worker_pool *pl) { _caller_pool = pl; }

protected:
    void clear_non_trivial_on_task_end() override { _cb = nullptr; }

private:
    message_ex *_request;
    message_ex *_response;
    task_worker_pool *_caller_pool;
    rpc_response_handler _cb;

    friend class rpc_engine;
};
typedef dsn::ref_ptr<rpc_response_task> rpc_response_task_ptr;

const std::vector<task_worker *> &get_threadpool_threads_info(threadpool_code code);

// ------------------------ inline implementations --------------------
__inline /*static*/ void task::check_tls_dsn()
{
    if (tls_dsn.magic != 0xdeadbeef) {
        on_tls_dsn_not_set();
    }
}

__inline /*static*/ task *task::get_current_task()
{
    check_tls_dsn();
    return tls_dsn.current_task;
}

__inline /*static*/ uint64_t task::get_current_task_id()
{
    if (tls_dsn.magic == 0xdeadbeef)
        return tls_dsn.current_task ? tls_dsn.current_task->id() : 0;
    else
        return 0;
}

__inline /*static*/ task_worker *task::get_current_worker()
{
    check_tls_dsn();
    return tls_dsn.worker;
}

__inline /*static*/ task_worker *task::get_current_worker2()
{
    return tls_dsn.magic == 0xdeadbeef ? tls_dsn.worker : nullptr;
}

__inline /*static*/ service_node *task::get_current_node()
{
    check_tls_dsn();
    return tls_dsn.node;
}

__inline /*static*/ int task::get_current_node_id()
{
    return tls_dsn.magic == 0xdeadbeef ? tls_dsn.node_id : 0;
}

__inline /*static*/ service_node *task::get_current_node2()
{
    return tls_dsn.magic == 0xdeadbeef ? tls_dsn.node : nullptr;
}

__inline /*static*/ int task::get_current_worker_index()
{
    check_tls_dsn();
    return tls_dsn.worker_index;
}

__inline /*static*/ rpc_engine *task::get_current_rpc()
{
    check_tls_dsn();
    return tls_dsn.rpc;
}

__inline /*static*/ env_provider *task::get_current_env()
{
    check_tls_dsn();
    return tls_dsn.env;
}

} // namespace dsn
