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
# include <dsn/service_api.h>
# include <dsn/internal/task.h>
# include "service_engine.h"
# include <dsn/internal/env_provider.h>
# include "task_engine.h"
# include <dsn/internal/utils.h>
# include <dsn/internal/service_app.h>
# include "service_engine.h"
# include "disk_engine.h"
# include "rpc_engine.h"
# include <dsn/internal/synchronize.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "task"

namespace dsn {

__thread struct __tls_task_info__ tls_task_info;

/*static*/ void task::set_current_worker(task_worker* worker)
{
    if (tls_task_info.magic == 0xdeadbeef)
    {
        tls_task_info.worker = worker;
        tls_task_info.worker_index = worker ? worker->index() : -1;
    }
    else
    {
        tls_task_info.magic = 0xdeadbeef;
        tls_task_info.worker = worker;
        tls_task_info.worker_index = worker ? worker->index() : -1;
        tls_task_info.current_task = nullptr;
    }
}

task::task(task_code code, int hash, service_node* node)
    : _state(TASK_STATE_READY)
{
    _spec = task_spec::get(code);
    _task_id = (uint64_t)(this);
    _wait_event.store(nullptr);
    _hash = hash;
    _delay_milliseconds = 0;
    _wait_for_cancel = false;
    _is_null = false;
    
    if (node != nullptr)
    {
        _node = node;
    }
    else
    {
        auto p = get_current_task();
        dassert(p != nullptr, "tasks without explicit service node "
            "can only be created inside other tasks");
        _node = p->node();
    }
}

task::~task()
{
    if (nullptr != _wait_event.load())
    {
        delete (utils::notify_event*)_wait_event.load();
        _wait_event.store(nullptr);
    }
}

void task::exec_internal()
{
    task_state READY_STATE = TASK_STATE_READY;
    task_state RUNNING_STATE = TASK_STATE_RUNNING;

    if (_state.compare_exchange_strong(READY_STATE, TASK_STATE_RUNNING))
    {
        task* parent_task = nullptr;
        if (tls_task_info.magic == 0xdeadbeef)
        {
            parent_task = tls_task_info.current_task;
        }
        else
        {
            set_current_worker(nullptr);
        }
        
        tls_task_info.current_task = this;

        _spec->on_task_begin.execute(this);

        exec();
        
        if (_state.compare_exchange_strong(RUNNING_STATE, TASK_STATE_FINISHED))
        {
            _spec->on_task_end.execute(this);

            // signal_waiters(); [
            // inline for performance
            void* evt = _wait_event.load();
            if (evt != nullptr)
            {
                auto nevt = (utils::notify_event*)evt;
                nevt->notify();
            }
            // ]
        }

        // for timer
        else
        {
            if (!_wait_for_cancel)
            {
                _spec->on_task_end.execute(this);
                enqueue();
            }   
            else
            {
                _state.compare_exchange_strong(READY_STATE, TASK_STATE_CANCELLED);
                _spec->on_task_end.execute(this);

                // signal_waiters(); [
                // inline for performance
                void* evt = _wait_event.load();
                if (evt != nullptr)
                {
                    auto nevt = (utils::notify_event*)evt;
                    nevt->notify();
                }
                // ]
            }
        }
        
        tls_task_info.current_task = parent_task;
    }

    if (!_spec->allow_inline && !_is_null)
    {
        service::lock_checker::check_dangling_lock();
    }
}

void task::signal_waiters()
{
    void* evt = _wait_event.load();
    if (evt != nullptr)
    {
        auto nevt = (utils::notify_event*)evt;
        nevt->notify();
    }
}

// multiple callers may wait on this
bool task::wait(int timeout_milliseconds, bool on_cancel)
{
    dassert (this != task::get_current_task(), "task cannot wait itself");

    if (!spec().on_task_wait_pre.execute(task::get_current_task(), this, (uint32_t)timeout_milliseconds, true))
    {
        spec().on_task_wait_post.execute(task::get_current_task(), this, false);
        return false;
    }

    auto cs = state();
    if (!on_cancel)
    {
        service::lock_checker::check_wait_task(this);
    }

    if (cs >= TASK_STATE_FINISHED)
    {
        spec().on_task_wait_post.execute(task::get_current_task(), this, true);
        return true;
    }

    // TODO: using event pool instead
    void* evt = _wait_event.load();
    if (evt == nullptr)
    {
        evt = new utils::notify_event();

        void* null_h = nullptr;
        if (!_wait_event.compare_exchange_strong(null_h, evt))
        {
            delete (utils::notify_event*)evt;
            evt = _wait_event.load();
        }
    }

    bool ret = (state() >= TASK_STATE_FINISHED);
    if (!ret)
    {
        auto nevt = (utils::notify_event*)evt;
        ret = (nevt->wait_for(timeout_milliseconds));
    }

    spec().on_task_wait_post.execute(task::get_current_task(), this, ret);
    return ret;
}

//
// return - whether this cancel succeed
//
bool task::cancel(bool wait_until_finished, /*out*/ bool* finished /*= nullptr*/)
{
    task_state READY_STATE = TASK_STATE_READY;
    task *current_tsk = task::get_current_task();
    bool finish = false;
    bool succ = false;
    
    if (current_tsk == this)
    {
        /*dwarn(
            "task %s (id=%016llx) cannot cancel itself",                
            spec().name,
            id()
            );*/

        if (finished)
            *finished = false;

        return false;
    }
    
    if (_state.compare_exchange_strong(READY_STATE, TASK_STATE_CANCELLED))
    {
        succ = true;
        finish = true;
    }
    else
    {
        task_state old_state = _state.load();
        if (old_state == TASK_STATE_CANCELLED)
        {
            succ = false; // this cancellation fails
            finish = true;
        }
        else if (old_state == TASK_STATE_FINISHED)
        {
            succ = false;
            finish = true;
        }
        else if (wait_until_finished)
        {
            _wait_for_cancel = true;
            bool r  = wait(TIME_MS_MAX, true);
            dassert(r, "wait failed, it is only possible when task runs for more than 0x0fffffff ms");

            succ = false;
            finish = true;
        }
        else
        {
            succ = false;
            finish = false;
        }
    }

    if (current_tsk != nullptr)
    {
        current_tsk->spec().on_task_cancel_post.execute(current_tsk, this, succ);
    }

    if (succ)
    {
        spec().on_task_cancelled.execute(this);
        signal_waiters();
    }

    if (finished)
        *finished = finish;

    return succ;
}

const char* task::node_name() const
{
    return node()->name();
}

void task::enqueue()
{        
    dassert(_node != nullptr, "service node unknown for this task");
    dassert(_spec->type != TASK_TYPE_RPC_RESPONSE, "tasks with TASK_TYPE_RPC_RESPONSE type use task::enqueue(caller_pool()) instead");
    auto pool = node()->computation()->get_pool(spec().pool_code);
    enqueue(pool);
}

void task::enqueue(task_worker_pool* pool)
{
    if (spec().type == TASK_TYPE_COMPUTE)
    {
        spec().on_task_enqueue.execute(task::get_current_task(), this);
    }

    // fast execution
    if (_delay_milliseconds == 0
        && (_spec->allow_inline || _spec->fast_execution_in_network_thread || _is_null)
       )
    {
        exec_internal();
    }

    // normal path
    else
    {
        dassert(pool != nullptr, "pool %s not ready, and there are usually two cases: "
            "(1). thread pool not designatd in '[%s] pools'; "
            "(2). the caller is executed in io threads "
            "which is forbidden unless you explicitly set [task.%s].fast_execution_in_network_thread = true",            
            _spec->pool_code.to_string(),
            _node->spec().config_section.c_str(),
            _spec->name
            );

        pool->enqueue(this);
    }
}

timer_task::timer_task(task_code code,  uint32_t interval_milliseconds, int hash) 
    : task(code, hash), _interval_milliseconds(interval_milliseconds) 
{
    dassert (TASK_TYPE_COMPUTE == spec().type, "this must be a computation type task, please use DEFINE_TASK_CODE to define the task code");

    // enable timer randomization to avoid lots of timers execution simultaneously
    set_delay(::dsn::service::env::random32(0, interval_milliseconds));
}

void timer_task::exec()
{
    task_state RUNNING_STATE = TASK_STATE_RUNNING;
    
    bool conti = on_timer();

    if (conti && _interval_milliseconds > 0)
    {
        if (_state.compare_exchange_strong(RUNNING_STATE, TASK_STATE_READY))
        {
            set_delay(_interval_milliseconds);            
        }        
    }
}

rpc_request_task::rpc_request_task(message_ptr& request, service_node* node) 
    : task(task_code(request->header().local_rpc_code), request->header().client.hash, node), 
    _request(request)
{

    dbg_dassert (TASK_TYPE_RPC_REQUEST == spec().type, "task type must be RPC_REQUEST, please use DEFINE_TASK_CODE_RPC to define the task code");
}

void rpc_request_task::enqueue(service_node* node)
{
    spec().on_rpc_request_enqueue.execute(this);
    task::enqueue(node->computation()->get_pool(spec().pool_code));
}

void rpc_response_task::exec() 
{ 
    on_response(error(), _request, _response);
}

rpc_response_task::rpc_response_task(message_ptr& request, int hash)
    : task(task_spec::get(request->header().local_rpc_code)->rpc_paired_code, 
           hash == 0 ? request->header().client.hash : hash)
{
    set_error_code(ERR_IO_PENDING);

    dbg_dassert (TASK_TYPE_RPC_RESPONSE == spec().type, "task must be of RPC_RESPONSE type, please use DEFINE_TASK_CODE_RPC to define the request task code");

    _request = request;
    _caller_pool = task::get_current_worker() ? 
        task::get_current_worker()->pool() : nullptr;
}

void rpc_response_task::enqueue(error_code err, message_ptr& reply)
{
    set_error_code(err);
    _response = (err == ERR_OK ? reply : nullptr);

    if (spec().on_rpc_response_enqueue.execute(this, true))
    {
        task::enqueue(_caller_pool);
    }
}

rpc_response_task_empty::rpc_response_task_empty(message_ptr& request, int hash)
    : rpc_response_task(request, hash)
{
    _is_null = true;
}

aio_task::aio_task(task_code code, int hash) 
    : task(code, hash)
{
    dassert (TASK_TYPE_AIO == spec().type, "task must be of AIO type, please use DEFINE_TASK_CODE_AIO to define the task code");
    set_error_code(ERR_IO_PENDING);

    _aio = node()->disk()->prepare_aio_context(this);
}

void aio_task::exec() 
{ 
    on_completed(error(), _transferred_size);
}

void aio_task::enqueue(error_code err, uint32_t transferred_size, service_node* node)
{
    set_error_code(err);
    _transferred_size = transferred_size;

    spec().on_aio_enqueue.execute(this);

    if (node != nullptr)
    {
        task::enqueue(node->computation()->get_pool(spec().pool_code));
    }
    else
    {
        task::enqueue();
    }
}

} // end namespace
