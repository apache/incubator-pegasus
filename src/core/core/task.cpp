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
 *     task is the execution of a piece of sequence code, which completes
 *     a meaningful application level task.
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

# include <dsn/service_api_c.h>
# include <dsn/internal/task.h>
# include <dsn/internal/env_provider.h>
# include <dsn/cpp/utils.h>
# include <dsn/internal/synchronize.h>
# include <dsn/tool/node_scoper.h>

# include "task_engine.h"
# include "service_engine.h"
# include "service_engine.h"
# include "disk_engine.h"
# include "rpc_engine.h"


# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "task"

namespace dsn 
{
__thread struct __tls_dsn__ tls_dsn;
__thread uint16_t tls_dsn_lower32_task_id_mask = 0;

/*static*/ void task::set_tls_dsn_context(
    service_node* node,  // cannot be null
    task_worker* worker, // null for io or timer threads if they are not worker threads
    task_queue* queue    // owner queue if io_mode == IOE_PER_QUEUE
    )
{
    memset((void*)&dsn::tls_dsn, 0, sizeof(dsn::tls_dsn));
    dsn::tls_dsn.magic = 0xdeadbeef;
    dsn::tls_dsn.worker_index = -1;

    if (node)
    {
        if (worker != nullptr)
        {
            dassert(worker->pool()->node() == node,
                "worker not belonging to the given node: %s vs %s",
                worker->pool()->node()->name(),
                node->name()
                );
        }

        if (queue != nullptr)
        {
            dassert(queue->pool()->node() == node,
                "queue not belonging to the given node: %s vs %s",
                queue->pool()->node()->name(),
                node->name()
                );
        }

        tls_dsn.node = node;
        tls_dsn.worker = worker;
        tls_dsn.worker_index = worker ? worker->index() : -1;
        tls_dsn.current_task = nullptr;
        tls_dsn.rpc = node->rpc(queue ? queue : (worker ? worker->queue() : nullptr));
        tls_dsn.disk = node->disk(queue ? queue : (worker ? worker->queue() : nullptr));
        tls_dsn.env = service_engine::fast_instance().env();
        tls_dsn.nfs = node->nfs(queue ? queue : (worker ? worker->queue() : nullptr));
        tls_dsn.tsvc = node->tsvc(queue ? queue : (worker ? worker->queue() : nullptr));
    }

    tls_dsn.node_pool_thread_ids = (node ? ((uint64_t)(uint8_t)node->id()) : 0) << (64 - 8); // high 8 bits for node id
    tls_dsn.node_pool_thread_ids |= (worker ? ((uint64_t)(uint8_t)(int)worker->pool_spec().pool_code) : 0) << (64 - 8 - 8); // next 8 bits for pool id
    auto worker_idx = worker ? worker->index() : -1;
    if (worker_idx == -1)
    {
        worker_idx = ::dsn::utils::get_current_tid();
    }
    tls_dsn.node_pool_thread_ids |= ((uint64_t)(uint16_t)worker_idx) << 32; // next 16 bits for thread id
    tls_dsn.last_lower32_task_id = worker ? 0 : ((uint32_t)(++tls_dsn_lower32_task_id_mask)) << 16;
}

/*static*/ void task::on_tls_dsn_not_set()
{
    if (service_engine::instance().spec().enable_default_app_mimic)
    {
        dsn_mimic_app("mimic", 1);
    }
    else
    {
        dassert(false, "rDSN context is not initialized properly, to be fixed as follows:\n"
            "(1). the current thread does NOT belongs to any rDSN service node, please invoke dsn_mimic_app first,\n"
            "     or, you can enable [core] enable_default_app_mimic = true in your config file so mimic_app can be omitted\n"
            "(2). the current thread belongs to a rDSN service node, and you are writing providers for rDSN, please use\n"
            "     task::set_tls_dsn_context(...) at the beginning of your new thread in your providers;\n"
            "(3). this should not happen, please help fire an issue so we we can investigate"            
            );
    }
}

task::task(dsn_task_code_t code, void* context, dsn_task_cancelled_handler_t on_cancel, int hash, service_node* node)
    : _state(TASK_STATE_READY), _wait_event(nullptr)
{
    _spec = task_spec::get(code);
    _context = context;
    _on_cancel = on_cancel;
    _hash = hash;
    _delay_milliseconds = 0;
    _wait_for_cancel = false;
    _is_null = false;
    next = nullptr;
    
    if (node != nullptr)
    {
        _node = node;
    }
    else
    {
        auto p = get_current_node();
        dassert(p != nullptr, "tasks without explicit service node "
            "can only be created inside threads which is attached to specific node");
        _node = p;
    }

    if (tls_dsn.magic != 0xdeadbeef)
    {
        task::set_tls_dsn_context(nullptr, nullptr, nullptr);
    }

    _task_id = tls_dsn.node_pool_thread_ids + (++tls_dsn.last_lower32_task_id);
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
    bool notify_if_necessary = true;

    if (_state.compare_exchange_strong(READY_STATE, TASK_STATE_RUNNING, std::memory_order_relaxed))
    {
        dassert(tls_dsn.magic == 0xdeadbeef, "thread is not inited with task::set_tls_dsn_context");

        task* parent_task = tls_dsn.current_task;
        tls_dsn.current_task = this;

        _spec->on_task_begin.execute(this);

        exec();
        if (_state.compare_exchange_strong(RUNNING_STATE, TASK_STATE_FINISHED, std::memory_order_release, std::memory_order_relaxed))
        {
            _spec->on_task_end.execute(this);
        }
        else
        {
            if (!_wait_for_cancel)
            {
                // for timer
                notify_if_necessary = false;
                _spec->on_task_end.execute(this);
                enqueue();
            }   
            else
            {
                // for cancelled
                if (_state.compare_exchange_strong(READY_STATE, TASK_STATE_CANCELLED, std::memory_order_release, std::memory_order_relaxed))
                {
                    _spec->on_task_cancelled.execute(this);
                }

                // always call on_task_end()
                _spec->on_task_end.execute(this);
            }
        }
        
        tls_dsn.current_task = parent_task;
    }
    else
    {
        // task cancelled, so
        _error.end_tracking();
    }
    
    // signal_waiters(); [
    // inline for performance
    if (notify_if_necessary)
    {
        void* evt = _wait_event.load();
        if (evt != nullptr)
        {
            auto nevt = (utils::notify_event*)evt;
            nevt->notify();
            spec().on_task_wait_notified.execute(this);
        }
    }    
    // ]

    if (!_spec->allow_inline && !_is_null)
    {
        lock_checker::check_dangling_lock();
    }

    this->release_ref(); // added in enqueue(pool)
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

    auto cs = state();
    if (!on_cancel)
    {
        lock_checker::check_wait_task(this);
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

    spec().on_task_wait_pre.execute(task::get_current_task(), this, (uint32_t)timeout_milliseconds);

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
            spec().name(),
            id()
            );*/
        
        // make sure timers are cancelled
        _wait_for_cancel = true;

        if (finished)
            *finished = false;

        return false;
    }
    
    if (_state.compare_exchange_strong(READY_STATE, TASK_STATE_CANCELLED, std::memory_order_relaxed))
    {
        succ = true;
        finish = true;
    }
    else
    {
        task_state old_state = READY_STATE;
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
        //
        // TODO: pros and cons of executing on_cancel here
        // or in exec_internal
        //
        if (_on_cancel)
        {
            _on_cancel(_context);
        }

        spec().on_task_cancelled.execute(this);
        signal_waiters();

        _error.end_tracking();
    }

    if (finished)
        *finished = finish;

    return succ;
}

const char* task::get_current_node_name()
{
    auto n = task::get_current_node2();
    return n ? n->name() : "unknown";
}

void task::enqueue()
{        
    dassert(_node != nullptr, "service node unknown for this task");
    dassert(_spec->type != TASK_TYPE_RPC_RESPONSE,
        "tasks with TASK_TYPE_RPC_RESPONSE type use task::enqueue(caller_pool()) instead");
    auto pool = node()->computation()->get_pool(spec().pool_code);
    enqueue(pool);
}

void task::enqueue(task_worker_pool* pool)
{
    this->add_ref(); // released in exec_internal (even when cancelled)

    if (spec().type == TASK_TYPE_COMPUTE)
    {
        spec().on_task_enqueue.execute(task::get_current_task(), this);
    }

    // for delayed tasks, refering to timer service
    if (_delay_milliseconds != 0)
    {
        pool->add_timer(this);
        return;
    }

    // fast execution
    if (_is_null)
    {
        dassert (_node == task::get_current_node(), "");
        exec_internal();
        return;
    }
    else if (_spec->allow_inline)
    {
        // inlined
        // warning - this may lead to deadlocks, e.g., allow_inlined
        // task tries to get a non-recursive lock that is already hold
        // by the caller task
        if (_spec->type == TASK_TYPE_COMPUTE)
        {
            if (_node != task::get_current_node())
            {
                tools::node_scoper ns(_node);
                exec_internal();
            }
            else
            {
                exec_internal();
            }
            return;
        }

        // io tasks only inlined in io threads
        else if (task::get_current_worker2() == nullptr)
        {
            dassert(_node == task::get_current_node(), "");
            exec_internal();
            return;
        }
    }

    // normal path
    dassert(pool != nullptr, "pool %s not ready, and there are usually two cases: "
        "(1). thread pool not designatd in '[%s] pools'; "
        "(2). the caller is executed in io threads "
        "which is forbidden unless you explicitly set [task.%s].allow_inline = true",
        dsn_threadpool_code_to_string(_spec->pool_code),
        _node->spec().config_section.c_str(),
        _spec->name.c_str()
        );

    pool->enqueue(this);
}

timer_task::timer_task(
    dsn_task_code_t code, 
    dsn_task_handler_t cb, 
    void* context,
    dsn_task_cancelled_handler_t on_cancel,
    uint32_t interval_milliseconds, 
    int hash, 
    service_node* node
    )
    : task(code, context, on_cancel, hash, node),
    _interval_milliseconds(interval_milliseconds),
    _cb(cb)
{
    dassert (TASK_TYPE_COMPUTE == spec().type,
        "%s is not a computation type task, please use DEFINE_TASK_CODE to define the task code",
        spec().name.c_str()
        );
}

void timer_task::enqueue()
{
    // enable timer randomization to avoid lots of timers execution simultaneously
    if (delay_milliseconds() == 0 && spec().randomize_timer_delay_if_zero)
    {
        set_delay(dsn_random32(0, _interval_milliseconds));
    }
    
    return task::enqueue();
}

void timer_task::exec()
{
    _cb(_context);

    if (_interval_milliseconds > 0)
    {
        //_state must be TASK_STATE_RUNNING here
        dbg_dassert(_state.load(std::memory_order_relaxed) == TASK_STATE_RUNNING, "corrupted timer task state");
        _state.store(TASK_STATE_READY, std::memory_order_release);
        set_delay(_interval_milliseconds);
    }
}

rpc_request_task::rpc_request_task(message_ex* request, rpc_handler_info* h, service_node* node)
    : task(dsn_task_code_t(request->local_rpc_code), nullptr, 
        [](void*) { dassert(false, "rpc request task cannot be cancelled"); },
        request->header->client.hash, node),
    _request(request),
    _handler(h),
    _enqueue_ts_ns(0)
{
    dbg_dassert (TASK_TYPE_RPC_REQUEST == spec().type, 
        "%s is not a RPC_REQUEST task, please use DEFINE_TASK_CODE_RPC to define the task code",
        spec().name.c_str()
        );

    _request->add_ref(); // released in dctor
}

rpc_request_task::~rpc_request_task()
{
    _request->release_ref(); // added in ctor
}

void rpc_request_task::enqueue()
{
    if (spec().rpc_request_dropped_before_execution_when_timeout)
    {
        _enqueue_ts_ns = dsn_now_ns();
    }
    task::enqueue(node()->computation()->get_pool(spec().pool_code));
}

rpc_response_task::rpc_response_task(
    message_ex* request, 
    dsn_rpc_response_handler_t cb,
    void* context, 
    dsn_task_cancelled_handler_t on_cancel,
    int hash, 
    service_node* node
    )
    : task(task_spec::get(request->local_rpc_code)->rpc_paired_code, context, on_cancel,
           hash == 0 ? request->header->client.hash : hash, node)
{
    _cb = cb;
    _is_null = (_cb == nullptr);

    set_error_code(ERR_IO_PENDING);

    dbg_dassert (TASK_TYPE_RPC_RESPONSE == spec().type, 
        "%s is not of RPC_RESPONSE type, please use DEFINE_TASK_CODE_RPC to define the request task code",
        spec().name.c_str()
        );

    _request = request;
    _response = nullptr;

    _caller_pool = task::get_current_worker() ? 
        task::get_current_worker()->pool() : nullptr;

    _request->add_ref(); // released in dctor
}

rpc_response_task::~rpc_response_task()
{
    _request->release_ref(); // added in ctor

    if (_response != nullptr)
        _response->release_ref(); // added in enqueue
}

void rpc_response_task::enqueue(error_code err, message_ex* reply)
{
    set_error_code(err);
    _response = reply;

    if (nullptr != reply)
    {
        dassert(err == ERR_OK, "error code must be success when reply is present");
        reply->add_ref(); // released in dctor
    }

    rpc_response_task::enqueue();
}

void rpc_response_task::enqueue()
{
    if (_caller_pool)
        task::enqueue(_caller_pool);

    // possible when it is called in non-rDSN threads
    else
    {
        auto pool = node()->computation()->get_pool(spec().pool_code);
        task::enqueue(pool);
    }
}

void rpc_response_task::add_hook(dsn_rpc_response_handler_t callback, void* context, bool is_before_hook)
{
    struct hook_context : public transient_object
    {
        dsn_rpc_response_handler_t old_callback;
        dsn_task_cancelled_handler_t old_on_cancel;
        void* old_context;

        dsn_rpc_response_handler_t new_callback;
        void* new_context;

        bool is_before_hook;
    };

    hook_context* nc = new hook_context();
    nc->old_callback = _cb;
    nc->old_context = _context;
    nc->old_on_cancel = _on_cancel;
    nc->new_callback = callback;
    nc->new_context = context;
    nc->is_before_hook = is_before_hook;

    _context = nc;

    _cb = [](dsn_error_t err, dsn_message_t req, dsn_message_t resp, void* ctx)
    {
        auto nc = (hook_context*)ctx;

        if (nc->is_before_hook)
        {
            nc->new_callback(err, req, resp, nc->new_context);
        }

        if (nc->old_callback)
        {
            nc->old_callback(err, req, resp, nc->old_context);
        }

        if (!nc->is_before_hook)
        {
            nc->new_callback(err, req, resp, nc->new_context);
        }
        delete nc;
    };

    _on_cancel = [](void* ctx)
    {
        auto nc = (hook_context*)ctx;
        if (nc->old_on_cancel != nullptr)
        {
            nc->old_on_cancel(nc->old_context);
        }

        delete nc;
    };
}

aio_task::aio_task(
    dsn_task_code_t code, 
    dsn_aio_handler_t cb, 
    void* context, 
    dsn_task_cancelled_handler_t on_cancel,
    int hash,
    service_node* node
    )
    : task(code, context, on_cancel, hash, node)
{
    _cb = cb;
    _is_null = (_cb == nullptr);

    dassert (TASK_TYPE_AIO == spec().type, 
        "%s is not of AIO type, please use DEFINE_TASK_CODE_AIO to define the task code",
        spec().name.c_str()
        );
    set_error_code(ERR_IO_PENDING);

    auto disk = task::get_current_disk();
    if (!disk) disk = node->node_disk();
    _aio = disk->prepare_aio_context(this);
}

aio_task::~aio_task()
{
    delete _aio;
}

void aio_task::enqueue(error_code err, size_t transferred_size)
{
    set_error_code(err);
    _transferred_size = transferred_size;

    spec().on_aio_enqueue.execute(this);

    task::enqueue(node()->computation()->get_pool(spec().pool_code));
}

} // end namespace
