# include <rdsn/service_api.h>
# include <rdsn/internal/task.h>
# include "service_engine.h"
# include <rdsn/internal/env_provider.h>
# include "task_engine.h"
# include <rdsn/internal/utils.h>
# include <rdsn/internal/service_app.h>
# include "service_engine.h"
# include "disk_engine.h"
# include <rdsn/internal/synchronize.h>

#define __TITLE__ "task"

namespace rdsn {

static __thread
struct 
{ 
    uint32_t    magic; 
    task       *currentTask;
    task_worker *currentWorker;
} tls_taskInfo;  

/*static*/ task* task::get_current_task()
{
    if (tls_taskInfo.magic == 0xdeadbeef)
        return tls_taskInfo.currentTask;
    else
        return nullptr;
}

/*static*/ uint64_t task::get_current_task_id()
{
    if (tls_taskInfo.magic == 0xdeadbeef)
        return tls_taskInfo.currentTask->id();
    else
        return 0;
}


/*static*/ task_worker* task::get_current_worker()
{
    if (tls_taskInfo.magic == 0xdeadbeef)
        return tls_taskInfo.currentWorker;
    else
        return nullptr;
}

/*static*/ task_worker_pool* task::get_current_worker_pool()
{
    if (tls_taskInfo.magic == 0xdeadbeef)
        return tls_taskInfo.currentWorker->pool();
    else
        return nullptr;
}

/*static*/ service_node* task::get_current_node()
{
    if (tls_taskInfo.magic == 0xdeadbeef)
        return tls_taskInfo.currentWorker->pool()->node();
    else
        return nullptr;
}

/*static*/ void task::set_current_worker(task_worker* worker)
{
    tls_taskInfo.magic = 0xdeadbeef;
    tls_taskInfo.currentWorker = worker;
    tls_taskInfo.currentTask = nullptr;
}

task::task(task_code code, int hash)
    : _state(TASK_STATE_READY)
{
    _spec = task_spec::get(code);
    _task_id = utils::get_random64(); 
    _wait_event.store(nullptr);
    _hash = hash;
    _delay_milliseconds = 0;
    _caller_worker = task::get_current_worker();
    memset((void*)&internal_use_only, 0, sizeof(internal_use_only));
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
        auto parentTask = tls_taskInfo.currentTask;
        tls_taskInfo.currentTask = this;

        _spec->on_task_begin.execute(this);

        exec();
        
        _state.compare_exchange_strong(RUNNING_STATE, TASK_STATE_FINISHED);
        
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
        
        tls_taskInfo.currentTask = parentTask;
    }

    if (!_spec->allow_inline)
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
bool task::wait(int timeout_milliseconds)
{
    service::lock_checker::check_wait_safety();

    rdsn_assert (this != task::get_current_task(), "task cannot wait itself");

    if (!spec().on_task_wait_pre.execute(task::get_current_task(), this, (uint32_t)timeout_milliseconds, true))
    {
        spec().on_task_wait_post.execute(task::get_current_task(), this, false);
        return false;
    }

    if (state() >= TASK_STATE_FINISHED)
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

bool task::cancel(bool wait_until_finished)
{
    task_state READY_STATE = TASK_STATE_READY;
    task *current_tsk = task::get_current_task();
    bool ret = true;
    bool succ = false;

    if (current_tsk == this)
    {
        rdsn_warn(
            "task %s (id=%016llx) cannot cancel itself",                
            spec().name,
            id()
            );
        return false;
    }
    
    if (_state.compare_exchange_strong(READY_STATE, TASK_STATE_CANCELLED))
    {
        succ = true;
    }
    else
    {
        task_state old_state = _state.load();
        if ((old_state == TASK_STATE_CANCELLED) || (old_state == TASK_STATE_FINISHED))
        {
        }
        else if (wait_until_finished)
        {
            wait();
        }
        else
        {
            ret = false;
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

    return ret;
}

void task::enqueue(int delay_milliseconds, service::service_app* app)
{        
    task_worker_pool* pool = nullptr;
    if (caller_worker() != nullptr)
    {
        rdsn_debug_assert(app == nullptr || caller_worker()->pool()->engine() == app->svc_node()->computation(), "tasks can only be dispatched to local node");
        if (spec().type != TASK_TYPE_RPC_RESPONSE)
        {
            pool = caller_worker()->pool()->engine()->get_pool(spec().pool_code);
        }
        else
        {
            pool = caller_worker()->pool();
        }
    }
    else if (app != nullptr)
    {
        //rdsn_assert (app != nullptr, "tasks enqueued outside tasks must be specified with which service app");
        pool = app->svc_node()->computation()->get_pool(spec().pool_code);
    }
    else
    {
		rdsn_assert(false, "neither inside a service, nor service app is specified, unable to find the right engine to execute this");
        //pool = service_engine::instance().primary_node()->computation()->get_pool(spec().pool_code);
    }

    enqueue(delay_milliseconds, pool);
}

void task::enqueue(int delay_milliseconds, task_worker_pool* pool)
{
    rdsn_assert(pool != nullptr, "pool not exist");

    set_delay(delay_milliseconds);

    if (spec().type == TASK_TYPE_COMPUTE)
    {
        spec().on_task_enqueue.execute(task::get_current_task(), this);
    }

    if (spec().allow_inline)
    {
        exec_internal();
    }
    else
    {
        task_ptr this_(this);
        pool->enqueue_task(this_);
    }
}

timer_task::timer_task(task_code code,  uint32_t interval_milliseconds, int hash) 
    : task(code, hash), _interval_milliseconds(interval_milliseconds) 
{
    rdsn_assert (TASK_TYPE_COMPUTE == spec().type, "this must be a computation type task");
}

void timer_task::exec()
{
    task_state RUNNING_STATE = TASK_STATE_RUNNING;
    
    bool conti = on_timer();

    if (conti && _interval_milliseconds > 0)
    {
        if (_state.compare_exchange_strong(RUNNING_STATE, TASK_STATE_READY))
        {
            enqueue(_interval_milliseconds);
        }        
    }
}

rpc_request_task::rpc_request_task(message_ptr& request) 
    : task(task_code(request->header().local_rpc_code), request->header().hash), 
      _request(request)
{
    rdsn_debug_assert (TASK_TYPE_RPC_REQUEST == spec().type, "task type must be RPC_REQUEST");
}

void rpc_request_task::enqueue(int delay_milliseconds, service_node* node)
{
    spec().on_rpc_request_enqueue.execute(this);
    task::enqueue(delay_milliseconds, node->computation()->get_pool(spec().pool_code));
}

void rpc_response_task::exec() 
{ 
    on_response(error(), _request, _response);
}

rpc_response_task::rpc_response_task(message_ptr& request, int hash)
    : task(task_spec::get(request->header().local_rpc_code)->rpc_paired_code, hash)
{
    set_error_code(ERR_IO_PENDING);

    rdsn_debug_assert (TASK_TYPE_RPC_RESPONSE == spec().type, "task must be of RPC_RESPONSE type");

    _request = request;
}

void rpc_response_task::enqueue(error_code err, message_ptr& reply, int delay_milliseconds)
{
    set_error_code(err);
    _response = (err == ERR_SUCCESS ? reply : nullptr);

    if (spec().on_rpc_response_enqueue.execute(this, true))
    {
        task::enqueue(delay_milliseconds);
    }
}

aio_task::aio_task(task_code code, int hash) 
    : task(code, hash)
{
    rdsn_assert (TASK_TYPE_AIO == spec().type, "task must be of AIO type");
    set_error_code(ERR_IO_PENDING);

    auto node = task::get_current_node();
    rdsn_assert(node != nullptr, "this function can only be invoked inside tasks");

    _aio = node->disk()->prepare_aio_context(this);
}

void aio_task::exec() 
{ 
    on_completed(error(), _transferred_size);
}

void aio_task::enqueue(error_code err, uint32_t transferred_size, int delay_milliseconds, service_node* node)
{
    set_error_code(err);
    _transferred_size = transferred_size;

    spec().on_aio_enqueue.execute(this);

    if (node != nullptr)
    {
        task::enqueue(delay_milliseconds, node->computation()->get_pool(spec().pool_code));
    }
    else
    {
        task::enqueue(delay_milliseconds, (service::service_app*)nullptr);
    }
}

} // end namespace
