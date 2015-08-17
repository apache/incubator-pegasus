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

# include <dsn/ports.h>
# include <dsn/internal/extensible_object.h>
# include <dsn/internal/task_tracker.h>
# include <dsn/internal/task_spec.h>
# include <dsn/internal/rpc_message.h>
# include <dsn/internal/link.h>
# include <dsn/cpp/auto_codes.h>
# include <dsn/cpp/utils.h>

namespace dsn {

namespace lock_checker {
    extern __thread int zlock_exclusive_count;
    extern __thread int zlock_shared_count;
    extern void check_wait_safety();
    extern void check_dangling_lock();
    extern void check_wait_task(task* waitee);
}

//----------------- common task -------------------------------------------------------

class task_worker;
class task_worker_pool;
class service_node;
class task;

struct __tls_task_info__
{
    uint32_t     magic;    
    task         *current_task;    
    task_worker  *worker;
    int           worker_index;
    service_node *current_node;
};

extern __thread struct __tls_task_info__ tls_task_info;

class task :
    public ref_counter, 
    public extensible_object<task, 4>
{
public:
    task(dsn_task_code_t code, int hash = 0, service_node* node = nullptr);
    virtual ~task();
        
    virtual void exec() = 0;

    void                    exec_internal();    
    bool                    cancel(bool wait_until_finished, /*out*/ bool* finished = nullptr); // return whether *this* cancel success
    bool                    wait(int timeout_milliseconds = TIME_MS_MAX, bool on_cancel = false);
    virtual void            enqueue();
    void                    set_error_code(error_code err) { _error = err; }
    void                    set_delay(int delay_milliseconds = 0) { _delay_milliseconds = delay_milliseconds; }
    void                    set_tracker(task_tracker* tracker) { _context_tracker.set_tracker(tracker, this); }

    uint64_t                id() const { return _task_id; }
    task_state              state() const { return _state.load(); }
    dsn_task_code_t         code() const { return _spec->code; }
    task_spec&              spec() const { return *_spec; }
    int                     hash() const { return _hash; }
    int                     delay_milliseconds() const { return _delay_milliseconds; }
    error_code              error() const { return _error; }
    service_node*           node() const { return _node; }
    bool                    is_empty() const { return _is_null; }

    
    static task*            get_current_task();
    static uint64_t         get_current_task_id();
    static task_worker*     get_current_worker();
    static service_node*    get_current_node();
    static int              get_current_worker_index();
    static const char*      get_current_node_name();
    static void             set_current_worker(task_worker* worker, service_node* node);

protected:
    void                    signal_waiters();
    void                    enqueue(task_worker_pool* pool);
    void                    set_task_id(uint64_t tid) { _task_id = tid;  }

    mutable std::atomic<task_state> _state;
    bool                   _is_null;
    error_code             _error;

private:
    task(const task&);

    uint64_t               _task_id; 
    std::atomic<void*>     _wait_event;
    int                    _hash;
    int                    _delay_milliseconds;
    bool                   _wait_for_cancel;
    task_spec              *_spec;
    service_node           *_node;
    trackable_task         _context_tracker; // when tracker is gone, the task is cancelled automatically

public:
    // used by task queue only
    dlink                  _task_queue_dl;
};

class task_c : public task
{
public:
    task_c(dsn_task_code_t code, dsn_task_handler_t cb, void* param, int hash = 0, service_node* node = nullptr)
        : task(code, hash, node)
    {
        _cb = cb;
        _param = param;
    }

    virtual void exec() override
    {
        _cb(_param);
    }

private:
    dsn_task_handler_t _cb;
    void               *_param;
};


//----------------- timer task -------------------------------------------------------

class timer_task : public task
{
public:
    timer_task(dsn_task_code_t code, dsn_task_handler_t cb, void* param, uint32_t interval_milliseconds, int hash = 0);
    virtual void exec();
    
private:
    uint32_t           _interval_milliseconds;
    dsn_task_handler_t _cb;
    void*              _param;
};

//----------------- rpc task -------------------------------------------------------

struct rpc_handler_info
{
    dsn_task_code_t           code;
    std::string               name;
    bool                      unregistered;
    std::atomic<int>          running_count;
    dsn_rpc_request_handler_t c_handler;
    void*                     parameter;

    rpc_handler_info(dsn_task_code_t code)
        : code(code), unregistered(false), running_count(0) 
    {
    }
    ~rpc_handler_info() { }

    void run(dsn_message_t req)
    {
        running_count++;
        if (!unregistered)
        {
            c_handler(req, parameter);
        }
        running_count--;
    }

    void unregister()
    {
        unregistered = true;
        while (running_count.load(std::memory_order_relaxed) != 0)
        {
            // TODO: nop
        }
    }
};

typedef std::shared_ptr<rpc_handler_info> rpc_handler_ptr;

class service_node;
class rpc_request_task : public task
{
public:
    rpc_request_task(message_ex* request, rpc_handler_ptr& h, service_node* node);
    ~rpc_request_task();

    message_ex*  get_request() { return _request; }
    virtual void enqueue() override;

    virtual void  exec()
    {
        _handler->run(_request);
    }

protected:
    message_ex      *_request;
    rpc_handler_ptr _handler;
};

class rpc_response_task : public task
{
public:
    rpc_response_task(message_ex* request, dsn_rpc_response_handler_t cb, void* param, int hash = 0);
    ~rpc_response_task();

    void             enqueue(error_code err, message_ex* reply);
    virtual void     enqueue() { task::enqueue(_caller_pool); } // re-enqueue after above enqueue
    message_ex*      get_request() { return _request; }
    message_ex*      get_response() { return _response; }

    virtual void  exec()
    {
        if (_cb)
        {
            _cb(_error.get(), _request, _response, _param);
        }
        else
        {
            _error.end_tracking();
        }
    }

private:
    message_ex*                _request;
    message_ex*                _response;
    task_worker_pool *         _caller_pool;
    dsn_rpc_response_handler_t _cb;
    void*                      _param;

    friend class rpc_engine;    
};

//------------------------- disk AIO task ---------------------------------------------------

enum aio_type
{
    AIO_Invalid,
    AIO_Read,
    AIO_Write
};

class disk_engine;
class disk_aio
{
public:    
    // filled by apps
    dsn_handle_t file;
    void*        buffer;
    uint32_t     buffer_size;    
    uint64_t     file_offset;

    // filled by frameworks
    aio_type     type;
    disk_engine *engine;

    disk_aio() : type(aio_type::AIO_Invalid) {}
    virtual ~disk_aio(){}
};

class aio_task : public task
{
public:
    aio_task(dsn_task_code_t code, dsn_aio_handler_t cb, void* param, int hash = 0);
    ~aio_task();

    void            enqueue(error_code err, size_t transferred_size, service_node* node);
    size_t          get_transferred_size() const { return _transferred_size; }
    disk_aio*       aio() { return _aio; }

    void            exec() // aio completed
    {
        if (nullptr != _cb)
        {
            _cb(_error.get(), _transferred_size, _param);
        }
        else
        {
            _error.end_tracking();
        }
    }

private:
    disk_aio*         _aio;
    size_t            _transferred_size;
    dsn_aio_handler_t _cb;
    void*             _param;
};

// ------------------------ inline implementations --------------------
__inline /*static*/ task* task::get_current_task()
{
    if (tls_task_info.magic == 0xdeadbeef)
        return tls_task_info.current_task;
    else
        return nullptr;
}

__inline /*static*/ uint64_t task::get_current_task_id()
{
    if (tls_task_info.magic == 0xdeadbeef)
        return tls_task_info.current_task ? tls_task_info.current_task->id() : 0;
    else
        return 0;
}


__inline /*static*/ task_worker* task::get_current_worker()
{
    if (tls_task_info.magic == 0xdeadbeef)
        return tls_task_info.worker;
    else
        return nullptr;
}

__inline /*static*/ service_node* task::get_current_node()
{
    if (tls_task_info.magic == 0xdeadbeef)
        return tls_task_info.current_node;
    else
        return nullptr;
}

__inline /*static*/ int task::get_current_worker_index()
{
    if (tls_task_info.magic == 0xdeadbeef)
        return tls_task_info.worker_index;
    else
        return -1;
}

} // end namespace
