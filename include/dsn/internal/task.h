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
 *     the task abstraction in zion, as well as the derived various types of
 *     tasks in our system
 *
 * Revision history:
 *     Mar., 2015, @imzhenyu (Zhenyu Guo), first version
 *     xxxx-xx-xx, author, fix bug about xxx
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

namespace dsn 
{

namespace lock_checker 
{
    extern __thread int zlock_exclusive_count;
    extern __thread int zlock_shared_count;
    extern void check_wait_safety();
    extern void check_dangling_lock();
    extern void check_wait_task(task* waitee);
}

class task_worker;
class task_worker_pool;
class service_node;
class task_engine;
class task_queue;
class rpc_engine;
class disk_engine;
class env_provider;
class nfs_node;
class timer_service;
class task;

struct __tls_dsn__
{
    uint32_t      magic;    
    task          *current_task;   

    task_worker   *worker;
    int           worker_index;
    service_node  *node;
    rpc_engine    *rpc;
    disk_engine   *disk;
    env_provider  *env;
    nfs_node      *nfs;
    timer_service *tsvc;

    uint64_t      node_pool_thread_ids; // 8,8,16 bits
    uint32_t      last_lower32_task_id; // 32bits

    char          scratch_buffer[4][256]; // for temp to_string() etc., 4 buffers in maximum
    int           scratch_buffer_index;
    char*         scratch_next() { return scratch_buffer[++scratch_buffer_index % 4]; }
};

extern __thread struct __tls_dsn__ tls_dsn;

//----------------- common task -------------------------------------------------------

class task :
    public ref_counter, 
    public extensible_object<task, 4>
{
public:
    task(
        dsn_task_code_t code, 
        void* context, 
        dsn_task_cancelled_handler_t on_cancel, 
        int hash = 0, 
        service_node* node = nullptr
        );
    virtual ~task();
        
    virtual void exec() = 0;

    void                    exec_internal();    
    // return whether *this* cancel success, 
    // for timers, even return value is false, the further timer execs are cancelled
    bool                    cancel(bool wait_until_finished, /*out*/ bool* finished = nullptr);
    bool                    wait(int timeout_milliseconds = TIME_MS_MAX, bool on_cancel = false);
    virtual void            enqueue();
    void                    set_error_code(error_code err) { _error = err; }
    void                    set_delay(int delay_milliseconds = 0) { _delay_milliseconds = delay_milliseconds; }
    void                    set_tracker(task_tracker* tracker) { _context_tracker.set_tracker(tracker, this); }

    uint64_t                id() const { return _task_id; }
    task_state              state() const { return _state.load(std::memory_order_acquire); }
    dsn_task_code_t         code() const { return _spec->code; }
    task_spec&              spec() const { return *_spec; }
    int                     hash() const { return _hash; }
    int                     delay_milliseconds() const { return _delay_milliseconds; }
    error_code              error() const { return _error; }
    service_node*           node() const { return _node; }
    bool                    is_empty() const { return _is_null; }
    uint64_t                enqueue_ts_ns() { return _recv_ts_ns; }

    // static helper utilities
    static task*            get_current_task();
    static uint64_t         get_current_task_id();
    static task_worker*     get_current_worker();
    static task_worker*     get_current_worker2();
    static service_node*    get_current_node();
    static service_node*    get_current_node2();
    static int              get_current_worker_index();
    static const char*      get_current_node_name();
    static rpc_engine*      get_current_rpc();
    static disk_engine*     get_current_disk();
    static env_provider*    get_current_env();
    static nfs_node*        get_current_nfs();
    static timer_service*   get_current_tsvc();

    static void             set_tls_dsn_context(
                                service_node* node,  // cannot be null
                                task_worker* worker, // null for io or timer threads if they are not worker threads
                                task_queue* queue   // owner queue if io_mode == IOE_PER_QUEUE
                                );

protected:
    void                    signal_waiters();
    void                    enqueue(task_worker_pool* pool);
    void                    set_task_id(uint64_t tid) { _task_id = tid;  }

    mutable std::atomic<task_state> _state;
    bool                   _is_null;
    error_code             _error;
    void                   *_context; // the context for the task/on_cancel callbacks

private:
    task(const task&);
    static void            check_tls_dsn();
    static void            on_tls_dsn_not_set();

    uint64_t               _task_id; 
    std::atomic<void*>     _wait_event;
    int                    _hash;
    int                    _delay_milliseconds;
    bool                   _wait_for_cancel;
    task_spec              *_spec;
    service_node           *_node;
    trackable_task         _context_tracker; // when tracker is gone, the task is cancelled automatically
    dsn_task_cancelled_handler_t _on_cancel;
    uint64_t               _recv_ts_ns;

public:
    // used by task queue only
    task*                  next;
};

class task_c : public task
{
public:
    task_c(
        dsn_task_code_t code,
        dsn_task_handler_t cb, 
        void* context, 
        dsn_task_cancelled_handler_t on_cancel,
        int hash = 0, 
        service_node* node = nullptr
        )
        : task(code, context, on_cancel, hash, node)
    {
        _cb = cb;
    }

    virtual void exec() override
    {
        _cb(_context);
    }

private:
    dsn_task_handler_t _cb;
};


//----------------- timer task -------------------------------------------------------

class timer_task : public task
{
public:
    timer_task(
        dsn_task_code_t code, 
        dsn_task_handler_t cb, 
        void* context, 
        dsn_task_cancelled_handler_t on_cancel, 
        uint32_t interval_milliseconds,
        int hash = 0, 
        service_node* node = nullptr
        );
    virtual void exec();
    
private:
    uint32_t           _interval_milliseconds;
    dsn_task_handler_t _cb;
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

class service_node;
class rpc_request_task : public task
{
public:
    rpc_request_task(message_ex* request, rpc_handler_ptr& h, service_node* node);
    ~rpc_request_task();

    message_ex*  get_request() { return _request; }
    virtual void enqueue() override;

    virtual void  exec() override
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
    rpc_response_task(
        message_ex* request, 
        dsn_rpc_response_handler_t cb,
        void* context, 
        dsn_task_cancelled_handler_t on_cancel, 
        int hash = 0, 
        service_node* node = nullptr
        );
    ~rpc_response_task();

    void             enqueue(error_code err, message_ex* reply);
    virtual void     enqueue(); // re-enqueue after above enqueue, e.g., after delay
    message_ex*      get_request() { return _request; }
    message_ex*      get_response() { return _response; }

    virtual void  exec()
    {
        if (_cb)
        {
            _cb(_error.get(), _request, _response, _context);
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
    void*        file_object;

    disk_aio() : type(aio_type::AIO_Invalid) {}
    virtual ~disk_aio(){}
};

class aio_task : public task
{
public:
    aio_task(
        dsn_task_code_t code,
        dsn_aio_handler_t cb, 
        void* context, 
        dsn_task_cancelled_handler_t on_cancel, 
        int hash = 0,
        service_node* node = nullptr
        );
    ~aio_task();

    void            enqueue(error_code err, size_t transferred_size);
    size_t          get_transferred_size() const { return _transferred_size; }
    disk_aio*       aio() { return _aio; }

    void copy_to(char* dest)
    {
        if (!_unmerged_write_buffers.empty())
        {
            for (auto &buffer : _unmerged_write_buffers)
            {
                memcpy(dest, buffer.buffer, buffer.size);
                dest += buffer.size;
            }
        }
        else
        {
            memcpy(dest, _aio->buffer, _aio->buffer_size);
        }
    }

    void collapse() {
        if (!_unmerged_write_buffers.empty()) {
            auto buffer = std::shared_ptr<char>(new char[_aio->buffer_size]);
            _merged_write_buffer_holder.assign(buffer, 0, _aio->buffer_size);
            _aio->buffer = buffer.get();
            copy_to(buffer.get());
        }
    }

    virtual void exec() override // aio completed
    {
        if (nullptr != _cb)
        {
            _cb(_error.get(), _transferred_size, _context);
        }
        else
        {
            _error.end_tracking();
        }
    }

    std::vector<dsn_file_buffer_t> _unmerged_write_buffers;
    blob                           _merged_write_buffer_holder;
protected:
    disk_aio*         _aio;
    size_t            _transferred_size;
    dsn_aio_handler_t _cb;
};

// ------------------------ inline implementations --------------------
__inline /*static*/ void task::check_tls_dsn()
{
    if (tls_dsn.magic != 0xdeadbeef)
    {
        task::on_tls_dsn_not_set();
    }
}

__inline /*static*/ task* task::get_current_task()
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


__inline /*static*/ task_worker* task::get_current_worker()
{
    check_tls_dsn();
    return tls_dsn.worker;
}

__inline /*static*/ task_worker* task::get_current_worker2()
{
    return tls_dsn.magic == 0xdeadbeef ? tls_dsn.worker : nullptr;
}

__inline /*static*/ service_node* task::get_current_node()
{
    check_tls_dsn();
    return tls_dsn.node;
}
__inline /*static*/ service_node* task::get_current_node2()
{
    return tls_dsn.magic == 0xdeadbeef ? tls_dsn.node : nullptr;
}


__inline /*static*/ int task::get_current_worker_index()
{
    check_tls_dsn();
    return tls_dsn.worker_index;
}

__inline /*static*/ rpc_engine* task::get_current_rpc()
{
    check_tls_dsn();
    return tls_dsn.rpc;
}

__inline /*static*/ disk_engine* task::get_current_disk()
{
    check_tls_dsn();
    return tls_dsn.disk;
}

__inline /*static*/ env_provider* task::get_current_env()
{
    check_tls_dsn();
    return tls_dsn.env;
}

__inline /*static*/ nfs_node* task::get_current_nfs()
{
    check_tls_dsn();
    return tls_dsn.nfs;
}

__inline /*static*/ timer_service* task::get_current_tsvc()
{
    check_tls_dsn();
    return tls_dsn.tsvc;
}

} // end namespace
