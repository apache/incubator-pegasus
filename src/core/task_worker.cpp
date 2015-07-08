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
# include <dsn/internal/task_worker.h>
# include "task_engine.h"
# include <sstream>

# ifdef _WIN32

# else 
//# include <sys/prctl.h>
# endif


# define __TITLE__ "task.worker"

namespace dsn {

join_point<void, task_worker*> task_worker::on_start("task_worker::on_start");
join_point<void, task_worker*> task_worker::on_create("task_worker::on_create");

task_worker::task_worker(task_worker_pool* pool, task_queue* q, int index, task_worker* inner_provider)
{
    _owner_pool = pool;
    _input_queue = q;
    _index = index;

    char name[256];
    sprintf(name, "%5s.%s.%u", pool->node()->name(), pool->spec().name.c_str(), index);
    _name = std::string(name);
    _is_running = false;

    _thread = nullptr;
}

task_worker::~task_worker()
{
    stop();
}

void task_worker::start()
{    
    if (_is_running)
        return;

    _is_running = true;

    _thread = new std::thread(std::bind(&task_worker::run_internal, this));

    _started.wait();
}

void task_worker::stop()
{
    if (!_is_running)
        return;

    _is_running = false;

    _thread->join();
    delete _thread;
    _thread = nullptr;

    _is_running = false;
}

void task_worker::set_name()
{
# ifdef _WIN32

    #ifndef MS_VC_EXCEPTION
    #define MS_VC_EXCEPTION 0x406D1388
    #endif

    typedef struct tagTHREADNAME_INFO
    {
        uint32_t  dwType; // Must be 0x1000.
        LPCSTR szName; // Pointer to name (in user addr space).
        uint32_t  dwThreadID; // Thread ID (-1=caller thread).
        uint32_t  dwFlags; // Reserved for future use, must be zero.
    }THREADNAME_INFO;

    THREADNAME_INFO info;
    info.dwType = 0x1000;
    info.szName = name().c_str();
    info.dwThreadID = (uint32_t)-1;
    info.dwFlags = 0;

    __try
    {
        ::RaiseException (MS_VC_EXCEPTION, 0, sizeof(info)/sizeof(uint32_t), (ULONG_PTR*)&info);
    }
    __except(EXCEPTION_CONTINUE_EXECUTION)
    {
    }

# else
//    prctl(PR_SET_NAME, name(), 0, 0, 0)
# endif
}

void task_worker::set_priority(worker_priority_t pri)
{
# ifdef _WIN32
    static int g_thread_priority_map[] = 
    {
        THREAD_PRIORITY_LOWEST,
        THREAD_PRIORITY_BELOW_NORMAL,
        THREAD_PRIORITY_NORMAL,
        THREAD_PRIORITY_ABOVE_NORMAL,
        THREAD_PRIORITY_HIGHEST
    };

    C_ASSERT(ARRAYSIZE(g_thread_priority_map) == THREAD_xPRIORITY_COUNT);

    ::SetThreadPriority(_thread->native_handle(), g_thread_priority_map[(pool_spec().worker_priority)]);
# else
//# error "not implemented"
# endif
}

void task_worker::set_affinity(uint64_t affinity)
{
# ifdef _WIN32
    ::SetThreadAffinityMask(_thread->native_handle(), static_cast<DWORD_PTR>(affinity));
# else
//# error "not implemented"
# endif
}

void task_worker::run_internal()
{
    while (_thread == nullptr)
    {
        std::this_thread::sleep_for(std::chrono::milliseconds(1));
    }

    task::set_current_worker(this);
    
    set_name();
    set_priority(pool_spec().worker_priority);
    
    if (true == pool_spec().worker_share_core)
    {
        if (pool_spec().worker_affinity_mask > 0) 
        {
            set_affinity(pool_spec().worker_affinity_mask);
        }
    }
    else
    {
        uint64_t current_mask = pool_spec().worker_affinity_mask;
        for (int i = 0; i < _index; ++i)
        {            
            current_mask &= (current_mask - 1);
            if (0 == current_mask)
            {
                current_mask = pool_spec().worker_affinity_mask;
            }
        }
        current_mask -= (current_mask & (current_mask - 1));

        set_affinity(current_mask);
    }

    _started.notify();

    on_start.execute(this);

    loop();
}

void task_worker::loop()
{
    task_queue* q = queue();

    //try {
        while (_is_running)
        {
            task_ptr task = q->dequeue();
            if (task != nullptr)
            {
                task->exec_internal();
            }
        }
    /*}
    catch (std::exception& ex)
    {
        dassert (false, "%s: unhandled exception '%s'", name().c_str(), ex.what());
    }*/
}

const threadpool_spec& task_worker::pool_spec() const
{
    return pool()->spec();
}

} // end namespace
