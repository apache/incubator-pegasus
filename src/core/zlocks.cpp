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
# include <dsn/internal/zlocks.h>
# include <dsn/internal/factory_store.h>
# include <dsn/internal/task.h>
# include "service_engine.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "lock"

using namespace dsn::utils;

namespace dsn { namespace service {

    namespace lock_checker 
    {
        __thread int zlock_exclusive_count = 0;
        __thread int zlock_shared_count = 0;

        void check_wait_safety()
        {
            if (zlock_exclusive_count + zlock_shared_count > 0)
            {
                dassert (false, "wait inside locks may lead to deadlocks - current thread owns %u exclusive locks and %u shared locks now.",
                    zlock_exclusive_count, zlock_shared_count
                    );
            }
        }

        void check_dangling_lock()
        {
            if (zlock_exclusive_count + zlock_shared_count > 0)
            {
                dassert (false, "locks should not be hold at this point - current thread owns %u exclusive locks and %u shared locks now.",
                    zlock_exclusive_count, zlock_shared_count
                    );
            }
        }

        void check_wait_task(task* waitee)
        {
            check_wait_safety();

            if (nullptr != task::get_current_task() && !waitee->is_empty())
            {
                if (TASK_TYPE_RPC_RESPONSE == waitee->spec().type ||
                    task::get_current_task()->spec().pool_code == waitee->spec().pool_code)
                {
                    dassert(false, "task %s waits for another task %s sharing the same thread pool - will lead to deadlocks easily (e.g., when worker_count = 1 or when the pool is partitioned)",
                        task::get_current_task()->spec().code.to_string(),
                        waitee->spec().code.to_string()
                        );
                }
            }
        }
    }

zlock::zlock(void)
{
    lock_provider* last = factory_store<lock_provider>::create(service_engine::instance().spec().lock_factory_name.c_str(), PROVIDER_TYPE_MAIN, this, nullptr);

    // TODO: perf opt by saving the func ptrs somewhere
    for (auto it = service_engine::instance().spec().lock_aspects.begin();
        it != service_engine::instance().spec().lock_aspects.end();
        it++)
    {
        last = factory_store<lock_provider>::create(it->c_str(), PROVIDER_TYPE_ASPECT, this, last);
    }

    _provider = last;
}

zlock::~zlock(void)
{
    delete _provider;
}


zrwlock_nr::zrwlock_nr(void)
{
    rwlock_nr_provider* last = factory_store<rwlock_nr_provider>::create(service_engine::instance().spec().rwlock_nr_factory_name.c_str(), PROVIDER_TYPE_MAIN, this, nullptr);

    // TODO: perf opt by saving the func ptrs somewhere
    for (auto it = service_engine::instance().spec().rwlock_aspects.begin();
        it != service_engine::instance().spec().rwlock_aspects.end();
        it++)
    {
        last = factory_store<rwlock_nr_provider>::create(it->c_str(), PROVIDER_TYPE_ASPECT, this, last);
    }

    _provider = last;
}

zrwlock_nr::~zrwlock_nr(void)
{
    delete _provider;
}

zsemaphore::zsemaphore(int initialCount)
{
    semaphore_provider* last = factory_store<semaphore_provider>::create(service_engine::instance().spec().semaphore_factory_name.c_str(), PROVIDER_TYPE_MAIN, this, initialCount, nullptr);

    // TODO: perf opt by saving the func ptrs somewhere
    for (auto it = service_engine::instance().spec().semaphore_aspects.begin();
        it != service_engine::instance().spec().semaphore_aspects.end();
        it++)
    {
        last = factory_store<semaphore_provider>::create(it->c_str(), PROVIDER_TYPE_ASPECT, this, initialCount, last);
    }

    _provider = last;
}

zsemaphore::~zsemaphore()
{
    delete _provider;
}

//------------------------------- event ----------------------------------

zevent::zevent(bool manualReset, bool initState/* = false*/)
{
    _manualReset = manualReset;
    _signaled = initState;
    if (_signaled)
    {
        _sema.signal();
    }
}

zevent::~zevent()
{
}

void zevent::set()
{
    bool nonsignaled = false;
    if (std::atomic_compare_exchange_strong(&_signaled, &nonsignaled, true))
    {
        _sema.signal();
    }
}

void zevent::reset()
{
    if (_manualReset)
    {
        bool signaled = true;
        if (std::atomic_compare_exchange_strong(&_signaled, &signaled, false))
        {
        }
    }
}

bool zevent::wait(int timeout_milliseconds)
{
    lock_checker::check_wait_safety();

    if (_manualReset)
    {
        if (std::atomic_load(&_signaled))
            return true;

        _sema.wait(timeout_milliseconds);
        return std::atomic_load(&_signaled);
    }

    else
    {
        bool signaled = true;
        if (std::atomic_compare_exchange_strong(&_signaled, &signaled, false))
            return true;

        _sema.wait(timeout_milliseconds);
        return std::atomic_compare_exchange_strong(&_signaled, &signaled, false);
    }
}

}} // end namespace dsn::service
