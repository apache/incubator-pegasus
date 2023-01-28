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

#include "utils/factory_store.h"
#include "utils/zlocks.h"
#include "utils/zlock_provider.h"
#include "runtime/service_engine.h"

namespace dsn {

namespace lock_checker {
__thread int zlock_exclusive_count;
__thread int zlock_shared_count;

void check_wait_safety()
{
    if (zlock_exclusive_count + zlock_shared_count > 0) {
        LOG_WARNING("wait inside locks may lead to deadlocks - current thread owns {} exclusive "
                    "locks and {} shared locks now.",
                    zlock_exclusive_count,
                    zlock_shared_count);
    }
}

void check_dangling_lock()
{
    if (zlock_exclusive_count + zlock_shared_count > 0) {
        LOG_WARNING("locks should not be hold at this point - current thread owns {} exclusive "
                    "locks and {} shared locks now.",
                    zlock_exclusive_count,
                    zlock_shared_count);
    }
}
} // namespace lock_checker

zlock::zlock(bool recursive)
{
    if (recursive) {
        lock_provider *last = utils::factory_store<lock_provider>::create(
            dsn::service_engine::instance().spec().lock_factory_name.c_str(),
            dsn::PROVIDER_TYPE_MAIN,
            nullptr);
        _h = last;
    } else {
        lock_nr_provider *last = utils::factory_store<lock_nr_provider>::create(
            dsn::service_engine::instance().spec().lock_nr_factory_name.c_str(),
            dsn::PROVIDER_TYPE_MAIN,
            nullptr);
        _h = last;
    }
}

zlock::~zlock() { delete _h; }

void zlock::lock()
{
    _h->lock();
    ++lock_checker::zlock_exclusive_count;
}

bool zlock::try_lock()
{
    auto r = _h->try_lock();
    if (r) {
        ++lock_checker::zlock_exclusive_count;
    }
    return r;
}

void zlock::unlock()
{
    --lock_checker::zlock_exclusive_count;
    _h->unlock();
}

zrwlock_nr::zrwlock_nr()
{
    rwlock_nr_provider *last = utils::factory_store<rwlock_nr_provider>::create(
        service_engine::instance().spec().rwlock_nr_factory_name.c_str(),
        dsn::PROVIDER_TYPE_MAIN,
        nullptr);
    _h = last;
}

zrwlock_nr::~zrwlock_nr() { delete _h; }

void zrwlock_nr::lock_read()
{
    _h->lock_read();
    ++lock_checker::zlock_shared_count;
}

void zrwlock_nr::unlock_read()
{
    --lock_checker::zlock_shared_count;
    _h->unlock_read();
}

bool zrwlock_nr::try_lock_read()
{
    auto r = _h->try_lock_read();
    if (r)
        ++lock_checker::zlock_shared_count;
    return r;
}

void zrwlock_nr::lock_write()
{
    _h->lock_write();
    ++lock_checker::zlock_exclusive_count;
}

void zrwlock_nr::unlock_write()
{
    --lock_checker::zlock_exclusive_count;
    _h->unlock_write();
}

bool zrwlock_nr::try_lock_write()
{
    auto r = _h->try_lock_write();
    if (r)
        ++lock_checker::zlock_exclusive_count;
    return r;
}

zsemaphore::zsemaphore(int initial_count)
{
    semaphore_provider *last = utils::factory_store<semaphore_provider>::create(
        service_engine::instance().spec().semaphore_factory_name.c_str(),
        PROVIDER_TYPE_MAIN,
        initial_count,
        nullptr);
    _h = last;
}

zsemaphore::~zsemaphore() { delete _h; }

void zsemaphore::signal(int count) { _h->signal(count); }

bool zsemaphore::wait(int timeout_milliseconds)
{
    if (static_cast<unsigned int>(timeout_milliseconds) == TIME_MS_MAX) {
        lock_checker::check_wait_safety();
        _h->wait();
        return true;
    } else {
        return _h->wait(timeout_milliseconds);
    }
}

zevent::zevent(bool manualReset, bool initState /* = false*/)
{
    _manualReset = manualReset;
    _signaled = initState;
    if (_signaled) {
        _sema.signal();
    }
}

zevent::~zevent() {}

void zevent::set()
{
    bool nonsignaled = false;
    if (std::atomic_compare_exchange_strong(&_signaled, &nonsignaled, true)) {
        _sema.signal();
    }
}

void zevent::reset()
{
    if (_manualReset) {
        bool signaled = true;
        if (std::atomic_compare_exchange_strong(&_signaled, &signaled, false)) {
        }
    }
}

bool zevent::wait(int timeout_milliseconds)
{
    if (_manualReset) {
        if (std::atomic_load(&_signaled))
            return true;

        _sema.wait(timeout_milliseconds);
        return std::atomic_load(&_signaled);
    }

    else {
        bool signaled = true;
        if (std::atomic_compare_exchange_strong(&_signaled, &signaled, false))
            return true;

        _sema.wait(timeout_milliseconds);
        return std::atomic_compare_exchange_strong(&_signaled, &signaled, false);
    }
}
} // namespace dsn
