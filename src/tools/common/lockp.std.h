#pragma once

#include <rdsn/tool_api.h>
#include <rdsn/internal/synchronize.h>

namespace rdsn { namespace tools {

class std_lock_provider : public lock_provider
{
public:
    std_lock_provider(rdsn::service::zlock *lock, lock_provider* inner_provider) : lock_provider(lock, inner_provider) {}

    virtual void lock() { _lock.lock(); }
    virtual bool try_lock() { return _lock.try_lock(); }
    virtual void unlock() { _lock.unlock(); }

private:
    std::recursive_mutex _lock;
};

class std_rwlock_provider : public rwlock_provider
{
public:
    std_rwlock_provider(rdsn::service::zrwlock *lock, rwlock_provider* inner_provider) : rwlock_provider(lock, inner_provider) {}

    virtual void lock_read() { _lock.lock_read(); }
    virtual bool try_lock_read() { return _lock.try_lock_read(); }
    virtual void unlock_read() { _lock.unlock_read(); }

    virtual void lock_write() { _lock.lock_write(); }
    virtual bool try_lock_write() { return _lock.try_lock_write(); }
    virtual void unlock_write() { _lock.unlock_write(); }

private:
    utils::rw_lock _lock;
};

class std_semaphore_provider : public semaphore_provider
{
public:  
    std_semaphore_provider(rdsn::service::zsemaphore *sema, int initialCount, semaphore_provider *inner_provider)
        : semaphore_provider(sema, initialCount, inner_provider), _sema(initialCount)
    {
    }

public:
    virtual void signal(int count) { _sema.signal(count); }
    virtual bool wait(int timeout_milliseconds) { return _sema.wait(timeout_milliseconds); }

private:
    rdsn::utils::semaphore _sema;
};

}} // end namespace rdsn::tools
