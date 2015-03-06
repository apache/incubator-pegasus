#pragma once

# include <rdsn/internal/extensible_object.h>

namespace rdsn { namespace service {
class zlock;
class zrwlock;
class zsemaphore;
}}

namespace rdsn {

class lock_provider : public extensible_object<lock_provider, 4>
{
public:
    template <typename T> static lock_provider* create(rdsn::service::zlock *lock, lock_provider* inner_provider)
    {
        return new T(lock, inner_provider);
    }

public:
    lock_provider(rdsn::service::zlock *lock, lock_provider* inner_provider) { _inner_provider = inner_provider; }
    virtual ~lock_provider() { if (nullptr != _inner_provider) delete _inner_provider; }

    virtual void lock() = 0;
    virtual bool try_lock() = 0;
    virtual void unlock() = 0;

    lock_provider* get_inner_provider() const { return _inner_provider; }

private:
    lock_provider *_inner_provider;
};

class rwlock_provider : public extensible_object<lock_provider, 4>
{
public:
    template <typename T> static rwlock_provider* create(rdsn::service::zrwlock *lock, rwlock_provider* inner_provider)
    {
        return new T(lock, inner_provider);
    }

public:
    rwlock_provider(rdsn::service::zrwlock *lock, rwlock_provider* inner_provider) { _inner_provider = inner_provider; }
    virtual ~rwlock_provider() { if (nullptr != _inner_provider) delete _inner_provider; }

    virtual void lock_read() = 0;
    virtual bool try_lock_read() = 0;
    virtual void unlock_read() = 0;

    virtual void lock_write() = 0;
    virtual bool try_lock_write() = 0;
    virtual void unlock_write() = 0;

    rwlock_provider* get_inner_provider() const { return _inner_provider; }

private:
    rwlock_provider *_inner_provider;
};

class semaphore_provider : public extensible_object<lock_provider, 4>
{
public:
    template <typename T> static semaphore_provider* create(rdsn::service::zsemaphore *sema, int initCount, semaphore_provider* inner_provider)
    {
        return new T(sema, initCount, inner_provider);
    }

public:  
    semaphore_provider(rdsn::service::zsemaphore *sema, int initialCount, semaphore_provider* inner_provider) { _inner_provider = inner_provider; }
    virtual ~semaphore_provider() { if (nullptr != _inner_provider) delete _inner_provider; }

public:
    virtual void signal(int count) = 0;
    virtual bool wait(int timeout_milliseconds = INFINITE) = 0;
    
    semaphore_provider* get_inner_provider() const { return _inner_provider; }

private:
    semaphore_provider *_inner_provider;
};

} // end namespace
