#include "../../tools/common/lockp.std.h"
#include <gtest/gtest.h>

using namespace dsn;
using namespace dsn::tools;

TEST(tools_common, std_lock_provider)
{
    std_lock_provider* lock = new std_lock_provider(nullptr);
    lock->lock();
    EXPECT_TRUE(lock->try_lock());
    lock->unlock();
    lock->unlock();

    std_lock_nr_provider* nr_lock = new std_lock_nr_provider(nullptr);
    nr_lock->lock();
    EXPECT_FALSE(nr_lock->try_lock());
    nr_lock->unlock();

    std_rwlock_nr_provider* rwlock = new std_rwlock_nr_provider(nullptr);
    rwlock->lock_read();
    rwlock->unlock_read();
    rwlock->lock_write();
    rwlock->unlock_write();

    std_semaphore_provider* sema = new std_semaphore_provider(0, nullptr);
    std::thread t([](std_semaphore_provider* s){
        s->wait(1000000);
    }, sema);
    sema->signal(1);
    t.join();

    delete lock;
    delete nr_lock;
    delete rwlock;
    delete sema;
}
