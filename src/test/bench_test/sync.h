//
// Created by mi on 2019/8/5.
//

#ifndef PEGASUS_SYNC_H
#define PEGASUS_SYNC_H

#include <zconf.h>
#include <cstdint>

namespace pegasus {
namespace test {
class mutex {
public:
// We want to give users opportunity to default all the mutexes to adaptive if
// not specified otherwise. This enables a quick way to conduct various
// performance related experiements.
//
// NB! Support for adaptive mutexes is turned on by definining
// ROCKSDB_PTHREAD_ADAPTIVE_MUTEX during the compilation. If you use RocksDB
// build environment then this happens automatically; otherwise it's up to the
// consumer to define the identifier.
    explicit mutex(bool adaptive = true);
    ~mutex();

    void lock();
    void unlock();
    // this will assert if the mutex is not locked
    // it does NOT verify that mutex is held by a calling thread
    void assert_held();

private:
    friend class cond_var;
    pthread_mutex_t mu_;
#ifndef NDEBUG
    bool locked_;
#endif
    // No copying
    mutex(const mutex&);
    void operator=(const mutex&);
};

class cond_var {
public:
    explicit cond_var(mutex* mu);
    ~cond_var();
    void wait();
    bool timed_wait(uint64_t abs_time_us);
    // Timed condition wait.  Returns true if timeout occurred.
    void signal();
    void signal_all();
private:
    pthread_cond_t cv_;
    mutex* mu_;
};
}
}

#endif //PEGASUS_SYNC_H
