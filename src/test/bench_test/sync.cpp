//
// Created by mi on 2019/8/5.
//

#include <assert.h>
#include <cerrno>
#include <pthread.h>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include "sync.h"

namespace pegasus {
namespace test {
static int pthread_call(const char* label, int result)
{
        if (result != 0 && result != ETIMEDOUT) {
            fprintf(stderr, "pthread %s: %s\n", label, strerror(result));
            abort();
        }
        return result;
}

mutex::mutex(bool adaptive)
{
    if (!adaptive) {
        pthread_call("init mutex", pthread_mutex_init(&mu_, nullptr));
    } else {
        pthread_mutexattr_t mutex_attr;
        pthread_call("init mutex attr", pthread_mutexattr_init(&mutex_attr));
        pthread_call("set mutex attr",
                     pthread_mutexattr_settype(&mutex_attr,
                                               PTHREAD_MUTEX_ADAPTIVE_NP));
        pthread_call("init mutex", pthread_mutex_init(&mu_, &mutex_attr));
        pthread_call("destroy mutex attr",
                     pthread_mutexattr_destroy(&mutex_attr));
    }
}

mutex::~mutex()
{
    pthread_call("destroy mutex", pthread_mutex_destroy(&mu_));
}

void mutex::lock()
{
    pthread_call("lock", pthread_mutex_lock(&mu_));
#ifndef NDEBUG
    locked_ = true;
#endif
}

void mutex::unlock()
{
#ifndef NDEBUG
    locked_ = false;
#endif
    pthread_call("unlock", pthread_mutex_unlock(&mu_));
}

void mutex::assert_held()
{
#ifndef NDEBUG
    assert(locked_);
#endif
}

cond_var::cond_var(mutex* mu) : mu_(mu)
{
    pthread_call("init cv", pthread_cond_init(&cv_, nullptr));
}

cond_var::~cond_var()
{
    pthread_call("destroy cv", pthread_cond_destroy(&cv_));
}

void cond_var::wait()
{
#ifndef NDEBUG
    mu_->locked_ = false;
#endif
    pthread_call("wait", pthread_cond_wait(&cv_, &mu_->mu_));
#ifndef NDEBUG
    mu_->locked_ = true;
#endif
}

bool cond_var::timed_wait(uint64_t abs_time_us)
{
    struct timespec ts;
    ts.tv_sec = static_cast<time_t>(abs_time_us / 1000000);
    ts.tv_nsec = static_cast<suseconds_t>((abs_time_us % 1000000) * 1000);

#ifndef NDEBUG
    mu_->locked_ = false;
#endif
    int err = pthread_cond_timedwait(&cv_, &mu_->mu_, &ts);
#ifndef NDEBUG
    mu_->locked_ = true;
#endif
    if (err == ETIMEDOUT) {
        return true;
    }
    if (err != 0) {
        pthread_call("timedwait", err);
    }
    return false;
}

void cond_var::signal()
{
    pthread_call("signal", pthread_cond_signal(&cv_));
}

void cond_var::signal_all()
{
    pthread_call("broadcast", pthread_cond_broadcast(&cv_));
}
}
}
