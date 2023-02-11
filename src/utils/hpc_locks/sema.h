//---------------------------------------------------------
// For conditions of distribution and use, see
// https://github.com/preshing/cpp11-on-multicore/blob/master/LICENSE
//---------------------------------------------------------

#ifndef __CPP11OM_SEMAPHORE_H__
#define __CPP11OM_SEMAPHORE_H__

#include <atomic>
#include <cassert>
#include <cerrno>

#if defined(_WIN32)
//---------------------------------------------------------
// Semaphore (Windows)
//---------------------------------------------------------

#include <windows.h>

#undef min
#undef max

class Semaphore
{
private:
    HANDLE m_hSema;

    Semaphore(const Semaphore &other) = delete;
    Semaphore &operator=(const Semaphore &other) = delete;

public:
    Semaphore(int initial_count = 0)
    {
        assert(initial_count >= 0);
        m_hSema = CreateSemaphore(NULL, initial_count, MAXLONG, NULL);
    }

    ~Semaphore() { CloseHandle(m_hSema); }

    void wait() { WaitForSingleObject(m_hSema, INFINITE); }

    bool wait(int timeout_milliseconds)
    {
        return WAIT_OBJECT_0 == WaitForSingleObject(m_hSema, timeout_milliseconds);
    }

    void signal(int count = 1) { ReleaseSemaphore(m_hSema, count, NULL); }
};

#elif defined(__MACH__)
//---------------------------------------------------------
// Semaphore (Apple iOS and OSX)
// Can't use POSIX semaphores due to
// http://lists.apple.com/archives/darwin-kernel/2009/Apr/msg00010.html
//---------------------------------------------------------

#include <mach/mach.h>

class Semaphore
{
private:
    semaphore_t m_sema;

    Semaphore(const Semaphore &other) = delete;
    Semaphore &operator=(const Semaphore &other) = delete;

public:
    Semaphore(int initial_count = 0)
    {
        assert(initial_count >= 0);
        semaphore_create(mach_task_self(), &m_sema, SYNC_POLICY_FIFO, initial_count);
    }

    ~Semaphore() { semaphore_destroy(mach_task_self(), m_sema); }

    void wait() { semaphore_wait(m_sema); }

    bool wait(int timeout_milliseconds)
    {
        // TODO: timeout
        wait();
        return true;
    }

    void signal() { semaphore_signal(m_sema); }

    void signal(int count)
    {
        while (count-- > 0) {
            semaphore_signal(m_sema);
        }
    }
};

#elif defined(__unix__)
//---------------------------------------------------------
// Semaphore (POSIX, Linux)
//---------------------------------------------------------

#include <semaphore.h>
#include <time.h>

class Semaphore
{
private:
    sem_t m_sema;

    Semaphore(const Semaphore &other) = delete;
    Semaphore &operator=(const Semaphore &other) = delete;

public:
    Semaphore(int initial_count = 0)
    {
        assert(initial_count >= 0);
        sem_init(&m_sema, 0, initial_count);
    }

    ~Semaphore() { sem_destroy(&m_sema); }

    void wait()
    {
        // http://stackoverflow.com/questions/2013181/gdb-causes-sem-wait-to-fail-with-eintr-error
        int rc;
        do {
            rc = sem_wait(&m_sema);
        } while (rc == -1 && errno == EINTR);
    }

    bool wait(int timeout_milliseconds)
    {
        assert(timeout_milliseconds >= 0);
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += timeout_milliseconds / 1000;
        ts.tv_nsec += timeout_milliseconds % 1000 * 1000000;
        if (ts.tv_nsec >= 1000000000) {
            ++ts.tv_sec;
            ts.tv_nsec -= 1000000000;
        }
        assert(ts.tv_nsec >= 0);
        assert(ts.tv_nsec < 1000000000);

        return sem_timedwait(&m_sema, &ts) == 0;
    }

    void signal() { sem_post(&m_sema); }

    void signal(int count)
    {
        while (count-- > 0) {
            sem_post(&m_sema);
        }
    }
};

#else

#error Unsupported platform!

#endif

//---------------------------------------------------------
// LightweightSemaphore
//---------------------------------------------------------
class LightweightSemaphore
{
private:
    std::atomic<int> m_count;
    Semaphore m_sema;
    int m_spin_count;

    void waitWithPartialSpinning()
    {
        int oldCount;
        // Is there a better way to set the initial spin count?
        // If we lower it to 1000, testBenaphore becomes 15x slower on my Core i7-5930K Windows PC,
        // as threads start hitting the kernel semaphore.
        int spin = m_spin_count;
        while (spin--) {
            oldCount = m_count.load(std::memory_order_relaxed);
            if ((oldCount > 0) &&
                m_count.compare_exchange_strong(oldCount, oldCount - 1, std::memory_order_acquire))
                return;
            std::atomic_signal_fence(
                std::memory_order_acquire); // Prevent the compiler from collapsing the loop.
        }
        oldCount = m_count.fetch_sub(1, std::memory_order_acquire);
        if (oldCount <= 0) {
            m_sema.wait();
        }
    }

public:
    LightweightSemaphore(int initial_count = 0, int spin_count = 128)
        : m_count(initial_count), m_spin_count(spin_count)
    {
        assert(initial_count >= 0);
    }

    bool tryWait()
    {
        int oldCount = m_count.load(std::memory_order_relaxed);
        return (oldCount > 0 &&
                m_count.compare_exchange_strong(oldCount, oldCount - 1, std::memory_order_acquire));
    }

    void wait()
    {
        if (!tryWait())
            waitWithPartialSpinning();
    }

    // Be careful! Should check the return value, and can consume iff the return value is true.
    bool wait(int timeout_milliseconds)
    {
        int oldCount = m_count.fetch_sub(1, std::memory_order_acquire);
        if (oldCount > 0)
            return true;
        if (m_sema.wait(timeout_milliseconds))
            return true;
        m_count.fetch_add(1, std::memory_order_release); // restore the substracted count
        return false;
    }

    void signal(int count = 1)
    {
        assert(count >= 1);
        int oldCount = m_count.fetch_add(count, std::memory_order_release);
        int toRelease = -oldCount < count ? -oldCount : count;
        if (toRelease > 0) {
            m_sema.signal(toRelease);
        }
    }
};

typedef LightweightSemaphore DefaultSemaphoreType;

#endif // __CPP11OM_SEMAPHORE_H__
