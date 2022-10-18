//---------------------------------------------------------
// For conditions of distribution and use, see
// https://github.com/preshing/cpp11-on-multicore/blob/master/LICENSE
//---------------------------------------------------------

#ifndef __CPP11OM_BENAPHORE_H__
#define __CPP11OM_BENAPHORE_H__

#include <cassert>
#include <thread>
#include <atomic>
#include "utils/process_utils.h"
#include "utils/hpc_locks/sema.h"

//---------------------------------------------------------
// NonRecursiveBenaphore
//---------------------------------------------------------
class NonRecursiveBenaphore
{
private:
    std::atomic<int> m_contentionCount;
    DefaultSemaphoreType m_sema;

public:
    NonRecursiveBenaphore() : m_contentionCount(0) {}

    void lock()
    {
        if (m_contentionCount.fetch_add(1, std::memory_order_acquire) > 0) {
            m_sema.wait();
        }
    }

    bool tryLock()
    {
        if (m_contentionCount.load(std::memory_order_relaxed) != 0)
            return false;
        int expected = 0;
        return m_contentionCount.compare_exchange_strong(expected, 1, std::memory_order_acquire);
    }

    void unlock()
    {
        int oldCount = m_contentionCount.fetch_sub(1, std::memory_order_release);
        assert(oldCount > 0);
        if (oldCount > 1) {
            m_sema.signal();
        }
    }
};

//---------------------------------------------------------
// RecursiveBenaphore
//---------------------------------------------------------
class RecursiveBenaphore
{
private:
    std::atomic<int> m_contentionCount;
    std::atomic<int> m_owner;
    int m_recursion;
    DefaultSemaphoreType m_sema;

public:
    RecursiveBenaphore() : m_contentionCount(0), m_recursion(0)
    {
        m_owner = ::dsn::utils::INVALID_TID;
    }

    void lock()
    {
        auto tid = ::dsn::utils::get_current_tid();
        if (m_contentionCount.fetch_add(1, std::memory_order_acquire) > 0) {
            if (tid != m_owner.load(std::memory_order_relaxed))
                m_sema.wait();
        }
        //--- We are now inside the lock ---
        m_owner.store(tid, std::memory_order_relaxed);
        m_recursion++;
    }

    bool tryLock()
    {
        auto tid = ::dsn::utils::get_current_tid();
        if (m_owner.load(std::memory_order_relaxed) == tid) {
            // Already inside the lock
            m_contentionCount.fetch_add(1, std::memory_order_relaxed);
        } else {
            if (m_contentionCount.load(std::memory_order_relaxed) != 0)
                return false;
            int expected = 0;
            if (!m_contentionCount.compare_exchange_strong(expected, 1, std::memory_order_acquire))
                return false;
            //--- We are now inside the lock ---
            m_owner.store(tid, std::memory_order_relaxed);
        }
        m_recursion++;
        return true;
    }

    void unlock()
    {
#ifndef NDEBUG
        auto tid = ::dsn::utils::get_current_tid();
        assert(tid == m_owner.load(std::memory_order_relaxed));
#endif
        int recur = --m_recursion;
        if (recur == 0)
            m_owner.store(::dsn::utils::INVALID_TID, std::memory_order_relaxed);
        if (m_contentionCount.fetch_sub(1, std::memory_order_release) > 1) {
            if (recur == 0)
                m_sema.signal();
        }
        //--- We are now outside the lock ---
    }
};

#endif // __CPP11OM_BENAPHORE_H__
