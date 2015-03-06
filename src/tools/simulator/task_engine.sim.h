#pragma once

#include <rdsn/tool_api.h>
#include <rdsn/internal/priority_queue.h>

namespace rdsn { namespace tools {

class sim_task_queue : public task_queue
{
public:
    sim_task_queue(task_worker_pool* pool, int index, task_queue* inner_provider);

    virtual void    enqueue(task_ptr& task);
    virtual task_ptr dequeue();
    virtual int      count() const { return (int)_tasks.count(); }

private:
    utils::blocking_priority_queue<task_ptr, TASK_PRIORITY_COUNT> _tasks;
};

struct sim_worker_state;
class sim_semaphore_provider : public semaphore_provider
{
public:  
    sim_semaphore_provider(rdsn::service::zsemaphore *sema, int initialCount, semaphore_provider *inner_provider)
        : semaphore_provider(sema, initialCount, inner_provider), _count(initialCount)
    {
    }

public:
    virtual void signal(int count);
    virtual bool wait(int timeout_milliseconds);

private:
    int _count;
    std::list<sim_worker_state*>   _waitThreads;
};

}} // end namespace
