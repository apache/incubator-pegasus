#include "task_engine.sim.h"
#include "scheduler.h"

namespace rdsn { namespace tools {
    
sim_task_queue::sim_task_queue(task_worker_pool* pool, int index, task_queue* inner_provider)
: task_queue(pool, index, inner_provider), _tasks("")
{
}

void sim_task_queue::enqueue(task_ptr& task)
{
    if (0 == task->delay_milliseconds())    
    {
        _tasks.enqueue(task, task->spec().priority);
    }
    else
    {
        scheduler::instance().add_task(task, this);
    }
}

task_ptr sim_task_queue::dequeue()
{
    scheduler::instance().wait_schedule(false);

    long c = 0;
    return _tasks.dequeue(c);
}

void sim_semaphore_provider::signal(int count)
{
    _count += count;
    
    while (!_waitThreads.empty() && _count > 0)
    {
        --_count;

        sim_worker_state* thread = _waitThreads.front();
        _waitThreads.pop_front();
        thread->is_continuation_ready = true;
    }
}

bool sim_semaphore_provider::wait(int timeout_milliseconds)
{
    if (_count > 0)
    {
        --_count;
        scheduler::instance().wait_schedule(true, true);
        return true;
    }
    else
    {
        _waitThreads.push_back(scheduler::task_worker_ext::get(task::get_current_worker()));
        scheduler::instance().wait_schedule(true, false);
        return true;
    }
}

}} // end namespace
