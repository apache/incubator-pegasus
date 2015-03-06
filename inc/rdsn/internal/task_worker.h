# pragma once

# include <rdsn/internal/task_queue.h>
# include <rdsn/internal/extensible_object.h>
# include <rdsn/internal/synchronize.h>
# include <thread>

namespace rdsn {
 
class task_worker : public extensible_object<task_worker, 4>
{
public:
    template <typename T> static task_worker* create(task_worker_pool* pool, task_queue* q, int index, task_worker* inner_provider)
    {
        return new T(pool, q, index, inner_provider);
    }

public:
    task_worker(task_worker_pool* pool, task_queue* q, int index, task_worker* inner_provider);
    virtual ~task_worker(void);

    // service management
    void start();
    void stop();

    virtual void loop(); // run tasks from _input_queueu

    // inquery
    const std::string& name() const { return _name; }
    int index() const { return _index; }
    task_worker_pool* pool() const { return _owner_pool; }
    task_queue* queue() const { return _input_queue; }
    const threadpool_spec& pool_spec() const;
    static task_worker* current();

private:
    task_worker_pool* _owner_pool;    
    task_queue*      _input_queue;
    int             _index;
    std::string     _name;
    std::thread     *_thread;
    bool            _is_running;
    utils::notify_event _started;

private:
    void set_name();
    void set_priority(worker_priority pri);
    void set_affinity(uint64_t affinity);
    void run_internal();

public:
    static join_point<void, task_worker*> on_start;
    static join_point<void, task_worker*> on_create;
};

} // end namespace


