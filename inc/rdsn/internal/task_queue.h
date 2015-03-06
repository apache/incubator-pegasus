# pragma once

# include <rdsn/internal/task.h>

namespace rdsn {

class task_worker_pool;
class admission_controller;

class task_queue
{
public:
    template <typename T> static task_queue* create(task_worker_pool* pool, int index, task_queue* inner_provider)
    {
        return new T(pool, index, inner_provider);
    }

public:
    task_queue(task_worker_pool* pool, int index, task_queue* inner_provider); 
    
    virtual void     enqueue(task_ptr& task) = 0;
    virtual task_ptr dequeue() = 0;
    virtual int      count() const = 0;

    const std::string & get_name() { return _name; }    
    task_worker_pool* pool() const { return _pool; }
    perf_counter_ptr& get_qps_counter() { return _qps_counter; }
    admission_controller* controller() const { return _controller; }
    void set_controller(admission_controller* controller) { _controller = controller; }

private:
    task_worker_pool*      _pool;
    std::string            _name;
    perf_counter_ptr       _qps_counter;
    admission_controller*  _controller;
};

} // end namespace
