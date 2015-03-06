# pragma once

# include <rdsn/internal/task_queue.h>
# include <rdsn/internal/utils.h>

namespace rdsn {

class admission_controller
{
public:
    template <typename T> static admission_controller* create(task_queue* q, const char* args);

public:
    admission_controller(task_queue* q, std::vector<std::string>& sargs) : _queue(q) {}
    ~admission_controller(void) {}
    
    virtual bool is_task_accepted(task_ptr& task) = 0;
        
    task_queue* bound_queue() const { return _queue; }
    
private:
    task_queue* _queue;
};

// ----------------- inline implementation -----------------
template <typename T> 
admission_controller* admission_controller::create(task_queue* q, const char* args)
{
    std::vector<std::string> sargs;
    rdsn::utils::split_args(args, sargs, ' ');
    return new T(q, sargs);
}

} // end namespace
