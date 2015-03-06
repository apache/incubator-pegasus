#pragma once

#include <rdsn/tool_api.h>
#include <rdsn/internal/synchronize.h>

namespace rdsn { namespace tools {

class event_wheel
{
public:
    ~event_wheel() { clear(); }

    void add_event(uint64_t ts, task_ptr& task);
    std::vector<task_ptr>* pop_next_events(__out uint64_t& ts);
    void clear();
    bool has_more_events() const {  utils::auto_lock l(_lock); return _events.size() > 0; }

private:
    typedef std::map<uint64_t, std::vector<task_ptr>*>  Events;
    Events _events;
    mutable std::recursive_mutex  _lock;
};

struct sim_worker_state
{
    utils::semaphore  runnable;
    int               index;
    task_worker       *worker;
    bool              first_time_schedule;
    bool              in_continuation;
    bool              is_continuation_ready;

    static void deletor(void* p)
    {
        delete (sim_worker_state*)p;
    }
};

class scheduler : public utils::singleton<scheduler>
{
public:
    scheduler(void);
    ~scheduler(void);

    void start() { _running = true; }
    void reset();
    bool has_more_events() const { return _wheel.has_more_events(); }
    uint64_t now_ns() const { utils::auto_lock l(_lock); return _time_ns; }

    void add_task(task_ptr& task, task_queue* q);
    void wait_schedule(bool in_continue, bool is_continue_ready = false);
    
    typedef void (*StateWatcher)();
    void add_watcher(StateWatcher watcher);

public:
    struct task_state_ext
    {
        task_queue                   *queue;
        std::list<sim_worker_state*>   wait_threads;

        static void deletor(void* p)
        {
            delete (task_state_ext*)p;
        }
    };
    typedef object_extension_helper<sim_worker_state, task_worker> task_worker_ext;
    typedef object_extension_helper<task_state_ext, task> task_ext;

private:
    event_wheel _wheel;

    mutable std::recursive_mutex _lock;
    uint64_t         _time_ns;

    bool           _running;
    
    typedef std::list<StateWatcher> WatcherList;
    mutable utils::rw_lock _watcherLock;    
    WatcherList     _watchers;

    std::vector<sim_worker_state*> _threads;
    
private:
    void schedule();
    static void on_task_worker_create(task_worker* worker);
    static void on_task_worker_start(task_worker* worker);
    static void on_task_wait(task* waitor, task* waitee, uint32_t timeout_milliseconds);
    static void on_task_end(task* task);
};

// ------------------  inline implementation ----------------------------
inline void scheduler::add_watcher(StateWatcher watcher)
{
    utils::auto_write_lock l(_watcherLock);
    _watchers.push_back(watcher);
}

inline void scheduler::reset()
{
    _wheel.clear();
    {
        utils::auto_write_lock l(_watcherLock);
        _watchers.clear();
    }
}

}} // end namespace
