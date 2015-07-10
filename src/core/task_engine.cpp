/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 * 
 * -=- Robust Distributed System Nucleus (rDSN) -=- 
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
# include "task_engine.h"
# include <dsn/internal/perf_counters.h>
# include <dsn/internal/factory_store.h>
# include <dsn/service_api.h>

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "task_engine"

using namespace dsn::utils;

namespace dsn {

task_worker_pool::task_worker_pool(const threadpool_spec& opts, task_engine* owner)
    : _spec(opts), _owner(owner), _node(owner->node())
{
    _is_running = false;
}

void task_worker_pool::start()
{
    if (_is_running)
        return;
    
    int qCount = _spec.partitioned ?  _spec.worker_count : 1;
    for (int i = 0; i < qCount; i++)
    {
        task_queue* q = factory_store<task_queue>::create(_spec.queue_factory_name.c_str(), PROVIDER_TYPE_MAIN, this, i, nullptr);
        for (auto it = _spec.queue_aspects.begin();
            it != _spec.queue_aspects.end();
            it++)
        {
            q = factory_store<task_queue>::create(it->c_str(), PROVIDER_TYPE_ASPECT, this, i, q);
        }
        _queues.push_back(q);

        if (_spec.admission_controller_factory_name != "")
        {
            admission_controller* controller = factory_store<admission_controller>::create(_spec.admission_controller_factory_name.c_str(), 
                PROVIDER_TYPE_MAIN, 
                q, _spec.admission_controller_arguments.c_str());
        
            if (controller)
            {
                _controllers.push_back(controller);
                q->set_controller(controller);
            }
            else
            {
                _controllers.push_back(nullptr);
            }
        }
        else
        {
            _controllers.push_back(nullptr);
        }
    }

    for (int i = 0; i < _spec.worker_count; i++)
    {
        auto q = _queues[qCount == 1 ? 0 : i];
        task_worker* worker = factory_store<task_worker>::create(_spec.worker_factory_name.c_str(), PROVIDER_TYPE_MAIN, this, q, i, nullptr);
        for (auto it = _spec.worker_aspects.begin();
            it != _spec.worker_aspects.end();
            it++)
        {
            worker = factory_store<task_worker>::create(it->c_str(), PROVIDER_TYPE_ASPECT, this, q, i, worker);
        }
        task_worker::on_create.execute(worker);

        _workers.push_back(worker);
        worker->start();
    }

    _is_running = true;
}

void task_worker_pool::enqueue(task* t)
{
    dassert(t->spec().pool_code == spec().pool_code || t->spec().type == TASK_TYPE_RPC_RESPONSE, "Invalid thread pool used");

    if (_is_running)
    {
        int idx = (_spec.partitioned ? t->hash() % _queues.size() : 0);
        task_queue* q = _queues[idx];
        //dinfo("%s pool::enqueue %s (%016llx)", _node->name(), task->spec().name, task->id());
        if (t->delay_milliseconds() == 0)
        {
            auto controller = _controllers[idx];
            if (controller != nullptr)
            {
                while (!controller->is_task_accepted(t))
                {
                    // any customized rejection handler?
                    if (t->spec().rejection_handler != nullptr)
                    {
                        t->spec().rejection_handler(t, controller);

                        ddebug("task %s (%016llx) is rejected",                            
                            t->spec().name,
                            t->id()
                            );

                        return;
                    }

                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }
            else if (t->spec().type == TASK_TYPE_RPC_REQUEST && _spec.max_input_queue_length != 0xFFFFFFFFUL)
            {
                while ((uint32_t)q->count() >= _spec.max_input_queue_length)
                {
                    // any customized rejection handler?
                    if (t->spec().rejection_handler != nullptr)
                    {
                        t->spec().rejection_handler(t, controller);

                        ddebug("task %s (%016llx) is rejected because the target queue is full",                            
                            t->spec().name,
                            t->id()
                            );

                        return;
                    }

                    std::this_thread::sleep_for(std::chrono::milliseconds(1));
                }
            }
        }

        // add reference before adding to the queue
        t->add_ref();
        return q->enqueue(t);
    }
    else
    {
        dassert (false, "worker pool %s must be started before enqueue task %s",
            spec().name.c_str(),
            t->spec().name
            );
    }
}

bool task_worker_pool::shared_same_worker_with_current_task(task* tsk) const
{
    task* current = task::get_current_task();
    if (nullptr != current)
    {
        if (current->spec().pool_code != tsk->code())
            return false;
        else if (_workers.size() == 1)
            return true;
        else if (_spec.partitioned)
        {
            int sz = static_cast<int>(_workers.size());
            return current->hash() % sz == tsk->hash() % sz;
        }
        else
        {
            return false;
        }
    }
    else
    {
        return false;
    }
}

void task_worker_pool::get_runtime_info(const std::string& indent, const std::vector<std::string>& args, __out_param std::stringstream& ss)
{
    std::string indent2 = indent + "\t";
    ss << indent << "contains " << _workers.size() << " threads with " << _queues.size() << " queues" << std::endl;
    
    for (auto& q : _queues)
    {
        if (q)
        {
            ss << indent2 << q->get_name() << " now has " << q->count() << " pending tasks" << std::endl;
        }
    }

    for (auto& wk : _workers)
    {
        if (wk)
        {
            ss << indent2 << wk->index() << " (TID = " << wk->native_tid() << ") attached with queue " << wk->queue()->get_name() << std::endl;
        }
    }
}

task_engine::task_engine(service_node* node)
{
    _is_running = false;
    _node = node;
}

void task_engine::start(const std::list<threadpool_code>& pools)
{
    if (_is_running)
        return;

    // init pools
    _pools.resize(threadpool_code::max_value() + 1, nullptr);
    for (auto& p : pools)
    {
        auto& s = service_engine::instance().spec().threadpool_specs[p];
        auto workerPool = new task_worker_pool(s, this);
        workerPool->start();
        _pools[p] = workerPool;
    }

    _is_running = true;
}

void task_engine::get_runtime_info(const std::string& indent, const std::vector<std::string>& args, __out_param std::stringstream& ss)
{
    std::string indent2 = indent + "\t";
    for (auto& p : _pools)
    {
        if (p)
        {
            ss << indent << p->spec().pool_code.to_string() << std::endl;
            p->get_runtime_info(indent2, args, ss);
        }
    }
}

} // end namespace
