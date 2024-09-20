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

#pragma once

#include <list>
#include <memory>
#include <string>
#include <vector>

#include "nlohmann/json_fwd.hpp"
#include "task_code.h"
#include "utils/command_manager.h"
#include "utils/threadpool_spec.h"

namespace dsn {

class service_node;
class task;
class task_engine;
class task_queue;
class task_worker;
class threadpool_code;
class timer_service;

//
// a task_worker_pool is a set of TaskWorkers share the same configs;
// they may even share the same task_queue when partitioned == false
//
class task_worker_pool
{
public:
    task_worker_pool(const threadpool_spec &opts, task_engine *owner);

    // service management
    void create();
    void start();
    void stop();

    // task procecessing
    void enqueue(task *task);
    void on_dequeue(int count);

    // cached timer service access
    void add_timer(task *task);

    // inquery
    const threadpool_spec &spec() const { return _spec; }
    bool shared_same_worker_with_current_task(task *task) const;
    task_engine *engine() const { return _owner; }
    service_node *node() const { return _node; }
    nlohmann::json get_runtime_info(const std::vector<std::string> &args) const;
    nlohmann::json get_queue_info() const;
    std::vector<task_queue *> &queues() { return _queues; }
    std::vector<task_worker *> &workers() { return _workers; }

private:
    friend class task_engine;

    threadpool_spec _spec;
    task_engine *_owner;
    service_node *_node;

    std::vector<task_worker *> _workers;
    std::vector<task_queue *> _queues;

    std::vector<timer_service *> _per_queue_timer_svcs;

    bool _is_running;
};

class task_engine
{
public:
    task_engine(service_node *node);
    ~task_engine() { stop(); }

    //
    // service management routines
    //
    void create(const std::list<dsn::threadpool_code> &pools);
    void start();
    void stop();

    //
    // task management routines
    //
    task_worker_pool *get_pool(int code) const { return _pools[code]; }
    std::vector<task_worker_pool *> &pools() { return _pools; }

    bool is_started() const { return _is_running; }

    volatile int *get_task_queue_virtual_length_ptr(dsn::task_code code, int hash);

    service_node *node() const { return _node; }
    nlohmann::json get_runtime_info(const std::vector<std::string> &args) const;
    nlohmann::json get_queue_info() const;

private:
    void register_cli_commands();

    std::vector<task_worker_pool *> _pools;
    volatile bool _is_running;
    service_node *_node;
    std::unique_ptr<command_deregister> _task_queue_max_length_cmd;
};

// -------------------- inline implementation ----------------------------

} // namespace dsn
