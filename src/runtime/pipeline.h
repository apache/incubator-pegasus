/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <absl/utility/utility.h>
#include "runtime/task/task_code.h"
#include "runtime/task/task_tracker.h"
#include "runtime/task/async_calls.h"
#include "utils/chrono_literals.h"

namespace dsn {
namespace pipeline {

// The environment for execution.
struct environment
{
    template <typename F>
    void schedule(F &&f, std::chrono::milliseconds delay_ms = 0_ms) const
    {
        tasking::enqueue(__conf.thread_pool_code,
                         __conf.tracker,
                         std::forward<F>(f),
                         __conf.thread_hash,
                         delay_ms);
    }

    /// Fluent APIs to specify the environment configuration.
    environment &thread_pool(task_code tc)
    {
        __conf.thread_pool_code = tc;
        return *this;
    }
    environment &thread_hash(int hash)
    {
        __conf.thread_hash = hash;
        return *this;
    }
    environment &task_tracker(dsn::task_tracker *tracker)
    {
        __conf.tracker = tracker;
        return *this;
    }

    struct
    {
        task_code thread_pool_code;
        dsn::task_tracker *tracker{nullptr};
        int thread_hash{0};
    } __conf;
};

template <typename... Args>
struct result
{
    typedef std::tuple<Args...> ArgsTupleType;

    // Step down to next stage.
    // NOTE: Remember to exit from caller function after `step_down_next_stage`.
    // For example:
    //
    // ```
    //   pipeline::base base;
    //   ping_rpc rpc = ...;
    //
    //   pipeline::do_when<> ping([&ping]() {
    //        bool ok = rpc.call();
    //        if(ok) {
    //            step_down_next_stage();
    //            // when steps out, it goes to repeat this stage round and round.
    //        }
    //        repeat(1_s); // will repeat even after stepping down to next stage.
    //   });
    //
    //   base.thread_pool(LPC_DUPLICATE_MUTATIONS).task_tracker(&tracker).from(&s1);
    //   base.run_pipeline();
    //   base.wait_all();
    // ```
    //
    // To fix the problem, return immediately after `step_down_next_stage`.
    //
    // ```
    //   pipeline::do_when<> ping([&ping]() {
    //        bool ok = rpc.call();
    //        if(ok) {
    //            step_down_next_stage();
    //            return;
    //        }
    //        repeat(1_s); // will repeat even after stepping down to next stage.
    //   });
    // ```
    //
    void step_down_next_stage(Args &&... args)
    {
        CHECK_NOTNULL(__func, "no next stage is linked");
        __func(std::make_tuple(std::forward<Args>(args)...));
    }

    std::function<void(ArgsTupleType &&)> __func;
};

//
// Example:
//
// ```
//   pipeline::base base;
//
//   pipeline::do_when<> s1([&s1]() { s1.repeat(1_s); });
//   base.thread_pool(LPC_DUPLICATE_MUTATIONS).task_tracker(&tracker).from(&s1);
//
//   base.run_pipeline();
//   base.pause();
//   base.wait_all();
// ```
//
struct base : environment
{
    // Start this pipeline.
    // NOTE: Be careful when pipeline starting and pausing are running concurrently,
    //       though it's internally synchronized, the actual order is still non-deterministic
    //       from the user's view.
    //
    // ```
    //   base.schedule([&base]() { base.run_pipeline(); });
    //   base.pause();
    //   base.wait_all(); // the pipeline won't stop.
    // ```
    //
    void run_pipeline();

    void pause() { _paused.store(true, std::memory_order_release); }

    bool paused() const { return _paused.load(std::memory_order_acquire); }

    // Await for all running tasks to complete.
    void wait_all() { __conf.tracker->wait_outstanding_tasks(); }
    void cancel_all() { __conf.tracker->cancel_outstanding_tasks(); }

    /// === Pipeline Declaration === ///
    /// Declaration of pipeline is not thread-safe.

    template <typename Stage>
    struct node
    {
        // pipeline supports cyclic execution.
        // For example in "data verifier", we insert data into database, and verify
        // that it is applied successfully. After verification we make next insert.
        //
        // ```
        //      _insert = std::make_unique<insert_data>(...);
        //      _verify = std::make_unique<verify_data>(...);
        //      link(*_insert).link(*_verify).link(*_insert);
        // ```
        //
        // Here we construct a infinite loop.
        // When first `_insert` steps down to `_verify`, it directly calls the function
        // `_verify->run(...)`.
        // However when `_verify` is stepping down, in order to avoid infinite recursion
        // which will cause stack overflow, it calls `_insert->async(..)`, which enqueues
        // a new task into rdsn task engine.
        template <typename NextStage>
        node<NextStage> link(NextStage &next)
        {
            using ArgsTupleType = typename Stage::ArgsTupleType;

            // link to node of existing pipeline
            if (next.__pipeline != nullptr) {
                this_stage->__func = [next_ptr = &next](ArgsTupleType && args) mutable
                {
                    absl::apply(&NextStage::async,
                                std::tuple_cat(std::make_tuple(next_ptr), std::move(args)));
                };
            } else {
                next.__conf = this_stage->__conf;
                next.__pipeline = this_stage->__pipeline;
                this_stage->__func = [next_ptr = &next](ArgsTupleType && args) mutable
                {
                    if (next_ptr->paused()) {
                        return;
                    }
                    absl::apply(&NextStage::run,
                                std::tuple_cat(std::make_tuple(next_ptr), std::move(args)));
                };
            }
            return node<NextStage>(&next);
        }

        explicit node(Stage *s) : this_stage(s) {}

    private:
        Stage *this_stage;
    };

    template <typename Stage>
    node<Stage> from(Stage &start)
    {
        start.__conf = __conf;
        start.__pipeline = this;
        _root_stage = &start;
        return node<Stage>(&start);
    }

    // Create a fork of the pipeline, which shares the same task tracker,
    // but with different thread pool, thread hash.
    template <typename NextStage>
    node<NextStage> fork(NextStage &next, task_code tc, int thread_hash)
    {
        next.__conf.thread_pool_code = tc;
        next.__conf.thread_hash = thread_hash;
        next.__conf.tracker = __conf.tracker;

        next.__pipeline = this;
        return node<NextStage>(&next);
    }

private:
    environment *_root_stage{nullptr};
    std::atomic_bool _paused{true};
};

// A piece of execution, receiving argument `Args`, running in the environment
// created by `pipeline::base`.
template <typename... Args>
struct when : environment
{
    /// Run this stage within current context.
    virtual void run(Args &&... in) = 0;

    void repeat(Args &&... in, std::chrono::milliseconds delay_ms = 0_ms)
    {
        auto arg_tuple = std::make_tuple(this, std::forward<Args>(in)...);
        schedule([ this, args = std::move(arg_tuple) ]() mutable {
            if (paused()) {
                return;
            }
            absl::apply(&when<Args...>::run, std::move(args));
        },
                 delay_ms);
    }

    /// Run this stage asynchronously in its environment.
    void async(Args &&... in) { repeat(std::forward<Args>(in)...); }

    bool paused() const { return __pipeline->paused(); }

    base *__pipeline{nullptr};
};

inline void base::run_pipeline()
{
    CHECK_NOTNULL(__conf.tracker, "must configure task tracker");

    _paused.store(false, std::memory_order_release);

    schedule([stage = static_cast<when<> *>(_root_stage)]() {
        // static_cast for downcast, but completely safe.
        stage->run();
    });
}

/// A simple utility for definition of a `when` using lambda.
/// It's useful for unit test.
template <typename... Args>
struct do_when : when<Args...>
{
    explicit do_when(std::function<void(Args &&... args)> &&func) : _cb(std::move(func)) {}

    void run(Args &&... args) override { _cb(std::forward<Args>(args)...); }

    virtual ~do_when() = default;

private:
    std::function<void(Args &&...)> _cb;
};

/// Runnable must extend from pipeline::environment and implement
/// a public method: `void run();`
template <typename Runnable>
static void repeat(Runnable &&r, std::chrono::milliseconds delay_ms = 0_ms)
{
    environment env = r;
    env.schedule([r = std::move(r)]() mutable { r.run(); }, delay_ms);
}

} // namespace pipeline
} // namespace dsn
