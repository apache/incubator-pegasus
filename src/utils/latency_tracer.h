// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once
#include "utils/synchronize.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "runtime/task/task_code.h"
#include "common/replication.codes.h"

namespace dsn {
namespace utils {

#define ADD_POINT(tracer)                                                                          \
    do {                                                                                           \
        if (dsn_unlikely(tracer != nullptr && (tracer)->enabled()))                                \
            (tracer)->add_point(fmt::format("{}:{}:{}", __FILENAME__, __LINE__, __FUNCTION__));    \
    } while (0)

#define ADD_CUSTOM_POINT(tracer, message)                                                          \
    do {                                                                                           \
        if (dsn_unlikely(tracer != nullptr && (tracer)->enabled()))                                \
            (tracer)->add_point(                                                                   \
                fmt::format("{}:{}:{}_{}", __FILENAME__, __LINE__, __FUNCTION__, (message)));      \
    } while (0)

#define APPEND_EXTERN_POINT(tracer, ts, message)                                                   \
    do {                                                                                           \
        if (dsn_unlikely(tracer != nullptr && (tracer)->enabled()))                                \
            (tracer)->append_point(                                                                \
                fmt::format("{}:{}:{}_{}", __FILENAME__, __LINE__, __FUNCTION__, (message)),       \
                (ts));                                                                             \
    } while (0)

/**
 * latency_tracer is a tool for tracking the time spent in each of the stages during request
 * execution. It can help users to figure out where the latency bottleneck is located. User needs to
 * use `add_point` before entering one stage, which will record the name of this stage and its start
 * time. When the request is finished, the formatted result can be dumped automatically in
 * deconstructer
 *
 * For example, given a request with a 4-stage pipeline (the `latency_tracer` need to
 * be held by this request throughout the execution):
 *
 * ```
 * class request {
 *      latency_tracer tracer;
 * }
 * void start(request req){
 *      req.tracer.add_point("start");
 * }
 * void stageA(request req){
 *      req.tracer.add_point("stageA");
 * }
 * void stageB(request req){
 *      req.tracer.add_point("stageB");
 * }
 * void end(request req){
 *      req.tracer.add_point("end");
 * }
 * ```
 *
 *  point1     point2     point3    point4
 *    |         |           |         |
 *    |         |           |         |
 *  start---->stageA----->stageB---->end
 *
 * "request.tracer" will record the time duration among all trace points.
 **/
DSN_DECLARE_bool(enable_latency_tracer);
DSN_DECLARE_bool(enable_latency_tracer_report);

class latency_tracer
{
public:
    //-is_sub:
    //  if `is_sub`=true means its points will be dumped by parent tracer and won't be dumped
    //  repeatedly in destructor
    //-threshold:
    //  threshold < 0: don't dump any trace points
    //  threshold = 0: dump all trace points
    //  threshold > 0: dump the trace point when time_used > threshold
    //-task_code:
    //  (1) use task code to judge if the task need trace, LPC_LATENCY_TRACE passed by default
    //  means _enable_trace = true, for other code, it will get config value(see the implement of
    //  the constructor) to judge the code whether to enable trace.
    //  (2) the variable is used to trace the common low task work, for example, `aio task` is used
    //  for nfs/private log/shared log, it will trace all type task if we want trace the `aio task`,
    //  support the variable, the `aio task tracer` will filter out some unnecessary task base on
    //  the code type.
    latency_tracer(bool is_sub,
                   std::string name,
                   uint64_t threshold,
                   const dsn::task_code &code = LPC_LATENCY_TRACE);

    ~latency_tracer();

    // add a trace point to the tracer, it will record the timestamp of point
    //
    // -name: user specified name of the trace point
    void add_point(const std::string &stage_name);

    // append a trace point, the timestamp is passed. it will always append at last position
    //
    // NOTE: The method is used for custom stage duration which must make sure the point is
    // sequential, for example, in the trace link of cross node, receive side timestamp must after
    // the send side timestamp, you need use the method to make sure the rule to avoid the clock
    // asynchronization problem. the detail resolution see the method implement
    //
    // -name: user specified name of the trace point
    // -timestamp: user specified timestamp of the trace point
    void append_point(const std::string &stage_name, uint64_t timestamp);

    // sub_tracer is used for tracking the request which may transfer the other thread, for example:
    // rdsn "mutataion" will async to execute send "mutation" to remote rpc node and execute io
    // task, the "tracking  responsibility" is also passed on the async task:
    //
    // stageA[mutation]--stageB[mutation]--|-->stageC0[mutation]-->....
    //                                     |-->stageC1[io]-->....
    //                                     |-->stageC2[rpc]-->....
    void add_sub_tracer(const std::shared_ptr<latency_tracer> &tracer);

    void add_sub_tracer(const std::string &name);

    std::shared_ptr<latency_tracer> sub_tracer(const std::string &name);

    void set_name(const std::string &name) { _name = name; }

    void set_description(const std::string &description) { _description = description; }

    void set_parent_point_name(const std::string &name) { _parent_point_name = name; }

    void set_start_time(uint64_t start_time) { _start_time = start_time; }

    const std::string &name() const { return _name; }

    const std::string &description() const { return _description; }

    uint64_t start_time() const { return _start_time; }

    uint64_t last_time() const { return _last_time; }

    const std::string &last_stage_name() const { return _last_stage; }

    bool enabled() const { return _enable_trace; }

private:
    // report the trace point duration to monitor system
    static void report_trace_point(const std::string &name, uint64_t span);

    // dump and print the trace point into log file
    void dump_trace_points(/*out*/ std::string &traces);

    bool _is_sub;
    std::string _name;
    std::string _description;
    uint64_t _threshold;
    uint64_t _start_time;
    uint64_t _last_time;
    std::string _last_stage;

    dsn::task_code _task_code;
    bool _enable_trace;

    utils::rw_lock_nr _point_lock; //{
    std::map<int64_t, std::string> _points;
    // }

    std::string _parent_point_name;
    utils::rw_lock_nr _sub_lock; //{
    std::unordered_map<std::string, std::shared_ptr<latency_tracer>> _sub_tracers;
    // }

    friend class latency_tracer_test;
};
} // namespace utils
} // namespace dsn
