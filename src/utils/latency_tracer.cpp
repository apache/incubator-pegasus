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

#include "utils/latency_tracer.h"
#include "perf_counter/perf_counters.h"
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/fmt_logging.h"
#include "utils/config_api.h"
#include "utils/flags.h"

#include <utility>
#include "lockp.std.h"
#include "shared_io_service.h"

namespace dsn {
namespace utils {

DSN_DEFINE_bool(replication,
                enable_latency_tracer,
                false,
                "whether enable the global latency tracer");
DSN_TAG_VARIABLE(enable_latency_tracer, FT_MUTABLE);

DSN_DEFINE_bool(replication,
                enable_latency_tracer_report,
                false,
                "whether open the latency tracer report perf counter");
DSN_TAG_VARIABLE(enable_latency_tracer_report, FT_MUTABLE);

DSN_DEFINE_string(replication,
                  latency_tracer_counter_name_prefix,
                  "trace_latency",
                  "perf counter common name prefix");

utils::rw_lock_nr counter_lock; //{
std::unordered_map<std::string, perf_counter_ptr> counters_trace_latency;
// }

utils::rw_lock_nr task_code_lock; //{
std::unordered_map<std::string, bool> task_codes;
// }

perf_counter_ptr get_trace_counter(const std::string &name)
{
    {
        utils::auto_read_lock read(counter_lock);
        auto iter = counters_trace_latency.find(name);
        if (iter != counters_trace_latency.end()) {
            return iter->second;
        }
    }

    utils::auto_write_lock write(counter_lock);
    auto iter = counters_trace_latency.find(name);
    if (iter != counters_trace_latency.end()) {
        return iter->second;
    }

    auto perf_counter =
        dsn::perf_counters::instance().get_app_counter(FLAGS_latency_tracer_counter_name_prefix,
                                                       name.c_str(),
                                                       COUNTER_TYPE_NUMBER_PERCENTILES,
                                                       name.c_str(),
                                                       true);

    counters_trace_latency.emplace(name, perf_counter);
    return perf_counter;
}

bool is_enable_trace(const dsn::task_code &code)
{
    if (!FLAGS_enable_latency_tracer) {
        return false;
    }

    if (code == LPC_LATENCY_TRACE) {
        return true;
    }

    std::string code_name(dsn::task_code(code).to_string());
    {
        utils::auto_read_lock read(task_code_lock);
        auto iter = task_codes.find(code_name);
        if (iter != task_codes.end()) {
            return iter->second;
        }
    }

    utils::auto_write_lock write(task_code_lock);
    auto iter = task_codes.find(code_name);
    if (iter != task_codes.end()) {
        return iter->second;
    }

    std::string section_name = std::string("task.") + code_name;
    auto enable_trace = dsn_config_get_value_bool(
        section_name.c_str(), "enable_trace", false, "whether to enable trace this kind of task");

    task_codes.emplace(code_name, enable_trace);
    return enable_trace;
}

latency_tracer::latency_tracer(bool is_sub,
                               std::string name,
                               uint64_t threshold,
                               const dsn::task_code &code)
    : _is_sub(is_sub),
      _name(std::move(name)),
      _description("default"),
      _threshold(threshold),
      _start_time(dsn_now_ns()),
      _last_time(_start_time),
      _task_code(code),
      _enable_trace(is_enable_trace(code))
{
    append_point(fmt::format("{}:{}:{}", __FILENAME__, __LINE__, __FUNCTION__), _start_time);
}

latency_tracer::~latency_tracer()
{
    if (!_enable_trace || _is_sub) {
        return;
    }

    std::string traces;
    dump_trace_points(traces);
}

void latency_tracer::add_point(const std::string &stage_name)
{
    if (!_enable_trace) {
        return;
    }

    uint64_t ts = dsn_now_ns();
    utils::auto_write_lock write(_point_lock);
    _points.emplace(ts, stage_name);
    _last_time = ts;
    _last_stage = stage_name;
}

void latency_tracer::append_point(const std::string &stage_name, uint64_t timestamp)
{
    if (!_enable_trace) {
        return;
    }

    utils::auto_write_lock write(_point_lock);
    uint64_t cur_ts = timestamp > _last_time ? timestamp : _last_time + 1;
    _points.emplace(cur_ts, stage_name);
    _last_time = cur_ts;
    _last_stage = stage_name;
}

void latency_tracer::add_sub_tracer(const std::string &name)
{
    if (!_enable_trace) {
        return;
    }

    auto sub_tracer = std::make_shared<dsn::utils::latency_tracer>(true, name, 0);
    sub_tracer->set_parent_point_name(_last_stage);
    sub_tracer->set_description(_description);
    utils::auto_write_lock write(_sub_lock);
    _sub_tracers.emplace(name, sub_tracer);
}

void latency_tracer::add_sub_tracer(const std::shared_ptr<latency_tracer> &tracer)
{
    if (!_enable_trace) {
        return;
    }

    utils::auto_write_lock write(_sub_lock);
    _sub_tracers.emplace(tracer->name(), tracer);
}

std::shared_ptr<latency_tracer> latency_tracer::sub_tracer(const std::string &name)
{
    if (!_enable_trace) {
        return nullptr;
    }

    utils::auto_read_lock read(_sub_lock);
    auto iter = _sub_tracers.find(name);
    if (iter != _sub_tracers.end()) {
        return iter->second;
    }
    LOG_WARNING("can't find the [{}] sub tracer of {}", name, _name);
    return nullptr;
}

void latency_tracer::dump_trace_points(/*out*/ std::string &traces)
{
    if (!_enable_trace || _threshold < 0) {
        return;
    }

    uint64_t total_time_used;
    {
        utils::auto_read_lock point_lock(_point_lock);
        if (_points.empty()) {
            return;
        }

        uint64_t start_time = _points.begin()->first;
        total_time_used = _points.rbegin()->first - start_time;
        std::string header_format = _is_sub ? "          " : "***************";
        traces.append(fmt::format("\t{}[TRACE:[{}.{}]{}]{}\n",
                                  header_format,
                                  _description,
                                  dsn::task_code(_task_code).to_string(),
                                  _name,
                                  header_format));
        uint64_t previous_point_ts = _points.begin()->first;
        std::string previous_point_name = _points.begin()->second;
        for (const auto &point : _points) {
            if (point.first == start_time) {
                continue;
            }
            auto cur_point_ts = point.first;
            auto cur_point_name = point.second;
            auto span_duration = point.first - previous_point_ts;
            auto total_latency = point.first - start_time;

            if (FLAGS_enable_latency_tracer_report) {
                std::string counter_name =
                    fmt::format("[{}]{}@{}", _description, previous_point_name, cur_point_name);
                report_trace_point(counter_name, span_duration);
            }

            if (total_time_used >= _threshold) {
                std::string trace_format = _is_sub ? " " : "";
                std::string trace_name =
                    _is_sub ? fmt::format("{}.{}", _parent_point_name, cur_point_name)
                            : cur_point_name;
                std::string trace_log =
                    fmt::format("\t{}TRACE:name={:<110}, span={:>20}, total={:>20}, "
                                "ts={:<20}\n",
                                trace_format,
                                trace_name,
                                span_duration,
                                total_latency,
                                cur_point_ts);
                traces.append(trace_log);
            }

            previous_point_ts = cur_point_ts;
            previous_point_name = cur_point_name;
        }
    }

    {
        utils::auto_read_lock tracer_lock(_sub_lock);
        for (const auto &sub : _sub_tracers) {
            sub.second->dump_trace_points(traces);
        }
    }

    if (!_is_sub && total_time_used >= _threshold) {
        LOG_WARNING("TRACE:the traces as fallow:\n{}", traces);
        return;
    }
}

void latency_tracer::report_trace_point(const std::string &name, uint64_t span)
{
    auto perf_counter = get_trace_counter(name);
    if (perf_counter) {
        perf_counter->set(span);
    }
}

} // namespace utils
} // namespace dsn
