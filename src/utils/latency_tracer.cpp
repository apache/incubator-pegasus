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

#include <dsn/utils/latency_tracer.h>
#include <dsn/service_api_c.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/flags.h>

namespace dsn {
namespace utils {

DSN_DEFINE_bool("replication", enable_latency_tracer, false, "whether enable the latency tracer");

latency_tracer::latency_tracer(const std::string &name, bool is_sub, uint64_t threshold)
    : _name(name), _threshold(threshold), _is_sub(is_sub), _start_time(dsn_now_ns())
{
}

latency_tracer::~latency_tracer()
{
    if (_is_sub) {
        return;
    }

    std::string traces;
    dump_trace_points(traces);
}

void latency_tracer::add_point(const std::string &stage_name)
{
    if (!FLAGS_enable_latency_tracer) {
        return;
    }

    uint64_t ts = dsn_now_ns();
    utils::auto_write_lock write(_lock);
    _points[ts] = stage_name;
}

void latency_tracer::set_sub_tracer(const std::shared_ptr<latency_tracer> &tracer)
{
    if (!FLAGS_enable_latency_tracer) {
        return;
    }

    _sub_tracer = tracer;
}

void latency_tracer::dump_trace_points(/*out*/ std::string &traces)
{
    if (!FLAGS_enable_latency_tracer || _threshold < 0 || _points.empty()) {
        return;
    }

    utils::auto_read_lock read(_lock);

    uint64_t time_used = _points.rbegin()->first - _start_time;

    if (time_used < _threshold) {
        return;
    }

    traces.append(fmt::format("\t***************[TRACE:{}]***************\n", _name));
    uint64_t previous_time = _start_time;
    for (const auto &point : _points) {
        std::string trace = fmt::format("\tTRACE:name={:<70}, span={:>20}, total={:>20}, "
                                        "ts={:<20}\n",
                                        point.second,
                                        point.first - previous_time,
                                        point.first - _start_time,
                                        point.first);
        traces.append(trace);
        previous_time = point.first;
    }

    if (_sub_tracer == nullptr) {
        dwarn_f("TRACE:the traces as fallow:\n{}", traces);
        return;
    }

    _sub_tracer->dump_trace_points(traces);
}

} // namespace utils
} // namespace dsn
