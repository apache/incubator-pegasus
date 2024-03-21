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

#include <iomanip>

#include "utils/autoref_ptr.h"
#include "utils/metrics.h"

namespace dsn {
namespace tools {

struct task_spec_profiler
{
    bool collect_call_count;
    bool is_profile;
    std::unique_ptr<std::atomic<int64_t>[]> call_counts;

    task_spec_profiler() = default;
    task_spec_profiler(int code);
    const metric_entity_ptr &profiler_metric_entity() const;

    METRIC_DEFINE_INCREMENT_NOTNULL(profiler_queued_tasks)
    METRIC_DEFINE_DECREMENT_NOTNULL(profiler_queued_tasks)
    METRIC_DEFINE_SET_NOTNULL(profiler_queue_latency_ns, int64_t)
    METRIC_DEFINE_SET_NOTNULL(profiler_execute_latency_ns, int64_t)
    METRIC_DEFINE_INCREMENT_NOTNULL(profiler_executed_tasks)
    METRIC_DEFINE_INCREMENT_NOTNULL(profiler_cancelled_tasks)
    METRIC_DEFINE_SET_NOTNULL(profiler_server_rpc_latency_ns, int64_t)
    METRIC_DEFINE_SET_NOTNULL(profiler_server_rpc_request_bytes, int64_t)
    METRIC_DEFINE_SET_NOTNULL(profiler_server_rpc_response_bytes, int64_t)
    METRIC_DEFINE_INCREMENT_NOTNULL(profiler_dropped_timeout_rpcs)
    METRIC_DEFINE_SET_NOTNULL(profiler_client_rpc_latency_ns, int64_t)
    METRIC_DEFINE_INCREMENT_NOTNULL(profiler_client_timeout_rpcs)
    METRIC_DEFINE_SET_NOTNULL(profiler_aio_latency_ns, int64_t)

private:
    const std::string _task_name;
    const metric_entity_ptr _profiler_metric_entity;

    METRIC_VAR_DECLARE_gauge_int64(profiler_queued_tasks);
    METRIC_VAR_DECLARE_percentile_int64(profiler_queue_latency_ns);
    METRIC_VAR_DECLARE_percentile_int64(profiler_execute_latency_ns);
    METRIC_VAR_DECLARE_counter(profiler_executed_tasks);
    METRIC_VAR_DECLARE_counter(profiler_cancelled_tasks);
    METRIC_VAR_DECLARE_percentile_int64(profiler_server_rpc_latency_ns);
    METRIC_VAR_DECLARE_percentile_int64(profiler_server_rpc_request_bytes);
    METRIC_VAR_DECLARE_percentile_int64(profiler_server_rpc_response_bytes);
    METRIC_VAR_DECLARE_counter(profiler_dropped_timeout_rpcs);
    METRIC_VAR_DECLARE_percentile_int64(profiler_client_rpc_latency_ns);
    METRIC_VAR_DECLARE_counter(profiler_client_timeout_rpcs);
    METRIC_VAR_DECLARE_percentile_int64(profiler_aio_latency_ns);
};

} // namespace tools
} // namespace dsn
