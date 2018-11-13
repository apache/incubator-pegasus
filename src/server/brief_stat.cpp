// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <iomanip>
#include <dsn/c/api_utilities.h>
#include <dsn/perf_counter/perf_counters.h>

#include "brief_stat.h"

namespace pegasus {

static std::map<std::string, std::string> s_brief_stat_map = {
    {"zion*profiler*RPC_RRDB_RRDB_GET.qps", "get_qps"},
    {"zion*profiler*RPC_RRDB_RRDB_GET.latency.server", "get_p99(ns)"},
    {"zion*profiler*RPC_RRDB_RRDB_MULTI_GET.qps", "multi_get_qps"},
    {"zion*profiler*RPC_RRDB_RRDB_MULTI_GET.latency.server", "multi_get_p99(ns)"},
    {"zion*profiler*RPC_RRDB_RRDB_PUT.qps", "put_qps"},
    {"zion*profiler*RPC_RRDB_RRDB_PUT.latency.server", "put_p99(ns)"},
    {"zion*profiler*RPC_RRDB_RRDB_MULTI_PUT.qps", "multi_put_qps"},
    {"zion*profiler*RPC_RRDB_RRDB_MULTI_PUT.latency.server", "multi_put_p99(ns)"},
    {"replica*eon.replica_stub*replica(Count)", "serving_replica_count"},
    {"replica*eon.replica_stub*opening.replica(Count)", "opening_replica_count"},
    {"replica*eon.replica_stub*closing.replica(Count)", "closing_replica_count"},
    {"replica*eon.replica_stub*replicas.commit.qps", "commit_throughput"},
    {"replica*eon.replica_stub*replicas.learning.count", "learning_count"},
    {"replica*app.pegasus*manual.compact.running.count", "manual_compact_running_count"},
    {"replica*app.pegasus*manual.compact.enqueue.count", "manual_compact_enqueue_count"},
    {"replica*app.pegasus*rdb.block_cache.memory_usage", "rdb_block_cache_memory_usage"},
    {"replica*eon.replica_stub*shared.log.size(MB)", "shared_log_size(MB)"},
    {"replica*server*memused.virt(MB)", "memused_virt(MB)"},
    {"replica*server*memused.res(MB)", "memused_res(MB)"},
    {"replica*eon.replica_stub*disk.capacity.total(MB)", "disk_capacity_total(MB)"},
    {"replica*eon.replica_stub*disk.available.total.ratio", "disk_available_total_ratio"},
    {"replica*eon.replica_stub*disk.available.min.ratio", "disk_available_min_ratio"},
    {"replica*eon.replica_stub*disk.available.max.ratio", "disk_available_max_ratio"},
};

std::string get_brief_stat()
{
    std::vector<std::string> stat_counters;
    for (const auto &kv : s_brief_stat_map) {
        stat_counters.push_back(kv.first);
    }

    std::ostringstream oss;
    oss << std::fixed << std::setprecision(0);
    bool first_item = true;
    dsn::perf_counters::snapshot_iterator iter =
        [&oss, &first_item](const dsn::perf_counter_ptr &counter, double value) mutable {
            if (!first_item)
                oss << ", ";
            oss << s_brief_stat_map.find(counter->full_name())->second << "=" << value;
            first_item = false;
        };
    std::vector<bool> match_result;
    dsn::perf_counters::instance().query_snapshot(stat_counters, iter, &match_result);

    dassert(stat_counters.size() == match_result.size(), "");
    for (int i = 0; i < match_result.size(); ++i) {
        if (!match_result[i]) {
            if (!first_item)
                oss << ", ";
            oss << stat_counters[i] << "=not_found";
            first_item = false;
        }
    }
    return oss.str();
}
}
