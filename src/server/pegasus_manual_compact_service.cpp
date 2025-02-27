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

#include "pegasus_manual_compact_service.h"

#include <string_view>
#include <limits.h>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <rocksdb/options.h>
#include <list>
#include <set>
#include <utility>

#include "common/replica_envs.h"
#include "common/replication.codes.h"
#include "pegasus_server_impl.h"
#include "runtime/api_layer1.h"
#include "task/async_calls.h"
#include "task/task_code.h"
#include "utils/autoref_ptr.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/string_conv.h"
#include "utils/strings.h"
#include "utils/time_utils.h"

METRIC_DEFINE_gauge_int64(replica,
                          rdb_manual_compact_queued_tasks,
                          dsn::metric_unit::kTasks,
                          "The number of current queued tasks of rocksdb manual compaction");

METRIC_DEFINE_gauge_int64(replica,
                          rdb_manual_compact_running_tasks,
                          dsn::metric_unit::kTasks,
                          "The number of current running tasks of rocksdb manual compaction");

DSN_DEFINE_int32(pegasus.server,
                 manual_compact_min_interval_seconds,
                 0,
                 "minimal interval time in seconds to start a new manual compaction, <= 0 "
                 "means no interval limit");
namespace pegasus {
namespace server {

DEFINE_TASK_CODE(LPC_MANUAL_COMPACT, TASK_PRIORITY_COMMON, THREAD_POOL_COMPACT)

pegasus_manual_compact_service::pegasus_manual_compact_service(pegasus_server_impl *app)
    : replica_base(*app),
      _app(app),
      _disabled(false),
      _max_concurrent_running_count(INT_MAX),
      _manual_compact_enqueue_time_ms(0),
      _manual_compact_start_running_time_ms(0),
      _manual_compact_last_finish_time_ms(0),
      _manual_compact_last_time_used_ms(0),
      METRIC_VAR_INIT_replica(rdb_manual_compact_queued_tasks),
      METRIC_VAR_INIT_replica(rdb_manual_compact_running_tasks)
{
}

void pegasus_manual_compact_service::init_last_finish_time_ms(uint64_t last_finish_time_ms)
{
    _manual_compact_last_finish_time_ms.store(last_finish_time_ms);
}

void pegasus_manual_compact_service::start_manual_compact_if_needed(
    const std::map<std::string, std::string> &envs)
{
    if (check_compact_disabled(envs)) {
        LOG_INFO_PREFIX("ignored compact because disabled");
        return;
    }

    if (check_compact_max_concurrent_running_count(envs) <= 0) {
        LOG_INFO_PREFIX("ignored compact because max_concurrent_running_count <= 0");
        return;
    }

    std::string compact_rule;
    if (check_once_compact(envs)) {
        compact_rule = dsn::replica_envs::MANUAL_COMPACT_ONCE_PREFIX;
    }

    if (compact_rule.empty() && check_periodic_compact(envs)) {
        compact_rule = dsn::replica_envs::MANUAL_COMPACT_PERIODIC_PREFIX;
    }

    if (compact_rule.empty()) {
        return;
    }

    if (check_manual_compact_state()) {
        rocksdb::CompactRangeOptions options;
        extract_manual_compact_opts(envs, compact_rule, options);

        METRIC_VAR_INCREMENT(rdb_manual_compact_queued_tasks);
        dsn::tasking::enqueue(LPC_MANUAL_COMPACT, &_app->_tracker, [this, options]() {
            METRIC_VAR_DECREMENT(rdb_manual_compact_queued_tasks);
            manual_compact(options);
        });
    } else {
        LOG_INFO_PREFIX("ignored compact because last one is on going or just finished");
    }
}

bool pegasus_manual_compact_service::check_compact_disabled(
    const std::map<std::string, std::string> &envs)
{
    bool new_disabled = false;
    auto find = envs.find(dsn::replica_envs::MANUAL_COMPACT_DISABLED);
    if (find != envs.end() && find->second == "true") {
        new_disabled = true;
    }

    bool old_disabled = _disabled.load();
    if (new_disabled != old_disabled) {
        // flag changed
        if (new_disabled) {
            LOG_INFO_PREFIX("manual compact is set to disabled now");
            _disabled.store(true);
        } else {
            LOG_INFO_PREFIX("manual compact is set to enabled now");
            _disabled.store(false);
        }
    }

    return new_disabled;
}

int pegasus_manual_compact_service::check_compact_max_concurrent_running_count(
    const std::map<std::string, std::string> &envs)
{
    int new_count = INT_MAX;
    auto find = envs.find(dsn::replica_envs::MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT);
    if (find != envs.end() && !dsn::buf2int32(find->second, new_count)) {
        LOG_ERROR_PREFIX("{}={} is invalid.", find->first, find->second);
    }

    int old_count = _max_concurrent_running_count.load();
    if (new_count != old_count) {
        // count changed
        LOG_INFO_PREFIX("max_concurrent_running_count changed from {} to {}", old_count, new_count);
        _max_concurrent_running_count.store(new_count);
    }

    return new_count;
}

bool pegasus_manual_compact_service::check_once_compact(
    const std::map<std::string, std::string> &envs)
{
    auto find = envs.find(dsn::replica_envs::MANUAL_COMPACT_ONCE_TRIGGER_TIME);
    if (find == envs.end()) {
        return false;
    }

    int64_t trigger_time = 0;
    if (!dsn::buf2int64(find->second, trigger_time) || trigger_time <= 0) {
        LOG_ERROR_PREFIX("{}={} is invalid.", find->first, find->second);
        return false;
    }

    return trigger_time > _manual_compact_last_finish_time_ms.load() / 1000;
}

bool pegasus_manual_compact_service::check_periodic_compact(
    const std::map<std::string, std::string> &envs)
{
    auto find = envs.find(dsn::replica_envs::MANUAL_COMPACT_PERIODIC_TRIGGER_TIME);
    if (find == envs.end()) {
        return false;
    }

    std::list<std::string> trigger_time_strs;
    dsn::utils::split_args(find->second.c_str(), trigger_time_strs, ',');
    if (trigger_time_strs.empty()) {
        LOG_ERROR_PREFIX("{}={} is invalid.", find->first, find->second);
        return false;
    }

    std::set<int64_t> trigger_time;
    for (auto &tts : trigger_time_strs) {
        int64_t tt = dsn::utils::hh_mm_today_to_unix_sec(tts);
        if (tt != -1) {
            trigger_time.emplace(tt);
        }
    }
    if (trigger_time.empty()) {
        LOG_ERROR_PREFIX("{}={} is invalid.", find->first, find->second);
        return false;
    }

    auto now = static_cast<int64_t>(now_timestamp());
    for (auto t : trigger_time) {
        auto t_ms = t * 1000;
        if (_manual_compact_last_finish_time_ms.load() < t_ms && t_ms < now) {
            return true;
        }
    }

    return false;
}

uint64_t pegasus_manual_compact_service::now_timestamp()
{
#ifdef PEGASUS_UNIT_TEST
    LOG_INFO_PREFIX("_mock_now_timestamp={}", _mock_now_timestamp);
    return _mock_now_timestamp == 0 ? dsn_now_ms() : _mock_now_timestamp;
#else
    return dsn_now_ms();
#endif
}

void pegasus_manual_compact_service::extract_manual_compact_opts(
    const std::map<std::string, std::string> &envs,
    const std::string &key_prefix,
    rocksdb::CompactRangeOptions &options)
{
    options.exclusive_manual_compaction = true;
    options.change_level = true;
    options.target_level = -1;
    auto find = envs.find(key_prefix + dsn::replica_envs::MANUAL_COMPACT_TARGET_LEVEL);
    if (find != envs.end()) {
        int32_t target_level;
        if (dsn::buf2int32(find->second, target_level) &&
            (target_level == -1 ||
             (target_level >= 1 && target_level <= _app->_data_cf_opts.num_levels))) {
            options.target_level = target_level;
        } else {
            LOG_WARNING_PREFIX("{}={} is invalid, use default value {}",
                               find->first,
                               find->second,
                               options.target_level);
        }
    }

    options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kSkip;
    find = envs.find(key_prefix + dsn::replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION);
    if (find != envs.end()) {
        const std::string &argv = find->second;
        if (argv == dsn::replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_FORCE) {
            options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
        } else if (argv == dsn::replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP) {
            options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kSkip;
        } else {
            LOG_WARNING_PREFIX(
                "{}={} is invalid, use default value {}",
                find->first,
                find->second,
                // NOTICE associate with options.bottommost_level_compaction's default value above
                dsn::replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP);
        }
    }
}

bool pegasus_manual_compact_service::check_manual_compact_state()
{
    uint64_t not_enqueue = 0;
    uint64_t now = now_timestamp();
    if (FLAGS_manual_compact_min_interval_seconds <= 0 ||  // no interval limit
        _manual_compact_last_finish_time_ms.load() == 0 || // has not compacted yet
        now - _manual_compact_last_finish_time_ms.load() >
            (uint64_t)FLAGS_manual_compact_min_interval_seconds * 1000) { // interval past
        // when _manual_compact_enqueue_time_ms is `not_enqueue`(which is 0), return true to allow a
        // compact task enqueue, and update the value to `now`,
        // otherwise, return false to not allow, and keep the old value.
        return _manual_compact_enqueue_time_ms.compare_exchange_strong(not_enqueue,
                                                                       now); // not enqueue
    } else {
        return false;
    }
}

void pegasus_manual_compact_service::manual_compact(const rocksdb::CompactRangeOptions &options)
{
    // if we find manual compaction is disabled when transfer from queue to running,
    // it would not to be started.
    if (_disabled.load()) {
        LOG_INFO_PREFIX("ignored compact because disabled");
        _manual_compact_enqueue_time_ms.store(0);
        return;
    }

    // if current running count exceeds the limit, it would not to be started.
    METRIC_VAR_AUTO_COUNT(rdb_manual_compact_running_tasks);
    if (METRIC_VAR_VALUE(rdb_manual_compact_running_tasks) > _max_concurrent_running_count) {
        LOG_INFO_PREFIX("ignored compact because exceed max_concurrent_running_count({})",
                        _max_concurrent_running_count.load());
        _manual_compact_enqueue_time_ms.store(0);
        return;
    }

    uint64_t start = begin_manual_compact();
    uint64_t finish = _app->do_manual_compact(options);
    end_manual_compact(start, finish);
}

uint64_t pegasus_manual_compact_service::begin_manual_compact()
{
    LOG_INFO_PREFIX("start to execute manual compaction");
    uint64_t start = now_timestamp();
    _manual_compact_start_running_time_ms.store(start);
    return start;
}

void pegasus_manual_compact_service::end_manual_compact(uint64_t start, uint64_t finish)
{
    LOG_INFO_PREFIX("finish to execute manual compaction, time_used = {}ms", finish - start);
    _manual_compact_last_finish_time_ms.store(finish);
    _manual_compact_last_time_used_ms.store(finish - start);
    _manual_compact_enqueue_time_ms.store(0);
    _manual_compact_start_running_time_ms.store(0);
}

std::string pegasus_manual_compact_service::query_compact_state() const
{
    uint64_t enqueue_time_ms = _manual_compact_enqueue_time_ms.load();
    uint64_t start_time_ms = _manual_compact_start_running_time_ms.load();
    uint64_t last_finish_time_ms = _manual_compact_last_finish_time_ms.load();
    uint64_t last_time_used_ms = _manual_compact_last_time_used_ms.load();

    nlohmann::json info;
    info["recent_enqueue_at"] =
        enqueue_time_ms > 0 ? dsn::utils::time_s_to_date_time(enqueue_time_ms / 1000) : "-";
    info["recent_start_at"] =
        start_time_ms > 0 ? dsn::utils::time_s_to_date_time(start_time_ms / 1000) : "-";
    info["last_finish"] =
        last_finish_time_ms > 0 ? dsn::utils::time_s_to_date_time(last_finish_time_ms / 1000) : "-";
    info["last_used_ms"] = last_time_used_ms > 0 ? std::to_string(last_time_used_ms) : "-";
    return info.dump();
}

dsn::replication::manual_compaction_status::type
pegasus_manual_compact_service::query_compact_status() const
{
    // Case1. last finish at [-]
    // - partition is not running manual compaction
    // Case2. last finish at [timestamp], last used {time_used} ms
    // - partition manual compaction finished
    // Case3. last finish at [-], recent enqueue at [timestamp]
    // - partition is in manual compaction queue
    // Case4. last finish at [-], recent enqueue at [timestamp], recent start at [timestamp]
    // - partition is running manual compaction
    uint64_t enqueue_time_ms = _manual_compact_enqueue_time_ms.load();
    uint64_t start_time_ms = _manual_compact_start_running_time_ms.load();
    uint64_t last_finish_time_ms = _manual_compact_last_finish_time_ms.load();
    uint64_t last_time_used_ms = _manual_compact_last_time_used_ms.load();

    if (start_time_ms > 0) {
        return dsn::replication::manual_compaction_status::RUNNING;
    } else if (enqueue_time_ms > 0) {
        return dsn::replication::manual_compaction_status::QUEUING;
    } else if (last_time_used_ms > 0 && last_finish_time_ms > 0) {
        return dsn::replication::manual_compaction_status::FINISHED;
    } else {
        return dsn::replication::manual_compaction_status::IDLE;
    }
}

} // namespace server
} // namespace pegasus
