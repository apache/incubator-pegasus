// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_manual_compact_service.h"

#include <dsn/utility/string_conv.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/dist/replication/replication.codes.h>
#include <dsn/tool-api/async_calls.h>

#include "base/pegasus_const.h"
#include "pegasus_server_impl.h"

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
      _manual_compact_last_time_used_ms(0)
{
    _manual_compact_min_interval_seconds = (int32_t)dsn_config_get_value_uint64(
        "pegasus.server",
        "manual_compact_min_interval_seconds",
        0,
        "minimal interval time in seconds to start a new manual compaction, "
        "<= 0 means no interval limit");

    _pfc_manual_compact_enqueue_count.init_app_counter("app.pegasus",
                                                       "manual.compact.enqueue.count",
                                                       COUNTER_TYPE_NUMBER,
                                                       "current manual compact in queue count");

    _pfc_manual_compact_running_count.init_app_counter("app.pegasus",
                                                       "manual.compact.running.count",
                                                       COUNTER_TYPE_NUMBER,
                                                       "current manual compact running count");
}

void pegasus_manual_compact_service::init_last_finish_time_ms(uint64_t last_finish_time_ms)
{
    _manual_compact_last_finish_time_ms.store(last_finish_time_ms);
}

void pegasus_manual_compact_service::start_manual_compact_if_needed(
    const std::map<std::string, std::string> &envs)
{
    if (check_compact_disabled(envs)) {
        ddebug_replica("ignored compact because disabled");
        return;
    }

    if (check_compact_max_concurrent_running_count(envs) <= 0) {
        ddebug_replica("ignored compact because max_concurrent_running_count <= 0");
        return;
    }

    std::string compact_rule;
    if (check_once_compact(envs)) {
        compact_rule = MANUAL_COMPACT_ONCE_KEY_PREFIX;
    }

    if (compact_rule.empty() && check_periodic_compact(envs)) {
        compact_rule = MANUAL_COMPACT_PERIODIC_KEY_PREFIX;
    }

    if (compact_rule.empty()) {
        return;
    }

    if (check_manual_compact_state()) {
        rocksdb::CompactRangeOptions options;
        extract_manual_compact_opts(envs, compact_rule, options);

        _pfc_manual_compact_enqueue_count->increment();
        dsn::tasking::enqueue(LPC_MANUAL_COMPACT, &_app->_tracker, [this, options]() {
            _pfc_manual_compact_enqueue_count->decrement();
            manual_compact(options);
        });
    } else {
        ddebug_replica("ignored compact because last one is on going or just finished");
    }
}

bool pegasus_manual_compact_service::check_compact_disabled(
    const std::map<std::string, std::string> &envs)
{
    bool new_disabled = false;
    auto find = envs.find(MANUAL_COMPACT_DISABLED_KEY);
    if (find != envs.end() && find->second == "true") {
        new_disabled = true;
    }

    bool old_disabled = _disabled.load();
    if (new_disabled != old_disabled) {
        // flag changed
        if (new_disabled) {
            ddebug_replica("manual compact is set to disabled now");
            _disabled.store(true);
        } else {
            ddebug_replica("manual compact is set to enabled now");
            _disabled.store(false);
        }
    }

    return new_disabled;
}

int pegasus_manual_compact_service::check_compact_max_concurrent_running_count(
    const std::map<std::string, std::string> &envs)
{
    int new_count = INT_MAX;
    auto find = envs.find(MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT_KEY);
    if (find != envs.end() && !dsn::buf2int32(find->second, new_count)) {
        derror_replica("{}={} is invalid.", find->first, find->second);
    }

    int old_count = _max_concurrent_running_count.load();
    if (new_count != old_count) {
        // count changed
        ddebug_replica("max_concurrent_running_count changed from {} to {}", old_count, new_count);
        _max_concurrent_running_count.store(new_count);
    }

    return new_count;
}

bool pegasus_manual_compact_service::check_once_compact(
    const std::map<std::string, std::string> &envs)
{
    auto find = envs.find(MANUAL_COMPACT_ONCE_TRIGGER_TIME_KEY);
    if (find == envs.end()) {
        return false;
    }

    int64_t trigger_time = 0;
    if (!dsn::buf2int64(find->second, trigger_time) || trigger_time <= 0) {
        derror_replica("{}={} is invalid.", find->first, find->second);
        return false;
    }

    return trigger_time > _manual_compact_last_finish_time_ms.load() / 1000;
}

bool pegasus_manual_compact_service::check_periodic_compact(
    const std::map<std::string, std::string> &envs)
{
    auto find = envs.find(MANUAL_COMPACT_PERIODIC_TRIGGER_TIME_KEY);
    if (find == envs.end()) {
        return false;
    }

    std::list<std::string> trigger_time_strs;
    dsn::utils::split_args(find->second.c_str(), trigger_time_strs, ',');
    if (trigger_time_strs.empty()) {
        derror_replica("{}={} is invalid.", find->first, find->second);
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
        derror_replica("{}={} is invalid.", find->first, find->second);
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
    ddebug_replica("_mock_now_timestamp={}", _mock_now_timestamp);
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
    auto find = envs.find(key_prefix + MANUAL_COMPACT_TARGET_LEVEL_KEY);
    if (find != envs.end()) {
        int32_t target_level;
        if (dsn::buf2int32(find->second, target_level) &&
            (target_level == -1 ||
             (target_level >= 1 && target_level <= _app->_db_opts.num_levels))) {
            options.target_level = target_level;
        } else {
            dwarn_replica("{}={} is invalid, use default value {}",
                          find->first,
                          find->second,
                          options.target_level);
        }
    }

    options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kSkip;
    find = envs.find(key_prefix + MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_KEY);
    if (find != envs.end()) {
        const std::string &argv = find->second;
        if (argv == MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_FORCE) {
            options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
        } else if (argv == MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP) {
            options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kSkip;
        } else {
            dwarn_replica(
                "{}={} is invalid, use default value {}",
                find->first,
                find->second,
                // NOTICE associate with options.bottommost_level_compaction's default value above
                MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP);
        }
    }
}

bool pegasus_manual_compact_service::check_manual_compact_state()
{
    uint64_t not_enqueue = 0;
    uint64_t now = now_timestamp();
    if (_manual_compact_min_interval_seconds <= 0 ||       // no interval limit
        _manual_compact_last_finish_time_ms.load() == 0 || // has not compacted yet
        now - _manual_compact_last_finish_time_ms.load() >
            (uint64_t)_manual_compact_min_interval_seconds * 1000) { // interval past
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
        ddebug_replica("ignored compact because disabled");
        _manual_compact_enqueue_time_ms.store(0);
        return;
    }

    // if current running count exceeds the limit, it would not to be started.
    _pfc_manual_compact_running_count->increment();
    if (_pfc_manual_compact_running_count->get_integer_value() > _max_concurrent_running_count) {
        _pfc_manual_compact_running_count->decrement();
        ddebug_replica("ignored compact because exceed max_concurrent_running_count({})",
                       _max_concurrent_running_count.load());
        _manual_compact_enqueue_time_ms.store(0);
        return;
    }

    uint64_t start = begin_manual_compact();
    uint64_t finish = _app->do_manual_compact(options);
    end_manual_compact(start, finish);

    _pfc_manual_compact_running_count->decrement();
}

uint64_t pegasus_manual_compact_service::begin_manual_compact()
{
    ddebug_replica("start to execute manual compaction");
    uint64_t start = now_timestamp();
    _manual_compact_start_running_time_ms.store(start);
    return start;
}

void pegasus_manual_compact_service::end_manual_compact(uint64_t start, uint64_t finish)
{
    ddebug_replica("finish to execute manual compaction, time_used = {}ms", finish - start);
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
    std::stringstream state;
    if (last_finish_time_ms > 0) {
        char str[24];
        dsn::utils::time_ms_to_string(last_finish_time_ms, str);
        state << "last finish at [" << str << "]";
    } else {
        state << "last finish at [-]";
    }

    if (last_time_used_ms > 0) {
        state << ", last used " << last_time_used_ms << " ms";
    }

    if (enqueue_time_ms > 0) {
        char str[24];
        dsn::utils::time_ms_to_string(enqueue_time_ms, str);
        state << ", recent enqueue at [" << str << "]";
    }

    if (start_time_ms > 0) {
        char str[24];
        dsn::utils::time_ms_to_string(start_time_ms, str);
        state << ", recent start at [" << str << "]";
    }
    return state.str();
}

} // namespace server
} // namespace pegasus
