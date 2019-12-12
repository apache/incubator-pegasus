// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "info_collector.h"

#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <vector>
#include <chrono>
#include <functional>
#include <dsn/tool-api/group_address.h>

#include "base/pegasus_utils.h"
#include "base/pegasus_const.h"
#include "result_writer.h"

#define METRICSNUM 3

using namespace ::dsn;
using namespace ::dsn::replication;

namespace pegasus {
namespace server {

DEFINE_TASK_CODE(LPC_PEGASUS_APP_STAT_TIMER, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE(LPC_PEGASUS_CAPACITY_UNIT_STAT_TIMER,
                 TASK_PRIORITY_COMMON,
                 ::dsn::THREAD_POOL_DEFAULT)
DEFINE_TASK_CODE(LPC_PEGASUS_STORAGE_SIZE_STAT_TIMER,
                 TASK_PRIORITY_COMMON,
                 ::dsn::THREAD_POOL_DEFAULT)

info_collector::info_collector()
{
    std::vector<::dsn::rpc_address> meta_servers;
    replica_helper::load_meta_servers(meta_servers);

    _meta_servers.assign_group("meta-servers");
    for (auto &ms : meta_servers) {
        _meta_servers.group_address()->add(ms);
    }

    _cluster_name = dsn_config_get_value_string("pegasus.collector", "cluster", "", "cluster name");
    dassert(!_cluster_name.empty(), "");

    _shell_context.current_cluster_name = _cluster_name;
    _shell_context.meta_list = meta_servers;
    _shell_context.ddl_client.reset(new replication_ddl_client(meta_servers));

    _app_stat_interval_seconds = (uint32_t)dsn_config_get_value_uint64("pegasus.collector",
                                                                       "app_stat_interval_seconds",
                                                                       10, // default value 10s
                                                                       "app stat interval seconds");

    _usage_stat_app = dsn_config_get_value_string(
        "pegasus.collector", "usage_stat_app", "", "app for recording usage statistics");
    dassert(!_usage_stat_app.empty(), "");
    // initialize the _client.
    if (!pegasus_client_factory::initialize(nullptr)) {
        dassert(false, "Initialize the pegasus client failed");
    }
    _client = pegasus_client_factory::get_client(_cluster_name.c_str(), _usage_stat_app.c_str());
    dassert(_client != nullptr, "Initialize the client failed");
    _result_writer = dsn::make_unique<result_writer>(_client);

    _capacity_unit_fetch_interval_seconds =
        (uint32_t)dsn_config_get_value_uint64("pegasus.collector",
                                              "capacity_unit_fetch_interval_seconds",
                                              8, // default value 8s
                                              "capacity unit fetch interval seconds");
    // _capacity_unit_retry_wait_seconds is in range of [1, 10]
    _capacity_unit_retry_wait_seconds =
        std::min(10u, std::max(1u, _capacity_unit_fetch_interval_seconds / 10));
    // _capacity_unit_retry_max_count is in range of [0, 3]
    _capacity_unit_retry_max_count =
        std::min(3u, _capacity_unit_fetch_interval_seconds / _capacity_unit_retry_wait_seconds);

    _storage_size_fetch_interval_seconds =
        (uint32_t)dsn_config_get_value_uint64("pegasus.collector",
                                              "storage_size_fetch_interval_seconds",
                                              3600, // default value 1h
                                              "storage size fetch interval seconds");
    // _storage_size_retry_wait_seconds is in range of [1, 60]
    _storage_size_retry_wait_seconds =
        std::min(60u, std::max(1u, _storage_size_fetch_interval_seconds / 10));
    // _storage_size_retry_max_count is in range of [0, 3]
    _storage_size_retry_max_count =
        std::min(3u, _storage_size_fetch_interval_seconds / _storage_size_retry_wait_seconds);
}

info_collector::~info_collector()
{
    stop();
    for (auto kv : _app_stat_counters) {
        delete kv.second;
    }
    for (auto store: _calculator_store) {
        delete store.second;
    }
}

void info_collector::start()
{
    _app_stat_timer_task =
        ::dsn::tasking::enqueue_timer(LPC_PEGASUS_APP_STAT_TIMER,
                                      &_tracker,
                                      [this] { on_app_stat(); },
                                      std::chrono::seconds(_app_stat_interval_seconds),
                                      0,
                                      std::chrono::minutes(1));

    _capacity_unit_stat_timer_task = ::dsn::tasking::enqueue_timer(
        LPC_PEGASUS_CAPACITY_UNIT_STAT_TIMER,
        &_tracker,
        [this] { on_capacity_unit_stat(_capacity_unit_retry_max_count); },
        std::chrono::seconds(_capacity_unit_fetch_interval_seconds),
        0,
        std::chrono::minutes(1));

    _storage_size_stat_timer_task = ::dsn::tasking::enqueue_timer(
        LPC_PEGASUS_STORAGE_SIZE_STAT_TIMER,
        &_tracker,
        [this] { on_storage_size_stat(_storage_size_retry_max_count); },
        std::chrono::seconds(_storage_size_fetch_interval_seconds),
        0,
        std::chrono::minutes(1));
}

void info_collector::stop() { _tracker.cancel_outstanding_tasks(); }

void info_collector::on_app_stat()
{
    ddebug("start to stat apps");
    std::map<std::string, std::vector<row_data>> all_rows;
    if (!get_app_partition_stat(&_shell_context, all_rows)) {
        derror("call get_app_stat() failed");
        return;
    }
    for (auto app_rows : all_rows) {
        Hotpot_calculator *app_store = nullptr;
        get_store_handler(app_rows.first,app_rows.second.size(),app_store);
        app_store->aggregate(&app_rows.second);
    }
}

info_collector::AppStatCounters *info_collector::get_app_counters(const std::string &app_name)
{
    ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_app_stat_counter_lock);
    auto find = _app_stat_counters.find(app_name);
    if (find != _app_stat_counters.end()) {
        return find->second;
    }
    AppStatCounters *counters = new AppStatCounters();

    char counter_name[1024];
    char counter_desc[1024];
#define INIT_COUNTER(name)                                                                         \
    do {                                                                                           \
        sprintf(counter_name, "app.stat." #name "#%s", app_name.c_str());                          \
        sprintf(counter_desc, "statistic the " #name " of app %s", app_name.c_str());              \
        counters->name.init_app_counter(                                                           \
            "app.pegasus", counter_name, COUNTER_TYPE_NUMBER, counter_desc);                       \
    } while (0)

    INIT_COUNTER(get_qps);
    INIT_COUNTER(multi_get_qps);
    INIT_COUNTER(put_qps);
    INIT_COUNTER(multi_put_qps);
    INIT_COUNTER(remove_qps);
    INIT_COUNTER(multi_remove_qps);
    INIT_COUNTER(incr_qps);
    INIT_COUNTER(check_and_set_qps);
    INIT_COUNTER(check_and_mutate_qps);
    INIT_COUNTER(scan_qps);
    INIT_COUNTER(recent_read_cu);
    INIT_COUNTER(recent_write_cu);
    INIT_COUNTER(recent_expire_count);
    INIT_COUNTER(recent_filter_count);
    INIT_COUNTER(recent_abnormal_count);
    INIT_COUNTER(recent_write_throttling_delay_count);
    INIT_COUNTER(recent_write_throttling_reject_count);
    INIT_COUNTER(storage_mb);
    INIT_COUNTER(storage_count);
    INIT_COUNTER(rdb_block_cache_hit_rate);
    INIT_COUNTER(rdb_index_and_filter_blocks_mem_usage);
    INIT_COUNTER(rdb_memtable_mem_usage);
    INIT_COUNTER(read_qps);
    INIT_COUNTER(write_qps);
    INIT_COUNTER(qps_max_min_scale);
    INIT_COUNTER(cu_max_min_scale);
    _app_stat_counters[app_name] = counters;
    return counters;
}

void info_collector::on_capacity_unit_stat(int remaining_retry_count)
{
    ddebug("start to stat capacity unit, remaining_retry_count = %d", remaining_retry_count);
    std::vector<node_capacity_unit_stat> nodes_stat;
    if (!get_capacity_unit_stat(&_shell_context, nodes_stat)) {
        if (remaining_retry_count > 0) {
            dwarn("get capacity unit stat failed, remaining_retry_count = %d, "
                  "wait %u seconds to retry",
                  remaining_retry_count,
                  _capacity_unit_retry_wait_seconds);
            ::dsn::tasking::enqueue(LPC_PEGASUS_CAPACITY_UNIT_STAT_TIMER,
                                    &_tracker,
                                    [=] { on_capacity_unit_stat(remaining_retry_count - 1); },
                                    0,
                                    std::chrono::seconds(_capacity_unit_retry_wait_seconds));
        } else {
            derror("get capacity unit stat failed, remaining_retry_count = 0, no retry anymore");
        }
        return;
    }
    for (node_capacity_unit_stat &elem : nodes_stat) {
        if (elem.node_address.empty() || elem.timestamp.empty() ||
            !has_capacity_unit_updated(elem.node_address, elem.timestamp)) {
            dinfo("recent read/write capacity unit value of node %s has not updated",
                  elem.node_address.c_str());
            continue;
        }
        _result_writer->set_result(elem.timestamp, "cu@" + elem.node_address, elem.dump_to_json());
    }
}

bool info_collector::has_capacity_unit_updated(const std::string &node_address,
                                               const std::string &timestamp)
{
    ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_capacity_unit_update_info_lock);
    auto find = _capacity_unit_update_info.find(node_address);
    if (find == _capacity_unit_update_info.end()) {
        _capacity_unit_update_info[node_address] = timestamp;
        return true;
    }
    if (timestamp > find->second) {
        find->second = timestamp;
        return true;
    }
    return false;
}

void info_collector::on_storage_size_stat(int remaining_retry_count)
{
    ddebug("start to stat storage size, remaining_retry_count = %d", remaining_retry_count);
    app_storage_size_stat st_stat;
    if (!get_storage_size_stat(&_shell_context, st_stat)) {
        if (remaining_retry_count > 0) {
            dwarn("get storage size stat failed, remaining_retry_count = %d, "
                  "wait %u seconds to retry",
                  remaining_retry_count,
                  _storage_size_retry_wait_seconds);
            ::dsn::tasking::enqueue(LPC_PEGASUS_STORAGE_SIZE_STAT_TIMER,
                                    &_tracker,
                                    [=] { on_storage_size_stat(remaining_retry_count - 1); },
                                    0,
                                    std::chrono::seconds(_storage_size_retry_wait_seconds));
        } else {
            derror("get storage size stat failed, remaining_retry_count = 0, no retry anymore");
        }
        return;
    }
    _result_writer->set_result(st_stat.timestamp, "ss", st_stat.dump_to_json());
}

} // namespace server
} // namespace pegasus
