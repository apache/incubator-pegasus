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

#define METRICSNUM 3

using namespace ::dsn;
using namespace ::dsn::replication;

namespace pegasus {
namespace server {

DEFINE_TASK_CODE(LPC_PEGASUS_APP_STAT_TIMER, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

info_collector::info_collector()
{
    std::vector<::dsn::rpc_address> meta_servers;
    replica_helper::load_meta_servers(meta_servers);

    _meta_servers.assign_group("meta-servers");
    for (auto &ms : meta_servers) {
        _meta_servers.group_address()->add(ms);
    }

    _cluster_name = dsn_config_get_value_string("pegasus.collector", "cluster", "", "cluster name");
    dassert(_cluster_name.size() > 0, "");

    _shell_context.current_cluster_name = _cluster_name;
    _shell_context.meta_list = meta_servers;
    _shell_context.ddl_client.reset(new replication_ddl_client(meta_servers));

    _app_stat_interval_seconds = (uint32_t)dsn_config_get_value_uint64("pegasus.collector",
                                                                       "app_stat_interval_seconds",
                                                                       10, // default value 10s
                                                                       "app stat interval seconds");
}

info_collector::~info_collector()
{
    _tracker.cancel_outstanding_tasks();
    for (auto kv : _app_stat_counters) {
        delete kv.second;
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
}

void info_collector::stop() { _app_stat_timer_task->cancel(true); }

void info_collector::on_app_stat()
{
    ddebug("start to stat apps");
    std::vector<row_data> rows;
    if (get_app_stat(&_shell_context, "", rows)) {
        std::vector<double> read_qps;
        std::vector<double> write_qps;
        rows.resize(rows.size() + 1);
        read_qps.resize(rows.size());
        write_qps.resize(rows.size());
        row_data &all = rows.back();
        all.row_name = "_all_";
        for (int i = 0; i < rows.size() - 1; ++i) {
            row_data &row = rows[i];
            all.get_qps += row.get_qps;
            all.multi_get_qps += row.multi_get_qps;
            all.put_qps += row.put_qps;
            all.multi_put_qps += row.multi_put_qps;
            all.remove_qps += row.remove_qps;
            all.multi_remove_qps += row.multi_remove_qps;
            all.incr_qps += row.incr_qps;
            all.check_and_set_qps += row.check_and_set_qps;
            all.check_and_mutate_qps += row.check_and_mutate_qps;
            all.scan_qps += row.scan_qps;
            all.recent_expire_count += row.recent_expire_count;
            all.recent_filter_count += row.recent_filter_count;
            all.recent_abnormal_count += row.recent_abnormal_count;
            all.recent_write_throttling_delay_count += row.recent_write_throttling_delay_count;
            all.recent_write_throttling_reject_count += row.recent_write_throttling_reject_count;
            all.storage_mb += row.storage_mb;
            all.storage_count += row.storage_count;
            all.rdb_block_cache_hit_count += row.rdb_block_cache_hit_count;
            all.rdb_block_cache_total_count += row.rdb_block_cache_total_count;
            all.rdb_index_and_filter_blocks_mem_usage += row.rdb_index_and_filter_blocks_mem_usage;
            all.rdb_memtable_mem_usage += row.rdb_memtable_mem_usage;
            read_qps[i] = row.get_qps + row.multi_get_qps + row.scan_qps;
            write_qps[i] = row.put_qps + row.multi_put_qps + row.remove_qps + row.multi_remove_qps +
                           row.incr_qps + row.check_and_set_qps + row.check_and_mutate_qps;
        }
        read_qps[read_qps.size() - 1] = all.get_qps + all.multi_get_qps + all.scan_qps;
        write_qps[read_qps.size() - 1] = all.put_qps + all.multi_put_qps + all.remove_qps +
                                         all.multi_remove_qps + all.incr_qps +
                                         all.check_and_set_qps + all.check_and_mutate_qps;
        for (int i = 0; i < rows.size(); ++i) {
            row_data &row = rows[i];
            AppStatCounters *counters = get_app_counters(row.row_name);
            counters->get_qps->set(row.get_qps);
            counters->multi_get_qps->set(row.multi_get_qps);
            counters->put_qps->set(row.put_qps);
            counters->multi_put_qps->set(row.multi_put_qps);
            counters->remove_qps->set(row.remove_qps);
            counters->multi_remove_qps->set(row.multi_remove_qps);
            counters->incr_qps->set(row.incr_qps);
            counters->check_and_set_qps->set(row.check_and_set_qps);
            counters->check_and_mutate_qps->set(row.check_and_mutate_qps);
            counters->scan_qps->set(row.scan_qps);
            counters->recent_expire_count->set(row.recent_expire_count);
            counters->recent_filter_count->set(row.recent_filter_count);
            counters->recent_abnormal_count->set(row.recent_abnormal_count);
            counters->recent_write_throttling_delay_count->set(
                row.recent_write_throttling_delay_count);
            counters->recent_write_throttling_reject_count->set(
                row.recent_write_throttling_reject_count);
            counters->storage_mb->set(row.storage_mb);
            counters->storage_count->set(row.storage_count);
            counters->rdb_block_cache_hit_rate->set(
                std::abs(row.rdb_block_cache_total_count) < 1e-6
                    ? 0
                    : row.rdb_block_cache_hit_count / row.rdb_block_cache_total_count * 1000000);
            counters->rdb_index_and_filter_blocks_mem_usage->set(
                row.rdb_index_and_filter_blocks_mem_usage);
            counters->rdb_memtable_mem_usage->set(row.rdb_memtable_mem_usage);
            counters->read_qps->set(read_qps[i]);
            counters->write_qps->set(write_qps[i]);
        }
        ddebug("stat apps succeed, app_count = %d, total_read_qps = %.2f, total_write_qps = %.2f",
               (int)(rows.size() - 1),
               read_qps[read_qps.size() - 1],
               write_qps[read_qps.size() - 1]);
    } else {
        derror("call get_app_stat() failed");
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
    _app_stat_counters[app_name] = counters;
    return counters;
}
} // namespace server
} // namespace pegasus
