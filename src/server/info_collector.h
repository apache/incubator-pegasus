// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/cpp/clientlet.h>
#include <dsn/dist/replication.h>
#include <dsn/dist/replication/replication_other_types.h>
#include <dsn/cpp/perf_counter_.h>

#include <stdlib.h>
#include <signal.h>
#include <unistd.h>
#include <evhttp.h>
#include <event2/event.h>
#include <event2/http.h>
#include <event2/bufferevent.h>
#include <fstream>

#include "../shell/commands.h"

namespace pegasus {
namespace server {

class info_collector : public virtual ::dsn::clientlet
{
public:
    struct AppStatCounters
    {
        ::dsn::perf_counter_ get_qps;
        ::dsn::perf_counter_ multi_get_qps;
        ::dsn::perf_counter_ put_qps;
        ::dsn::perf_counter_ multi_put_qps;
        ::dsn::perf_counter_ remove_qps;
        ::dsn::perf_counter_ multi_remove_qps;
        ::dsn::perf_counter_ scan_qps;
        ::dsn::perf_counter_ recent_expire_count;
        ::dsn::perf_counter_ storage_mb;
        ::dsn::perf_counter_ storage_count;
        ::dsn::perf_counter_ read_qps;
        ::dsn::perf_counter_ write_qps;
    };

    info_collector();
    virtual ~info_collector();

    void start();
    void stop();

    void on_app_stat();
    AppStatCounters *get_app_counters(const std::string &app_name);

private:
    ::dsn::rpc_address _meta_servers;
    std::string _cluster_name;

    shell_context _shell_context;
    uint32_t _app_stat_interval_seconds;
    ::dsn::task_ptr _app_stat_timer_task;
    ::dsn::utils::ex_lock_nr _app_stat_counter_lock;
    std::map<std::string, AppStatCounters *> _app_stat_counters;
};
}
} // namespace
