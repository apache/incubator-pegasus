// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <pegasus/client.h>
#include <dsn/dist/replication/replication_ddl_client.h>
#include <dsn/perf_counter/perf_counter_wrapper.h>

namespace pegasus {
namespace server {

using ::dsn::replication::replication_ddl_client;

DEFINE_TASK_CODE(LPC_DETECT_AVAILABLE, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

class available_detector
{
public:
    available_detector();
    virtual ~available_detector();

    void start();
    void stop();

private:
    void set_detect_result(const std::string &hash_key,
                           const std::string &sort_key,
                           const std::string &value,
                           int try_count);
    // generate hash_keys that can access every partition.
    bool generate_hash_keys();
    void on_detect(int32_t idx);
    void check_and_send_email(std::atomic<int> *cnt, int32_t idx);
    void detect_available();
    void report_availability_info();

    void on_day_report();
    void on_hour_report();
    void on_minute_report();

private:
    std::string _cluster_name;
    std::string _app_name;
    // client to access server.
    pegasus_client *_client;
    std::shared_ptr<replication_ddl_client> _ddl_client;
    std::vector<dsn::rpc_address> _meta_list;
    uint32_t _detect_interval_seconds;
    int32_t _alert_fail_count;
    ::dsn::utils::ex_lock_nr _alert_lock;
    // for record partition fail times.
    std::vector<std::shared_ptr<std::atomic<int32_t>>> _fail_count;
    // for detect.
    std::vector<std::string> _hash_keys;
    ::dsn::task_ptr _detect_timer;
    std::vector<::dsn::task_ptr> _detect_tasks;
    int32_t _app_id;
    int32_t _partition_count;
    std::vector<::dsn::partition_configuration> partitions;
    uint32_t _detect_timeout;

    std::string _send_alert_email_cmd;
    std::string _send_availability_info_email_cmd;
    std::string _alert_script_dir;
    std::string _alert_email_address;

    // total detect times and total fail times
    std::atomic<int64_t> _recent_day_detect_times;
    std::atomic<int64_t> _recent_day_fail_times;
    std::atomic<int64_t> _recent_hour_detect_times;
    std::atomic<int64_t> _recent_hour_fail_times;
    std::atomic<int64_t> _recent_minute_detect_times;
    std::atomic<int64_t> _recent_minute_fail_times;
    ::dsn::task_ptr _report_task;
    std::string _old_day;
    std::string _old_hour;
    std::string _old_minute;
    ::dsn::perf_counter_wrapper _pfc_detect_times_day;
    ::dsn::perf_counter_wrapper _pfc_fail_times_day;
    ::dsn::perf_counter_wrapper _pfc_available_day;
    ::dsn::perf_counter_wrapper _pfc_detect_times_hour;
    ::dsn::perf_counter_wrapper _pfc_fail_times_hour;
    ::dsn::perf_counter_wrapper _pfc_available_hour;
    ::dsn::perf_counter_wrapper _pfc_detect_times_minute;
    ::dsn::perf_counter_wrapper _pfc_fail_times_minute;
    ::dsn::perf_counter_wrapper _pfc_available_minute;
};
}
}
