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

#pragma once

#include <stdint.h>
#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "perf_counter/perf_counter_wrapper.h"
#include "rpc/rpc_host_port.h"
#include "task/task.h"
#include "task/task_tracker.h"
#include "utils/synchronize.h"

namespace dsn {
namespace replication {
class replication_ddl_client;
} // namespace replication
} // namespace dsn

namespace pegasus {
class pegasus_client;

namespace server {

using ::dsn::replication::replication_ddl_client;

class result_writer;

class available_detector
{
public:
    available_detector();
    ~available_detector();

    void start();
    void stop();

private:
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
    dsn::task_tracker _tracker;
    std::string _cluster_name;
    // for writing detect result
    std::unique_ptr<result_writer> _result_writer;
    // client to access server.
    pegasus_client *_client;
    std::shared_ptr<replication_ddl_client> _ddl_client;
    std::vector<dsn::host_port> _meta_list;
    ::dsn::utils::ex_lock_nr _alert_lock;
    // for record partition fail times.
    std::vector<std::shared_ptr<std::atomic<int32_t>>> _fail_count;
    // for detect.
    std::vector<std::string> _hash_keys;
    ::dsn::task_ptr _detect_timer;
    std::vector<::dsn::task_ptr> _detect_tasks;
    int32_t _app_id;
    int32_t _partition_count;

    std::string _send_alert_email_cmd;
    std::string _send_availability_info_email_cmd;

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
} // namespace server
} // namespace pegasus
