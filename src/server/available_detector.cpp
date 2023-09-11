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

#include "available_detector.h"

#include <fmt/core.h>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <pegasus/error.h>
#include <stdlib.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <sstream>
#include <type_traits>
#include <utility>

#include <fmt/std.h> // IWYU pragma: keep

#include "base/pegasus_key_schema.h"
#include "client/replication_ddl_client.h"
#include "common/common.h"
#include "common/replication_other_types.h"
#include "pegasus/client.h"
#include "perf_counter/perf_counter.h"
#include "result_writer.h"
#include "runtime/api_layer1.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task_code.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/strings.h"
#include "utils/threadpool_code.h"
#include "utils/time_utils.h"

namespace pegasus {
namespace server {

DEFINE_TASK_CODE(LPC_DETECT_AVAILABLE, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

DSN_DEFINE_int32(pegasus.collector,
                 available_detect_alert_fail_count,
                 30,
                 "available detect alert fail count");
DSN_DEFINE_uint32(pegasus.collector,
                  available_detect_interval_seconds,
                  3,
                  "detect interval seconds");
DSN_DEFINE_uint32(pegasus.collector,
                  available_detect_timeout,
                  1000,
                  "available detect timeout in millisecond");
DSN_DEFINE_string(pegasus.collector, available_detect_app, "", "available detector app name");
DSN_DEFINE_validator(available_detect_app,
                     [](const char *value) -> bool { return !dsn::utils::is_empty(value); });
DSN_DEFINE_string(pegasus.collector,
                  available_detect_alert_script_dir,
                  ".",
                  "available detect alert script dir");
DSN_DEFINE_validator(available_detect_alert_script_dir,
                     [](const char *value) -> bool { return !dsn::utils::is_empty(value); });
DSN_DEFINE_string(pegasus.collector,
                  available_detect_alert_email_address,
                  "",
                  "available detect alert email address, empty means not send email");

available_detector::available_detector()
    : _client(nullptr),
      _ddl_client(nullptr),
      _app_id(-1),
      _partition_count(0),
      _recent_day_detect_times(0),
      _recent_day_fail_times(0),
      _recent_hour_detect_times(0),
      _recent_hour_fail_times(0),
      _recent_minute_detect_times(0),
      _recent_minute_fail_times(0)
{
    // initialize information for available_detector.
    _cluster_name = dsn::get_current_cluster_name();
    _meta_list.clear();
    dsn::replication::replica_helper::load_meta_servers(_meta_list);
    CHECK(!_meta_list.empty(), "");
    // initialize the _client.
    if (!pegasus_client_factory::initialize(nullptr)) {
        CHECK(false, "Initialize the pegasus client failed");
    }
    _client = pegasus_client_factory::get_client(_cluster_name.c_str(), FLAGS_available_detect_app);
    CHECK_NOTNULL(_client, "Initialize the _client failed");
    _result_writer = std::make_unique<result_writer>(_client);
    _ddl_client.reset(new replication_ddl_client(_meta_list));
    CHECK_NOTNULL(_ddl_client, "Initialize the _ddl_client failed");
    if (!dsn::utils::is_empty(FLAGS_available_detect_alert_email_address)) {
        _send_alert_email_cmd = std::string("cd ") + FLAGS_available_detect_alert_script_dir +
                                "; bash sendmail.sh alert " +
                                FLAGS_available_detect_alert_email_address + " " + _cluster_name +
                                " " + FLAGS_available_detect_app + " ";
        _send_availability_info_email_cmd =
            std::string("cd ") + FLAGS_available_detect_alert_script_dir +
            "; bash sendmail.sh availability_info " + FLAGS_available_detect_alert_email_address +
            " " + _cluster_name + " ";
    }

    _pfc_detect_times_day.init_app_counter("app.pegasus",
                                           "cluster.available.detect.times.day",
                                           COUNTER_TYPE_NUMBER,
                                           "statistic the available detect total times every day");
    _pfc_fail_times_day.init_app_counter("app.pegasus",
                                         "cluster.available.fail.times.day",
                                         COUNTER_TYPE_NUMBER,
                                         "statistic the available detect fail times every day");
    _pfc_available_day.init_app_counter("app.pegasus",
                                        "cluster.available.day",
                                        COUNTER_TYPE_NUMBER,
                                        "statistic the availability of cluster every day");
    _pfc_detect_times_hour.init_app_counter(
        "app.pegasus",
        "cluster.available.detect.times.hour",
        COUNTER_TYPE_NUMBER,
        "statistic the available detect total times every hour");
    _pfc_fail_times_hour.init_app_counter("app.pegasus",
                                          "cluster.available.fail.times.hour",
                                          COUNTER_TYPE_NUMBER,
                                          "statistic the available detect fail times every hour");
    _pfc_available_hour.init_app_counter("app.pegasus",
                                         "cluster.available.hour",
                                         COUNTER_TYPE_NUMBER,
                                         "statistic the availability of cluster every hour");
    _pfc_detect_times_minute.init_app_counter(
        "app.pegasus",
        "cluster.available.detect.times.minute",
        COUNTER_TYPE_NUMBER,
        "statistic the available detect total times every minute");
    _pfc_fail_times_minute.init_app_counter(
        "app.pegasus",
        "cluster.available.fail.times.minute",
        COUNTER_TYPE_NUMBER,
        "statistic the available detect fail times every minute");
    _pfc_available_minute.init_app_counter("app.pegasus",
                                           "cluster.available.minute",
                                           COUNTER_TYPE_NUMBER,
                                           "statistic the availability of cluster every minute");
    _pfc_available_day->set(1000000);    // init to 100%
    _pfc_available_hour->set(1000000);   // init to 100%
    _pfc_available_minute->set(1000000); // init to 100%
}

available_detector::~available_detector() = default;

void available_detector::start()
{
    // available detector delay 60s to wait the pegasus finishing the initialization.
    _detect_timer = ::dsn::tasking::enqueue(LPC_DETECT_AVAILABLE,
                                            &_tracker,
                                            std::bind(&available_detector::detect_available, this),
                                            0,
                                            std::chrono::minutes(1));
    report_availability_info();
}

void available_detector::stop() { _tracker.cancel_outstanding_tasks(); }

void available_detector::detect_available()
{
    if (!generate_hash_keys()) {
        LOG_ERROR("initialize hash_keys failed, do not detect available, retry after 60 seconds");
        _detect_timer =
            ::dsn::tasking::enqueue(LPC_DETECT_AVAILABLE,
                                    &_tracker,
                                    std::bind(&available_detector::detect_available, this),
                                    0,
                                    std::chrono::minutes(1));
        return;
    }
    _detect_tasks.clear();
    _detect_tasks.resize(_partition_count);
    _fail_count.resize(_partition_count);
    // initialize detect_times to zero, this is used to generate  distinct value.
    for (auto i = 0; i < _partition_count; i++) {
        _fail_count[i].reset(new std::atomic<int32_t>(0));
        _fail_count[i]->store(0);
        auto call_func = std::bind(&available_detector::on_detect, this, i);
        _detect_tasks[i] = ::dsn::tasking::enqueue_timer(
            LPC_DETECT_AVAILABLE,
            &_tracker,
            std::move(call_func),
            std::chrono::seconds(FLAGS_available_detect_interval_seconds));
    }
}

std::string get_date_to_string(uint64_t now_ms)
{
    char buf[50] = {'\0'};
    ::dsn::utils::time_ms_to_date(now_ms, buf, 50);
    return std::string(buf);
}

std::string get_hour_to_string(uint64_t now_ms)
{
    char buf[50] = {'\0'};
    ::dsn::utils::time_ms_to_date_time(now_ms, buf, 50);
    return std::string(buf, 13);
}

std::string get_minute_to_string(uint64_t now_ms)
{
    char buf[50] = {'\0'};
    ::dsn::utils::time_ms_to_date_time(now_ms, buf, 50);
    return std::string(buf, 16);
}

void available_detector::report_availability_info()
{
    uint64_t now_ms = dsn_now_ms();
    _old_day = get_date_to_string(now_ms);
    _old_hour = get_hour_to_string(now_ms);
    _old_minute = get_minute_to_string(now_ms);
    auto call_func = [this]() {
        uint64_t now_ms = dsn_now_ms();
        std::string new_day = get_date_to_string(now_ms);
        std::string new_hour = get_hour_to_string(now_ms);
        std::string new_minute = get_minute_to_string(now_ms);
        if (new_day != _old_day) {
            this->on_day_report();
            _old_day = new_day;
        }
        if (new_hour != _old_hour) {
            this->on_hour_report();
            _old_hour = new_hour;
        }
        if (new_minute != _old_minute) {
            this->on_minute_report();
            _old_minute = new_minute;
        }
    };
    _report_task = ::dsn::tasking::enqueue_timer(
        LPC_DETECT_AVAILABLE,
        &_tracker,
        std::move(call_func),
        std::chrono::minutes(1),
        0,
        std::chrono::minutes(2) // waiting for pegasus finishing start.
        );
}

bool available_detector::generate_hash_keys()
{
    // get app_id and partition_count.
    auto err =
        _ddl_client->list_app(FLAGS_available_detect_app, _app_id, _partition_count, partitions);
    if (err == ::dsn::ERR_OK && _app_id >= 0) {
        _hash_keys.clear();
        for (auto pidx = 0; pidx < _partition_count; pidx++) {
            std::string base_key = "detect_available_p" + std::to_string(pidx) + "_";
            // loop to generate the key that can fall into pidx-th partition.
            int32_t base_idx = 0;
            while (true) {
                ++base_idx;
                std::string key = base_key + std::to_string(base_idx);
                ::dsn::blob my_hash_key(key.c_str(), 0, key.length());
                ::dsn::blob tmp_key;
                pegasus_generate_key(tmp_key, my_hash_key, ::dsn::blob());
                if (pegasus_key_hash(tmp_key) % _partition_count == pidx) {
                    _hash_keys.emplace_back(key);
                    break;
                }
            }
        }
        return true;
    } else {
        LOG_WARNING("Get partition count of table '{}' on cluster '{}' failed",
                    FLAGS_available_detect_app,
                    _cluster_name);
        return false;
    }
}

void available_detector::on_detect(int32_t idx)
{
    if (idx == 0) {
        LOG_INFO("detecting table[{}] with app_id[{}] and partition_count[{}] on cluster[{}], "
                 "recent_day_detect_times({}), recent_day_fail_times({}), "
                 "recent_hour_detect_times({}), recent_hour_fail_times({}) "
                 "recent_minute_detect_times({}), recent_minute_fail_times({})",
                 FLAGS_available_detect_app,
                 _app_id,
                 _partition_count,
                 _cluster_name,
                 _recent_day_detect_times,
                 _recent_day_fail_times,
                 _recent_hour_detect_times,
                 _recent_hour_fail_times,
                 _recent_minute_detect_times,
                 _recent_minute_fail_times);
    }
    LOG_DEBUG("available_detector begin to detect partition[{}] of table[{}] with id[{}] on the "
              "cluster[{}]",
              idx,
              FLAGS_available_detect_app,
              _app_id,
              _cluster_name);
    auto time = dsn_now_ms();
    std::string value = "detect_value_" + std::to_string((time / 1000));
    _recent_day_detect_times.fetch_add(1);
    _recent_hour_detect_times.fetch_add(1);
    _recent_minute_detect_times.fetch_add(1);

    // define async_get callback function.
    auto async_get_callback = [this, idx](
        int err, std::string &&_value, pegasus_client::internal_info &&info) {
        std::atomic<int> &cnt = (*_fail_count[idx]);
        if (err != PERR_OK) {
            int prev = cnt.fetch_add(1);
            _recent_day_fail_times.fetch_add(1);
            _recent_hour_fail_times.fetch_add(1);
            _recent_minute_fail_times.fetch_add(1);
            LOG_ERROR("async_get partition[{}] fail, fail_count = {}, hash_key = {}, error = {}",
                      idx,
                      prev + 1,
                      _hash_keys[idx],
                      _client->get_error_string(err));
            check_and_send_email(&cnt, idx);
        } else {
            cnt.store(0);
            LOG_DEBUG("async_get partition[{}] ok, hash_key = {}, value = {}",
                      idx,
                      _hash_keys[idx],
                      _value);
        }
    };

    // define async_set callback function.
    auto async_set_callback =
        [ this, idx, user_async_get_callback = std::move(async_get_callback) ](
            int err, pegasus_client::internal_info &&info)
    {
        std::atomic<int> &cnt = (*_fail_count[idx]);
        if (err != PERR_OK) {
            int prev = cnt.fetch_add(1);
            _recent_day_fail_times.fetch_add(1);
            _recent_hour_fail_times.fetch_add(1);
            _recent_minute_fail_times.fetch_add(1);
            LOG_ERROR("async_set partition[{}] fail, fail_count = {}, hash_key = {}, error = {}",
                      idx,
                      prev + 1,
                      _hash_keys[idx],
                      _client->get_error_string(err));
            check_and_send_email(&cnt, idx);
        } else {
            LOG_DEBUG("async_set partition[{}] ok, hash_key = {}", idx, _hash_keys[idx]);
            _client->async_get(_hash_keys[idx],
                               "",
                               std::move(user_async_get_callback),
                               FLAGS_available_detect_timeout);
        }
    };

    _client->async_set(
        _hash_keys[idx], "", value, std::move(async_set_callback), FLAGS_available_detect_timeout);
}

void available_detector::check_and_send_email(std::atomic<int> *cnt, int32_t idx)
{
    bool send_email = false;
    if (cnt->load() >= FLAGS_available_detect_alert_fail_count) {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_alert_lock);
        if (cnt->load() >= FLAGS_available_detect_alert_fail_count) {
            for (auto i = 0; i < _partition_count; i++) {
                std::atomic<int> &c = (*_fail_count[i]);
                c.store(0);
            }
            send_email = true;
        }
    }
    if (send_email) {
        LOG_INFO("start to send alert email, partition_index = {}", idx);
        if (_send_alert_email_cmd.empty()) {
            LOG_INFO("ignore sending alert email because email address is not set, "
                     "partition_index = {}",
                     idx);
        } else {
            int r = system((_send_alert_email_cmd + std::to_string(idx)).c_str());
            if (r == 0) {
                LOG_INFO("send alert email done, partition_index = {}", idx);
            } else {
                LOG_ERROR(
                    "send alert email failed, partition_index = {}, command_return = {}", idx, r);
            }
        }
    }
}

void available_detector::on_day_report()
{
    LOG_INFO("start to report on new day, last_day = {}", _old_day);
    int64_t detect_times = _recent_day_detect_times.fetch_and(0);
    int64_t fail_times = _recent_day_fail_times.fetch_and(0);
    int64_t succ_times = std::max<int64_t>(0L, detect_times - fail_times);
    int64_t available = 0;
    std::string hash_key("detect_available_day");
    std::string sort_key(_old_day);
    std::string value("0,0,0");
    if (detect_times > 0) {
        available = (int64_t)((double)succ_times / detect_times * 1000000);
        std::ostringstream oss;
        oss << detect_times << ',' << succ_times << ',' << available;
        value = oss.str();
    }

    _pfc_detect_times_day->set(detect_times);
    _pfc_fail_times_day->set(fail_times);
    _pfc_available_day->set(available);

    LOG_INFO("start to send availability email, date = {}", _old_day);
    if (_send_availability_info_email_cmd.empty()) {
        LOG_INFO("ignore sending availability email because email address is not set, "
                 "date = {}, total_detect_times = {}, total_fail_times = {}",
                 _old_day,
                 detect_times,
                 fail_times);
    } else {
        int r = system((_send_availability_info_email_cmd + std::to_string(detect_times) + " " +
                        std::to_string(fail_times) + " " + _old_day)
                           .c_str());
        if (r == 0) {
            LOG_INFO("send availability email done, date = {}, "
                     "total_detect_times = {}, total_fail_times = {}",
                     _old_day,
                     detect_times,
                     fail_times);
        } else {
            LOG_ERROR("send availability email fail, date = {}, total_detect_times = {}, "
                      "total_fail_times = {}, command_return = {}",
                      _old_day,
                      detect_times,
                      fail_times,
                      r);
        }
    }

    _result_writer->set_result(hash_key, sort_key, value);
}

void available_detector::on_hour_report()
{
    LOG_INFO("start to report on new hour, last_hour = {}", _old_hour);
    int64_t detect_times = _recent_hour_detect_times.fetch_and(0);
    int64_t fail_times = _recent_hour_fail_times.fetch_and(0);
    int64_t succ_times = std::max<int64_t>(0L, detect_times - fail_times);
    int64_t available = 0;
    std::string hash_key("detect_available_hour");
    std::string sort_key(_old_hour);
    std::string value("0,0,0");
    if (detect_times > 0) {
        available = (int64_t)((double)succ_times / detect_times * 1000000);
        std::ostringstream oss;
        oss << detect_times << ',' << succ_times << ',' << available;
        value = oss.str();
    }

    _pfc_detect_times_hour->set(detect_times);
    _pfc_fail_times_hour->set(fail_times);
    _pfc_available_hour->set(available);

    _result_writer->set_result(hash_key, sort_key, value);
}

void available_detector::on_minute_report()
{
    LOG_INFO("start to report on new minute, last_minute = {}", _old_minute);
    int64_t detect_times = _recent_minute_detect_times.fetch_and(0);
    int64_t fail_times = _recent_minute_fail_times.fetch_and(0);
    int64_t succ_times = std::max<int64_t>(0L, detect_times - fail_times);
    int64_t available = 0;
    std::string hash_key("detect_available_minute");
    std::string sort_key(_old_minute);
    std::string value("0,0,0");
    if (detect_times > 0) {
        available = (int64_t)((double)succ_times / detect_times * 1000000);
        std::ostringstream oss;
        oss << detect_times << ',' << succ_times << ',' << available;
        value = oss.str();
    }

    _pfc_detect_times_minute->set(detect_times);
    _pfc_fail_times_minute->set(fail_times);
    _pfc_available_minute->set(available);

    _result_writer->set_result(hash_key, sort_key, value);
}
} // namespace server
} // namespace pegasus
