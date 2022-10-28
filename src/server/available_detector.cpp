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

#include <algorithm>
#include "common/common.h"
#include "utils/time_utils.h"
#include <iomanip>
#include <sstream>

#include "base/pegasus_key_schema.h"
#include "result_writer.h"

namespace pegasus {
namespace server {

DEFINE_TASK_CODE(LPC_DETECT_AVAILABLE, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

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
    _app_name = dsn_config_get_value_string(
        "pegasus.collector", "available_detect_app", "", "available detector app name");
    CHECK(!_app_name.empty(), "");
    _alert_script_dir = dsn_config_get_value_string("pegasus.collector",
                                                    "available_detect_alert_script_dir",
                                                    ".",
                                                    "available detect alert script dir");
    CHECK(!_alert_script_dir.empty(), "");
    _alert_email_address = dsn_config_get_value_string(
        "pegasus.collector",
        "available_detect_alert_email_address",
        "",
        "available detect alert email address, empty means not send email");
    _meta_list.clear();
    dsn::replication::replica_helper::load_meta_servers(_meta_list);
    CHECK(!_meta_list.empty(), "");
    _detect_interval_seconds =
        (uint32_t)dsn_config_get_value_uint64("pegasus.collector",
                                              "available_detect_interval_seconds",
                                              3, // default value 3s
                                              "detect interval seconds");
    _alert_fail_count = (int32_t)dsn_config_get_value_uint64("pegasus.collector",
                                                             "available_detect_alert_fail_count",
                                                             30,
                                                             "available detect alert fail count");
    _detect_timeout =
        (uint32_t)dsn_config_get_value_uint64("pegasus.collector",
                                              "available_detect_timeout",
                                              1000, // unit is millisecond,default is 1s = 1000ms
                                              "available detect timeout");
    // initialize the _client.
    if (!pegasus_client_factory::initialize(nullptr)) {
        CHECK(false, "Initialize the pegasus client failed");
    }
    _client = pegasus_client_factory::get_client(_cluster_name.c_str(), _app_name.c_str());
    CHECK_NOTNULL(_client, "Initialize the _client failed");
    _result_writer = dsn::make_unique<result_writer>(_client);
    _ddl_client.reset(new replication_ddl_client(_meta_list));
    CHECK_NOTNULL(_ddl_client, "Initialize the _ddl_client failed");
    if (!_alert_email_address.empty()) {
        _send_alert_email_cmd = "cd " + _alert_script_dir + "; bash sendmail.sh alert " +
                                _alert_email_address + " " + _cluster_name + " " + _app_name + " ";
        _send_availability_info_email_cmd = "cd " + _alert_script_dir +
                                            "; bash sendmail.sh availability_info " +
                                            _alert_email_address + " " + _cluster_name + " ";
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
        _detect_tasks[i] =
            ::dsn::tasking::enqueue_timer(LPC_DETECT_AVAILABLE,
                                          &_tracker,
                                          std::move(call_func),
                                          std::chrono::seconds(_detect_interval_seconds));
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
    auto err = _ddl_client->list_app(_app_name, _app_id, _partition_count, partitions);
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
        LOG_WARNING("Get partition count of table '%s' on cluster '%s' failed",
                    _app_name.c_str(),
                    _cluster_name.c_str());
        return false;
    }
}

void available_detector::on_detect(int32_t idx)
{
    if (idx == 0) {
        LOG_INFO("detecting table[%s] with app_id[%d] and partition_count[%d] on cluster[%s], "
                 "recent_day_detect_times(%" PRId64 "), recent_day_fail_times(%" PRId64 "), "
                 "recent_hour_detect_times(%" PRId64 "), recent_hour_fail_times(%" PRId64 ") "
                 "recent_minute_detect_times(%" PRId64 "), recent_minute_fail_times(%" PRId64 ")",
                 _app_name.c_str(),
                 _app_id,
                 _partition_count,
                 _cluster_name.c_str(),
                 _recent_day_detect_times.load(),
                 _recent_day_fail_times.load(),
                 _recent_hour_detect_times.load(),
                 _recent_hour_fail_times.load(),
                 _recent_minute_detect_times.load(),
                 _recent_minute_fail_times.load());
    }
    LOG_DEBUG("available_detector begin to detect partition[%d] of table[%s] with id[%d] on the "
              "cluster[%s]",
              idx,
              _app_name.c_str(),
              _app_id,
              _cluster_name.c_str());
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
            LOG_ERROR("async_get partition[%d] fail, fail_count = %d, hash_key = %s, error = %s",
                      idx,
                      prev + 1,
                      _hash_keys[idx].c_str(),
                      _client->get_error_string(err));
            check_and_send_email(&cnt, idx);
        } else {
            cnt.store(0);
            LOG_DEBUG("async_get partition[%d] ok, hash_key = %s, value = %s",
                      idx,
                      _hash_keys[idx].c_str(),
                      _value.c_str());
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
            LOG_ERROR("async_set partition[%d] fail, fail_count = %d, hash_key = %s , error = %s",
                      idx,
                      prev + 1,
                      _hash_keys[idx].c_str(),
                      _client->get_error_string(err));
            check_and_send_email(&cnt, idx);
        } else {
            LOG_DEBUG("async_set partition[%d] ok, hash_key = %s", idx, _hash_keys[idx].c_str());
            _client->async_get(
                _hash_keys[idx], "", std::move(user_async_get_callback), _detect_timeout);
        }
    };

    _client->async_set(_hash_keys[idx], "", value, std::move(async_set_callback), _detect_timeout);
}

void available_detector::check_and_send_email(std::atomic<int> *cnt, int32_t idx)
{
    bool send_email = false;
    if (cnt->load() >= _alert_fail_count) {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_alert_lock);
        if (cnt->load() >= _alert_fail_count) {
            for (auto i = 0; i < _partition_count; i++) {
                std::atomic<int> &c = (*_fail_count[i]);
                c.store(0);
            }
            send_email = true;
        }
    }
    if (send_email) {
        LOG_INFO("start to send alert email, partition_index = %d", idx);
        if (_send_alert_email_cmd.empty()) {
            LOG_INFO("ignore sending alert email because email address is not set, "
                     "partition_index = %d",
                     idx);
        } else {
            int r = system((_send_alert_email_cmd + std::to_string(idx)).c_str());
            if (r == 0) {
                LOG_INFO("send alert email done, partition_index = %d", idx);
            } else {
                LOG_ERROR("send alert email failed, partition_index = %d, "
                          "command_return = %d",
                          idx,
                          r);
            }
        }
    }
}

void available_detector::on_day_report()
{
    LOG_INFO("start to report on new day, last_day = %s", _old_day.c_str());
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

    LOG_INFO("start to send availability email, date = %s", _old_day.c_str());
    if (_send_availability_info_email_cmd.empty()) {
        LOG_INFO("ignore sending availability email because email address is not set, "
                 "date = %s, total_detect_times = %u, total_fail_times = %u",
                 _old_day.c_str(),
                 detect_times,
                 fail_times);
    } else {
        int r = system((_send_availability_info_email_cmd + std::to_string(detect_times) + " " +
                        std::to_string(fail_times) + " " + _old_day)
                           .c_str());
        if (r == 0) {
            LOG_INFO("send availability email done, date = %s, "
                     "total_detect_times = %u, total_fail_times = %u",
                     _old_day.c_str(),
                     detect_times,
                     fail_times);
        } else {
            LOG_ERROR("send availability email fail, date = %s, "
                      "total_detect_times = %u, total_fail_times = %u, command_return = %d",
                      _old_day.c_str(),
                      detect_times,
                      fail_times,
                      r);
        }
    }

    _result_writer->set_result(hash_key, sort_key, value);
}

void available_detector::on_hour_report()
{
    LOG_INFO("start to report on new hour, last_hour = %s", _old_hour.c_str());
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
    LOG_INFO("start to report on new minute, last_minute = %s", _old_minute.c_str());
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
