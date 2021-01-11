// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "hotspot_partition_calculator.h"

#include <math.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/cpp/serialization_helper/dsn.layer2_types.h>
#include <dsn/utility/flags.h>
#include <dsn/utility/error_code.h>
#include <dsn/utility/fail_point.h>
#include <dsn/dist/replication/duplication_common.h>

namespace pegasus {
namespace server {

DSN_DEFINE_int64("pegasus.collector",
                 max_hotspot_store_size,
                 100,
                 "the max count of historical data "
                 "stored in calculator, The FIFO "
                 "queue design is used to "
                 "eliminate outdated historical "
                 "data");

DSN_DEFINE_bool("pegasus.collector",
                enable_detect_hotkey,
                false,
                "auto detect hot key in the hot paritition");
DSN_TAG_VARIABLE(enable_detect_hotkey, FT_MUTABLE);

DSN_DEFINE_int32("pegasus.collector",
                 hot_partition_threshold,
                 3,
                 "threshold of hotspot partition value, if app.stat.hotspots >= "
                 "FLAGS_hotpartition_threshold, this partition is a hot partition");
DSN_TAG_VARIABLE(hot_partition_threshold, FT_MUTABLE);
DSN_DEFINE_validator(hot_partition_threshold,
                     [](int32_t threshold) -> bool { return threshold >= 0; });

DSN_DEFINE_int32("pegasus.collector",
                 occurrence_threshold,
                 3,
                 "hot paritiotion occurrence times' threshold to send rpc to detect hotkey");
DSN_TAG_VARIABLE(occurrence_threshold, FT_MUTABLE);
DSN_DEFINE_validator(occurrence_threshold,
                     [](int32_t threshold) -> bool { return threshold >= 0; });

void hotspot_partition_calculator::data_aggregate(const std::vector<row_data> &partition_stats)
{
    while (_partitions_stat_histories.size() >= FLAGS_max_hotspot_store_size) {
        _partitions_stat_histories.pop_front();
    }
    std::vector<hotspot_partition_stat> temp;
    for (const auto &partition_stat : partition_stats) {
        temp.emplace_back(hotspot_partition_stat(partition_stat));
    }
    _partitions_stat_histories.emplace_back(temp);
}

void hotspot_partition_calculator::init_perf_counter(int partition_count)
{
    for (int data_type = 0; data_type <= 1; data_type++) {
        for (int i = 0; i < partition_count; i++) {
            string partition_desc =
                _app_name + '.' +
                (data_type == partition_qps_type::WRITE_HOTSPOT_DATA ? "write." : "read.") +
                std::to_string(i);
            std::string counter_name = fmt::format("app.stat.hotspots@{}", partition_desc);
            std::string counter_desc =
                fmt::format("statistic the hotspots of app {}", partition_desc);
            _hot_points[i][data_type].init_app_counter(
                "app.pegasus", counter_name.c_str(), COUNTER_TYPE_NUMBER, counter_desc.c_str());
        }
    }
}

void hotspot_partition_calculator::stat_histories_analyse(int data_type,
                                                          std::vector<int> &hot_points)
{
    double table_qps_sum = 0, standard_deviation = 0, table_qps_avg = 0;
    int sample_count = 0;
    for (const auto &one_partition_stat_histories : _partitions_stat_histories) {
        for (const auto &partition_stat : one_partition_stat_histories) {
            table_qps_sum += partition_stat.total_qps[data_type];
            sample_count++;
        }
    }
    if (sample_count <= 1) {
        ddebug("_partitions_stat_histories size <= 1, not enough data for calculation");
        return;
    }
    table_qps_avg = table_qps_sum / sample_count;
    for (const auto &one_partition_stat_histories : _partitions_stat_histories) {
        for (const auto &partition_stat : one_partition_stat_histories) {
            standard_deviation += pow((partition_stat.total_qps[data_type] - table_qps_avg), 2);
        }
    }
    standard_deviation = sqrt(standard_deviation / (sample_count - 1));
    const auto &anly_data = _partitions_stat_histories.back();
    int hot_point_size = _hot_points.size();
    hot_points.resize(hot_point_size);
    for (int i = 0; i < hot_point_size; i++) {
        double hot_point = 0;
        if (standard_deviation != 0) {
            hot_point = (anly_data[i].total_qps[data_type] - table_qps_avg) / standard_deviation;
        }
        // perf_counter->set can only be unsigned uint64_t
        // use ceil to guarantee conversion results
        hot_points[i] = ceil(std::max(hot_point, double(0)));
    }
}

void hotspot_partition_calculator::update_hot_point(int data_type, std::vector<int> &hot_points)
{
    dcheck_eq(_hot_points.size(), hot_points.size());
    int size = hot_points.size();
    for (int i = 0; i < size; i++) {
        _hot_points[i][data_type].get()->set(hot_points[i]);
    }
}

void hotspot_partition_calculator::data_analyse()
{
    dassert(_partitions_stat_histories.back().size() == _hot_points.size(),
            "The number of partitions in this table has changed, and hotspot analysis cannot be "
            "performed,in %s",
            _app_name.c_str());
    for (int data_type = 0; data_type <= 1; data_type++) {
        // data_type 0: READ_HOTSPOT_DATA; 1: WRITE_HOTSPOT_DATA
        std::vector<int> hot_points;
        stat_histories_analyse(data_type, hot_points);
        update_hot_point(data_type, hot_points);
    }
    if (!FLAGS_enable_detect_hotkey) {
        return;
    }
    for (int data_type = 0; data_type <= 1; data_type++) {
        detect_hotkey_in_hotpartition(data_type);
    }
}

void hotspot_partition_calculator::detect_hotkey_in_hotpartition(int data_type)
{
    auto now_hot_partition_threshold = FLAGS_hot_partition_threshold;
    auto now_occurrence_threshold = FLAGS_occurrence_threshold;
    for (int index = 0; index < _hot_points.size(); index++) {
        if (_hot_points[index][data_type].get()->get_value() >= now_hot_partition_threshold) {
            if (++_hotpartition_counter[index][data_type] >= now_occurrence_threshold) {
                derror_f("Find a {} hot partition {}.{}",
                         (data_type == partition_qps_type::READ_HOTSPOT_DATA ? "read" : "write"),
                         _app_name,
                         index);
                send_detect_hotkey_request(_app_name,
                                           index,
                                           (data_type == dsn::replication::hotkey_type::type::READ)
                                               ? dsn::replication::hotkey_type::type::READ
                                               : dsn::replication::hotkey_type::type::WRITE,
                                           dsn::replication::detect_action::type::START);
            }
        } else {
            _hotpartition_counter[index][data_type] =
                std::max(_hotpartition_counter[index][data_type] - 1, 0);
        }
    }
}

void hotspot_partition_calculator::send_detect_hotkey_request(
    const std::string &app_name,
    const uint64_t partition_index,
    const dsn::replication::hotkey_type::type hotkey_type,
    const dsn::replication::detect_action::type action)
{
    FAIL_POINT_INJECT_F("send_detect_hotkey_request", [](dsn::string_view) {});

    int app_id = -1;
    int partition_count = -1;
    std::vector<dsn::partition_configuration> partitions;
    _shell_context->ddl_client->list_app(app_name, app_id, partition_count, partitions);

    auto target_address = partitions[partition_index].primary;
    dsn::replication::detect_hotkey_response resp;
    dsn::replication::detect_hotkey_request req;
    req.type = hotkey_type;
    req.action = action;
    req.pid = dsn::gpid(app_id, partition_index);
    auto error = _shell_context->ddl_client->detect_hotkey(target_address, req, resp);

    ddebug_f("{} {} hotkey detection in {}.{}, server address: {}",
             (action == dsn::replication::detect_action::STOP) ? "Stop" : "Start",
             (hotkey_type == dsn::replication::hotkey_type::WRITE) ? "write" : "read",
             app_name,
             partition_index,
             target_address.to_string());

    if (error != dsn::ERR_OK) {
        derror_f("Hotkey detect rpc sending failed, in {}.{}, error_hint:{}",
                 app_name,
                 partition_index,
                 error.to_string());
    }

    if (resp.err != dsn::ERR_OK) {
        derror_f("Hotkey detect rpc executing failed, in {}.{}, error_hint:{} {}",
                 app_name,
                 partition_index,
                 resp.err,
                 resp.err_hint);
    }
}

} // namespace server
} // namespace pegasus
