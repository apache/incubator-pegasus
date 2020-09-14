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

#include <algorithm>
#include <math.h>
#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/flags.h>
#include <dsn/tool-api/rpc_address.h>
#include <dsn/tool-api/group_address.h>
#include <dsn/utility/error_code.h>
#include <rrdb/rrdb_types.h>
#include <dsn/dist/replication/duplication_common.h>
#include <dsn/tool-api/task_tracker.h>

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

void hotspot_partition_calculator::data_aggregate(const std::vector<row_data> &partitions)
{
    while (_partition_stat_histories.size() > FLAGS_max_hotspot_store_size - 1) {
        _partition_stat_histories.pop();
    }
    std::vector<hotspot_partition_data> temp(partitions.size());
    // TODO refactor the data structure
    for (int i = 0; i < partitions.size(); i++) {
        temp[i] = std::move(hotspot_partition_data(partitions[i]));
    }
    _partition_stat_histories.emplace(temp);
}

void hotspot_partition_calculator::init_perf_counter(int partition_count)
{
    std::string counter_name;
    std::string counter_desc;
    for (int i = 0; i < partition_count; i++) {
        string partition_desc = _app_name + '.' + std::to_string(i);
        counter_name = fmt::format("app.stat.hotspots@{}", partition_desc);
        counter_desc = fmt::format("statistic the hotspots of app {}", partition_desc);
        _hot_points[i].init_app_counter(
            "app.pegasus", counter_name.c_str(), COUNTER_TYPE_NUMBER, counter_desc.c_str());
    }
}

void hotspot_partition_calculator::data_analyse()
{
    dassert(_partition_stat_histories.back().size() == _hot_points.size(),
            "partition counts error, please check");
    std::vector<double> data_samples;
    data_samples.reserve(_partition_stat_histories.size() * _hot_points.size());
    auto temp_data = _partition_stat_histories;
    double table_qps_sum = 0, standard_deviation = 0, table_qps_avg = 0;
    int sample_count = 0;
    while (!temp_data.empty()) {
        for (const auto &partition_data : temp_data.front()) {
            if (partition_data.total_qps - 1.00 > 0) {
                data_samples.push_back(partition_data.total_qps);
                table_qps_sum += partition_data.total_qps;
                sample_count++;
            }
        }
        temp_data.pop();
    }
    if (sample_count == 0) {
        ddebug("_partition_stat_histories size == 0");
        return;
    }
    table_qps_avg = table_qps_sum / sample_count;
    for (const auto &data_sample : data_samples) {
        standard_deviation += pow((data_sample - table_qps_avg), 2);
    }
    standard_deviation = sqrt(standard_deviation / sample_count);
    const auto &anly_data = _partition_stat_histories.back();
    for (int i = 0; i < _hot_points.size(); i++) {
        double hot_point = (anly_data[i].total_qps - table_qps_avg) / standard_deviation;
        // perf_counter->set can only be unsigned __int64
        // use ceil to guarantee conversion results
        hot_point = ceil(std::max(hot_point, double(0)));
        _hot_points[i]->set(hot_point);
    }
}

/*static*/ void
hotspot_partition_calculator::send_hotkey_detect_request(const std::string &app_name,
                                                         const int partition_index,
                                                         const hotkey_detect_type hotkey_type,
                                                         const hotkey_detect_action action)
{
    ::dsn::apps::hotkey_detect_request req;
    req.type = (hotkey_type == hotkey_detect_type::WRITE_HOTKEY_DETECT)
                   ? dsn::apps::hotkey_type::WRITE
                   : dsn::apps::hotkey_type::READ;
    req.action = (action == hotkey_detect_action::STOP_HOTKEY_DETECT)
                     ? dsn::apps::hotkey_detect_action::STOP
                     : dsn::apps::hotkey_detect_action::START;
    ddebug_f("{} {} hotkey detection",
             (action == hotkey_detect_action::STOP_HOTKEY_DETECT) ? "Stop" : "Start",
             (hotkey_type == hotkey_detect_type::WRITE_HOTKEY_DETECT) ? "write" : "read");
    dsn::rpc_address meta_server;
    meta_server.assign_group("meta-servers");
    std::vector<dsn::rpc_address> meta_servers;
    replica_helper::load_meta_servers(meta_servers);
    for (auto &address : meta_servers) {
        meta_server.group_address()->add(address);
    }
    auto cluster_name = dsn::replication::get_current_cluster_name();
    auto resolver = partition_resolver::get_resolver(cluster_name, meta_servers, app_name.c_str());
    dsn::task_tracker tracker;
    resolver->call_op(RPC_DETECT_HOTKEY,
                      req,
                      &tracker,
                      [app_name, partition_index](dsn::error_code error,
                                                  dsn::message_ex *request,
                                                  dsn::message_ex *response_msg) {
                          if (error == dsn::ERR_OK) {
                              ::dsn::apps::hotkey_detect_response response;
                              ::dsn::unmarshall(response_msg, response);
                              if (response.err == dsn::ERR_OK) {
                                  ddebug("Hotkey detect rpc sending successed");
                              } else {
                                  ddebug("Hotkey detect rpc sending failed");
                              }
                          } else {
                              ddebug_f("Hotkey detect rpc sending failed, {}", error.to_string());
                          }
                      },
                      std::chrono::seconds(10),
                      partition_index,
                      0);
}

} // namespace server
} // namespace pegasus
