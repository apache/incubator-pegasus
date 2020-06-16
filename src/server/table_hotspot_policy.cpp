// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "table_hotspot_policy.h"

#include <dsn/dist/fmt_logging.h>
#include <dsn/tool-api/rpc_address.h>
#include <dsn/tool-api/group_address.h>
#include <dsn/utility/error_code.h>
#include <rrdb/rrdb_types.h>
#include "hotkey_collector.h"

using namespace dsn;

namespace pegasus {
namespace server {

DSN_DEFINE_bool("pegasus.collector",
                enable_hotkey_auto_detect,
                false,
                "auto detect hot key in the hot paritition");

// if hot_partition_counter >= FLAGS_hotpartition_threshold, This partition is a hot partition
DSN_DEFINE_int32("pegasus.collector",
                 hot_partition_threshold,
                 3,
                 "threshold of hotspot partition value");

// if one partition's _over_threshold_times_read/write >= FLAGS_occurrence_threshold
// collctor will ask the corresponding partition to start hotkey detection
DSN_DEFINE_int32("pegasus.collector",
                 occurrence_threshold,
                 2,
                 "hot paritiotion occurrence times'threshold to send rpc to detect hotkey");

hotspot_calculator::hotspot_calculator(const std::string &app_name,
                                       const int partition_num,
                                       std::unique_ptr<hotspot_policy> policy)
    : _app_name(app_name), _hot_partition_points(partition_num), _policy(std::move(policy))
{
    init_perf_counter(partition_num);
    if (FLAGS_enable_hotkey_auto_detect) {
        _over_threshold_times_read.resize(partition_num);
        _over_threshold_times_write.resize(partition_num);
    }
}

void hotspot_calculator::aggregate(const std::vector<row_data> &partitions)
{
    while (_app_data.size() > kMaxQueueSize - 1) {
        _app_data.pop_front();
    }
    std::vector<hotspot_partition_data> temp(partitions.size());
    for (int i = 0; i < partitions.size(); i++) {
        temp[i] = std::move(hotspot_partition_data(partitions[i]));
    }
    _app_data.emplace_back(temp);
}

void hotspot_calculator::init_perf_counter(const int perf_counter_count)
{
    std::string read_counter_name, write_counter_name;
    std::string read_counter_desc, write_counter_desc;
    for (int i = 0; i < perf_counter_count; i++) {
        string read_paritition_desc = _app_name + '.' + "read." + std::to_string(i);
        read_counter_name = fmt::format("app.stat.hotspots@{}", read_paritition_desc);
        read_counter_desc = fmt::format("statistic the hotspots of app {}", read_paritition_desc);
        _hot_partition_points[i].resize(2);
        _hot_partition_points[i][READ_HOTSPOT_DATA].reset(new perf_counter_wrapper());
        _hot_partition_points[i][READ_HOTSPOT_DATA]->init_app_counter("app.pegasus",
                                                                      read_counter_name.c_str(),
                                                                      COUNTER_TYPE_NUMBER,
                                                                      read_counter_desc.c_str());
        string write_paritition_desc = _app_name + '.' + "write." + std::to_string(i);
        write_counter_name = fmt::format("app.stat.hotspots@{}", write_paritition_desc);
        write_counter_desc = fmt::format("statistic the hotspots of app {}", write_paritition_desc);
        _hot_partition_points[i][WRITE_HOTSPOT_DATA].reset(new perf_counter_wrapper());
        _hot_partition_points[i][WRITE_HOTSPOT_DATA]->init_app_counter("app.pegasus",
                                                                       write_counter_name.c_str(),
                                                                       COUNTER_TYPE_NUMBER,
                                                                       write_counter_desc.c_str());
    }
}

/*static*/ void
hotspot_calculator::notify_replica(const std::string &app_name,
                                   const int partition_index,
                                   const hotkey_detect_type write_hotkey_detect,
                                   const hotkey_collector_operation stop_detect_hotkey)
{
    ::dsn::apps::hotkey_detect_request req;
    req.type = write_hotkey_detect ? dsn::apps::hotkey_type::WRITE : dsn::apps::hotkey_type::READ;
    req.operation = stop_detect_hotkey ? dsn::apps::hotkey_collector_operation::STOP
                                       : dsn::apps::hotkey_collector_operation::START;
    derror_f("{} {} hotkey detection",
             stop_detect_hotkey ? "Stop" : "Start",
             write_hotkey_detect ? "write" : "read");
    std::vector<rpc_address> meta_servers;
    replica_helper::load_meta_servers(meta_servers);
    rpc_address meta_server;

    meta_server.assign_group("meta-servers");
    for (auto &ms : meta_servers) {
        meta_server.group_address()->add(ms);
    }
    auto cluster_name = replication::get_current_cluster_name();
    auto resolver = partition_resolver::get_resolver(cluster_name, meta_servers, app_name.c_str());
    resolver->call_op(RPC_DETECT_HOTKEY,
                      req,
                      nullptr,
                      [app_name, partition_index](
                          error_code err, dsn::message_ex *request, dsn::message_ex *resp) {
                          if (err == ERR_OK) {
                              ::dsn::apps::hotkey_detect_response response;
                              ::dsn::unmarshall(resp, response);
                              if (response.err == ERR_OK) {
                                  ddebug("Hotkey detect rpc sending successed");
                              } else {
                                  ddebug("Hotkey detect rpc sending failed");
                              }
                          } else {
                              ddebug("Hotkey detect rpc sending failed, %s", err.to_string());
                          }
                      },
                      std::chrono::seconds(10),
                      partition_index,
                      0);
}

void hotspot_calculator::start_alg()
{
    ddebug("Start to detect hotspot partition");
    _policy->analysis(_app_data, _hot_partition_points);

    if (!FLAGS_enable_hotkey_auto_detect) {
        return;
    }

    for (int i = 0; i < _hot_partition_points.size(); i++) {
        if (_hot_partition_points[i][READ_HOTSPOT_DATA]->get()->get_value() >=
            FLAGS_hot_partition_threshold) {
            if (++_over_threshold_times_read[i] >= FLAGS_occurrence_threshold) {
                ddebug("Find a read hot partition %s.%d", _app_name.c_str(), i);
                notify_replica(_app_name, i, READ_HOTKEY_DETECT, START_HOTKEY_DETECT);
                read_hot_partition.insert(i);
            }
        } else if (_over_threshold_times_read[i] > 0) {
            _over_threshold_times_read[i]--;
        }

        if (_hot_partition_points[i][WRITE_HOTSPOT_DATA]->get()->get_value() >=
            FLAGS_hot_partition_threshold) {
            if (++_over_threshold_times_write[i] >= FLAGS_occurrence_threshold) {
                ddebug("Find a write hot partition %s.%d", _app_name.c_str(), i);
                notify_replica(_app_name, i, WRITE_HOTKEY_DETECT, START_HOTKEY_DETECT);
                write_hot_partition.insert(i);
            }
        } else if (_over_threshold_times_write[i] > 0) {
            _over_threshold_times_write[i]--;
        }
    }
    if (!read_hot_partition.empty()) {
        for (auto iter = read_hot_partition.begin(); iter != read_hot_partition.end();) {
            if (_hot_partition_points[*iter][READ_HOTSPOT_DATA]->get()->get_value() <
                FLAGS_hot_partition_threshold) {
                ddebug("Read hot partition %s.%d is cleared, stop hotkey detection",
                       _app_name.c_str(),
                       *iter);
                iter = read_hot_partition.erase(iter);
                notify_replica(_app_name, *iter, READ_HOTKEY_DETECT, STOP_HOTKEY_DETECT);
            } else {
                ++iter;
            }
        }
    }
    if (!write_hot_partition.empty()) {
        for (auto iter = write_hot_partition.begin(); iter != write_hot_partition.end();) {
            if (_hot_partition_points[*iter][WRITE_HOTSPOT_DATA]->get()->get_value() <
                FLAGS_hot_partition_threshold) {
                ddebug("Write hot partition %s.%d is cleared, stop hotkey detection",
                       _app_name.c_str(),
                       *iter);
                iter = write_hot_partition.erase(iter);
                notify_replica(_app_name, *iter, WRITE_HOTKEY_DETECT, STOP_HOTKEY_DETECT);
            } else {
                ++iter;
            }
        }
    }
}

} // namespace server
} // namespace pegasus
