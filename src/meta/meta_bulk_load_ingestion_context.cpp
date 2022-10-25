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

#include "meta_bulk_load_ingestion_context.h"

#include "utils/fmt_logging.h"
#include "utils/fail_point.h"

namespace dsn {
namespace replication {

DSN_DEFINE_uint32("meta_server",
                  bulk_load_node_max_ingesting_count,
                  4,
                  "max partition_count executing ingestion for one node at the same time");
DSN_TAG_VARIABLE(bulk_load_node_max_ingesting_count, FT_MUTABLE);

DSN_DEFINE_uint32("meta_server", bulk_load_node_min_disk_count, 1, "min disk count of one node");
DSN_TAG_VARIABLE(bulk_load_node_min_disk_count, FT_MUTABLE);

ingestion_context::ingestion_context() { reset_all(); }

ingestion_context::~ingestion_context() { reset_all(); }

void ingestion_context::partition_node_info::create(const partition_configuration &config,
                                                    const config_context &cc)
{
    pid = config.pid;
    std::unordered_set<rpc_address> current_nodes;
    current_nodes.insert(config.primary);
    for (const auto &secondary : config.secondaries) {
        current_nodes.insert(secondary);
    }
    for (const auto &node : current_nodes) {
        std::string disk_tag;
        if (cc.get_disk_tag(node, disk_tag)) {
            node_disk[node] = disk_tag;
        }
    }
}

void ingestion_context::node_context::init_disk(const std::string &disk_tag)
{
    if (disk_ingesting_counts.find(disk_tag) != disk_ingesting_counts.end()) {
        return;
    }
    disk_ingesting_counts[disk_tag] = 0;
}

uint32_t ingestion_context::node_context::get_max_disk_ingestion_count(
    const uint32_t max_node_ingestion_count) const
{
    FAIL_POINT_INJECT_F("ingestion_node_context_disk_count", [](string_view count_str) -> uint32_t {
        uint32_t count = 0;
        buf2uint32(count_str, count);
        return count;
    });

    const auto node_disk_count = disk_ingesting_counts.size() > FLAGS_bulk_load_node_min_disk_count
                                     ? disk_ingesting_counts.size()
                                     : FLAGS_bulk_load_node_min_disk_count;
    return (max_node_ingestion_count + node_disk_count - 1) / node_disk_count;
}

bool ingestion_context::node_context::check_if_add(const std::string &disk_tag)
{
    auto max_node_ingestion_count = FLAGS_bulk_load_node_max_ingesting_count;
    if (node_ingesting_count >= max_node_ingestion_count) {
        LOG_WARNING_F("node[{}] has {} partition executing ingestion, max_count = {}",
                      address.to_string(),
                      node_ingesting_count,
                      max_node_ingestion_count);
        return false;
    }

    auto max_disk_ingestion_count = get_max_disk_ingestion_count(max_node_ingestion_count);
    if (disk_ingesting_counts[disk_tag] >= max_disk_ingestion_count) {
        LOG_WARNING_F("node[{}] disk[{}] has {} partition executing ingestion, max_count = {}",
                      address.to_string(),
                      disk_tag,
                      disk_ingesting_counts[disk_tag],
                      max_disk_ingestion_count);
        return false;
    }
    return true;
}

void ingestion_context::node_context::add(const std::string &disk_tag)
{
    disk_ingesting_counts[disk_tag]++;
    node_ingesting_count++;
}

void ingestion_context::node_context::decrease(const std::string &disk_tag)
{
    node_ingesting_count--;
    disk_ingesting_counts[disk_tag]--;
}

bool ingestion_context::try_partition_ingestion(const partition_configuration &config,
                                                const config_context &cc)
{
    FAIL_POINT_INJECT_F("ingestion_try_partition_ingestion", [=](string_view) -> bool {
        auto info = partition_node_info();
        info.pid = config.pid;
        _running_partitions[config.pid] = info;
        return true;
    });
    partition_node_info info(config, cc);
    for (const auto &kv : info.node_disk) {
        if (!check_node_ingestion(kv.first, kv.second)) {
            return false;
        }
    }
    add_partition(info);
    return true;
}

bool ingestion_context::check_node_ingestion(const rpc_address &node, const std::string &disk_tag)
{
    if (_nodes_context.find(node) == _nodes_context.end()) {
        _nodes_context[node] = node_context(node, disk_tag);
    }
    return _nodes_context[node].check_if_add(disk_tag);
}

void ingestion_context::add_partition(const partition_node_info &info)
{
    for (const auto &kv : info.node_disk) {
        _nodes_context[kv.first].add(kv.second);
    }
    _running_partitions[info.pid] = info;
}

void ingestion_context::remove_partition(const gpid &pid)
{
    FAIL_POINT_INJECT_F("ingestion_context_remove_partition",
                        [=](string_view) { _running_partitions.erase(pid); });

    if (_running_partitions.find(pid) == _running_partitions.end()) {
        return;
    }
    auto &info = _running_partitions[pid];
    for (const auto &kv : info.node_disk) {
        _nodes_context[kv.first].decrease(kv.second);
    }
    _running_partitions.erase(pid);
}

uint32_t ingestion_context::get_app_ingesting_count(const uint32_t app_id) const
{
    uint32_t running_count = 0;
    for (const auto &kv : _running_partitions) {
        if (kv.first.get_app_id() == app_id) {
            running_count++;
        }
    }
    return running_count;
}

void ingestion_context::reset_app(const uint32_t app_id)
{
    std::unordered_set<gpid> removing_partitions;
    for (const auto &kv : _running_partitions) {
        if (kv.first.get_app_id() == app_id) {
            removing_partitions.insert(kv.first);
        }
    }
    for (const auto &pid : removing_partitions) {
        remove_partition(pid);
    }
}

void ingestion_context::reset_all()
{
    _running_partitions.clear();
    _nodes_context.clear();
}

} // namespace replication
} // namespace dsn
