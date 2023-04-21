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

#pragma once

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <gtest/gtest_prod.h>
#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/gpid.h"
#include "load_balance_policy.h"
#include "meta/meta_data.h"
#include "metadata_types.h"
#include "runtime/rpc/rpc_address.h"

namespace dsn {
namespace replication {
class meta_service;

uint32_t get_partition_count(const node_state &ns, balance_type type, int32_t app_id);
uint32_t get_skew(const std::map<rpc_address, uint32_t> &count_map);
void get_min_max_set(const std::map<rpc_address, uint32_t> &node_count_map,
                     /*out*/ std::set<rpc_address> &min_set,
                     /*out*/ std::set<rpc_address> &max_set);

class cluster_balance_policy : public load_balance_policy
{
public:
    cluster_balance_policy(meta_service *svc);
    ~cluster_balance_policy() = default;

    void balance(bool checker, const meta_view *global_view, migration_list *list) override;

private:
    struct app_disk_info;
    struct app_migration_info;
    struct cluster_migration_info;
    struct move_info;
    struct node_migration_info;

    bool cluster_replica_balance(const meta_view *global_view,
                                 const balance_type type,
                                 /*out*/ migration_list &list);
    bool do_cluster_replica_balance(const meta_view *global_view,
                                    const balance_type type,
                                    /*out*/ migration_list &list);
    bool get_cluster_migration_info(const meta_view *global_view,
                                    const balance_type type,
                                    /*out*/ cluster_migration_info &cluster_info);
    bool get_app_migration_info(std::shared_ptr<app_state> app,
                                const node_mapper &nodes,
                                const balance_type type,
                                /*out*/ app_migration_info &info);
    void get_node_migration_info(const node_state &ns,
                                 const app_mapper &all_apps,
                                 /*out*/ node_migration_info &info);
    bool get_next_move(const cluster_migration_info &cluster_info,
                       const partition_set &selected_pid,
                       /*out*/ move_info &next_move);
    bool pick_up_move(const cluster_migration_info &cluster_info,
                      const std::set<rpc_address> &max_nodes,
                      const std::set<rpc_address> &min_nodes,
                      const int32_t app_id,
                      const partition_set &selected_pid,
                      /*out*/ move_info &move_info);
    void get_max_load_disk_set(const cluster_migration_info &cluster_info,
                               const std::set<rpc_address> &max_nodes,
                               const int32_t app_id,
                               /*out*/ std::set<app_disk_info> &max_load_disk_set);
    std::map<std::string, partition_set> get_disk_partitions_map(
        const cluster_migration_info &cluster_info, const rpc_address &addr, const int32_t app_id);
    bool pick_up_partition(const cluster_migration_info &cluster_info,
                           const rpc_address &min_node_addr,
                           const partition_set &max_load_partitions,
                           const partition_set &selected_pid,
                           /*out*/ gpid &picked_pid);
    bool apply_move(const move_info &move,
                    /*out*/ partition_set &selected_pids,
                    /*out*/ migration_list &list,
                    /*out*/ cluster_migration_info &cluster_info);

    struct app_migration_info
    {
        int32_t app_id;
        std::string app_name;
        std::vector<std::map<rpc_address, partition_status::type>> partitions;
        std::map<rpc_address, uint32_t> replicas_count;
        bool operator<(const app_migration_info &another) const
        {
            if (app_id < another.app_id)
                return true;
            return false;
        }
        bool operator==(const app_migration_info &another) const
        {
            return app_id == another.app_id;
        }
        partition_status::type get_partition_status(int32_t pidx, rpc_address addr)
        {
            for (const auto &kv : partitions[pidx]) {
                if (kv.first == addr) {
                    return kv.second;
                }
            }
            return partition_status::PS_INACTIVE;
        }
    };

    struct node_migration_info
    {
        rpc_address address;
        // key-disk tag, value-partition set
        std::map<std::string, partition_set> partitions;
        partition_set future_partitions;
        bool operator<(const node_migration_info &another) const
        {
            return address < another.address;
        }
        bool operator==(const node_migration_info &another) const
        {
            return address == another.address;
        }
    };

    struct cluster_migration_info
    {
        balance_type type;
        std::map<int32_t, uint32_t> apps_skew;
        std::map<int32_t, app_migration_info> apps_info;
        std::map<rpc_address, node_migration_info> nodes_info;
        std::map<rpc_address, uint32_t> replicas_count;
    };

    struct app_disk_info
    {
        int32_t app_id;
        rpc_address node;
        std::string disk_tag;
        partition_set partitions;
        bool operator==(const app_disk_info &another) const
        {
            return app_id == another.app_id && node == another.node && disk_tag == another.disk_tag;
        }
        bool operator<(const app_disk_info &another) const
        {
            if (app_id < another.app_id || (app_id == another.app_id && node < another.node) ||
                (app_id == another.app_id && node == another.node && disk_tag < another.disk_tag))
                return true;
            return false;
        }
    };

    struct move_info
    {
        gpid pid;
        rpc_address source_node;
        std::string source_disk_tag;
        rpc_address target_node;
        balance_type type;
    };

    FRIEND_TEST(cluster_balance_policy, app_migration_info);
    FRIEND_TEST(cluster_balance_policy, node_migration_info);
    FRIEND_TEST(cluster_balance_policy, get_skew);
    FRIEND_TEST(cluster_balance_policy, get_partition_count);
    FRIEND_TEST(cluster_balance_policy, get_app_migration_info);
    FRIEND_TEST(cluster_balance_policy, get_node_migration_info);
    FRIEND_TEST(cluster_balance_policy, get_disk_partitions_map);
    FRIEND_TEST(cluster_balance_policy, get_max_load_disk_set);
    FRIEND_TEST(cluster_balance_policy, apply_move);
    FRIEND_TEST(cluster_balance_policy, pick_up_partition);
    FRIEND_TEST(cluster_balance_policy, execute_balance);
};
} // namespace replication
} // namespace dsn
