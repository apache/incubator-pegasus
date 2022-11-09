/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <vector>
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"
#include "utils/error_code.h"
#include "utils/threadpool_code.h"
#include "runtime/task/task_code.h"
#include "common/gpid.h"
#include "runtime/rpc/serialization.h"
#include "runtime/rpc/rpc_stream.h"
#include "runtime/serverlet.h"
#include "runtime/service_app.h"
#include "utils/rpc_address.h"
#include "meta/meta_data.h"
#include "common/fs_manager.h"

typedef std::map<dsn::rpc_address, std::shared_ptr<dsn::replication::fs_manager>> nodes_fs_manager;

inline dsn::replication::fs_manager *get_fs_manager(nodes_fs_manager &nfm,
                                                    const dsn::rpc_address &node)
{
    auto iter = nfm.find(node);
    if (nfm.end() == iter)
        return nullptr;
    return iter->second.get();
}

// Generates a random number between [min, max]
uint32_t random32(uint32_t min, uint32_t max);

// Generates a random number [min_count, max_count] of node addresses
// each node is given a random port value in range of [min_count, max_count]
void generate_node_list(/*out*/ std::vector<dsn::rpc_address> &output_list,
                        int min_count,
                        int max_count);

// Generates `size` of node addresses, each with port value in range [start_port, start_port + size]
inline std::vector<dsn::rpc_address> generate_node_list(size_t size, int start_port = 12321)
{
    std::vector<dsn::rpc_address> result;
    result.resize(size);
    for (int i = 0; i < size; ++i)
        result[i].assign_ipv4("127.0.0.1", static_cast<uint16_t>(start_port + i + 1));
    return result;
}

// This func randomly picks 3 nodes from `node_list` for each of the partition of the app.
// For each partition, it picks one node as primary, the others as secondaries.
// REQUIRES: node_list.size() >= 3
void generate_app(
    /*out*/ std::shared_ptr<dsn::replication::app_state> &app,
    const std::vector<dsn::rpc_address> &node_list);

void generate_node_mapper(
    /*out*/ dsn::replication::node_mapper &output_nodes,
    const dsn::replication::app_mapper &input_apps,
    const std::vector<dsn::rpc_address> &input_node_list);

void generate_app_serving_replica_info(/*out*/ std::shared_ptr<dsn::replication::app_state> &app,
                                       int total_disks);

void generate_node_fs_manager(const dsn::replication::app_mapper &apps,
                              const dsn::replication::node_mapper &nodes,
                              /*out*/ nodes_fs_manager &nfm,
                              int total_disks);

void generate_apps(/*out*/ dsn::replication::app_mapper &apps,
                   const std::vector<dsn::rpc_address> &node_list,
                   int apps_count,
                   int disks_per_node,
                   std::pair<uint32_t, uint32_t> partitions_range,
                   bool generate_serving_info);

// when the test need to track the disk info, please input the fs_manager of all disks,
// the check_apply routine will modify it accordingly.
// if track disk info is not necessary, please input a nullptr.
void migration_check_and_apply(
    /*in-out*/ dsn::replication::app_mapper &apps,
    /*in-out*/ dsn::replication::node_mapper &nodes,
    /*in-out*/ dsn::replication::migration_list &ml,
    /*in-out*/ nodes_fs_manager *manager);

// when the test need to track the disk info, please input the fs_manager of all disks,
// the check_apply routine will modify it accordingly.
// if track disk info is not necessary, please input a nullptr.
void proposal_action_check_and_apply(const dsn::replication::configuration_proposal_action &act,
                                     const dsn::gpid &pid,
                                     dsn::replication::app_mapper &apps,
                                     dsn::replication::node_mapper &nodes,
                                     nodes_fs_manager *manager);

void track_disk_info_check_and_apply(const dsn::replication::configuration_proposal_action &act,
                                     const dsn::gpid &pid,
                                     /*in-out*/ dsn::replication::app_mapper &apps,
                                     /*in-out*/ dsn::replication::node_mapper &nodes,
                                     /*in-out*/ nodes_fs_manager &manager);

void app_mapper_compare(const dsn::replication::app_mapper &mapper1,
                        const dsn::replication::app_mapper &mapper2);

void verbose_apps(const dsn::replication::app_mapper &input_apps);

bool spin_wait_condition(const std::function<bool()> &pred, int seconds);
