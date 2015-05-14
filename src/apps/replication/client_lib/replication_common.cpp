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
#include "replication_common.h"
#include <dsn/internal/configuration.h>

namespace dsn { namespace replication {

replication_options::replication_options()
{
    prepare_timeout_ms_for_secondaries = 1000;
    prepare_timeout_ms_for_potential_secondaries = 3000;
    staleness_for_commit = 10;
    staleness_for_start_prepare_for_potential_secondary = 110;
    mutation_2pc_min_replica_count = 1;
    preapre_list_max_size_mb = 250;
    group_check_internal_ms = 100000;
    group_check_disabled = false;
    gc_interval_ms = 30 * 1000; // 30000 milliseconds
    gc_disabled = false;
    gc_memory_replica_interval_ms = 5 * 60 * 1000; // 5 minutes
    gc_disk_error_replica_interval_seconds = 48 * 3600 * 1000; // 48 hrs
    log_batch_write = true;
    log_max_concurrent_writes = 4;
    fd_disabled = false;
    //_options.meta_servers = ...;
    fd_check_interval_seconds = 5;
    fd_beacon_interval_seconds = 3;
    fd_lease_seconds = 10;
    fd_grace_seconds = 15;
    working_dir = ".";
        
    log_buffer_size_mb = 1;
    log_pending_max_ms = 100;
    log_file_size_mb = 32;
    
    config_sync_interval_ms = 30000;
    config_sync_disabled = false;
}

replication_options::~replication_options()
{
}

void replication_options::read_meta_servers(configuration_ptr config)
{
    // read meta_servers from machine list file
    meta_servers.clear();

    std::vector<std::string> servers;
    config->get_all_keys("replication.meta_servers", servers);
    for (auto& s : servers)
    {
        // name:port
        auto pos1 = s.find_first_of(':');
        if (pos1 != std::string::npos)
        {
            end_point ep(s.substr(0, pos1).c_str(), atoi(s.substr(pos1 + 1).c_str()));
            meta_servers.push_back(ep);
        }
    }
}

void replication_options::initialize(configuration_ptr config)
{
    prepare_timeout_ms_for_secondaries =
        config->get_value<uint32_t>("replication", "prepare_timeout_ms_for_secondaries", prepare_timeout_ms_for_secondaries);
    prepare_timeout_ms_for_potential_secondaries = 
        config->get_value<uint32_t>("replication", "prepare_timeout_ms_for_potential_secondaries", prepare_timeout_ms_for_potential_secondaries);

    staleness_for_commit =
        config->get_value<uint32_t>("replication", "staleness_for_commit", staleness_for_commit);
    staleness_for_start_prepare_for_potential_secondary =
        config->get_value<uint32_t>("replication", "staleness_for_start_prepare_for_potential_secondary", staleness_for_start_prepare_for_potential_secondary);
    mutation_2pc_min_replica_count =
        config->get_value<uint32_t>("replication", "mutation_2pc_min_replica_count", mutation_2pc_min_replica_count);
    preapre_list_max_size_mb =
        config->get_value<uint32_t>("replication", "preapre_list_max_size_mb", preapre_list_max_size_mb);
    group_check_internal_ms =
        config->get_value<uint32_t>("replication", "group_check_internal_ms", group_check_internal_ms);
    group_check_disabled =
        config->get_value<bool>("replication", "group_check_disabled", group_check_disabled);
    gc_interval_ms =
        config->get_value<uint32_t>("replication", "gc_interval_ms", gc_interval_ms);
    gc_memory_replica_interval_ms =
        config->get_value<uint32_t>("replication", "gc_memory_replica_interval_ms", gc_memory_replica_interval_ms);
    gc_disk_error_replica_interval_seconds =
        config->get_value<uint32_t>("replication", "gc_disk_error_replica_interval_seconds", gc_disk_error_replica_interval_seconds);
    gc_disabled =
        config->get_value<bool>("replication", "gc_disabled", gc_disabled);

    fd_disabled =
        config->get_value<bool>("replication", "fd_disabled", fd_disabled);
    //_options.meta_servers = ...;
    fd_check_interval_seconds =
        config->get_value<uint32_t>("replication", "fd_check_interval_seconds", fd_check_interval_seconds);
    fd_beacon_interval_seconds =
        config->get_value<uint32_t>("replication", "fd_beacon_interval_seconds", fd_beacon_interval_seconds);
    fd_lease_seconds =
        config->get_value<uint32_t>("replication", "fd_lease_seconds", fd_lease_seconds);
    fd_grace_seconds =
        config->get_value<uint32_t>("replication", "fd_grace_seconds", fd_grace_seconds);
    working_dir = config->get_string_value("replication", "working_dir", working_dir.c_str());
    
    log_file_size_mb =
        config->get_value<uint32_t>("replication", "log_file_size_mb", log_file_size_mb);
    log_buffer_size_mb =
        config->get_value<uint32_t>("replication", "log_buffer_size_mb", log_buffer_size_mb);
    log_pending_max_ms =
        config->get_value<uint32_t>("replication", "log_pending_max_ms", log_pending_max_ms);
    log_batch_write = 
        config->get_value<bool>("replication", "log_batch_write", log_batch_write);
    log_max_concurrent_writes =
        config->get_value<uint32_t>("replication", "log_max_concurrent_writes", log_max_concurrent_writes);

     config_sync_disabled =
        config->get_value<bool>("replication", "config_sync_disabled", config_sync_disabled);
    //_options.meta_servers = ...;
    config_sync_interval_ms =
        config->get_value<uint32_t>("replication", "config_sync_interval_ms", config_sync_interval_ms);
        
    read_meta_servers(config);

    sanity_check();
}

void replication_options::sanity_check()
{
    dassert (staleness_for_start_prepare_for_potential_secondary >= staleness_for_commit, "");
}
   
/*static*/ bool replica_helper::remove_node(const end_point& node, __inout_param std::vector<end_point>& nodeList)
{
    auto it = std::find(nodeList.begin(), nodeList.end(), node);
    if (it != nodeList.end())
    {
        nodeList.erase(it);
        return true;
    }
    else
    {
        return false;
    }
}

/*static*/ bool replica_helper::get_replica_config(const partition_configuration& partitionConfig, const end_point& node, __out_param replica_configuration& replicaConfig)
{
    replicaConfig.gpid = partitionConfig.gpid;
    replicaConfig.primary = partitionConfig.primary;
    replicaConfig.ballot = partitionConfig.ballot;

    if (node == partitionConfig.primary)
    {
        replicaConfig.status = PS_PRIMARY;
        return true;
    }
    else if (std::find(partitionConfig.secondaries.begin(), partitionConfig.secondaries.end(), node) != partitionConfig.secondaries.end())
    {
        replicaConfig.status = PS_SECONDARY;
        return true;
    }
    else if (std::find(partitionConfig.drop_outs.begin(), partitionConfig.drop_outs.end(), node) != partitionConfig.drop_outs.end())
    {
        replicaConfig.status = PS_INACTIVE;
        return true;
    }
    else
    {
        replicaConfig.status = PS_INACTIVE;
        return false;
    }
}

}} // end namespace
