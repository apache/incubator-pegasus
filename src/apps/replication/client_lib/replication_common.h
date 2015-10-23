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
# pragma once

# include <dsn/dist/replication.h>
# include <string>
# include "replication_ds.h"

using namespace ::dsn::service;

namespace dsn { namespace replication {

inline bool operator < (const global_partition_id& l, const global_partition_id& r)
{
    return l.app_id < r.app_id || (l.app_id == r.app_id && l.pidx < r.pidx);
}

inline bool operator == (const global_partition_id& l, const global_partition_id& r)
{
    return l.app_id == r.app_id && l.pidx == r.pidx;
}

inline int gpid_to_hash(global_partition_id gpid)
{
    return static_cast<int>(gpid.app_id ^ gpid.pidx);
}

typedef std::unordered_map<::dsn::rpc_address, partition_status> node_statuses;
typedef std::unordered_map<::dsn::rpc_address, dsn::task_ptr> node_tasks;

class replication_options
{
public:
    std::string working_dir;
    std::vector<::dsn::rpc_address> meta_servers;

    int32_t prepare_timeout_ms_for_secondaries;
    int32_t prepare_timeout_ms_for_potential_secondaries;
        
    int32_t staleness_for_commit;
    int32_t max_mutation_count_in_prepare_list;
    int32_t mutation_2pc_min_replica_count;
    
    bool    group_check_disabled;
    int32_t group_check_internal_ms;

    int32_t gc_interval_ms;
    bool    gc_disabled;
    int32_t gc_memory_replica_interval_ms;
    int32_t gc_disk_error_replica_interval_seconds;
    
    bool    fd_disabled;
    int32_t fd_check_interval_seconds;
    int32_t fd_beacon_interval_seconds;
    int32_t fd_lease_seconds;
    int32_t fd_grace_seconds;

    bool    log_enable_private_prepare;

    int32_t log_file_size_mb;
    int32_t log_batch_buffer_MB;
    int32_t log_pending_max_ms;
    int32_t log_file_size_mb_private;
    int32_t log_buffer_size_mb_private;
    int32_t log_pending_max_ms_private;

    int32_t config_sync_interval_ms;
    bool    config_sync_disabled;

public:
    replication_options();
    void initialize();
    ~replication_options();

private:
    void read_meta_servers();
    void sanity_check();
};

class replica_helper
{
public:
    static bool remove_node(::dsn::rpc_address node, /*inout*/ std::vector<::dsn::rpc_address>& nodeList);
    static bool get_replica_config(const partition_configuration& partition_config, ::dsn::rpc_address node, /*out*/ replica_configuration& replica_config);
};

}} // namespace
