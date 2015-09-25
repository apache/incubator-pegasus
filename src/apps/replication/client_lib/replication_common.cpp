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
    prepare_list_max_size_mb = 250;
    prepare_ack_on_secondary_before_logging_allowed = false;

    staleness_for_commit = 10;
    max_mutation_count_in_prepare_list = 110;
    
    mutation_2pc_min_replica_count = 1;    

    group_check_internal_ms = 100000;
    group_check_disabled = false;

    gc_interval_ms = 30 * 1000; // 30000 milliseconds
    gc_disabled = false;
    gc_memory_replica_interval_ms = 5 * 60 * 1000; // 5 minutes
    gc_disk_error_replica_interval_seconds = 48 * 3600 * 1000; // 48 hrs
    
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
    log_batch_write = true;
    log_buffer_size_mb_private = 1;
    log_pending_max_ms_private = 100;
    log_file_size_mb_private = 32;
    log_batch_write_private = true;
    log_per_app_commit = true;
    
    config_sync_interval_ms = 30000;
    config_sync_disabled = false;
}

replication_options::~replication_options()
{
}

void replication_options::read_meta_servers()
{
    // read meta_servers from machine list file
    meta_servers.clear();

    const char* server_ss[10];
    int capacity = 10, need_count;
    need_count = dsn_config_get_all_keys("replication.meta_servers", server_ss, &capacity);
    dassert(need_count <= capacity, "too many meta servers specified");

    for (int i = 0; i < capacity; i++)
    {
        std::string s(server_ss[i]);
        // name:port
        auto pos1 = s.find_first_of(':');
        if (pos1 != std::string::npos)
        {
            ::dsn::rpc_address ep(HOST_TYPE_IPV4, s.substr(0, pos1).c_str(), atoi(s.substr(pos1 + 1).c_str()));
            meta_servers.push_back(ep);
        }
    }
}

void replication_options::initialize()
{
    prepare_timeout_ms_for_secondaries =
        (int)dsn_config_get_value_uint64("replication", 
        "prepare_timeout_ms_for_secondaries", 
        prepare_timeout_ms_for_secondaries,
        "timeout (ms) for prepare message to secondaries in two phase commit"
        );
    prepare_timeout_ms_for_potential_secondaries = 
        (int)dsn_config_get_value_uint64("replication", 
        "prepare_timeout_ms_for_potential_secondaries",
        prepare_timeout_ms_for_potential_secondaries,
        "timeout (ms) for prepare message to potential secondaries in two phase commit"
        );
    prepare_ack_on_secondary_before_logging_allowed =
        dsn_config_get_value_bool("replication", 
        "prepare_ack_on_secondary_before_logging_allowed", 
        prepare_ack_on_secondary_before_logging_allowed,
        "whether we need to ensure logging is completed before acking a prepare message"
        );

    staleness_for_commit =
        (int)dsn_config_get_value_uint64("replication", 
        "staleness_for_commit", 
        staleness_for_commit,
        "how many concurrent two phase commit rounds are allowed"
        );
    max_mutation_count_in_prepare_list =
        (int)dsn_config_get_value_uint64("replication", 
        "max_mutation_count_in_prepare_list", 
        max_mutation_count_in_prepare_list,
        "maximum number of mutations in prepare list"
        );
    mutation_2pc_min_replica_count =
        (int)dsn_config_get_value_uint64("replication", 
        "mutation_2pc_min_replica_count",
        mutation_2pc_min_replica_count,
        "minimum number of alive replicas under which write is allowed"
        );
    prepare_list_max_size_mb =
        (int)dsn_config_get_value_uint64("replication", 
        "prepare_list_max_size_mb",
        prepare_list_max_size_mb,
        "maximum size (Mb) for prepare list"
        );
    group_check_internal_ms =
        (int)dsn_config_get_value_uint64("replication",
        "group_check_internal_ms", 
        group_check_internal_ms,
        "every what period (ms) we check the replica healthness"
        );
    group_check_disabled =
        dsn_config_get_value_bool("replication",
        "group_check_disabled", 
        group_check_disabled,
        "whether group check is disabled"
        );
    gc_interval_ms =
        (int)dsn_config_get_value_uint64("replication", 
        "gc_interval_ms", 
        gc_interval_ms,
        "every what period (ms) we do garbage collection for dead replicas, on-disk state, log, etc."
        );
    gc_memory_replica_interval_ms =
        (int)dsn_config_get_value_uint64("replication", 
        "gc_memory_replica_interval_ms", 
        gc_memory_replica_interval_ms,
        "after closing a healthy replica (due to LB), the replica will remain in memory for this long (ms) for quick recover"
        );
    gc_disk_error_replica_interval_seconds =
        (int)dsn_config_get_value_uint64("replication", 
        "gc_disk_error_replica_interval_seconds", 
        gc_disk_error_replica_interval_seconds,
        "error replica are deleted after they have been closed and lasted on disk this long (seconds)"
        );
    gc_disabled =
        dsn_config_get_value_bool("replication", "gc_disabled", gc_disabled,
        "whether to disable garbage collection"
        );
    fd_disabled =
        dsn_config_get_value_bool("replication", "fd_disabled", fd_disabled,
        "whether to disable failure detection"
        );
    //_options.meta_servers = ...;
    fd_check_interval_seconds =
        (int)dsn_config_get_value_uint64("replication", 
        "fd_check_interval_seconds", 
        fd_check_interval_seconds,
        "every this period(seconds) the FD will check healthness of remote peers"
        );
    fd_beacon_interval_seconds =
        (int)dsn_config_get_value_uint64("replication", 
        "fd_beacon_interval_seconds", 
        fd_beacon_interval_seconds,
        "every this period(seconds) the FD sends beacon message to remote peers"
        );
    fd_lease_seconds =
        (int)dsn_config_get_value_uint64("replication", 
        "fd_lease_seconds", 
        fd_lease_seconds,
        "lease (seconds) get from remote FD master"
        );
    fd_grace_seconds =
        (int)dsn_config_get_value_uint64("replication", 
        "fd_grace_seconds", 
        fd_grace_seconds,
        "grace (seconds) assigned to remote FD slaves (grace > lease)"
        );
    working_dir = dsn_config_get_value_string("replication", 
        "working_dir", 
        working_dir.c_str(),
        "root working directory for replication"
        );
    
    log_file_size_mb =
        (int)dsn_config_get_value_uint64("replication", 
        "log_file_size_mb", 
        log_file_size_mb,
        "maximum log segment file size (MB)"
        );
    log_buffer_size_mb =
        (int)dsn_config_get_value_uint64("replication", 
        "log_buffer_size_mb", 
        log_buffer_size_mb,
        "log buffer size (MB) for batching incoming logs"
        );
    log_pending_max_ms =
        (int)dsn_config_get_value_uint64("replication", 
        "log_pending_max_ms", 
        log_pending_max_ms,
        "maximum duration (ms) the log entries reside in the buffer for batching"
        );
    log_batch_write = 
        dsn_config_get_value_bool("replication", 
        "log_batch_write", 
        log_batch_write,
        "whether to batch write the incoming logs"
        );

    log_file_size_mb_private =
        (int)dsn_config_get_value_uint64("replication",
        "log_file_size_mb_private",
        log_file_size_mb_private,
        "maximum log segment file size (MB) for private log"
        );
    log_buffer_size_mb_private =
        (int)dsn_config_get_value_uint64("replication",
        "log_buffer_size_mb_private",
        log_buffer_size_mb_private,
        "log buffer size (MB) for batching incoming logs for private log"
        );
    log_pending_max_ms_private =
        (int)dsn_config_get_value_uint64("replication",
        "log_pending_max_ms_private",
        log_pending_max_ms_private,
        "maximum duration (ms) the log entries reside in the buffer for batching for private log"
        );
    log_batch_write_private =
        dsn_config_get_value_bool("replication",
        "log_batch_write_private",
        log_batch_write_private,
        "whether to batch write the incoming logs for private log"
        );

    log_per_app_commit =
        dsn_config_get_value_bool("replication",
        "log_per_app_commit",
        log_per_app_commit,
        "whether to log committed mutations for each app, "
        "which is used for easier learning"
        );

     config_sync_disabled =
        dsn_config_get_value_bool("replication", 
        "config_sync_disabled",
        config_sync_disabled,
        "whether to disable replica configuration periodical sync with the meta server"
        );
    //_options.meta_servers = ...;
    config_sync_interval_ms =
        (int)dsn_config_get_value_uint64("replication", 
        "config_sync_interval_ms", 
        config_sync_interval_ms,
        "every this period(ms) the replica syncs replica configuration with the meta server"
        );
        
    read_meta_servers();

    sanity_check();
}

void replication_options::sanity_check()
{
    dassert (max_mutation_count_in_prepare_list >= staleness_for_commit, "");
}
   
/*static*/ bool replica_helper::remove_node(const ::dsn::rpc_address& node, /*inout*/ std::vector<::dsn::rpc_address>& nodeList)
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

/*static*/ bool replica_helper::get_replica_config(const partition_configuration& partition_config, const ::dsn::rpc_address& node, /*out*/ replica_configuration& replica_config)
{
    replica_config.gpid = partition_config.gpid;
    replica_config.primary = partition_config.primary;
    replica_config.ballot = partition_config.ballot;

    if (node == partition_config.primary)
    {
        replica_config.status = PS_PRIMARY;
        return true;
    }
    else if (std::find(partition_config.secondaries.begin(), partition_config.secondaries.end(), node) != partition_config.secondaries.end())
    {
        replica_config.status = PS_SECONDARY;
        return true;
    }
    else if (std::find(partition_config.drop_outs.begin(), partition_config.drop_outs.end(), node) != partition_config.drop_outs.end())
    {
        replica_config.status = PS_INACTIVE;
        return true;
    }
    else
    {
        replica_config.status = PS_INACTIVE;
        return false;
    }
}

}} // end namespace
