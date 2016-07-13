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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include "replication_common.h"

# ifdef __TITLE__
# undef __TITLE__
# endif
# define __TITLE__ "replication.common"

namespace dsn { namespace replication {

replication_options::replication_options()
{
    empty_write_disabled = false;
    prepare_timeout_ms_for_secondaries = 1000;
    prepare_timeout_ms_for_potential_secondaries = 3000;

    batch_write_disabled = false;
    staleness_for_commit = 10;
    max_mutation_count_in_prepare_list = 110;
    mutation_2pc_min_replica_count = 2;

    group_check_disabled = false;
    group_check_interval_ms = 10000;

    checkpoint_disabled = false;
    checkpoint_interval_seconds = 100;
    checkpoint_min_decree_gap = 10000;
    checkpoint_max_interval_hours = 24; // at least one checkpoint per day

    gc_disabled = false;
    gc_interval_ms = 30 * 1000; // 30000 milliseconds
    gc_memory_replica_interval_ms = 5 * 60 * 1000; // 5 minutes
    gc_disk_error_replica_interval_seconds = 48 * 3600 * 1000; // 48 hrs

    fd_disabled = false;
    fd_check_interval_seconds = 5;
    fd_beacon_interval_seconds = 3;
    fd_lease_seconds = 10;
    fd_grace_seconds = 15;

    log_private_file_size_mb = 32;
    log_private_batch_buffer_kb = 512;
    log_private_force_flush = true;

    log_shared_file_size_mb = 32;
    log_shared_batch_buffer_kb = 0;
    log_shared_force_flush = false;

    config_sync_disabled = false;
    config_sync_interval_ms = 30000;

    lb_interval_ms = 10000;
}

replication_options::~replication_options()
{
}

void replication_options::initialize()
{
    dsn_app_info app_info;
    bool r = dsn_get_current_app_info(&app_info);
    dassert(r, "get current app info failed");
    app_name = app_info.name;
    app_dir = app_info.data_dir;

    // slog_dir:
    // - if config[slog_dir] is empty: "app_dir/slog"
    // - else: "config[slog_dir]/app_name/slog"
    slog_dir = dsn_config_get_value_string("replication", "slog_dir", "", "shared log directory");
    if (slog_dir.empty())
    {
        slog_dir = app_dir;
    }
    else
    {
        slog_dir = utils::filesystem::path_combine(slog_dir, app_name);
    }
    slog_dir = utils::filesystem::path_combine(slog_dir, "slog");

    // data_dirs
    // - if config[data_dirs] is empty: "app_dir/reps"
    // - else: "config[data_dirs]/app_name/reps"
    std::string dirs_str = dsn_config_get_value_string("replication", "data_dirs", "", "replica directory list");
    std::vector<std::string> dirs;
    ::dsn::utils::split_args(dirs_str.c_str(), dirs, ',');
    if (dirs.empty())
    {
        dirs.push_back(app_dir);
    }
    else
    {
        for (auto& dir : dirs)
        {
            dir = utils::filesystem::path_combine(dir, app_name);
        }
    }
    for (auto& dir : dirs)
    {
        data_dirs.push_back(utils::filesystem::path_combine(dir, "reps"));
    }

    empty_write_disabled =
        dsn_config_get_value_bool("replication",
        "empty_write_disabled",
        empty_write_disabled,
        "whether to disable empty write, default is false"
        );
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

    batch_write_disabled =
        dsn_config_get_value_bool("replication",
        "batch_write_disabled",
        batch_write_disabled,
        "whether to disable auto-batch of replicated write requests"
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

    group_check_disabled =
        dsn_config_get_value_bool("replication",
        "group_check_disabled",
        group_check_disabled,
        "whether group check is disabled"
        );
    group_check_interval_ms =
        (int)dsn_config_get_value_uint64("replication",
        "group_check_interval_ms", 
        group_check_interval_ms,
        "every what period (ms) we check the replica healthness"
        );

    checkpoint_disabled =
        dsn_config_get_value_bool("replication",
        "checkpoint_disabled",
        checkpoint_disabled,
        "whether checkpoint is disabled"
        );
    checkpoint_interval_seconds =
        (int)dsn_config_get_value_uint64("replication",
        "checkpoint_interval_seconds",
        checkpoint_interval_seconds,
        "every what period (seconds) we do checkpoints for replicated apps"
        ); 
    checkpoint_min_decree_gap = 
        (int64_t)dsn_config_get_value_uint64("replication",
        "checkpoint_min_decree_gap",
        checkpoint_min_decree_gap,
        "minimum decree gap that triggers checkpoint"
        );
    checkpoint_max_interval_hours = 
        (int)dsn_config_get_value_uint64("replication",
        "checkpoint_max_interval_hours",
        checkpoint_max_interval_hours,
        "maximum time interval (hours) where a new checkpoint must be created"
        );

    gc_disabled =
        dsn_config_get_value_bool("replication",
        "gc_disabled",
        gc_disabled,
        "whether to disable garbage collection"
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

    fd_disabled =
        dsn_config_get_value_bool("replication",
        "fd_disabled",
        fd_disabled,
        "whether to disable failure detection"
        );
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

    log_private_file_size_mb =
        (int)dsn_config_get_value_uint64("replication",
        "log_private_file_size_mb",
        log_private_file_size_mb,
        "private log maximum segment file size (MB)"
        );
    log_private_batch_buffer_kb =
        (int)dsn_config_get_value_uint64("replication",
        "log_private_batch_buffer_kb",
        log_private_batch_buffer_kb,
        "private log buffer size (KB) for batching incoming logs"
        );
    log_private_force_flush =
        dsn_config_get_value_bool("replication",
        "log_private_force_flush",
        log_private_force_flush,
        "when write private log, whether to flush file after write done"
        );

    log_shared_file_size_mb =
        (int)dsn_config_get_value_uint64("replication", 
        "log_shared_file_size_mb",
        log_shared_file_size_mb,
        "shared log maximum segment file size (MB)"
        );
    log_shared_batch_buffer_kb =
        (int)dsn_config_get_value_uint64("replication", 
        "log_batch_buffer_KB_shared", 
        log_shared_batch_buffer_kb,
        "shared log buffer size (KB) for batching incoming logs"
        );
    log_shared_force_flush =
        dsn_config_get_value_bool("replication",
        "log_shared_force_flush",
        log_shared_force_flush,
        "when write shared log, whether to flush file after write done"
        );

    config_sync_disabled =
        dsn_config_get_value_bool("replication", 
        "config_sync_disabled",
        config_sync_disabled,
        "whether to disable replica configuration periodical sync with the meta server"
        );
    config_sync_interval_ms =
        (int)dsn_config_get_value_uint64("replication", 
        "config_sync_interval_ms", 
        config_sync_interval_ms,
        "every this period(ms) the replica syncs replica configuration with the meta server"
        );

    lb_interval_ms =
        (int)dsn_config_get_value_uint64("replication",
        "lb_interval_ms",
        lb_interval_ms,
        "every this period(ms) the meta server will do load balance"
        );
    
    replica_helper::load_meta_servers(meta_servers);

    sanity_check();
}

void replication_options::sanity_check()
{
    dassert (max_mutation_count_in_prepare_list >= staleness_for_commit, "");
}
   
/*static*/ bool replica_helper::remove_node(::dsn::rpc_address node, /*inout*/ std::vector< ::dsn::rpc_address>& nodeList)
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

/*static*/ bool replica_helper::get_replica_config(const partition_configuration& partition_config, ::dsn::rpc_address node, /*out*/ replica_configuration& replica_config)
{
    replica_config.pid = partition_config.pid;
    replica_config.primary = partition_config.primary;
    replica_config.ballot = partition_config.ballot;
    replica_config.learner_signature = invalid_signature;

    if (node == partition_config.primary)
    {
        replica_config.status = partition_status::PS_PRIMARY;
        return true;
    }
    else if (std::find(partition_config.secondaries.begin(), partition_config.secondaries.end(), node) != partition_config.secondaries.end())
    {
        replica_config.status = partition_status::PS_SECONDARY;
        return true;
    }
    else
    {
        replica_config.status = partition_status::PS_INACTIVE;
        return false;
    }
}

void replica_helper::load_meta_servers(/*out*/ std::vector<dsn::rpc_address>& servers, const char* section, const char* key)
{
    servers.clear();
    std::string server_list = dsn_config_get_value_string(section, key, "", "");
    std::vector<std::string> lv;
    ::dsn::utils::split_args(server_list.c_str(), lv, ',');
    for (auto& s : lv)
    {
        // name:port
        auto pos1 = s.find_first_of(':');
        if (pos1 != std::string::npos)
        {
            ::dsn::rpc_address ep(s.substr(0, pos1).c_str(), atoi(s.substr(pos1 + 1).c_str()));
            servers.push_back(ep);
        }
    }
    dassert(servers.size() > 0, "no meta server specified in config [%s].%s", section, key);
}

}} // end namespace
