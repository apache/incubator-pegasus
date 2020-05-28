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

#include <dsn/dist/replication.h>
#include <string>

namespace dsn {
namespace replication {

typedef std::unordered_map<::dsn::rpc_address, partition_status::type> node_statuses;
typedef std::unordered_map<::dsn::rpc_address, dsn::task_ptr> node_tasks;

typedef rpc_holder<start_bulk_load_request, start_bulk_load_response> start_bulk_load_rpc;
typedef rpc_holder<bulk_load_request, bulk_load_response> bulk_load_rpc;

class replication_options
{
public:
    std::vector<::dsn::rpc_address> meta_servers;

    std::string app_name;
    std::string app_dir;
    std::string slog_dir;
    std::vector<std::string> data_dirs;
    std::vector<std::string> data_dir_tags;

    bool deny_client_on_start;
    bool verbose_client_log_on_start;
    bool verbose_commit_log_on_start;
    bool delay_for_fd_timeout_on_start;
    bool empty_write_disabled;
    bool duplication_enabled;

    int32_t prepare_timeout_ms_for_secondaries;
    int32_t prepare_timeout_ms_for_potential_secondaries;
    int32_t prepare_decree_gap_for_debug_logging;

    bool batch_write_disabled;
    int32_t staleness_for_commit;
    int32_t max_mutation_count_in_prepare_list;
    int32_t mutation_2pc_min_replica_count;

    bool group_check_disabled;
    int32_t group_check_interval_ms;

    bool checkpoint_disabled;
    int32_t checkpoint_interval_seconds;
    int64_t checkpoint_min_decree_gap;
    int32_t checkpoint_max_interval_hours;

    bool gc_disabled;
    int32_t gc_interval_ms;
    int32_t gc_memory_replica_interval_ms;
    int32_t gc_disk_error_replica_interval_seconds;
    int32_t gc_disk_garbage_replica_interval_seconds;

    bool disk_stat_disabled;
    int32_t disk_stat_interval_seconds;

    bool fd_disabled;
    int32_t fd_check_interval_seconds;
    int32_t fd_beacon_interval_seconds;
    int32_t fd_lease_seconds;
    int32_t fd_grace_seconds;

    int32_t log_private_file_size_mb;
    int32_t log_private_batch_buffer_kb;
    int32_t log_private_batch_buffer_count;
    int32_t log_private_batch_buffer_flush_interval_ms;
    int32_t log_private_reserve_max_size_mb;
    int32_t log_private_reserve_max_time_seconds;

    int32_t log_shared_file_size_mb;
    int32_t log_shared_file_count_limit;
    int32_t log_shared_batch_buffer_kb;
    bool log_shared_force_flush;
    int32_t log_shared_pending_size_throttling_threshold_kb;
    int32_t log_shared_pending_size_throttling_delay_ms;

    bool config_sync_disabled;
    int32_t config_sync_interval_ms;

    bool mem_release_enabled;
    int32_t mem_release_check_interval_ms;
    int32_t mem_release_max_reserved_mem_percentage;

    int32_t lb_interval_ms;

    int32_t learn_app_max_concurrent_count;

    std::string cold_backup_root;
    int32_t max_concurrent_uploading_file_count;
    int32_t cold_backup_checkpoint_reserve_minutes;

    std::string bulk_load_provider_root;
    int32_t max_concurrent_bulk_load_downloading_count;

public:
    replication_options();
    void initialize();
    ~replication_options();

private:
    void sanity_check();
};

typedef rpc_holder<register_child_request, register_child_response> register_child_rpc;

extern const char *partition_status_to_string(partition_status::type status);

class cold_backup_constant
{
public:
    static const std::string APP_METADATA;
    static const std::string APP_BACKUP_STATUS;
    static const std::string CURRENT_CHECKPOINT;
    static const std::string BACKUP_METADATA;
    static const std::string BACKUP_INFO;
    static const int32_t PROGRESS_FINISHED;
};

class backup_restore_constant
{
public:
    static const std::string FORCE_RESTORE;
    static const std::string BLOCK_SERVICE_PROVIDER;
    static const std::string CLUSTER_NAME;
    static const std::string POLICY_NAME;
    static const std::string APP_NAME;
    static const std::string APP_ID;
    static const std::string BACKUP_ID;
    static const std::string SKIP_BAD_PARTITION;
};

class bulk_load_constant
{
public:
    static const std::string BULK_LOAD_INFO;
    static const int32_t BULK_LOAD_REQUEST_INTERVAL;
    static const int32_t BULK_LOAD_REQUEST_SHORT_INTERVAL;
    static const std::string BULK_LOAD_METADATA;
    static const std::string BULK_LOAD_LOCAL_ROOT_DIR;
    static const int32_t PROGRESS_FINISHED;
};

namespace cold_backup {
//
//  Attention: when compose the path on block service, we use appname_appid, because appname_appid
//              can identify the case below:
//     -- case: you create one app with name A and it's appid is 1, then after backup a time later,
//              you drop the table, then create a new app with name A and with appid 3
//              using appname_appid, can idenfity the backup data is belong to which app

// The directory structure on block service
//
//      <root>/<policy_name>/<backup_id>/<appname_appid>/meta/app_metadata
//                                                      /meta/app_backup_status
//                                                      /partition_1/checkpoint@ip:port/***.sst
//                                                      /partition_1/checkpoint@ip:port/CURRENT
//                                                      /partition_1/checkpoint@ip:port/backup_metadata
//                                                      /partition_1/current_checkpoint
//      <root>/<policy_name>/<backup_id>/<appname_appid>/meta/app_metadata
//                                                      /meta/app_backup_status
//                                                      /partition_1/checkpoint@ip:port/***.sst
//                                                      /partition_1/checkpoint@ip:port/CURRENT
//                                                      /partition_1/checkpoint@ip:port/backup_metadata
//                                                      /partition_1/current_checkpoint
//      <root>/<policy_name>/<backup_id>/backup_info
//

//
// the purpose of some file:
//      1, app_metadata : the metadata of the app, the same with the app's app_info
//      2, app_backup_status: the flag file, represent whether the app have finish backup, if this
//         file exist on block filesystem, backup is finished, otherwise, app haven't finished
//         backup, we ignore its context
//      3, backup_metadata : the file to statistic the information of a checkpoint, include all the
//         file's name, size and md5
//      4, current_checkpoint : specifing which checkpoint directory is valid
//      5, backup_info : recording the information of this backup
//

// compose the path for policy on block service
// input:
//  -- root: the prefix of path
// return:
//      the path: <root>/<policy_name>
std::string get_policy_path(const std::string &root, const std::string &policy_name);

// compose the path for app on block service
// input:
//  -- root:  the prefix of path
// return:
//      the path: <root>/<policy_name>/<backup_id>
std::string
get_backup_path(const std::string &root, const std::string &policy_name, int64_t backup_id);

// compose the path for app on block service
// input:
//  -- root:  the prefix of path
// return:
//      the path: <root>/<policy_name>/<backup_id>/<appname_appid>
std::string get_app_backup_path(const std::string &root,
                                const std::string &policy_name,
                                const std::string &app_name,
                                int32_t app_id,
                                int64_t backup_id);

// compose the path for replica on block service
// input:
//  -- root:  the prefix of the path
// return:
//      the path: <root>/<policy_name>/<backup_id>/<appname_appid>/<partition_index>
std::string get_replica_backup_path(const std::string &root,
                                    const std::string &policy_name,
                                    const std::string &app_name,
                                    gpid pid,
                                    int64_t backup_id);

// compose the path for meta on block service
// input:
//  -- root:  the prefix of the path
// return:
//      the path: <root>/<policy_name>/<backup_id>/<appname_appid>/meta
std::string get_app_meta_backup_path(const std::string &root,
                                     const std::string &policy_name,
                                     const std::string &app_name,
                                     int32_t app_id,
                                     int64_t backup_id);

// compose the absolute path(AP) of app_metadata_file on block service
// input:
//  -- prefix:      the prefix of AP
// return:
//      the AP of app meta data file:
//      <root>/<policy_name>/<backup_id>/<appname_appid>/meta/app_metadata
std::string get_app_metadata_file(const std::string &root,
                                  const std::string &policy_name,
                                  const std::string &app_name,
                                  int32_t app_id,
                                  int64_t backup_id);

// compose the absolute path(AP) of app_backup_status file on block service
// input:
//  -- prefix:      the prefix of AP
// return:
//      the AP of flag-file, which represent whether the app have finished backup:
//      <root>/<policy_name>/<backup_id>/<appname_appid>/meta/app_backup_status
std::string get_app_backup_status_file(const std::string &root,
                                       const std::string &policy_name,
                                       const std::string &app_name,
                                       int32_t app_id,
                                       int64_t backup_id);

// compose the absolute path(AP) of current chekpoint file on block service
// input:
//  -- root:      the prefix of AP on block service
//  -- pid:         gpid of replica
// return:
//      the AP of current checkpoint file:
//      <root>/<policy_name>/<backup_id>/<appname_appid>/<partition_index>/current_checkpoint
std::string get_current_chkpt_file(const std::string &root,
                                   const std::string &policy_name,
                                   const std::string &app_name,
                                   gpid pid,
                                   int64_t backup_id);

// compose the checkpoint directory name on block service
// return:
//      checkpoint directory name: checkpoint@<ip:port>
std::string get_remote_chkpt_dirname();

// compose the absolute path(AP) of checkpoint dir for replica on block service
// input:
//  -- root:       the prefix of the AP
//  -- pid:          gpid of replcia
// return:
//      the AP of the checkpoint dir:
//      <root>/<policy_name>/<backup_id>/<appname_appid>/<partition_index>/checkpoint@<ip:port>
std::string get_remote_chkpt_dir(const std::string &root,
                                 const std::string &policy_name,
                                 const std::string &app_name,
                                 gpid pid,
                                 int64_t backup_id);

// compose the absolute path(AP) of checkpoint meta for replica on block service
// input:
//  -- root:       the prefix of the AP
//  -- pid:          gpid of replcia
// return:
//      the AP of the checkpoint file metadata:
//      <root>/<policy_name>/<backup_id>/<appname_appid>/<partition_index>/checkpoint@<ip:port>/backup_metadata
std::string get_remote_chkpt_meta_file(const std::string &root,
                                       const std::string &policy_name,
                                       const std::string &app_name,
                                       gpid pid,
                                       int64_t backup_id);
} // namespace cold_backup
} // namespace replication
} // namespace dsn
