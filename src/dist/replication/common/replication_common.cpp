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
#include <dsn/utility/filesystem.h>
#include <fstream>

namespace dsn {
namespace replication {

replication_options::replication_options()
{
    deny_client_on_start = false;
    verbose_client_log_on_start = false;
    verbose_commit_log_on_start = false;
    delay_for_fd_timeout_on_start = false;
    empty_write_disabled = false;
    allow_non_idempotent_write = false;

    prepare_timeout_ms_for_secondaries = 1000;
    prepare_timeout_ms_for_potential_secondaries = 3000;
    prepare_decree_gap_for_debug_logging = 10000;

    batch_write_disabled = false;
    staleness_for_commit = 10;
    max_mutation_count_in_prepare_list = 110;
    mutation_2pc_min_replica_count = 2;

    group_check_disabled = false;
    group_check_interval_ms = 10000;

    checkpoint_disabled = false;
    checkpoint_interval_seconds = 100;
    checkpoint_min_decree_gap = 10000;
    checkpoint_max_interval_hours = 2;

    gc_disabled = false;
    gc_interval_ms = 30 * 1000;                             // 30 seconds
    gc_memory_replica_interval_ms = 10 * 60 * 1000;         // 10 minutes
    gc_disk_error_replica_interval_seconds = 7 * 24 * 3600; // 1 week
    gc_disk_garbage_replica_interval_seconds = 24 * 3600;   // 1 day

    disk_stat_disabled = false;
    disk_stat_interval_seconds = 600;

    fd_disabled = false;
    fd_check_interval_seconds = 2;
    fd_beacon_interval_seconds = 3;
    fd_lease_seconds = 9;
    fd_grace_seconds = 10;

    log_private_file_size_mb = 32;
    log_private_batch_buffer_kb = 512;
    log_private_batch_buffer_count = 512;
    log_private_batch_buffer_flush_interval_ms = 10000;
    log_private_reserve_max_size_mb = 0;
    log_private_reserve_max_time_seconds = 0;

    log_shared_file_size_mb = 32;
    log_shared_file_count_limit = 100;
    log_shared_batch_buffer_kb = 0;
    log_shared_force_flush = false;
    log_shared_pending_size_throttling_threshold_kb = 0;
    log_shared_pending_size_throttling_delay_ms = 0;

    config_sync_disabled = false;
    config_sync_interval_ms = 30000;

    lb_interval_ms = 10000;

    learn_app_max_concurrent_count = 5;

    max_concurrent_uploading_file_count = 10;
}

replication_options::~replication_options() {}

void replication_options::initialize()
{
    const service_app_info &info = service_app::current_service_app_info();
    app_name = info.full_name;
    app_dir = info.data_dir;

    // slog_dir:
    // - if config[slog_dir] is empty: "app_dir/slog"
    // - else: "config[slog_dir]/app_name/slog"
    slog_dir = dsn_config_get_value_string("replication", "slog_dir", "", "shared log directory");
    if (slog_dir.empty()) {
        slog_dir = app_dir;
    } else {
        slog_dir = utils::filesystem::path_combine(slog_dir, app_name);
    }
    slog_dir = utils::filesystem::path_combine(slog_dir, "slog");

    // data_dirs
    // - if config[data_dirs] is empty: "app_dir/reps"
    // - else:
    //       config[data_dirs] = "tag1:dir1,tag2:dir2:tag3:dir3"
    //       data_dir = "config[data_dirs]/app_name/reps"
    std::string dirs_str =
        dsn_config_get_value_string("replication", "data_dirs", "", "replica directory list");
    std::vector<std::string> dirs;
    std::vector<std::string> dir_tags;
    ::dsn::utils::split_args(dirs_str.c_str(), dirs, ',');
    if (dirs.empty()) {
        dirs.push_back(app_dir);
        dir_tags.push_back("default");
    } else {
        for (auto &dir : dirs) {
            std::vector<std::string> tag_and_dir;
            ::dsn::utils::split_args(dir.c_str(), tag_and_dir, ':');
            if (tag_and_dir.size() != 2) {
                dassert(false, "invalid data_dir item(%s) in config", dir.c_str());
            } else {
                dassert(!tag_and_dir[0].empty() && !tag_and_dir[1].empty(),
                        "invalid data_dir item(%s) in config",
                        dir.c_str());
                dir = utils::filesystem::path_combine(tag_and_dir[1], app_name);
                for (unsigned i = 0; i < dir_tags.size(); ++i) {
                    dassert(dirs[i] != dir,
                            "dir(%s) and dir(%s) conflict",
                            dirs[i].c_str(),
                            dir.c_str());
                }
                for (unsigned i = 0; i < dir_tags.size(); ++i) {
                    dassert(dir_tags[i] != tag_and_dir[0],
                            "dir(%s) and dir(%s) have same tag(%s)",
                            dirs[i].c_str(),
                            dir.c_str(),
                            tag_and_dir[0].c_str());
                }
                dir_tags.push_back(tag_and_dir[0]);
            }
        }
    }

    std::string black_list_file =
        dsn_config_get_value_string("replication",
                                    "data_dirs_black_list_file",
                                    "/home/work/.pegasus_data_dirs_black_list",
                                    "replica directory black list file");
    std::vector<std::string> black_list;
    if (!black_list_file.empty() && dsn::utils::filesystem::file_exists(black_list_file)) {
        ddebug("data_dirs_black_list_file[%s] found, apply it", black_list_file.c_str());

        std::ifstream file(black_list_file);
        if (!file) {
            dassert(false, "open data_dirs_black_list_file failed: %s", black_list_file.c_str());
        }

        std::string str;
        int count = 0;
        while (std::getline(file, str)) {
            std::string str2 = dsn::utils::trim_string((char *)str.c_str());
            if (str2.empty())
                continue;
            if (str2.back() != '/')
                str2.append("/");
            black_list.push_back(str2);
            count++;
            ddebug("black_list[%d] = [%s]", count, str2.c_str());
            str.clear();
        }
    } else {
        ddebug("data_dirs_black_list_file[%s] not found, ignore it", black_list_file.c_str());
    }

    int dir_count = 0;
    for (unsigned i = 0; i < dirs.size(); ++i) {
        std::string &dir = dirs[i];
        bool in_black_list = false;
        if (!black_list.empty()) {
            std::string dir2 = dir;
            if (dir2.back() != '/')
                dir2.append("/");
            for (std::string &black : black_list) {
                if (dir2.find(black) == 0) {
                    in_black_list = true;
                    break;
                }
            }
        }

        if (in_black_list) {
            dwarn("replica data dir %s is in black list, ignore it", dir.c_str());
        } else {
            ddebug("data_dirs[%d] = %s, tag = %s", dir_count++, dir.c_str(), dir_tags[i].c_str());
            data_dirs.push_back(utils::filesystem::path_combine(dir, "reps"));
            data_dir_tags.push_back(dir_tags[i]);
        }
    }

    if (data_dirs.empty()) {
        dassert(false, "no replica data dir found, maybe not set or excluded by black list");
    }

    deny_client_on_start = dsn_config_get_value_bool("replication",
                                                     "deny_client_on_start",
                                                     deny_client_on_start,
                                                     "whether to deny client read "
                                                     "and write requests when "
                                                     "starting the server, default "
                                                     "is false");
    verbose_client_log_on_start = dsn_config_get_value_bool("replication",
                                                            "verbose_client_log_on_start",
                                                            verbose_client_log_on_start,
                                                            "whether to print verbose error "
                                                            "log when reply to client read "
                                                            "and write requests when "
                                                            "starting the server, default "
                                                            "is false");
    verbose_commit_log_on_start = dsn_config_get_value_bool("replication",
                                                            "verbose_commit_log_on_start",
                                                            verbose_commit_log_on_start,
                                                            "whether to print verbose log "
                                                            "when commit mutation when "
                                                            "starting the server, default "
                                                            "is false");
    delay_for_fd_timeout_on_start =
        dsn_config_get_value_bool("replication",
                                  "delay_for_fd_timeout_on_start",
                                  delay_for_fd_timeout_on_start,
                                  "whether to delay for beacon grace period to make failure "
                                  "detector timeout when starting the server, default is false");
    empty_write_disabled =
        dsn_config_get_value_bool("replication",
                                  "empty_write_disabled",
                                  empty_write_disabled,
                                  "whether to disable empty write, default is false");
    allow_non_idempotent_write =
        dsn_config_get_value_bool("replication",
                                  "allow_non_idempotent_write",
                                  allow_non_idempotent_write,
                                  "whether to allow non-idempotent write, default is false");

    prepare_timeout_ms_for_secondaries = (int)dsn_config_get_value_uint64(
        "replication",
        "prepare_timeout_ms_for_secondaries",
        prepare_timeout_ms_for_secondaries,
        "timeout (ms) for prepare message to secondaries in two phase commit");
    prepare_timeout_ms_for_potential_secondaries = (int)dsn_config_get_value_uint64(
        "replication",
        "prepare_timeout_ms_for_potential_secondaries",
        prepare_timeout_ms_for_potential_secondaries,
        "timeout (ms) for prepare message to potential secondaries in two phase commit");
    prepare_decree_gap_for_debug_logging = (int)dsn_config_get_value_uint64(
        "replication",
        "prepare_decree_gap_for_debug_logging",
        prepare_decree_gap_for_debug_logging,
        "if greater than 0, then print debug log every decree gap of preparing");

    batch_write_disabled =
        dsn_config_get_value_bool("replication",
                                  "batch_write_disabled",
                                  batch_write_disabled,
                                  "whether to disable auto-batch of replicated write requests");
    staleness_for_commit =
        (int)dsn_config_get_value_uint64("replication",
                                         "staleness_for_commit",
                                         staleness_for_commit,
                                         "how many concurrent two phase commit rounds are allowed");
    max_mutation_count_in_prepare_list =
        (int)dsn_config_get_value_uint64("replication",
                                         "max_mutation_count_in_prepare_list",
                                         max_mutation_count_in_prepare_list,
                                         "maximum number of mutations in prepare list");
    mutation_2pc_min_replica_count = (int)dsn_config_get_value_uint64(
        "replication",
        "mutation_2pc_min_replica_count",
        mutation_2pc_min_replica_count,
        "minimum number of alive replicas under which write is allowed");

    group_check_disabled = dsn_config_get_value_bool("replication",
                                                     "group_check_disabled",
                                                     group_check_disabled,
                                                     "whether group check is disabled");
    group_check_interval_ms =
        (int)dsn_config_get_value_uint64("replication",
                                         "group_check_interval_ms",
                                         group_check_interval_ms,
                                         "every what period (ms) we check the replica healthness");

    checkpoint_disabled = dsn_config_get_value_bool("replication",
                                                    "checkpoint_disabled",
                                                    checkpoint_disabled,
                                                    "whether checkpoint is disabled");
    checkpoint_interval_seconds = (int)dsn_config_get_value_uint64(
        "replication",
        "checkpoint_interval_seconds",
        checkpoint_interval_seconds,
        "every what period (seconds) we do checkpoints for replicated apps");
    checkpoint_min_decree_gap =
        (int64_t)dsn_config_get_value_uint64("replication",
                                             "checkpoint_min_decree_gap",
                                             checkpoint_min_decree_gap,
                                             "minimum decree gap that triggers checkpoint");
    checkpoint_max_interval_hours = (int)dsn_config_get_value_uint64(
        "replication",
        "checkpoint_max_interval_hours",
        checkpoint_max_interval_hours,
        "maximum time interval (hours) where a new checkpoint must be created");

    gc_disabled = dsn_config_get_value_bool(
        "replication", "gc_disabled", gc_disabled, "whether to disable garbage collection");
    gc_interval_ms = (int)dsn_config_get_value_uint64("replication",
                                                      "gc_interval_ms",
                                                      gc_interval_ms,
                                                      "every what period (ms) we do garbage "
                                                      "collection for dead replicas, on-disk "
                                                      "state, log, etc.");
    gc_memory_replica_interval_ms = (int)dsn_config_get_value_uint64(
        "replication",
        "gc_memory_replica_interval_ms",
        gc_memory_replica_interval_ms,
        "after closing a healthy replica (due to LB), the replica will remain in memory for this "
        "long (ms) for quick recover");
    gc_disk_error_replica_interval_seconds =
        (int)dsn_config_get_value_uint64("replication",
                                         "gc_disk_error_replica_interval_seconds",
                                         gc_disk_error_replica_interval_seconds,
                                         "error replica are deleted after they have been closed "
                                         "and lasted on disk this long (seconds)");
    gc_disk_garbage_replica_interval_seconds =
        (int)dsn_config_get_value_uint64("replication",
                                         "gc_disk_garbage_replica_interval_seconds",
                                         gc_disk_garbage_replica_interval_seconds,
                                         "garbage replica are deleted after they have been closed "
                                         "and lasted on disk this long (seconds)");

    disk_stat_disabled = dsn_config_get_value_bool(
        "replication", "disk_stat_disabled", disk_stat_disabled, "whether to disable disk stat");
    disk_stat_interval_seconds =
        (int)dsn_config_get_value_uint64("replication",
                                         "disk_stat_interval_seconds",
                                         disk_stat_interval_seconds,
                                         "every what period (ms) we do disk stat");

    fd_disabled = dsn_config_get_value_bool(
        "replication", "fd_disabled", fd_disabled, "whether to disable failure detection");
    fd_check_interval_seconds = (int)dsn_config_get_value_uint64(
        "replication",
        "fd_check_interval_seconds",
        fd_check_interval_seconds,
        "every this period(seconds) the FD will check healthness of remote peers");
    fd_beacon_interval_seconds = (int)dsn_config_get_value_uint64(
        "replication",
        "fd_beacon_interval_seconds",
        fd_beacon_interval_seconds,
        "every this period(seconds) the FD sends beacon message to remote peers");
    fd_lease_seconds =
        (int)dsn_config_get_value_uint64("replication",
                                         "fd_lease_seconds",
                                         fd_lease_seconds,
                                         "lease (seconds) get from remote FD master");
    fd_grace_seconds = (int)dsn_config_get_value_uint64(
        "replication",
        "fd_grace_seconds",
        fd_grace_seconds,
        "grace (seconds) assigned to remote FD slaves (grace > lease)");

    log_private_file_size_mb =
        (int)dsn_config_get_value_uint64("replication",
                                         "log_private_file_size_mb",
                                         log_private_file_size_mb,
                                         "private log maximum segment file size (MB)");
    log_private_batch_buffer_kb =
        (int)dsn_config_get_value_uint64("replication",
                                         "log_private_batch_buffer_kb",
                                         log_private_batch_buffer_kb,
                                         "private log buffer size (KB) for batching incoming logs");
    log_private_batch_buffer_count = (int)dsn_config_get_value_uint64(
        "replication",
        "log_private_batch_buffer_count",
        log_private_batch_buffer_count,
        "private log buffer max item count for batching incoming logs");
    log_private_batch_buffer_flush_interval_ms =
        (int)dsn_config_get_value_uint64("replication",
                                         "log_private_batch_buffer_flush_interval_ms",
                                         log_private_batch_buffer_flush_interval_ms,
                                         "private log buffer flush interval in milli-seconds");
    // ATTENTION: only when log_private_reserve_max_size_mb and log_private_reserve_max_time_seconds
    // are both satisfied, the useless logs can be reserved.
    log_private_reserve_max_size_mb =
        (int)dsn_config_get_value_uint64("replication",
                                         "log_private_reserve_max_size_mb",
                                         log_private_reserve_max_size_mb,
                                         "max size of useless private log to be reserved");
    log_private_reserve_max_time_seconds = (int)dsn_config_get_value_uint64(
        "replication",
        "log_private_reserve_max_time_seconds",
        log_private_reserve_max_time_seconds,
        "max time in seconds of useless private log to be reserved");

    log_shared_file_size_mb =
        (int)dsn_config_get_value_uint64("replication",
                                         "log_shared_file_size_mb",
                                         log_shared_file_size_mb,
                                         "shared log maximum segment file size (MB)");
    log_shared_file_count_limit = (int)dsn_config_get_value_uint64("replication",
                                                                   "log_shared_file_count_limit",
                                                                   log_shared_file_count_limit,
                                                                   "shared log maximum file count");
    log_shared_batch_buffer_kb =
        (int)dsn_config_get_value_uint64("replication",
                                         "log_shared_batch_buffer_kb",
                                         log_shared_batch_buffer_kb,
                                         "shared log buffer size (KB) for batching incoming logs");
    log_shared_force_flush =
        dsn_config_get_value_bool("replication",
                                  "log_shared_force_flush",
                                  log_shared_force_flush,
                                  "when write shared log, whether to flush file after write done");
    log_shared_pending_size_throttling_threshold_kb =
        (int)dsn_config_get_value_uint64("replication",
                                         "log_shared_pending_size_throttling_threshold_kb",
                                         log_shared_pending_size_throttling_threshold_kb,
                                         "log_shared_pending_size_throttling_threshold_kb");
    log_shared_pending_size_throttling_delay_ms =
        (int)dsn_config_get_value_uint64("replication",
                                         "log_shared_pending_size_throttling_delay_ms",
                                         log_shared_pending_size_throttling_delay_ms,
                                         "log_shared_pending_size_throttling_delay_ms");

    config_sync_disabled = dsn_config_get_value_bool(
        "replication",
        "config_sync_disabled",
        config_sync_disabled,
        "whether to disable replica configuration periodical sync with the meta server");
    config_sync_interval_ms = (int)dsn_config_get_value_uint64(
        "replication",
        "config_sync_interval_ms",
        config_sync_interval_ms,
        "every this period(ms) the replica syncs replica configuration with the meta server");

    lb_interval_ms = (int)dsn_config_get_value_uint64(
        "replication",
        "lb_interval_ms",
        lb_interval_ms,
        "every this period(ms) the meta server will do load balance");

    learn_app_max_concurrent_count =
        (int)dsn_config_get_value_uint64("replication",
                                         "learn_app_max_concurrent_count",
                                         learn_app_max_concurrent_count,
                                         "max count of learning app concurrently");

    cold_backup_root = dsn_config_get_value_string(
        "replication", "cold_backup_root", "", "cold backup remote storage path prefix");

    max_concurrent_uploading_file_count =
        (int32_t)dsn_config_get_value_uint64("replication",
                                             "max_concurrent_uploading_file_count",
                                             max_concurrent_uploading_file_count,
                                             "concurrent uploading file count");

    replica_helper::load_meta_servers(meta_servers);

    sanity_check();
}

void replication_options::sanity_check()
{
    dassert(max_mutation_count_in_prepare_list >= staleness_for_commit,
            "%d VS %d",
            max_mutation_count_in_prepare_list,
            staleness_for_commit);
}

/*static*/ bool replica_helper::remove_node(::dsn::rpc_address node,
                                            /*inout*/ std::vector<::dsn::rpc_address> &nodeList)
{
    auto it = std::find(nodeList.begin(), nodeList.end(), node);
    if (it != nodeList.end()) {
        nodeList.erase(it);
        return true;
    } else {
        return false;
    }
}

/*static*/ bool replica_helper::get_replica_config(const partition_configuration &partition_config,
                                                   ::dsn::rpc_address node,
                                                   /*out*/ replica_configuration &replica_config)
{
    replica_config.pid = partition_config.pid;
    replica_config.primary = partition_config.primary;
    replica_config.ballot = partition_config.ballot;
    replica_config.learner_signature = invalid_signature;

    if (node == partition_config.primary) {
        replica_config.status = partition_status::PS_PRIMARY;
        return true;
    } else if (std::find(partition_config.secondaries.begin(),
                         partition_config.secondaries.end(),
                         node) != partition_config.secondaries.end()) {
        replica_config.status = partition_status::PS_SECONDARY;
        return true;
    } else {
        replica_config.status = partition_status::PS_INACTIVE;
        return false;
    }
}

void replica_helper::load_meta_servers(/*out*/ std::vector<dsn::rpc_address> &servers,
                                       const char *section,
                                       const char *key)
{
    servers.clear();
    std::string server_list = dsn_config_get_value_string(section, key, "", "");
    std::vector<std::string> lv;
    ::dsn::utils::split_args(server_list.c_str(), lv, ',');
    for (auto &s : lv) {
        ::dsn::rpc_address addr;
        if (!addr.from_string_ipv4(s.c_str())) {
            dassert(
                false, "invalid address '%s' specified in config [%s].%s", s.c_str(), section, key);
        }
        servers.push_back(addr);
    }
    dassert(servers.size() > 0, "no meta server specified in config [%s].%s", section, key);
}

const std::string cold_backup_constant::APP_METADATA("app_metadata");
const std::string cold_backup_constant::APP_BACKUP_STATUS("app_backup_status");
const std::string cold_backup_constant::CURRENT_CHECKPOINT("current_checkpoint");
const std::string cold_backup_constant::BACKUP_METADATA("backup_metadata");
const std::string cold_backup_constant::BACKUP_INFO("backup_info");
const int32_t cold_backup_constant::PROGRESS_FINISHED = 1000;

const std::string backup_restore_constant::FORCE_RESTORE("restore.force_restore");
const std::string backup_restore_constant::BLOCK_SERVICE_PROVIDER("restore.block_service_provider");
const std::string backup_restore_constant::CLUSTER_NAME("restore.cluster_name");
const std::string backup_restore_constant::POLICY_NAME("restore.policy_name");
const std::string backup_restore_constant::APP_NAME("restore.app_name");
const std::string backup_restore_constant::APP_ID("restore.app_id");
const std::string backup_restore_constant::BACKUP_ID("restore.backup_id");
const std::string backup_restore_constant::SKIP_BAD_PARTITION("restore.skip_bad_partition");

const std::string replica_envs::DENY_CLIENT_WRITE("replica.deny_client_write");
const std::string replica_envs::WRITE_THROTTLING("replica.write_throttling");

namespace cold_backup {
std::string get_policy_path(const std::string &root, const std::string &policy_name)
{
    std::stringstream ss;
    ss << root << "/" << policy_name;
    return ss.str();
}

std::string
get_backup_path(const std::string &root, const std::string &policy_name, int64_t backup_id)
{
    std::stringstream ss;
    ss << get_policy_path(root, policy_name) << "/" << backup_id;
    return ss.str();
}

std::string get_app_backup_path(const std::string &root,
                                const std::string &policy_name,
                                const std::string &app_name,
                                int32_t app_id,
                                int64_t backup_id)
{
    std::stringstream ss;
    ss << get_backup_path(root, policy_name, backup_id) << "/" << app_name << "_" << app_id;
    return ss.str();
}

std::string get_replica_backup_path(const std::string &root,
                                    const std::string &policy_name,
                                    const std::string &app_name,
                                    gpid pid,
                                    int64_t backup_id)
{
    std::stringstream ss;
    ss << get_policy_path(root, policy_name) << "/" << backup_id << "/" << app_name << "_"
       << pid.get_app_id() << "/" << pid.get_partition_index();
    return ss.str();
}

std::string get_app_meta_backup_path(const std::string &root,
                                     const std::string &policy_name,
                                     const std::string &app_name,
                                     int32_t app_id,
                                     int64_t backup_id)
{
    std::stringstream ss;
    ss << get_policy_path(root, policy_name) << "/" << backup_id << "/" << app_name << "_" << app_id
       << "/meta";
    return ss.str();
}

std::string get_app_metadata_file(const std::string &root,
                                  const std::string &policy_name,
                                  const std::string &app_name,
                                  int32_t app_id,
                                  int64_t backup_id)
{
    std::stringstream ss;
    ss << get_app_meta_backup_path(root, policy_name, app_name, app_id, backup_id) << "/"
       << cold_backup_constant::APP_METADATA;
    return ss.str();
}

std::string get_app_backup_status_file(const std::string &root,
                                       const std::string &policy_name,
                                       const std::string &app_name,
                                       int32_t app_id,
                                       int64_t backup_id)
{
    std::stringstream ss;
    ss << get_app_meta_backup_path(root, policy_name, app_name, app_id, backup_id) << "/"
       << cold_backup_constant::APP_BACKUP_STATUS;
    return ss.str();
}

std::string get_current_chkpt_file(const std::string &root,
                                   const std::string &policy_name,
                                   const std::string &app_name,
                                   gpid pid,
                                   int64_t backup_id)
{
    std::stringstream ss;
    ss << get_replica_backup_path(root, policy_name, app_name, pid, backup_id) << "/"
       << cold_backup_constant::CURRENT_CHECKPOINT;
    return ss.str();
}

std::string get_remote_chkpt_dirname()
{
    // here using server address as suffix of remote_chkpt_dirname
    rpc_address local_address = dsn_primary_address();
    std::stringstream ss;
    ss << "chkpt_" << local_address.ipv4_str() << "_" << local_address.port();
    return ss.str();
}

std::string get_remote_chkpt_dir(const std::string &root,
                                 const std::string &policy_name,
                                 const std::string &app_name,
                                 gpid pid,
                                 int64_t backup_id)
{
    std::stringstream ss;
    ss << get_replica_backup_path(root, policy_name, app_name, pid, backup_id) << "/"
       << get_remote_chkpt_dirname();
    return ss.str();
}

std::string get_remote_chkpt_meta_file(const std::string &root,
                                       const std::string &policy_name,
                                       const std::string &app_name,
                                       gpid pid,
                                       int64_t backup_id)
{
    std::stringstream ss;
    ss << get_remote_chkpt_dir(root, policy_name, app_name, pid, backup_id) << "/"
       << cold_backup_constant::BACKUP_METADATA;
    return ss.str();
}

} // end cold_backup namespace
}
} // end namespace
