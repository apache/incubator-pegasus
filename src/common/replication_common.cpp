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

#include <fstream>

#include "utils/fmt_logging.h"
#include "common/replica_envs.h"
#include "utils/flags.h"
#include "utils/filesystem.h"

#include "replication_common.h"

namespace dsn {
namespace replication {

DSN_DEFINE_int32("replication",
                 max_concurrent_bulk_load_downloading_count,
                 5,
                 "concurrent bulk load downloading replica count");

/**
 * Empty write is used for flushing WAL log entry which is submit asynchronously.
 * Make sure it can work well if you diable it.
 */
DSN_DEFINE_bool("replication",
                empty_write_disabled,
                false,
                "whether to disable empty write, default is false");
DSN_TAG_VARIABLE(empty_write_disabled, FT_MUTABLE);

replication_options::replication_options()
{
    deny_client_on_start = false;
    verbose_client_log_on_start = false;
    verbose_commit_log_on_start = false;
    delay_for_fd_timeout_on_start = false;
    duplication_enabled = true;

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
    gc_interval_ms = 30 * 1000;                     // 30 seconds
    gc_memory_replica_interval_ms = 10 * 60 * 1000; // 10 minutes

    disk_stat_disabled = false;
    disk_stat_interval_seconds = 600;

    fd_disabled = false;
    fd_check_interval_seconds = 2;
    fd_beacon_interval_seconds = 3;
    fd_lease_seconds = 9;
    fd_grace_seconds = 10;

    log_private_file_size_mb = 32;
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

    mem_release_enabled = true;
    mem_release_check_interval_ms = 3600000;
    mem_release_max_reserved_mem_percentage = 10;

    lb_interval_ms = 10000;

    learn_app_max_concurrent_count = 5;

    cold_backup_checkpoint_reserve_minutes = 10;
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

    // get config_data_dirs and config_data_dir_tags from config
    const std::string &dirs_str =
        dsn_config_get_value_string("replication", "data_dirs", "", "replica directory list");
    std::vector<std::string> config_data_dirs;
    std::vector<std::string> config_data_dir_tags;
    std::string error_msg = "";
    bool flag = get_data_dir_and_tag(
        dirs_str, app_dir, app_name, config_data_dirs, config_data_dir_tags, error_msg);
    CHECK(flag, error_msg);

    // check if data_dir in black list, data_dirs doesn't contain dir in black list
    std::string black_list_file =
        dsn_config_get_value_string("replication",
                                    "data_dirs_black_list_file",
                                    "/home/work/.pegasus_data_dirs_black_list",
                                    "replica directory black list file");
    std::vector<std::string> black_list_dirs;
    get_data_dirs_in_black_list(black_list_file, black_list_dirs);
    for (auto i = 0; i < config_data_dirs.size(); ++i) {
        if (check_if_in_black_list(black_list_dirs, config_data_dirs[i])) {
            continue;
        }
        data_dirs.emplace_back(config_data_dirs[i]);
        data_dir_tags.emplace_back(config_data_dir_tags[i]);
    }

    CHECK(!data_dirs.empty(), "no replica data dir found, maybe not set or excluded by black list");

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

    duplication_enabled = dsn_config_get_value_bool(
        "replication", "duplication_enabled", duplication_enabled, "is duplication enabled");

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
        "minimum number of alive replicas under which write is allowed. it's valid if larger than "
        "0, otherwise, the final value is based on app_max_replica_count");

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

    mem_release_enabled = dsn_config_get_value_bool("replication",
                                                    "mem_release_enabled",
                                                    mem_release_enabled,
                                                    "whether to enable periodic memory release");

    mem_release_check_interval_ms = (int)dsn_config_get_value_uint64(
        "replication",
        "mem_release_check_interval_ms",
        mem_release_check_interval_ms,
        "the replica check if should release memory to the system every this period of time(ms)");

    mem_release_max_reserved_mem_percentage = (int)dsn_config_get_value_uint64(
        "replication",
        "mem_release_max_reserved_mem_percentage",
        mem_release_max_reserved_mem_percentage,
        "if tcmalloc reserved but not-used memory exceed this percentage of application allocated "
        "memory, replica server will release the exceeding memory back to operating system");

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

    cold_backup_checkpoint_reserve_minutes =
        (int)dsn_config_get_value_uint64("replication",
                                         "cold_backup_checkpoint_reserve_minutes",
                                         cold_backup_checkpoint_reserve_minutes,
                                         "reserve minutes of cold backup checkpoint");

    max_concurrent_bulk_load_downloading_count = FLAGS_max_concurrent_bulk_load_downloading_count;

    CHECK(replica_helper::load_meta_servers(meta_servers), "invalid meta server config");

    sanity_check();
}

void replication_options::sanity_check()
{
    CHECK_GE(max_mutation_count_in_prepare_list, staleness_for_commit);
}

int32_t replication_options::app_mutation_2pc_min_replica_count(int32_t app_max_replica_count) const
{
    CHECK_GT(app_max_replica_count, 0);
    if (mutation_2pc_min_replica_count > 0) { //  >0 means use the user config
        return mutation_2pc_min_replica_count;
    } else { // otherwise, the value based on the table max_replica_count
        return app_max_replica_count <= 2 ? 1 : app_max_replica_count / 2 + 1;
    }
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

bool replica_helper::load_meta_servers(/*out*/ std::vector<dsn::rpc_address> &servers,
                                       const char *section,
                                       const char *key)
{
    servers.clear();
    std::string server_list = dsn_config_get_value_string(section, key, "", "");
    std::vector<std::string> lv;
    ::dsn::utils::split_args(server_list.c_str(), lv, ',');
    for (auto &s : lv) {
        ::dsn::rpc_address addr;
        std::vector<std::string> hostname_port;
        uint32_t ip = 0;
        utils::split_args(s.c_str(), hostname_port, ':');
        CHECK_EQ_MSG(2,
                     hostname_port.size(),
                     "invalid address '{}' specified in config [{}].{}",
                     s,
                     section,
                     key);
        uint32_t port_num = 0;
        CHECK(dsn::internal::buf2unsigned(hostname_port[1], port_num) && port_num < UINT16_MAX,
              "invalid address '{}' specified in config [{}].{}",
              s,
              section,
              key);
        if (0 != (ip = ::dsn::rpc_address::ipv4_from_host(hostname_port[0].c_str()))) {
            addr.assign_ipv4(ip, static_cast<uint16_t>(port_num));
        } else if (!addr.from_string_ipv4(s.c_str())) {
            LOG_ERROR_F("invalid address '{}' specified in config [{}].{}", s, section, key);
            return false;
        }
        servers.push_back(addr);
    }
    if (servers.empty()) {
        LOG_ERROR_F("no meta server specified in config [{}].{}", section, key);
        return false;
    }
    return true;
}

/*static*/ bool
replication_options::get_data_dir_and_tag(const std::string &config_dirs_str,
                                          const std::string &default_dir,
                                          const std::string &app_name,
                                          /*out*/ std::vector<std::string> &data_dirs,
                                          /*out*/ std::vector<std::string> &data_dir_tags,
                                          /*out*/ std::string &err_msg)
{
    // - if {config_dirs_str} is empty (return true):
    //   - dir = {default_dir}
    //   - dir_tag/data_dir_tag = "default"
    //   - data_dir = {default_dir}/"reps"
    // - else if {config_dirs_str} = "tag1:dir1,tag2:dir2:tag3:dir3" (return true):
    //   - dir1 = "dir1"/{app_name}
    //   - dir_tag1/data_dir_tag1 = "tag1"
    //   - data_dir1 = "dir1"/{app_name}/"reps"
    // - else (return false):
    //   - invalid format and set {err_msg}
    std::vector<std::string> dirs;
    std::vector<std::string> dir_tags;
    utils::split_args(config_dirs_str.c_str(), dirs, ',');
    if (dirs.empty()) {
        dirs.push_back(default_dir);
        dir_tags.push_back("default");
    } else {
        for (auto &dir : dirs) {
            std::vector<std::string> tag_and_dir;
            utils::split_args(dir.c_str(), tag_and_dir, ':');
            if (tag_and_dir.size() != 2) {
                err_msg = fmt::format("invalid data_dir item({}) in config", dir);
                return false;
            }
            if (tag_and_dir[0].empty() || tag_and_dir[1].empty()) {
                err_msg = fmt::format("invalid data_dir item({}) in config", dir);
                return false;
            }
            dir = utils::filesystem::path_combine(tag_and_dir[1], app_name);
            for (unsigned i = 0; i < dir_tags.size(); ++i) {
                if (dirs[i] == dir) {
                    err_msg = fmt::format("dir({}) and dir({}) conflict", dirs[i], dir);
                    return false;
                }
            }
            for (unsigned i = 0; i < dir_tags.size(); ++i) {
                if (dir_tags[i] == tag_and_dir[0]) {
                    err_msg = fmt::format(
                        "dir({}) and dir({}) have same tag({})", dirs[i], dir, tag_and_dir[0]);
                    return false;
                }
            }
            dir_tags.push_back(tag_and_dir[0]);
        }
    }

    for (unsigned i = 0; i < dirs.size(); ++i) {
        const std::string &dir = dirs[i];
        LOG_INFO_F("data_dirs[{}] = {}, tag = {}", i + 1, dir, dir_tags[i]);
        data_dirs.push_back(utils::filesystem::path_combine(dir, "reps"));
        data_dir_tags.push_back(dir_tags[i]);
    }
    return true;
}

/*static*/ void
replication_options::get_data_dirs_in_black_list(const std::string &fname,
                                                 /*out*/ std::vector<std::string> &dirs)
{
    if (fname.empty() || !utils::filesystem::file_exists(fname)) {
        LOG_INFO_F("data_dirs_black_list_file[{}] not found, ignore it", fname);
        return;
    }

    LOG_INFO_F("data_dirs_black_list_file[{}] found, apply it", fname);
    std::ifstream file(fname);
    CHECK(file, "open data_dirs_black_list_file failed: {}", fname);

    std::string str;
    int count = 0;
    while (std::getline(file, str)) {
        std::string str2 = utils::trim_string(const_cast<char *>(str.c_str()));
        if (str2.empty()) {
            continue;
        }
        if (str2.back() != '/') {
            str2.append("/");
        }
        dirs.push_back(str2);
        count++;
        LOG_INFO_F("black_list[{}] = [{}]", count, str2);
    }
}

/*static*/ bool
replication_options::check_if_in_black_list(const std::vector<std::string> &black_list_dir,
                                            const std::string &dir)
{
    std::string dir_str = dir;
    if (!black_list_dir.empty()) {
        if (dir_str.back() != '/') {
            dir_str.append("/");
        }
        for (const std::string &black : black_list_dir) {
            if (dir_str.find(black) == 0) {
                return true;
            }
        }
    }
    return false;
}

const std::string replica_envs::DENY_CLIENT_REQUEST("replica.deny_client_request");
const std::string replica_envs::WRITE_QPS_THROTTLING("replica.write_throttling");
const std::string replica_envs::WRITE_SIZE_THROTTLING("replica.write_throttling_by_size");
const uint64_t replica_envs::MIN_SLOW_QUERY_THRESHOLD_MS = 20;
const std::string replica_envs::SLOW_QUERY_THRESHOLD("replica.slow_query_threshold");
const std::string replica_envs::ROCKSDB_USAGE_SCENARIO("rocksdb.usage_scenario");
const std::string replica_envs::TABLE_LEVEL_DEFAULT_TTL("default_ttl");
const std::string MANUAL_COMPACT_PREFIX("manual_compact.");
const std::string replica_envs::MANUAL_COMPACT_DISABLED(MANUAL_COMPACT_PREFIX + "disabled");
const std::string replica_envs::MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT(
    MANUAL_COMPACT_PREFIX + "max_concurrent_running_count");
const std::string MANUAL_COMPACT_ONCE_PREFIX(MANUAL_COMPACT_PREFIX + "once.");
const std::string replica_envs::MANUAL_COMPACT_ONCE_TRIGGER_TIME(MANUAL_COMPACT_ONCE_PREFIX +
                                                                 "trigger_time");
const std::string replica_envs::MANUAL_COMPACT_ONCE_TARGET_LEVEL(MANUAL_COMPACT_ONCE_PREFIX +
                                                                 "target_level");
const std::string replica_envs::MANUAL_COMPACT_ONCE_BOTTOMMOST_LEVEL_COMPACTION(
    MANUAL_COMPACT_ONCE_PREFIX + "bottommost_level_compaction");
const std::string MANUAL_COMPACT_PERIODIC_PREFIX(MANUAL_COMPACT_PREFIX + "periodic.");
const std::string replica_envs::MANUAL_COMPACT_PERIODIC_TRIGGER_TIME(
    MANUAL_COMPACT_PERIODIC_PREFIX + "trigger_time");
const std::string replica_envs::MANUAL_COMPACT_PERIODIC_TARGET_LEVEL(
    MANUAL_COMPACT_PERIODIC_PREFIX + "target_level");
const std::string replica_envs::MANUAL_COMPACT_PERIODIC_BOTTOMMOST_LEVEL_COMPACTION(
    MANUAL_COMPACT_PERIODIC_PREFIX + "bottommost_level_compaction");
const std::string
    replica_envs::ROCKSDB_CHECKPOINT_RESERVE_MIN_COUNT("rocksdb.checkpoint.reserve_min_count");
const std::string replica_envs::ROCKSDB_CHECKPOINT_RESERVE_TIME_SECONDS(
    "rocksdb.checkpoint.reserve_time_seconds");
const std::string replica_envs::ROCKSDB_ITERATION_THRESHOLD_TIME_MS(
    "replica.rocksdb_iteration_threshold_time_ms");
const std::string replica_envs::ROCKSDB_BLOCK_CACHE_ENABLED("replica.rocksdb_block_cache_enabled");
const std::string replica_envs::BUSINESS_INFO("business.info");
const std::string replica_envs::REPLICA_ACCESS_CONTROLLER_ALLOWED_USERS(
    "replica_access_controller.allowed_users");
const std::string replica_envs::READ_QPS_THROTTLING("replica.read_throttling");
const std::string replica_envs::READ_SIZE_THROTTLING("replica.read_throttling_by_size");
const std::string
    replica_envs::SPLIT_VALIDATE_PARTITION_HASH("replica.split.validate_partition_hash");
const std::string replica_envs::USER_SPECIFIED_COMPACTION("user_specified_compaction");
const std::string replica_envs::BACKUP_REQUEST_QPS_THROTTLING("replica.backup_request_throttling");
const std::string replica_envs::ROCKSDB_ALLOW_INGEST_BEHIND("rocksdb.allow_ingest_behind");
const std::string replica_envs::UPDATE_MAX_REPLICA_COUNT("max_replica_count.update");

} // namespace replication
} // namespace dsn
