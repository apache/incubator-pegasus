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

#include <absl/strings/str_split.h>
#include <boost/algorithm/string/replace.hpp>
// IWYU pragma: no_include <ext/alloc_traits.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <nlohmann/json.hpp>
#include <rapidjson/ostreamwrapper.h>
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <iterator>
#include <mutex>
#include <queue>
#include <set>
#include <sstream>
#include <string_view>
#include <type_traits>
#include <vector>

#include "backup/replica_backup_server.h"
#include "bulk_load/replica_bulk_loader.h"
#include "common/backup_common.h"
#include "common/duplication_common.h"
#include "common/json_helper.h"
#include "common/replication.codes.h"
#include "common/replication_enums.h"
#include "disk_cleaner.h"
#include "duplication/duplication_sync_timer.h"
#include "meta_admin_types.h"
#include "mutation_log.h"
#include "nfs/nfs_node.h"
#include "nfs_types.h"
#include "ranger/access_type.h"
#include "replica.h"
#include "replica/duplication/replica_follower.h"
#include "replica/kms_key_provider.h"
#include "replica/replica_context.h"
#include "replica/replica_stub.h"
#include "replica/replication_app_base.h"
#include "replica_disk_migrator.h"
#include "rpc/rpc_message.h"
#include "rpc/serialization.h"
#include "runtime/api_layer1.h"
#include "security/access_controller.h"
#include "split/replica_split_manager.h"
#include "task/async_calls.h"
#include "task/task.h"
#include "task/task_engine.h"
#include "task/task_worker.h"
#include "utils/api_utilities.h"
#include "utils/command_manager.h"
#include "utils/env.h"
#include "utils/errors.h"
#include "utils/filesystem.h"
#include "utils/fmt_logging.h"
#include "utils/load_dump_object.h"
#include "utils/ports.h"
#include "utils/process_utils.h"
#include "utils/rand.h"
#include "utils/string_conv.h"
#include "utils/strings.h"
#include "utils/synchronize.h"
#include "utils/threadpool_spec.h"
#include "utils/timer.h"
#ifdef DSN_ENABLE_GPERF
#include <gperftools/malloc_extension.h>
#elif defined(DSN_USE_JEMALLOC)
#include "utils/je_ctl.h"
#endif
#include "nfs/nfs_code_definition.h"
#include "remote_cmd/remote_command.h"
#include "utils/fail_point.h"

namespace {

const char *kMaxReplicasOnLoadForEachDiskDesc =
    "The max number of replicas that are allowed to be loaded simultaneously for each disk dir.";

const char *kLoadReplicaMaxWaitTimeMsDesc = "The max waiting time for replica loading to complete.";

const char *kMaxConcurrentBulkLoadDownloadingCountDesc =
    "The maximum concurrent bulk load downloading replica count.";

} // anonymous namespace

METRIC_DEFINE_gauge_int64(server,
                          total_replicas,
                          dsn::metric_unit::kReplicas,
                          "The total number of replicas");

METRIC_DEFINE_gauge_int64(server,
                          opening_replicas,
                          dsn::metric_unit::kReplicas,
                          "The number of opening replicas");

METRIC_DEFINE_gauge_int64(server,
                          closing_replicas,
                          dsn::metric_unit::kReplicas,
                          "The number of closing replicas");

METRIC_DEFINE_gauge_int64(server,
                          inactive_replicas,
                          dsn::metric_unit::kReplicas,
                          "The number of inactive replicas");

METRIC_DEFINE_gauge_int64(server,
                          error_replicas,
                          dsn::metric_unit::kReplicas,
                          "The number of replicas with errors");

METRIC_DEFINE_gauge_int64(server,
                          primary_replicas,
                          dsn::metric_unit::kReplicas,
                          "The number of primary replicas");

METRIC_DEFINE_gauge_int64(server,
                          secondary_replicas,
                          dsn::metric_unit::kReplicas,
                          "The number of secondary replicas");

METRIC_DEFINE_gauge_int64(server,
                          learning_replicas,
                          dsn::metric_unit::kReplicas,
                          "The number of learning replicas");

METRIC_DEFINE_gauge_int64(server,
                          learning_replicas_max_duration_ms,
                          dsn::metric_unit::kMilliSeconds,
                          "The max duration among all learning replicas");

METRIC_DEFINE_gauge_int64(
    server,
    learning_replicas_max_copy_file_bytes,
    dsn::metric_unit::kBytes,
    "The max size of files that are copied from learnee among all learning replicas");

METRIC_DEFINE_counter(server,
                      moved_error_replicas,
                      dsn::metric_unit::kReplicas,
                      "The number of replicas whose dirs are moved as error");

METRIC_DEFINE_counter(server,
                      moved_garbage_replicas,
                      dsn::metric_unit::kReplicas,
                      "The number of replicas whose dirs are moved as garbage");

METRIC_DEFINE_counter(server,
                      replica_removed_dirs,
                      dsn::metric_unit::kDirs,
                      "The number of removed replica dirs");

METRIC_DEFINE_gauge_int64(server,
                          replica_error_dirs,
                          dsn::metric_unit::kDirs,
                          "The number of error replica dirs (*.err)");

METRIC_DEFINE_gauge_int64(server,
                          replica_garbage_dirs,
                          dsn::metric_unit::kDirs,
                          "The number of garbage replica dirs (*.gar)");

METRIC_DEFINE_gauge_int64(server,
                          replica_tmp_dirs,
                          dsn::metric_unit::kDirs,
                          "The number of tmp replica dirs (*.tmp) for disk migration");

METRIC_DEFINE_gauge_int64(server,
                          replica_origin_dirs,
                          dsn::metric_unit::kDirs,
                          "The number of origin replica dirs (*.ori) for disk migration");

#ifdef DSN_ENABLE_GPERF
METRIC_DEFINE_counter(server,
                      tcmalloc_released_bytes,
                      dsn::metric_unit::kBytes,
                      "The memory bytes that are released accumulatively by tcmalloc");
#endif

METRIC_DEFINE_counter(server,
                      read_failed_requests,
                      dsn::metric_unit::kRequests,
                      "The number of failed read requests");

METRIC_DEFINE_counter(server,
                      write_failed_requests,
                      dsn::metric_unit::kRequests,
                      "The number of failed write requests");

METRIC_DEFINE_counter(server,
                      read_busy_requests,
                      dsn::metric_unit::kRequests,
                      "The number of busy read requests");

METRIC_DEFINE_counter(server,
                      write_busy_requests,
                      dsn::metric_unit::kRequests,
                      "The number of busy write requests");

METRIC_DEFINE_gauge_int64(server,
                          bulk_load_running_count,
                          dsn::metric_unit::kBulkLoads,
                          "The number of current running bulk loads");

METRIC_DEFINE_gauge_int64(server,
                          bulk_load_ingestion_max_duration_ms,
                          dsn::metric_unit::kMilliSeconds,
                          "The max duration of ingestions for bulk loads");

METRIC_DEFINE_gauge_int64(server,
                          bulk_load_max_duration_ms,
                          dsn::metric_unit::kMilliSeconds,
                          "The max duration of bulk loads");

METRIC_DEFINE_gauge_int64(server,
                          splitting_replicas,
                          dsn::metric_unit::kReplicas,
                          "The number of current splitting replicas");

METRIC_DEFINE_gauge_int64(server,
                          splitting_replicas_max_duration_ms,
                          dsn::metric_unit::kMilliSeconds,
                          "The max duration among all splitting replicas");

METRIC_DEFINE_gauge_int64(server,
                          splitting_replicas_async_learn_max_duration_ms,
                          dsn::metric_unit::kMilliSeconds,
                          "The max duration among all splitting replicas for async learns");

METRIC_DEFINE_gauge_int64(server,
                          splitting_replicas_max_copy_file_bytes,
                          dsn::metric_unit::kBytes,
                          "The max size of copied files among all splitting replicas");

DSN_DECLARE_bool(duplication_enabled);
DSN_DECLARE_bool(empty_write_disabled);
DSN_DECLARE_bool(enable_acl);
DSN_DECLARE_bool(encrypt_data_at_rest);
DSN_DECLARE_int32(fd_beacon_interval_seconds);
DSN_DECLARE_int32(fd_check_interval_seconds);
DSN_DECLARE_int32(fd_grace_seconds);
DSN_DECLARE_int32(fd_lease_seconds);
DSN_DECLARE_string(data_dirs);
DSN_DECLARE_string(encryption_cluster_key_name);
DSN_DECLARE_string(server_key);

DSN_DEFINE_uint64(replication,
                  max_replicas_on_load_for_each_disk,
                  256,
                  kMaxReplicasOnLoadForEachDiskDesc);
DSN_TAG_VARIABLE(max_replicas_on_load_for_each_disk, FT_MUTABLE);
DSN_DEFINE_validator(max_replicas_on_load_for_each_disk,
                     [](uint64_t value) -> bool { return value > 0; });

DSN_DEFINE_uint64(replication, load_replica_max_wait_time_ms, 10, kLoadReplicaMaxWaitTimeMsDesc);
DSN_TAG_VARIABLE(load_replica_max_wait_time_ms, FT_MUTABLE);
DSN_DEFINE_validator(load_replica_max_wait_time_ms,
                     [](uint64_t value) -> bool { return value > 0; });

DSN_DEFINE_int32(replication,
                 max_concurrent_bulk_load_downloading_count,
                 5,
                 kMaxConcurrentBulkLoadDownloadingCountDesc);
DSN_DEFINE_validator(max_concurrent_bulk_load_downloading_count,
                     [](int32_t value) -> bool { return value >= 0; });

DSN_DEFINE_bool(replication,
                deny_client_on_start,
                false,
                "Whether to deny client read and write "
                "requests. The 'on_start' in the name is "
                "meaningless, this config takes effect "
                "all the time");
DSN_DEFINE_bool(replication,
                verbose_client_log_on_start,
                false,
                "whether to print verbose error log when reply to client read and write requests "
                "when starting the server");
DSN_DEFINE_bool(replication,
                mem_release_enabled,
                true,
                "whether to enable periodic memory release");
DSN_DEFINE_bool(replication, disk_stat_disabled, false, "whether to disable disk stat");
DSN_DEFINE_bool(
    replication,
    delay_for_fd_timeout_on_start,
    false,
    "Whether to delay for a period of time to make failure detector timeout when "
    "starting the server. The delayed time is depends on [replication]fd_grace_seconds");
DSN_DEFINE_bool(replication,
                config_sync_disabled,
                false,
                "Whether to disable replica server to send replica config-sync "
                "requests to meta server periodically");
DSN_DEFINE_bool(replication, fd_disabled, false, "Whether to disable failure detection");
DSN_DEFINE_bool(replication,
                verbose_commit_log_on_start,
                false,
                "whether to print verbose log when commit mutation when starting the server");
DSN_DEFINE_uint32(replication,
                  max_concurrent_manual_emergency_checkpointing_count,
                  10,
                  "max concurrent manual emergency checkpoint running count");
DSN_TAG_VARIABLE(max_concurrent_manual_emergency_checkpointing_count, FT_MUTABLE);

DSN_DEFINE_uint32(replication,
                  config_sync_interval_ms,
                  30000,
                  "The interval milliseconds of "
                  "replica server to send replica "
                  "config-sync requests to meta "
                  "server");
DSN_TAG_VARIABLE(config_sync_interval_ms, FT_MUTABLE);
DSN_DEFINE_validator(config_sync_interval_ms, [](uint32_t value) -> bool { return value > 0; });

DSN_DEFINE_int32(replication,
                 disk_stat_interval_seconds,
                 600,
                 "every what period (ms) we do disk stat");
DSN_DEFINE_int32(replication,
                 gc_memory_replica_interval_ms,
                 10 * 60 * 1000,
                 "The milliseconds of a replica remain in memory for quick recover aim after it's "
                 "closed in healthy state (due to LB)");
DSN_DEFINE_int32(
    replication,
    mem_release_check_interval_ms,
    3600000,
    "the replica check if should release memory to the system every this period of time(ms)");
DSN_DEFINE_int32(
    replication,
    mem_release_max_reserved_mem_percentage,
    10,
    "if tcmalloc reserved but not-used memory exceed this percentage of application allocated "
    "memory, replica server will release the exceeding memory back to operating system");
bool check_mem_release_max_reserved_mem_percentage(int32_t value)
{
    return value > 0 && value <= 100;
}
DSN_DEFINE_validator(mem_release_max_reserved_mem_percentage,
                     &check_mem_release_max_reserved_mem_percentage);

DSN_DEFINE_bool(replication, replicas_stat_disabled, false, "whether to disable replicas stat");

DSN_DEFINE_uint32(replication,
                  replicas_stat_interval_ms,
                  30000,
                  "period in milliseconds that stats for replicas are calculated");
DSN_TAG_VARIABLE(replicas_stat_interval_ms, FT_MUTABLE);
DSN_DEFINE_validator(replicas_stat_interval_ms, [](uint32_t value) -> bool { return value > 0; });

DSN_DEFINE_string(
    pegasus.server,
    hadoop_kms_url,
    "",
    "Provide the comma-separated list of URLs from which to retrieve the "
    "file system's server key. Example format: 'hostname1:1234/kms,hostname2:1234/kms'.");

DSN_DEFINE_group_validator(encrypt_data_at_rest_pre_check, [](std::string &message) -> bool {
    if (!FLAGS_enable_acl && FLAGS_encrypt_data_at_rest) {
        message = fmt::format("[pegasus.server] encrypt_data_at_rest should be enabled only if "
                              "[security] enable_acl is enabled.");
        return false;
    }
    return true;
});

DSN_DEFINE_group_validator(encrypt_data_at_rest_with_kms_url, [](std::string &message) -> bool {
#ifndef MOCK_TEST
    if (FLAGS_encrypt_data_at_rest && dsn::utils::is_empty(FLAGS_hadoop_kms_url)) {
        message = fmt::format("[security] hadoop_kms_url should not be empty when [pegasus.server] "
                              "encrypt_data_at_rest is enabled.");
        return false;
    }
#endif
    return true;
});

namespace dsn {
namespace replication {
bool replica_stub::s_not_exit_on_log_failure = false;

namespace {

// Register commands that get/set flag configurations.
void register_flags_ctrl_command()
{
    // For the reaonse why using std::call_once please see comments in
    // replica_stub::register_ctrl_command() for details.
    static std::once_flag flag;
    std::call_once(flag, []() mutable {
        dsn::command_manager::instance().add_global_cmd(
            dsn::command_manager::instance().register_int_command(
                FLAGS_max_replicas_on_load_for_each_disk,
                FLAGS_max_replicas_on_load_for_each_disk,
                "replica.max-replicas-on-load-for-each-disk",
                kMaxReplicasOnLoadForEachDiskDesc));

        dsn::command_manager::instance().add_global_cmd(
            dsn::command_manager::instance().register_int_command(
                FLAGS_load_replica_max_wait_time_ms,
                FLAGS_load_replica_max_wait_time_ms,
                "replica.load-replica-max-wait-time-ms",
                kLoadReplicaMaxWaitTimeMsDesc));

        dsn::command_manager::instance().add_global_cmd(
            dsn::command_manager::instance().register_bool_command(
                FLAGS_empty_write_disabled,
                "replica.disable-empty-write",
                "whether to disable empty writes"));

        dsn::command_manager::instance().add_global_cmd(
            dsn::command_manager::instance().register_int_command(
                FLAGS_max_concurrent_bulk_load_downloading_count,
                FLAGS_max_concurrent_bulk_load_downloading_count,
                "replica.max-concurrent-bulk-load-downloading-count",
                kMaxConcurrentBulkLoadDownloadingCountDesc));
    });
}

} // anonymous namespace

replica_stub::replica_stub(replica_state_subscriber subscriber /*= nullptr*/,
                           bool is_long_subscriber /* = true*/)
    : serverlet("replica_stub"),
      _state(NS_Disconnected),
      _replica_state_subscriber(std::move(subscriber)),
      _is_long_subscriber(is_long_subscriber),
      _deny_client(false),
      _verbose_client_log(false),
      _verbose_commit_log(false),
      _release_tcmalloc_memory(false),
      _mem_release_max_reserved_mem_percentage(10),
      _learn_app_concurrent_count(0),
      _bulk_load_downloading_count(0),
      _manual_emergency_checkpointing_count(0),
      _is_running(false),
#ifdef DSN_ENABLE_GPERF
      _is_releasing_memory(false),
#endif
      METRIC_VAR_INIT_server(total_replicas),
      METRIC_VAR_INIT_server(opening_replicas),
      METRIC_VAR_INIT_server(closing_replicas),
      METRIC_VAR_INIT_server(inactive_replicas),
      METRIC_VAR_INIT_server(error_replicas),
      METRIC_VAR_INIT_server(primary_replicas),
      METRIC_VAR_INIT_server(secondary_replicas),
      METRIC_VAR_INIT_server(learning_replicas),
      METRIC_VAR_INIT_server(learning_replicas_max_duration_ms),
      METRIC_VAR_INIT_server(learning_replicas_max_copy_file_bytes),
      METRIC_VAR_INIT_server(moved_error_replicas),
      METRIC_VAR_INIT_server(moved_garbage_replicas),
      METRIC_VAR_INIT_server(replica_removed_dirs),
      METRIC_VAR_INIT_server(replica_error_dirs),
      METRIC_VAR_INIT_server(replica_garbage_dirs),
      METRIC_VAR_INIT_server(replica_tmp_dirs),
      METRIC_VAR_INIT_server(replica_origin_dirs),
#ifdef DSN_ENABLE_GPERF
      METRIC_VAR_INIT_server(tcmalloc_released_bytes),
#endif
      METRIC_VAR_INIT_server(read_failed_requests),
      METRIC_VAR_INIT_server(write_failed_requests),
      METRIC_VAR_INIT_server(read_busy_requests),
      METRIC_VAR_INIT_server(write_busy_requests),
      METRIC_VAR_INIT_server(bulk_load_running_count),
      METRIC_VAR_INIT_server(bulk_load_ingestion_max_duration_ms),
      METRIC_VAR_INIT_server(bulk_load_max_duration_ms),
      METRIC_VAR_INIT_server(splitting_replicas),
      METRIC_VAR_INIT_server(splitting_replicas_max_duration_ms),
      METRIC_VAR_INIT_server(splitting_replicas_async_learn_max_duration_ms),
      METRIC_VAR_INIT_server(splitting_replicas_max_copy_file_bytes)
{
    // Some flags might need to be tuned on the stage of loading replicas (during
    // replica_stub::initialize()), thus register their control command just in the
    // constructor.
    register_flags_ctrl_command();
}

replica_stub::~replica_stub() { close(); }

void replica_stub::initialize(bool clear /* = false*/)
{
    replication_options opts;
    opts.initialize();
    initialize(opts, clear);
    _access_controller = std::make_unique<dsn::security::access_controller>();
}

std::vector<replica_stub::disk_replicas_info> replica_stub::get_all_disk_dirs() const
{
    std::vector<disk_replicas_info> disks;
    for (const auto &disk_node : _fs_manager.get_dir_nodes()) {
        if (dsn_unlikely(disk_node->status == disk_status::IO_ERROR)) {
            // Skip disks with IO errors.
            continue;
        }

        std::vector<std::string> sub_dirs;
        CHECK(utils::filesystem::get_subdirectories(disk_node->full_dir, sub_dirs, false),
              "failed to get sub_directories in {}",
              disk_node->full_dir);
        disks.push_back(disk_replicas_info{disk_node.get(), std::move(sub_dirs)});
    }

    return disks;
}

// TaskCode: LPC_REPLICATION_INIT_LOAD
// ThreadPool: THREAD_POOL_LOCAL_APP
void replica_stub::load_replica(dir_node *disk_node,
                                const std::string &replica_dir,
                                size_t total_dir_count,
                                utils::ex_lock &reps_lock,
                                replica_map_by_gpid &reps,
                                std::atomic<size_t> &finished_dir_count)
{
    // Measure execution time for loading a replica dir.
    //
    // TODO(wangdan): support decimal milliseconds or microseconds, since loading a small
    // replica tends to spend less than 1 milliseconds and show "0ms" in logging.
    SCOPED_LOG_TIMING(INFO, "on loading replica dir {}:{}", disk_node->tag, replica_dir);

    LOG_INFO("loading replica: replica_dir={}:{}", disk_node->tag, replica_dir);

    const auto *const worker = task::get_current_worker2();
    if (worker != nullptr) {
        CHECK(!(worker->pool()->spec().partitioned),
              "The thread pool THREAD_POOL_LOCAL_APP(task code: LPC_REPLICATION_INIT_LOAD) "
              "for loading replicas must not be partitioned since load balancing is required "
              "among multiple threads");
    }

    auto rep = load_replica(disk_node, replica_dir);
    if (rep == nullptr) {
        LOG_INFO("load replica failed: replica_dir={}:{}, progress={}/{}",
                 disk_node->tag,
                 replica_dir,
                 ++finished_dir_count,
                 total_dir_count);
        return;
    }

    LOG_INFO("{}@{}: load replica successfully, replica_dir={}:{}, progress={}/{}, "
             "last_durable_decree={}, last_committed_decree={}, last_prepared_decree={}",
             rep->get_gpid(),
             dsn_primary_host_port(),
             disk_node->tag,
             replica_dir,
             ++finished_dir_count,
             total_dir_count,
             rep->last_durable_decree(),
             rep->last_committed_decree(),
             rep->last_prepared_decree());

    utils::auto_lock<utils::ex_lock> l(reps_lock);
    const auto rep_iter = reps.find(rep->get_gpid());
    CHECK(rep_iter == reps.end(),
          "{}@{}: newly loaded dir {} conflicts with existing {} while loading replica",
          rep->get_gpid(),
          dsn_primary_host_port(),
          rep->dir(),
          rep_iter->second->dir());

    reps.emplace(rep->get_gpid(), rep);
}

void replica_stub::load_replicas(replica_map_by_gpid &reps)
{
    // Measure execution time for loading all replicas from all healthy disks without IO errors.
    //
    // TODO(wangdan): show both the size of output replicas and execution time on just one
    // logging line.
    SCOPED_LOG_TIMING(INFO, "on loading replicas");

    const auto &disks = get_all_disk_dirs();

    // The max index of dirs that are currently being loaded for each disk, which means the dirs
    // with higher indexes have not begun to be loaded (namely pushed into the queue).
    std::vector<size_t> replica_dir_indexes(disks.size(), 0);

    // Each loader is for a replica dir, including its path and loading task.
    struct replica_dir_loader
    {
        size_t replica_dir_index;
        std::string replica_dir_path;
        task_ptr load_replica_task;
    };

    // Each queue would cache the tasks that loading dirs for each disk. Once the task is
    // found finished (namely a dir has been loaded successfully), it would be popped from
    // the queue.
    std::vector<std::queue<replica_dir_loader>> load_disk_queues(disks.size());

    // The number of loading replica dirs that have been finished for each disk, used to show
    // current progress.
    //
    // TODO(wangdan): calculate the number of successful or failed loading of replica dirs,
    // and the number for each reason if failed.
    std::vector<std::atomic<size_t>> finished_replica_dirs(disks.size());
    for (auto &count : finished_replica_dirs) {
        count.store(0);
    }

    // The lock for operations on the loaded replicas as output.
    utils::ex_lock reps_lock;

    while (true) {
        size_t finished_disks = 0;

        // For each round, start loading one replica for each disk in case there are too many
        // replicas in a disk, except that all of the replicas of this disk are being loaded.
        for (size_t disk_index = 0; disk_index < disks.size(); ++disk_index) {
            // TODO(wangdan): Structured bindings can be captured by closures in g++, while
            // not supported well by clang. Thus we do not use following statement to bind
            // both variables until clang has been upgraded to version 16 which could support
            // that well:
            //
            //     const auto &[disk_node, replica_dirs] = disks[disk_index];
            //
            // For the docs of clang 16 please see:
            //
            // https://releases.llvm.org/16.0.0/tools/clang/docs/ReleaseNotes.html#c-20-feature-support.
            const auto &replica_dirs = disks[disk_index].replica_dirs;

            auto &replica_dir_index = replica_dir_indexes[disk_index];
            if (replica_dir_index >= replica_dirs.size()) {
                // All of the replicas for the disk `disks[disk_index]` have begun to be loaded,
                // thus just skip to next disk.
                ++finished_disks;
                continue;
            }

            const auto &disk_node = disks[disk_index].disk_node;
            auto &load_disk_queue = load_disk_queues[disk_index];
            if (load_disk_queue.size() >= FLAGS_max_replicas_on_load_for_each_disk) {
                // Loading replicas should be throttled in case that disk IO is saturated.
                if (!load_disk_queue.front().load_replica_task->wait(
                        static_cast<int>(FLAGS_load_replica_max_wait_time_ms))) {
                    // There might be too many replicas that are being loaded which lead to
                    // slow disk IO, thus turn to load replicas of next disk, and try to load
                    // dir `replica_dir_index` of this disk in the next round.
                    LOG_WARNING("after {} ms, loading dir({}, {}/{}) is still not finished, "
                                "there are {} replicas being loaded for disk({}:{}, {}/{}), "
                                "now turn to next disk, and will begin to load dir({}, {}/{}) "
                                "soon",
                                FLAGS_load_replica_max_wait_time_ms,
                                load_disk_queue.front().replica_dir_path,
                                load_disk_queue.front().replica_dir_index,
                                replica_dirs.size(),
                                load_disk_queue.size(),
                                disk_node->tag,
                                disk_node->full_dir,
                                disk_index,
                                disks.size(),
                                replica_dirs[replica_dir_index],
                                replica_dir_index,
                                replica_dirs.size());
                    continue;
                }

                // Now the queue size is within the limit again, continue to load a new replica dir.
                load_disk_queue.pop();
            }

            if (dsn::replication::is_data_dir_invalid(replica_dirs[replica_dir_index])) {
                LOG_WARNING("ignore dir({}, {}/{}) for disk({}:{}, {}/{})",
                            replica_dirs[replica_dir_index],
                            replica_dir_index,
                            replica_dirs.size(),
                            disk_node->tag,
                            disk_node->full_dir,
                            disk_index,
                            disks.size());
                ++replica_dir_index;
                continue;
            }

            LOG_DEBUG("ready to load dir({}, {}/{}) for disk({}:{}, {}/{})",
                      replica_dirs[replica_dir_index],
                      replica_dir_index,
                      replica_dirs.size(),
                      disk_node->tag,
                      disk_node->full_dir,
                      disk_index,
                      disks.size());

            load_disk_queue.push(replica_dir_loader{
                replica_dir_index,
                replica_dirs[replica_dir_index],
                tasking::create_task(
                    // Ensure that the thread pool is non-partitioned.
                    LPC_REPLICATION_INIT_LOAD,
                    &_tracker,
                    std::bind(static_cast<void (replica_stub::*)(dir_node *,
                                                                 const std::string &,
                                                                 size_t,
                                                                 utils::ex_lock &,
                                                                 replica_map_by_gpid &,
                                                                 std::atomic<size_t> &)>(
                                  &replica_stub::load_replica),
                              this,
                              disk_node,
                              replica_dirs[replica_dir_index],
                              replica_dirs.size(),
                              std::ref(reps_lock),
                              std::ref(reps),
                              std::ref(finished_replica_dirs[disk_index])))});

            load_disk_queue.back().load_replica_task->enqueue();

            ++replica_dir_index;
        }

        if (finished_disks >= disks.size()) {
            // All replicas of all disks have begun to be loaded.
            break;
        }
    }

    // All loading tasks have been in the queue. Just wait all tasks to be finished.
    for (auto &load_disk_queue : load_disk_queues) {
        while (!load_disk_queue.empty()) {
            CHECK_TRUE(load_disk_queue.front().load_replica_task->wait());
            load_disk_queue.pop();
        }
    }
}

void replica_stub::initialize(const replication_options &opts, bool clear /* = false*/)
{
    _primary_host_port = dsn_primary_host_port();
    _primary_host_port_cache = _primary_host_port.to_string();
    LOG_INFO("primary_host_port = {}", _primary_host_port_cache);

    set_options(opts);
    LOG_INFO("meta_servers = {}", fmt::join(_options.meta_servers, ", "));

    _deny_client = FLAGS_deny_client_on_start;
    _verbose_client_log = FLAGS_verbose_client_log_on_start;
    _verbose_commit_log = FLAGS_verbose_commit_log_on_start;
    _release_tcmalloc_memory = FLAGS_mem_release_enabled;
    _mem_release_max_reserved_mem_percentage = FLAGS_mem_release_max_reserved_mem_percentage;

    // clear dirs if need
    if (clear) {
        CHECK(dsn::utils::filesystem::remove_path(_options.slog_dir),
              "Fail to remove {}.",
              _options.slog_dir);
        for (auto &dir : _options.data_dirs) {
            CHECK(dsn::utils::filesystem::remove_path(dir), "Fail to remove {}.", dir);
        }
    }

    const auto &kms_path =
        utils::filesystem::path_combine(_options.data_dirs[0], kms_info::kKmsInfo);
    // FLAGS_data_dirs may be empty when load configuration, use LOG_FATAL instead of group
    // validator.
    if (!FLAGS_encrypt_data_at_rest && utils::filesystem::path_exists(kms_path)) {
        LOG_FATAL("The kms_info file exists at ({}), but [pegasus.server] "
                  "encrypt_data_at_rest is enbale."
                  "Encryption in Pegasus is irreversible after its initial activation.",
                  kms_path);
    }

    dsn::replication::kms_info kms_info;
    if (FLAGS_encrypt_data_at_rest && !utils::is_empty(FLAGS_hadoop_kms_url)) {
        _key_provider.reset(new dsn::security::kms_key_provider(
            ::absl::StrSplit(FLAGS_hadoop_kms_url, ",", ::absl::SkipEmpty()),
            FLAGS_encryption_cluster_key_name));
        const auto &ec = dsn::utils::load_rjobj_from_file(
            kms_path, dsn::utils::FileDataType::kNonSensitive, &kms_info);
        if (ec != dsn::ERR_PATH_NOT_FOUND && ec != dsn::ERR_OK) {
            CHECK_EQ_MSG(dsn::ERR_OK, ec, "Can't load kms key from kms-info file");
        }
        // Upon the first launch, the encryption key should be empty. The process will then retrieve
        // EEK, IV, and KV from KMS.
        // After the first launch, the encryption key, obtained from the kms-info file, should not
        // be empty. The process will then acquire the DEK from KMS.
        if (ec == dsn::ERR_PATH_NOT_FOUND) {
            LOG_WARNING("It's normal to encounter a temporary inability to open the kms-info file "
                        "during the first process launch.");
            CHECK_OK(_key_provider->GenerateEncryptionKey(&kms_info),
                     "Generate encryption key from kms failed");
        }
        CHECK_OK(_key_provider->DecryptEncryptionKey(kms_info, &_server_key),
                 "Get decryption key failed from {}",
                 kms_path);
        FLAGS_server_key = _server_key.c_str();
    }

    // Initialize the file system manager.
    _fs_manager.initialize(_options.data_dirs, _options.data_dir_tags);

    if (_key_provider && !utils::filesystem::path_exists(kms_path)) {
        const auto &err = dsn::utils::dump_rjobj_to_file(
            kms_info, dsn::utils::FileDataType::kNonSensitive, kms_path);
        CHECK_EQ_MSG(dsn::ERR_OK, err, "Can't store kms key to kms-info file");
    }

    // Check slog is not exist.
    auto full_slog_path = fmt::format("{}/replica/slog/", _options.slog_dir);
    if (utils::filesystem::directory_exists(full_slog_path)) {
        std::vector<std::string> slog_files;
        CHECK(utils::filesystem::get_subfiles(full_slog_path, slog_files, false),
              "check slog files failed");
        CHECK(slog_files.empty(),
              "slog({}) files are not empty. Make sure you are upgrading from 2.5.0",
              full_slog_path);
    }

    // Start to load replicas in available data directories.
    LOG_INFO("start to load replicas");

    replica_map_by_gpid reps;
    load_replicas(reps);

    LOG_INFO("load replicas succeed, replica_count = {}", reps.size());

    bool is_log_complete = true;
    for (auto it = reps.begin(); it != reps.end(); ++it) {
        CHECK_EQ_MSG(it->second->background_sync_checkpoint(), ERR_OK, "sync checkpoint failed");

        it->second->reset_prepare_list_after_replay();

        decree pmax = invalid_decree;
        decree pmax_commit = invalid_decree;
        if (it->second->private_log()) {
            pmax = it->second->private_log()->max_decree(it->first);
            pmax_commit = it->second->private_log()->max_commit_on_disk();
        }

        LOG_INFO(
            "{}: load replica done, durable = {}, committed = {}, "
            "prepared = {}, ballot = {}, "
            "valid_offset_in_plog = {}, max_decree_in_plog = {}, max_commit_on_disk_in_plog = {}",
            it->second->name(),
            it->second->last_durable_decree(),
            it->second->last_committed_decree(),
            it->second->max_prepared_decree(),
            it->second->get_ballot(),
            it->second->get_app()->init_info().init_offset_in_private_log,
            pmax,
            pmax_commit);
    }

    // we will mark all replicas inactive not transient unless all logs are complete
    if (!is_log_complete) {
        LOG_ERROR("logs are not complete for some replicas, which means that shared log is "
                  "truncated, mark all replicas as inactive");
        for (auto &[_, rep] : reps) {
            rep->set_inactive_state_transient(false);
        }
    }

    // replicas stat
    if (!FLAGS_replicas_stat_disabled) {
        _replicas_stat_timer_task = tasking::enqueue_timer(
            LPC_REPLICAS_STAT,
            &_tracker,
            [this] { on_replicas_stat(); },
            std::chrono::milliseconds(FLAGS_replicas_stat_interval_ms),
            0,
            std::chrono::milliseconds(rand::next_u32(0, FLAGS_replicas_stat_interval_ms)));
    }

    // disk stat
    if (!FLAGS_disk_stat_disabled) {
        _disk_stat_timer_task = ::dsn::tasking::enqueue_timer(
            LPC_DISK_STAT,
            &_tracker,
            [this]() { on_disk_stat(); },
            std::chrono::seconds(FLAGS_disk_stat_interval_seconds),
            0,
            std::chrono::seconds(FLAGS_disk_stat_interval_seconds));
    }

    // Attach `reps`.
    _replicas = std::move(reps);
    METRIC_VAR_INCREMENT_BY(total_replicas, _replicas.size());
    for (const auto &[pid, rep] : _replicas) {
        _fs_manager.add_replica(pid, rep->dir());
    }

    _nfs = dsn::nfs_node::create();
    _nfs->start();

    dist::cmd::register_remote_command_rpc();

    if (FLAGS_delay_for_fd_timeout_on_start) {
        uint64_t now_time_ms = dsn_now_ms();
        uint64_t delay_time_ms =
            (FLAGS_fd_grace_seconds + 3) * 1000; // for more 3 seconds than grace seconds
        if (now_time_ms < dsn::utils::process_start_millis() + delay_time_ms) {
            uint64_t delay = dsn::utils::process_start_millis() + delay_time_ms - now_time_ms;
            LOG_INFO("delay for {} ms to make failure detector timeout", delay);
            tasking::enqueue(
                LPC_REPLICA_SERVER_DELAY_START,
                &_tracker,
                [this]() { this->initialize_start(); },
                0,
                std::chrono::milliseconds(delay));
        } else {
            initialize_start();
        }
    } else {
        initialize_start();
    }
}

void replica_stub::initialize_start()
{
    if (_is_running) {
        return;
    }

    // start timer for configuration sync
    if (!FLAGS_config_sync_disabled) {
        _config_sync_timer_task = tasking::enqueue_timer(
            LPC_QUERY_CONFIGURATION_ALL,
            &_tracker,
            [this]() {
                zauto_lock l(_state_lock);
                this->query_configuration_by_node();
            },
            std::chrono::milliseconds(FLAGS_config_sync_interval_ms),
            0,
            std::chrono::milliseconds(FLAGS_config_sync_interval_ms));
    }

#ifdef DSN_ENABLE_GPERF
    _mem_release_timer_task =
        tasking::enqueue_timer(LPC_MEM_RELEASE,
                               &_tracker,
                               std::bind(&replica_stub::gc_tcmalloc_memory, this, false),
                               std::chrono::milliseconds(FLAGS_mem_release_check_interval_ms),
                               0,
                               std::chrono::milliseconds(FLAGS_mem_release_check_interval_ms));
#endif

    if (FLAGS_duplication_enabled) {
        _duplication_sync_timer = std::make_unique<duplication_sync_timer>(this);
        _duplication_sync_timer->start();
    }

    _backup_server = std::make_unique<replica_backup_server>(this);

    // init liveness monitor
    CHECK_EQ(NS_Disconnected, _state);
    if (!FLAGS_fd_disabled) {
        _failure_detector = std::make_shared<dsn::dist::slave_failure_detector_with_multimaster>(
            _options.meta_servers,
            [this]() { this->on_meta_server_disconnected(); },
            [this]() { this->on_meta_server_connected(); });

        CHECK_GT_MSG(FLAGS_fd_grace_seconds, FLAGS_fd_lease_seconds, "");
        CHECK_EQ_MSG(_failure_detector->start(FLAGS_fd_check_interval_seconds,
                                              FLAGS_fd_beacon_interval_seconds,
                                              FLAGS_fd_lease_seconds,
                                              FLAGS_fd_grace_seconds),
                     ERR_OK,
                     "FD start failed");

        _failure_detector->register_master(_failure_detector->current_server_contact());
    } else {
        _state = NS_Connected;
    }

    _is_running = true;
}

dsn::error_code replica_stub::on_kill_replica(gpid id)
{
    LOG_INFO("kill replica: gpid = {}", id);
    if (id.get_app_id() == -1 || id.get_partition_index() == -1) {
        replica_map_by_gpid rs;
        {
            zauto_read_lock l(_replicas_lock);
            rs = _replicas;
        }
        for (auto it = rs.begin(); it != rs.end(); ++it) {
            replica_ptr &r = it->second;
            if (id.get_app_id() == -1 || id.get_app_id() == r->get_gpid().get_app_id())
                r->inject_error(ERR_INJECTED);
        }
        return ERR_OK;
    } else {
        error_code err = ERR_INVALID_PARAMETERS;
        replica_ptr r = get_replica(id);
        if (r == nullptr) {
            err = ERR_OBJECT_NOT_FOUND;
        } else {
            r->inject_error(ERR_INJECTED);
            err = ERR_OK;
        }
        return err;
    }
}

std::vector<replica_ptr> replica_stub::get_all_replicas() const
{
    std::vector<replica_ptr> result;
    {
        zauto_read_lock l(_replicas_lock);
        std::transform(_replicas.begin(),
                       _replicas.end(),
                       std::back_inserter(result),
                       [](const std::pair<gpid, replica_ptr> &r) { return r.second; });
    }
    return result;
}

std::vector<replica_ptr> replica_stub::get_all_primaries() const
{
    std::vector<replica_ptr> result;
    {
        zauto_read_lock l(_replicas_lock);
        for (const auto &[_, r] : _replicas) {
            if (r->status() != partition_status::PS_PRIMARY) {
                continue;
            }
            result.push_back(r);
        }
    }
    return result;
}

replica_ptr replica_stub::get_replica(gpid id) const
{
    zauto_read_lock l(_replicas_lock);
    auto it = _replicas.find(id);
    if (it != _replicas.end())
        return it->second;
    else
        return nullptr;
}

replica_stub::replica_life_cycle replica_stub::get_replica_life_cycle(gpid id)
{
    zauto_read_lock l(_replicas_lock);
    if (_opening_replicas.find(id) != _opening_replicas.end())
        return replica_stub::RL_creating;
    if (_replicas.find(id) != _replicas.end())
        return replica_stub::RL_serving;
    if (_closing_replicas.find(id) != _closing_replicas.end())
        return replica_stub::RL_closing;
    if (_closed_replicas.find(id) != _closed_replicas.end())
        return replica_stub::RL_closed;
    return replica_stub::RL_invalid;
}

void replica_stub::on_client_write(gpid id, dsn::message_ex *request)
{
    if (_deny_client) {
        // ignore and do not reply
        return;
    }
    if (_verbose_client_log && request) {
        LOG_INFO("{}@{}: client = {}, code = {}, timeout = {}",
                 id,
                 _primary_host_port_cache,
                 request->header->from_address,
                 request->header->rpc_name,
                 request->header->client.timeout_ms);
    }
    replica_ptr rep = get_replica(id);
    if (rep != nullptr) {
        rep->on_client_write(request);
    } else {
        response_client(id, false, request, partition_status::PS_INVALID, ERR_OBJECT_NOT_FOUND);
    }
}

void replica_stub::on_client_read(gpid id, dsn::message_ex *request)
{
    if (_deny_client) {
        // ignore and do not reply
        return;
    }
    if (_verbose_client_log && request) {
        LOG_INFO("{}@{}: client = {}, code = {}, timeout = {}",
                 id,
                 _primary_host_port_cache,
                 request->header->from_address,
                 request->header->rpc_name,
                 request->header->client.timeout_ms);
    }
    replica_ptr rep = get_replica(id);
    if (rep != nullptr) {
        rep->on_client_read(request);
    } else {
        response_client(id, true, request, partition_status::PS_INVALID, ERR_OBJECT_NOT_FOUND);
    }
}

void replica_stub::on_config_proposal(const configuration_update_request &proposal)
{
    if (!is_connected()) {
        LOG_WARNING("{}@{}: received config proposal {} for {}: not connected, ignore",
                    proposal.config.pid,
                    _primary_host_port_cache,
                    enum_to_string(proposal.type),
                    FMT_HOST_PORT_AND_IP(proposal, node));
        return;
    }

    LOG_INFO("{}@{}: received config proposal {} for {}",
             proposal.config.pid,
             _primary_host_port_cache,
             enum_to_string(proposal.type),
             FMT_HOST_PORT_AND_IP(proposal, node));

    replica_ptr rep = get_replica(proposal.config.pid);
    if (rep == nullptr) {
        if (proposal.type == config_type::CT_ASSIGN_PRIMARY) {
            std::shared_ptr<configuration_update_request> req2(new configuration_update_request);
            *req2 = proposal;
            begin_open_replica(proposal.info, proposal.config.pid, nullptr, req2);
        } else if (proposal.type == config_type::CT_UPGRADE_TO_PRIMARY) {
            remove_replica_on_meta_server(proposal.info, proposal.config);
        }
    }

    if (rep != nullptr) {
        rep->on_config_proposal((configuration_update_request &)proposal);
    }
}

void replica_stub::on_query_decree(query_replica_decree_rpc rpc)
{
    const query_replica_decree_request &req = rpc.request();
    query_replica_decree_response &resp = rpc.response();

    replica_ptr rep = get_replica(req.pid);
    if (rep != nullptr) {
        resp.err = ERR_OK;
        if (partition_status::PS_POTENTIAL_SECONDARY == rep->status()) {
            resp.last_decree = 0;
        } else {
            resp.last_decree = rep->last_committed_decree();
            // TODO: use the following to alleviate data lost
            // resp.last_decree = rep->last_prepared_decree();
        }
    } else {
        resp.err = ERR_OBJECT_NOT_FOUND;
        resp.last_decree = 0;
    }
}

void replica_stub::on_query_replica_info(query_replica_info_rpc rpc)
{
    query_replica_info_response &resp = rpc.response();
    std::set<gpid> visited_replicas;
    {
        zauto_read_lock l(_replicas_lock);
        for (auto it = _replicas.begin(); it != _replicas.end(); ++it) {
            replica_ptr &r = it->second;
            replica_info info;
            get_replica_info(info, r);
            if (visited_replicas.find(info.pid) == visited_replicas.end()) {
                visited_replicas.insert(info.pid);
                resp.replicas.push_back(std::move(info));
            }
        }
        for (auto it = _closing_replicas.begin(); it != _closing_replicas.end(); ++it) {
            const replica_info &info = std::get<3>(it->second);
            if (visited_replicas.find(info.pid) == visited_replicas.end()) {
                visited_replicas.insert(info.pid);
                resp.replicas.push_back(info);
            }
        }
        for (auto it = _closed_replicas.begin(); it != _closed_replicas.end(); ++it) {
            const replica_info &info = it->second.second;
            if (visited_replicas.find(info.pid) == visited_replicas.end()) {
                visited_replicas.insert(info.pid);
                resp.replicas.push_back(info);
            }
        }
    }
    resp.err = ERR_OK;
}

void replica_stub::on_query_last_checkpoint(query_last_checkpoint_info_rpc rpc)
{
    const learn_request &request = rpc.request();
    learn_response &response = rpc.response();

    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->on_query_last_checkpoint(response);
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

// ThreadPool: THREAD_POOL_DEFAULT
void replica_stub::on_query_disk_info(query_disk_info_rpc rpc)
{
    const query_disk_info_request &req = rpc.request();
    query_disk_info_response &resp = rpc.response();
    int app_id = 0;
    if (!req.app_name.empty()) {
        zauto_read_lock l(_replicas_lock);
        app_id = get_app_id_from_replicas(req.app_name);
        if (app_id == 0) {
            resp.err = ERR_OBJECT_NOT_FOUND;
            return;
        }
    }

    resp.disk_infos = _fs_manager.get_disk_infos(app_id);
    // Get the statistics from fs_manager's metrics, they are thread-safe.
    resp.total_capacity_mb = _fs_manager._total_capacity_mb.load(std::memory_order_relaxed);
    resp.total_available_mb = _fs_manager._total_available_mb.load(std::memory_order_relaxed);
    resp.err = ERR_OK;
}

void replica_stub::on_disk_migrate(replica_disk_migrate_rpc rpc)
{
    const replica_disk_migrate_request &request = rpc.request();
    replica_disk_migrate_response &response = rpc.response();

    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->disk_migrator()->on_migrate_replica(rpc); // THREAD_POOL_DEFAULT
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::on_query_app_info(query_app_info_rpc rpc)
{
    const query_app_info_request &req = rpc.request();
    query_app_info_response &resp = rpc.response();

    LOG_INFO("got query app info request from ({})", FMT_HOST_PORT_AND_IP(req, meta_server));
    resp.err = dsn::ERR_OK;
    std::set<app_id> visited_apps;
    {
        zauto_read_lock l(_replicas_lock);
        for (auto it = _replicas.begin(); it != _replicas.end(); ++it) {
            replica_ptr &r = it->second;
            const app_info &info = *r->get_app_info();
            if (visited_apps.find(info.app_id) == visited_apps.end()) {
                resp.apps.push_back(info);
                visited_apps.insert(info.app_id);
            }
        }
        for (auto it = _closing_replicas.begin(); it != _closing_replicas.end(); ++it) {
            const app_info &info = std::get<2>(it->second);
            if (visited_apps.find(info.app_id) == visited_apps.end()) {
                resp.apps.push_back(info);
                visited_apps.insert(info.app_id);
            }
        }
        for (auto it = _closed_replicas.begin(); it != _closed_replicas.end(); ++it) {
            const app_info &info = it->second.first;
            if (visited_apps.find(info.app_id) == visited_apps.end()) {
                resp.apps.push_back(info);
                visited_apps.insert(info.app_id);
            }
        }
    }
}

// ThreadPool: THREAD_POOL_DEFAULT
void replica_stub::on_add_new_disk(add_new_disk_rpc rpc)
{
    const auto &disk_str = rpc.request().disk_str;
    auto &resp = rpc.response();
    resp.err = ERR_OK;

    std::vector<std::string> data_dirs;
    std::vector<std::string> data_dir_tags;
    std::string err_msg;
    if (disk_str.empty() ||
        !replication_options::get_data_dir_and_tag(disk_str,
                                                   "",
                                                   replication_options::kReplicaAppType,
                                                   data_dirs,
                                                   data_dir_tags,
                                                   err_msg)) {
        resp.err = ERR_INVALID_PARAMETERS;
        resp.__set_err_hint(fmt::format("invalid str({}), err_msg: {}", disk_str, err_msg));
        return;
    }

    for (auto i = 0; i < data_dir_tags.size(); ++i) {
        // TODO(yingchun): move the following code to fs_manager.
        auto dir = data_dirs[i];
        if (_fs_manager.is_dir_node_exist(dir, data_dir_tags[i])) {
            resp.err = ERR_NODE_ALREADY_EXIST;
            resp.__set_err_hint(
                fmt::format("data_dir({}) tag({}) already exist", dir, data_dir_tags[i]));
            return;
        }

        if (dsn_unlikely(utils::filesystem::directory_exists(dir) &&
                         !utils::filesystem::is_directory_empty(dir).second)) {
            resp.err = ERR_DIR_NOT_EMPTY;
            resp.__set_err_hint(fmt::format("Disk({}) directory is not empty", dir));
            return;
        }

        std::string cdir;
        if (dsn_unlikely(!utils::filesystem::create_directory(dir, cdir, err_msg) ||
                         !utils::filesystem::check_dir_rw(dir, err_msg))) {
            resp.err = ERR_FILE_OPERATION_FAILED;
            resp.__set_err_hint(err_msg);
            return;
        }

        LOG_INFO("Add a new disk in fs_manager, data_dir={}, tag={}", cdir, data_dir_tags[i]);
        // TODO(yingchun): there is a gap between _fs_manager.is_dir_node_exist() and
        // _fs_manager.add_new_dir_node() which is not atomic.
        _fs_manager.add_new_dir_node(cdir, data_dir_tags[i]);
    }
}

void replica_stub::on_nfs_copy(const ::dsn::service::copy_request &request,
                               ::dsn::rpc_replier<::dsn::service::copy_response> &reply)
{
    if (check_status_and_authz_with_reply(request, reply, ranger::access_type::kWrite)) {
        _nfs->on_copy(request, reply);
    }
}

void replica_stub::on_nfs_get_file_size(
    const ::dsn::service::get_file_size_request &request,
    ::dsn::rpc_replier<::dsn::service::get_file_size_response> &reply)
{
    if (check_status_and_authz_with_reply(request, reply, ranger::access_type::kWrite)) {
        _nfs->on_get_file_size(request, reply);
    }
}

void replica_stub::on_prepare(dsn::message_ex *request)
{
    gpid id;
    dsn::unmarshall(request, id);
    replica_ptr rep = get_replica(id);
    if (rep != nullptr) {
        rep->on_prepare(request);
    } else {
        prepare_ack resp;
        resp.pid = id;
        resp.err = ERR_OBJECT_NOT_FOUND;
        reply(request, resp);
    }
}

void replica_stub::on_group_check(group_check_rpc rpc)
{
    const group_check_request &request = rpc.request();
    group_check_response &response = rpc.response();
    if (!is_connected()) {
        LOG_WARNING("{}@{}: received group check: not connected, ignore",
                    request.config.pid,
                    _primary_host_port_cache);
        return;
    }

    LOG_INFO("{}@{}: received group check, primary = {}, ballot = {}, status = {}, "
             "last_committed_decree = {}",
             request.config.pid,
             _primary_host_port_cache,
             FMT_HOST_PORT_AND_IP(request.config, primary),
             request.config.ballot,
             enum_to_string(request.config.status),
             request.last_committed_decree);

    replica_ptr rep = get_replica(request.config.pid);
    if (rep != nullptr) {
        rep->on_group_check(request, response);
    } else {
        if (request.config.status == partition_status::PS_POTENTIAL_SECONDARY) {
            std::shared_ptr<group_check_request> req(new group_check_request);
            *req = request;

            begin_open_replica(request.app, request.config.pid, req, nullptr);
            response.err = ERR_OK;
            response.learner_signature = invalid_signature;
        } else {
            response.err = ERR_OBJECT_NOT_FOUND;
        }
    }
}

void replica_stub::on_learn(dsn::message_ex *msg)
{
    learn_response response;
    learn_request request;
    ::dsn::unmarshall(msg, request);

    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        if (!rep->access_controller_allowed(msg, ranger::access_type::kWrite)) {
            response.err = ERR_ACL_DENY;
            reply(msg, response);
            return;
        }
        rep->on_learn(msg, request);
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
        reply(msg, response);
    }
}

void replica_stub::on_learn_completion_notification(learn_completion_notification_rpc rpc)
{
    const group_check_response &report = rpc.request();
    learn_notify_response &response = rpc.response();
    response.pid = report.pid;
    response.signature = report.learner_signature;
    replica_ptr rep = get_replica(report.pid);
    if (rep != nullptr) {
        rep->on_learn_completion_notification(report, response);
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::on_add_learner(const group_check_request &request)
{
    if (!is_connected()) {
        LOG_WARNING("{}@{}: received add learner, primary = {}, not connected, ignore",
                    request.config.pid,
                    _primary_host_port_cache,
                    FMT_HOST_PORT_AND_IP(request.config, primary));
        return;
    }

    LOG_INFO("{}@{}: received add learner, primary = {}, ballot = {}, status = {}, "
             "last_committed_decree = {}",
             request.config.pid,
             _primary_host_port_cache,
             FMT_HOST_PORT_AND_IP(request.config, primary),
             request.config.ballot,
             enum_to_string(request.config.status),
             request.last_committed_decree);

    replica_ptr rep = get_replica(request.config.pid);
    if (rep != nullptr) {
        rep->on_add_learner(request);
    } else {
        std::shared_ptr<group_check_request> req(new group_check_request);
        *req = request;
        begin_open_replica(request.app, request.config.pid, req, nullptr);
    }
}

void replica_stub::on_remove(const replica_configuration &request)
{
    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->on_remove(request);
    }
}

void replica_stub::get_replica_info(replica_info &info, replica_ptr r)
{
    info.pid = r->get_gpid();
    info.ballot = r->get_ballot();
    info.status = r->status();
    info.app_type = r->get_app_info()->app_type;
    info.last_committed_decree = r->last_committed_decree();
    info.last_prepared_decree = r->last_prepared_decree();
    info.last_durable_decree = r->last_durable_decree();
    info.disk_tag = r->get_dir_node()->tag;
    info.__set_manual_compact_status(r->get_manual_compact_status());
}

void replica_stub::get_local_replicas(std::vector<replica_info> &replicas)
{
    zauto_read_lock l(_replicas_lock);
    // local_replicas = replicas + closing_replicas + closed_replicas
    int total_replicas = _replicas.size() + _closing_replicas.size() + _closed_replicas.size();
    replicas.reserve(total_replicas);

    for (auto &pairs : _replicas) {
        replica_ptr &rep = pairs.second;
        // child partition should not sync config from meta server
        // because it is not ready in meta view
        if (rep->status() == partition_status::PS_PARTITION_SPLIT) {
            continue;
        }
        replica_info info;
        get_replica_info(info, rep);
        replicas.push_back(std::move(info));
    }

    for (auto &pairs : _closing_replicas) {
        replicas.push_back(std::get<3>(pairs.second));
    }

    for (auto &pairs : _closed_replicas) {
        replicas.push_back(pairs.second.second);
    }
}

// run in THREAD_POOL_META_SERVER
// assert(_state_lock.locked())
void replica_stub::query_configuration_by_node()
{
    if (_state == NS_Disconnected) {
        return;
    }

    if (_config_query_task != nullptr) {
        return;
    }

    dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_CONFIG_SYNC);

    configuration_query_by_node_request req;
    SET_IP_AND_HOST_PORT(req, node, primary_address(), _primary_host_port);

    // TODO: send stored replicas may cost network, we shouldn't config the frequency
    get_local_replicas(req.stored_replicas);
    req.__isset.stored_replicas = true;

    ::dsn::marshall(msg, req);

    LOG_INFO("send query node partitions request to meta server, stored_replicas_count = {}",
             req.stored_replicas.size());

    const auto &target =
        dsn::dns_resolver::instance().resolve_address(_failure_detector->get_servers());
    _config_query_task =
        rpc::call(target,
                  msg,
                  &_tracker,
                  [this](error_code err, dsn::message_ex *request, dsn::message_ex *resp) {
                      on_node_query_reply(err, request, resp);
                  });
}

void replica_stub::on_meta_server_connected()
{
    LOG_INFO("meta server connected");

    zauto_lock l(_state_lock);
    if (_state == NS_Disconnected) {
        _state = NS_Connecting;
        tasking::enqueue(LPC_QUERY_CONFIGURATION_ALL, &_tracker, [this]() {
            zauto_lock l(_state_lock);
            this->query_configuration_by_node();
        });
    }
}

// run in THREAD_POOL_META_SERVER
void replica_stub::on_node_query_reply(error_code err,
                                       dsn::message_ex *request,
                                       dsn::message_ex *response)
{
    LOG_INFO("query node partitions replied, err = {}", err);

    zauto_lock sl(_state_lock);
    _config_query_task = nullptr;
    if (err != ERR_OK) {
        if (_state == NS_Connecting) {
            query_configuration_by_node();
        }
    } else {
        if (_state == NS_Connecting) {
            _state = NS_Connected;
        }

        // DO NOT UPDATE STATE WHEN DISCONNECTED
        if (_state != NS_Connected)
            return;

        configuration_query_by_node_response resp;
        ::dsn::unmarshall(response, resp);

        if (resp.err == ERR_BUSY) {
            int delay_ms = 500;
            LOG_INFO("resend query node partitions request after {} ms for resp.err = ERR_BUSY",
                     delay_ms);
            _config_query_task = tasking::enqueue(
                LPC_QUERY_CONFIGURATION_ALL,
                &_tracker,
                [this]() {
                    zauto_lock l(_state_lock);
                    _config_query_task = nullptr;
                    this->query_configuration_by_node();
                },
                0,
                std::chrono::milliseconds(delay_ms));
            return;
        }
        if (resp.err != ERR_OK) {
            LOG_INFO("ignore query node partitions response for resp.err = {}", resp.err);
            return;
        }

        LOG_INFO("process query node partitions response for resp.err = ERR_OK, "
                 "partitions_count({}), gc_replicas_count({})",
                 resp.partitions.size(),
                 resp.gc_replicas.size());

        replica_map_by_gpid reps;
        {
            zauto_read_lock rl(_replicas_lock);
            reps = _replicas;
        }

        for (const auto &config_update : resp.partitions) {
            reps.erase(config_update.config.pid);
            tasking::enqueue(
                LPC_QUERY_NODE_CONFIGURATION_SCATTER,
                &_tracker,
                std::bind(&replica_stub::on_node_query_reply_scatter, this, this, config_update),
                config_update.config.pid.thread_hash());
        }

        // For the replicas that do not exist on meta_servers.
        for (const auto &[pid, _] : reps) {
            tasking::enqueue(
                LPC_QUERY_NODE_CONFIGURATION_SCATTER2,
                &_tracker,
                std::bind(&replica_stub::on_node_query_reply_scatter2, this, this, pid),
                pid.thread_hash());
        }

        // handle the replicas which need to be gc
        if (resp.__isset.gc_replicas) {
            for (replica_info &rep : resp.gc_replicas) {
                replica_stub::replica_life_cycle lc = get_replica_life_cycle(rep.pid);
                if (lc == replica_stub::RL_closed) {
                    tasking::enqueue(LPC_GARBAGE_COLLECT_LOGS_AND_REPLICAS,
                                     &_tracker,
                                     std::bind(&replica_stub::on_gc_replica, this, this, rep.pid),
                                     0);
                }
            }
        }
    }
}

void replica_stub::set_meta_server_connected_for_test(
    const configuration_query_by_node_response &resp)
{
    zauto_lock l(_state_lock);
    CHECK_NE(_state, NS_Connected);
    _state = NS_Connected;

    for (auto it = resp.partitions.begin(); it != resp.partitions.end(); ++it) {
        tasking::enqueue(LPC_QUERY_NODE_CONFIGURATION_SCATTER,
                         &_tracker,
                         std::bind(&replica_stub::on_node_query_reply_scatter, this, this, *it),
                         it->config.pid.thread_hash());
    }
}

void replica_stub::set_replica_state_subscriber_for_test(replica_state_subscriber subscriber,
                                                         bool is_long_subscriber)
{
    _replica_state_subscriber = subscriber;
    _is_long_subscriber = is_long_subscriber;
}

// this_ is used to hold a ref to replica_stub so we don't need to cancel the task on
// replica_stub::close
// ThreadPool: THREAD_POOL_REPLICATION
void replica_stub::on_node_query_reply_scatter(replica_stub_ptr this_,
                                               const configuration_update_request &req)
{
    replica_ptr replica = get_replica(req.config.pid);
    if (replica != nullptr) {
        replica->on_config_sync(req.info,
                                req.config,
                                req.__isset.meta_split_status ? req.meta_split_status
                                                              : split_status::NOT_SPLIT);
    } else {
        if (req.config.hp_primary == _primary_host_port) {
            LOG_INFO("{}@{}: replica not exists on replica server, which is primary, remove it "
                     "from meta server",
                     req.config.pid,
                     _primary_host_port_cache);
            remove_replica_on_meta_server(req.info, req.config);
        } else {
            LOG_INFO(
                "{}@{}: replica not exists on replica server, which is not primary, just ignore",
                req.config.pid,
                _primary_host_port_cache);
        }
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_stub::on_node_query_reply_scatter2(replica_stub_ptr this_, gpid id)
{
    replica_ptr replica = get_replica(id);
    if (replica != nullptr && replica->status() != partition_status::PS_POTENTIAL_SECONDARY &&
        replica->status() != partition_status::PS_PARTITION_SPLIT) {
        if (replica->status() == partition_status::PS_INACTIVE &&
            dsn_now_ms() - replica->create_time_milliseconds() <
                FLAGS_gc_memory_replica_interval_ms) {
            LOG_INFO("{}: replica not exists on meta server, wait to close", replica->name());
            return;
        }

        LOG_INFO("{}: replica not exists on meta server, remove", replica->name());

        // TODO: set PS_INACTIVE instead for further state reuse
        replica->update_local_configuration_with_no_ballot_change(partition_status::PS_ERROR);
    }
}

void replica_stub::remove_replica_on_meta_server(const app_info &info,
                                                 const partition_configuration &pc)
{
    if (FLAGS_fd_disabled) {
        return;
    }

    dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_UPDATE_PARTITION_CONFIGURATION);

    std::shared_ptr<configuration_update_request> request(new configuration_update_request);
    request->info = info;
    request->config = pc;
    request->config.ballot++;
    SET_IP_AND_HOST_PORT(*request, node, primary_address(), _primary_host_port);
    request->type = config_type::CT_DOWNGRADE_TO_INACTIVE;

    if (_primary_host_port == pc.hp_primary) {
        RESET_IP_AND_HOST_PORT(request->config, primary);
    } else if (REMOVE_IP_AND_HOST_PORT(
                   primary_address(), _primary_host_port, request->config, secondaries)) {
    } else {
        return;
    }

    ::dsn::marshall(msg, *request);

    const auto &target =
        dsn::dns_resolver::instance().resolve_address(_failure_detector->get_servers());
    rpc::call(target, msg, nullptr, [](error_code err, dsn::message_ex *, dsn::message_ex *) {});
}

void replica_stub::on_meta_server_disconnected()
{
    LOG_INFO("meta server disconnected");

    zauto_lock sl(_state_lock);
    if (NS_Disconnected == _state)
        return;

    _state = NS_Disconnected;

    replica_map_by_gpid reps;
    {
        zauto_read_lock rl(_replicas_lock);
        reps = _replicas;
    }

    for (const auto &[pid, _] : reps) {
        tasking::enqueue(
            LPC_CM_DISCONNECTED_SCATTER,
            &_tracker,
            std::bind(&replica_stub::on_meta_server_disconnected_scatter, this, this, pid),
            pid.thread_hash());
    }
}

// this_ is used to hold a ref to replica_stub so we don't need to cancel the task on
// replica_stub::close
void replica_stub::on_meta_server_disconnected_scatter(replica_stub_ptr this_, gpid id)
{
    {
        zauto_lock l(_state_lock);
        if (_state != NS_Disconnected)
            return;
    }

    replica_ptr replica = get_replica(id);
    if (replica != nullptr) {
        replica->on_meta_server_disconnected();
    }
}

void replica_stub::response_client(gpid id,
                                   bool is_read,
                                   dsn::message_ex *request,
                                   partition_status::type status,
                                   error_code error)
{
    if (error == ERR_BUSY) {
        if (is_read) {
            METRIC_VAR_INCREMENT(read_busy_requests);
        } else {
            METRIC_VAR_INCREMENT(write_busy_requests);
        }
    } else if (error != ERR_OK) {
        if (is_read) {
            METRIC_VAR_INCREMENT(read_failed_requests);
        } else {
            METRIC_VAR_INCREMENT(write_failed_requests);
        }
        LOG_ERROR("{}@{}: {} fail: client = {}, code = {}, timeout = {}, status = {}, error = {}",
                  id,
                  _primary_host_port_cache,
                  is_read ? "read" : "write",
                  request == nullptr ? "null" : request->header->from_address.to_string(),
                  request == nullptr ? "null" : request->header->rpc_name,
                  request == nullptr ? 0 : request->header->client.timeout_ms,
                  enum_to_string(status),
                  error);
    }

    if (request != nullptr) {
        dsn_rpc_reply(request->create_response(), error);
    }
}

void replica_stub::on_gc_replica(replica_stub_ptr this_, gpid id)
{
    std::pair<app_info, replica_info> closed_info;
    {
        zauto_write_lock l(_replicas_lock);
        auto iter = _closed_replicas.find(id);
        if (iter == _closed_replicas.end())
            return;
        closed_info = iter->second;
        _closed_replicas.erase(iter);
    }
    _fs_manager.remove_replica(id);

    const auto *const dn = _fs_manager.find_replica_dir(closed_info.first.app_type, id);
    if (dn == nullptr) {
        LOG_WARNING(
            "gc closed replica({}.{}) failed, no exist data", id, closed_info.first.app_type);
        return;
    }

    const auto replica_path = dn->replica_dir(closed_info.first.app_type, id);
    CHECK(
        dsn::utils::filesystem::directory_exists(replica_path), "dir({}) not exist", replica_path);
    LOG_INFO("start to move replica({}) as garbage, path: {}", id, replica_path);
    const auto rename_path = fmt::format("{}.{}{}", replica_path, dsn_now_us(), kFolderSuffixGar);
    if (!dsn::utils::filesystem::rename_path(replica_path, rename_path)) {
        LOG_WARNING("gc_replica: failed to move directory '{}' to '{}'", replica_path, rename_path);

        // if gc the replica failed, add it back
        {
            zauto_write_lock l(_replicas_lock);
            _closed_replicas.emplace(id, closed_info);
        }
        _fs_manager.add_replica(id, replica_path);
    } else {
        LOG_WARNING("gc_replica: replica_dir_op succeed to move directory '{}' to '{}'",
                    replica_path,
                    rename_path);
        METRIC_VAR_INCREMENT(moved_garbage_replicas);
    }
}

void replica_stub::on_replicas_stat()
{
    uint64_t start = dsn_now_ns();

    replica_stat_info_by_gpid rep_stat_info_by_gpid;
    {
        zauto_read_lock l(_replicas_lock);
        // A replica was removed from _replicas before it would be closed by replica::close().
        // Thus it's safe to use the replica after fetching its ref pointer from _replicas.
        for (const auto &replica : _replicas) {
            const auto &rep = replica.second;

            auto &rep_stat_info = rep_stat_info_by_gpid[replica.first];
            rep_stat_info.rep = rep;
            rep_stat_info.status = rep->status();
            rep_stat_info.plog = rep->private_log();
            rep_stat_info.last_durable_decree = rep->last_durable_decree();
        }
    }

    LOG_INFO("start replicas statistics, replica_count = {}", rep_stat_info_by_gpid.size());

    // statistic learning info
    uint64_t learning_max_duration_time_ms = 0;
    uint64_t learning_max_copy_file_size = 0;
    uint64_t bulk_load_running_count = 0;
    uint64_t bulk_load_max_ingestion_time_ms = 0;
    uint64_t bulk_load_max_duration_time_ms = 0;
    uint64_t splitting_max_duration_time_ms = 0;
    uint64_t splitting_max_async_learn_time_ms = 0;
    uint64_t splitting_max_copy_file_size = 0;

    std::map<partition_status::type, size_t> status_counts;
    for (const auto &[_, rep_stat_info] : rep_stat_info_by_gpid) {
        const auto &rep = rep_stat_info.rep;
        ++status_counts[rep->status()];

        if (rep->status() == partition_status::PS_POTENTIAL_SECONDARY) {
            learning_max_duration_time_ms = std::max(
                learning_max_duration_time_ms, rep->_potential_secondary_states.duration_ms());
            learning_max_copy_file_size =
                std::max(learning_max_copy_file_size,
                         rep->_potential_secondary_states.learning_copy_file_size);

            continue;
        }

        if (rep->status() == partition_status::PS_PRIMARY ||
            rep->status() == partition_status::PS_SECONDARY) {
            if (rep->get_bulk_loader()->get_bulk_load_status() != bulk_load_status::BLS_INVALID) {
                bulk_load_running_count++;
                bulk_load_max_ingestion_time_ms =
                    std::max(bulk_load_max_ingestion_time_ms, rep->ingestion_duration_ms());
                bulk_load_max_duration_time_ms =
                    std::max(bulk_load_max_duration_time_ms, rep->get_bulk_loader()->duration_ms());
            }

            continue;
        }

        // splitting_max_copy_file_size, rep->_split_states.copy_file_size
        if (rep->status() == partition_status::PS_PARTITION_SPLIT) {
            splitting_max_duration_time_ms =
                std::max(splitting_max_duration_time_ms, rep->_split_states.total_ms());
            splitting_max_async_learn_time_ms =
                std::max(splitting_max_async_learn_time_ms, rep->_split_states.async_learn_ms());
            splitting_max_copy_file_size =
                std::max(splitting_max_copy_file_size, rep->_split_states.splitting_copy_file_size);

            continue;
        }
    }

    METRIC_VAR_SET(inactive_replicas, status_counts[partition_status::PS_INACTIVE]);
    METRIC_VAR_SET(error_replicas, status_counts[partition_status::PS_ERROR]);
    METRIC_VAR_SET(primary_replicas, status_counts[partition_status::PS_PRIMARY]);
    METRIC_VAR_SET(secondary_replicas, status_counts[partition_status::PS_SECONDARY]);
    METRIC_VAR_SET(learning_replicas, status_counts[partition_status::PS_POTENTIAL_SECONDARY]);
    METRIC_VAR_SET(learning_replicas_max_duration_ms, learning_max_duration_time_ms);
    METRIC_VAR_SET(learning_replicas_max_copy_file_bytes, learning_max_copy_file_size);
    METRIC_VAR_SET(bulk_load_running_count, bulk_load_running_count);
    METRIC_VAR_SET(bulk_load_ingestion_max_duration_ms, bulk_load_max_ingestion_time_ms);
    METRIC_VAR_SET(bulk_load_max_duration_ms, bulk_load_max_duration_time_ms);
    METRIC_VAR_SET(splitting_replicas, status_counts[partition_status::PS_PARTITION_SPLIT]);
    METRIC_VAR_SET(splitting_replicas_max_duration_ms, splitting_max_duration_time_ms);
    METRIC_VAR_SET(splitting_replicas_async_learn_max_duration_ms,
                   splitting_max_async_learn_time_ms);
    METRIC_VAR_SET(splitting_replicas_max_copy_file_bytes, splitting_max_copy_file_size);

    LOG_INFO("finish replicas statistics, time used {}ns", dsn_now_ns() - start);
}

void replica_stub::on_disk_stat()
{
    LOG_INFO("start to update disk stat");
    uint64_t start = dsn_now_ns();
    disk_cleaning_report report{};

    dsn::replication::disk_remove_useless_dirs(_fs_manager.get_dir_nodes(), report);
    _fs_manager.update_disk_stat();
    update_disk_holding_replicas();

    METRIC_VAR_SET(replica_error_dirs, report.error_replica_count);
    METRIC_VAR_SET(replica_garbage_dirs, report.garbage_replica_count);
    METRIC_VAR_SET(replica_tmp_dirs, report.disk_migrate_tmp_count);
    METRIC_VAR_SET(replica_origin_dirs, report.disk_migrate_origin_count);
    METRIC_VAR_INCREMENT_BY(replica_removed_dirs, report.remove_dir_count);

    LOG_INFO("finish to update disk stat, time_used_ns = {}", dsn_now_ns() - start);
}

task_ptr replica_stub::begin_open_replica(
    const app_info &app,
    gpid id,
    const std::shared_ptr<group_check_request> &group_check,
    const std::shared_ptr<configuration_update_request> &configuration_update)
{
    _replicas_lock.lock_write();

    if (_replicas.find(id) != _replicas.end()) {
        _replicas_lock.unlock_write();
        LOG_INFO("open replica '{}.{}' failed coz replica is already opened", app.app_type, id);
        return nullptr;
    }

    if (_opening_replicas.find(id) != _opening_replicas.end()) {
        _replicas_lock.unlock_write();
        LOG_INFO("open replica '{}.{}' failed coz replica is under opening", app.app_type, id);
        return nullptr;
    }

    auto it = _closing_replicas.find(id);
    if (it != _closing_replicas.end()) {
        task_ptr tsk = std::get<0>(it->second);
        replica_ptr rep = std::get<1>(it->second);
        if (rep->status() == partition_status::PS_INACTIVE && tsk->cancel(false)) {
            // reopen it
            _closing_replicas.erase(it);
            METRIC_VAR_DECREMENT(closing_replicas);

            _replicas.emplace(id, rep);
            METRIC_VAR_INCREMENT(total_replicas);

            _closed_replicas.erase(id);

            // unlock here to avoid dead lock
            _replicas_lock.unlock_write();

            LOG_INFO("open replica '{}.{}' which is to be closed, reopen it", app.app_type, id);

            // open by add learner
            if (group_check != nullptr) {
                on_add_learner(*group_check);
            }
        } else {
            _replicas_lock.unlock_write();
            LOG_INFO("open replica '{}.{}' failed coz replica is under closing", app.app_type, id);
        }
        return nullptr;
    }

    task_ptr task = tasking::enqueue(
        LPC_OPEN_REPLICA,
        &_tracker,
        std::bind(&replica_stub::open_replica, this, app, id, group_check, configuration_update));

    _opening_replicas[id] = task;
    METRIC_VAR_INCREMENT(opening_replicas);
    _closed_replicas.erase(id);

    _replicas_lock.unlock_write();
    return task;
}

void replica_stub::open_replica(
    const app_info &app,
    gpid id,
    const std::shared_ptr<group_check_request> &group_check,
    const std::shared_ptr<configuration_update_request> &configuration_update)
{
    replica_ptr rep;
    std::string dir;
    auto dn = _fs_manager.find_replica_dir(app.app_type, id);
    if (dn != nullptr) {
        dir = dn->replica_dir(app.app_type, id);
        CHECK(dsn::utils::filesystem::directory_exists(dir), "dir({}) not exist", dir);
        // NOTICE: if partition is DDD, and meta select one replica as primary, it will execute the
        // load-process because of a.b.pegasus is exist, so it will never execute the restore
        // process below
        LOG_INFO("{}@{}: start to load replica {} group check, dir = {}",
                 id,
                 _primary_host_port_cache,
                 group_check ? "with" : "without",
                 dir);
        rep = load_replica(dn, dir);

        // if load data failed, re-open the `*.ori` folder which is the origin replica dir of disk
        // migration
        if (rep == nullptr) {
            const auto origin_dir_type =
                fmt::format("{}{}", app.app_type, replica_disk_migrator::kReplicaDirOriginSuffix);
            const auto origin_dn = _fs_manager.find_replica_dir(origin_dir_type, id);
            if (origin_dn != nullptr) {
                const auto origin_tmp_dir = origin_dn->replica_dir(origin_dir_type, id);
                CHECK(dsn::utils::filesystem::directory_exists(origin_tmp_dir),
                      "dir({}) not exist",
                      origin_tmp_dir);
                LOG_INFO("mark the dir {} as garbage, start revert and load disk migration origin "
                         "replica data({})",
                         dir,
                         origin_tmp_dir);
                dsn::utils::filesystem::rename_path(dir,
                                                    fmt::format("{}{}", dir, kFolderSuffixGar));

                std::string origin_dir = origin_tmp_dir;
                // revert the origin replica dir
                boost::replace_first(
                    origin_dir, replica_disk_migrator::kReplicaDirOriginSuffix, "");
                dsn::utils::filesystem::rename_path(origin_tmp_dir, origin_dir);
                rep = load_replica(origin_dn, origin_dir);

                FAIL_POINT_INJECT_F("mock_replica_load", [&](std::string_view) -> void {});
            }
        }
    }

    if (rep == nullptr) {
        // NOTICE: if dir a.b.pegasus does not exist, or .app-info does not exist, but the ballot >
        // 0, or the last_committed_decree > 0, start replica will fail
        if ((configuration_update != nullptr) && (configuration_update->info.is_stateful)) {
            CHECK(configuration_update->config.ballot == 0 &&
                      configuration_update->config.last_committed_decree == 0,
                  "{}@{}: cannot load replica({}.{}), ballot = {}, "
                  "last_committed_decree = {}, but it does not existed!",
                  id,
                  _primary_host_port_cache,
                  id,
                  app.app_type.c_str(),
                  configuration_update->config.ballot,
                  configuration_update->config.last_committed_decree);
        }

        // NOTICE: only new_replica_group's assign_primary will execute this; if server restart when
        // download restore-data from cold backup media, the a.b.pegasus will move to
        // a.b.pegasus.timestamp.err when replica-server load all the replicas, so restore-flow will
        // do it again

        bool restore_if_necessary =
            ((configuration_update != nullptr) &&
             (configuration_update->type == config_type::CT_ASSIGN_PRIMARY) &&
             (app.envs.find(backup_restore_constant::POLICY_NAME) != app.envs.end()));

        bool is_duplication_follower =
            ((configuration_update != nullptr) &&
             (configuration_update->type == config_type::CT_ASSIGN_PRIMARY) &&
             (app.envs.find(duplication_constants::kEnvMasterClusterKey) != app.envs.end()) &&
             (app.envs.find(duplication_constants::kEnvMasterMetasKey) != app.envs.end()));

        // NOTICE: when we don't need execute restore-process, we should remove a.b.pegasus
        // directory because it don't contain the valid data dir and also we need create a new
        // replica(if contain valid data, it will execute load-process)

        if (!restore_if_necessary && ::dsn::utils::filesystem::directory_exists(dir)) {
            CHECK(::dsn::utils::filesystem::remove_path(dir),
                  "remove useless directory({}) failed",
                  dir);
        }
        rep = new_replica(id, app, restore_if_necessary, is_duplication_follower);
    }

    if (rep == nullptr) {
        LOG_WARNING("{}@{}: open replica failed, erase from opening replicas",
                    id,
                    _primary_host_port_cache);
        zauto_write_lock l(_replicas_lock);
        CHECK_GT_MSG(_opening_replicas.erase(id), 0, "replica {} is not in _opening_replicas", id);
        METRIC_VAR_DECREMENT(opening_replicas);
        return;
    }

    {
        zauto_write_lock l(_replicas_lock);
        CHECK_GT_MSG(_opening_replicas.erase(id), 0, "replica {} is not in _opening_replicas", id);
        METRIC_VAR_DECREMENT(opening_replicas);

        CHECK(_replicas.find(id) == _replicas.end(), "replica {} is already in _replicas", id);
        _replicas.insert(replica_map_by_gpid::value_type(rep->get_gpid(), rep));
        METRIC_VAR_INCREMENT(total_replicas);

        _closed_replicas.erase(id);
    }

    if (nullptr != group_check) {
        rpc::call_one_way_typed(primary_address(),
                                RPC_LEARN_ADD_LEARNER,
                                *group_check,
                                group_check->config.pid.thread_hash());
    } else if (nullptr != configuration_update) {
        rpc::call_one_way_typed(primary_address(),
                                RPC_CONFIG_PROPOSAL,
                                *configuration_update,
                                configuration_update->config.pid.thread_hash());
    }
}

replica *replica_stub::new_replica(gpid gpid,
                                   const app_info &app,
                                   bool restore_if_necessary,
                                   bool is_duplication_follower,
                                   const std::string &parent_dir)
{
    dir_node *dn = nullptr;
    if (parent_dir.empty()) {
        dn = _fs_manager.create_replica_dir_if_necessary(app.app_type, gpid);
    } else {
        dn = _fs_manager.create_child_replica_dir(app.app_type, gpid, parent_dir);
    }
    if (dn == nullptr) {
        LOG_ERROR("could not allocate a new directory for replica {}", gpid);
        return nullptr;
    }
    const auto &dir = dn->replica_dir(app.app_type, gpid);
    CHECK(dsn::utils::filesystem::directory_exists(dir), "dir({}) not exist", dir);
    auto *rep = new replica(this, gpid, app, dn, restore_if_necessary, is_duplication_follower);
    error_code err;
    if (restore_if_necessary && (err = rep->restore_checkpoint()) != dsn::ERR_OK) {
        LOG_ERROR("{}: try to restore replica failed, error({})", rep->name(), err);
        clear_on_failure(rep);
        return nullptr;
    }

    if (is_duplication_follower &&
        (err = rep->get_replica_follower()->duplicate_checkpoint()) != dsn::ERR_OK) {
        LOG_ERROR("{}: try to duplicate replica checkpoint failed, error({}) and please check "
                  "previous detail error log",
                  rep->name(),
                  err);
        clear_on_failure(rep);
        return nullptr;
    }

    err = rep->initialize_on_new();
    if (err != ERR_OK) {
        LOG_ERROR("{}: new replica failed, err = {}", rep->name(), err);
        clear_on_failure(rep);
        return nullptr;
    }

    LOG_DEBUG("{}: new replica succeed", rep->name());
    return rep;
}

replica *replica_stub::new_replica(gpid gpid,
                                   const app_info &app,
                                   bool restore_if_necessary,
                                   bool is_duplication_follower)
{
    return new_replica(gpid, app, restore_if_necessary, is_duplication_follower, {});
}

/*static*/ std::string replica_stub::get_replica_dir_name(const std::string &dir)
{
    static const char splitters[] = {'\\', '/', 0};
    return utils::get_last_component(dir, splitters);
}

/* static */ bool
replica_stub::parse_replica_dir_name(const std::string &dir_name, gpid &pid, std::string &app_type)
{
    std::vector<uint32_t> ids(2, 0);
    size_t begin = 0;
    for (auto &id : ids) {
        size_t end = dir_name.find('.', begin);
        if (end == std::string::npos) {
            return false;
        }

        if (!buf2uint32(std::string_view(dir_name.data() + begin, end - begin), id)) {
            return false;
        }

        begin = end + 1;
    }

    if (begin >= dir_name.size()) {
        return false;
    }

    pid.set_app_id(static_cast<int32_t>(ids[0]));
    pid.set_partition_index(static_cast<int32_t>(ids[1]));

    // TODO(wangdan): the 3rd parameter `count` does not support default argument for CentOS 7
    // (gcc 7.3.1). After CentOS 7 is deprecated, consider dropping std::string::npos.
    app_type.assign(dir_name, begin, std::string::npos);
    return true;
}

bool replica_stub::validate_replica_dir(const std::string &dir,
                                        app_info &ai,
                                        gpid &pid,
                                        std::string &hint_message)
{
    if (!utils::filesystem::directory_exists(dir)) {
        hint_message = fmt::format("replica dir '{}' not exist", dir);
        return false;
    }

    const auto &dir_name = get_replica_dir_name(dir);
    if (dir_name.empty()) {
        hint_message = fmt::format("invalid replica dir '{}'", dir);
        return false;
    }

    std::string app_type;
    if (!parse_replica_dir_name(dir_name, pid, app_type)) {
        hint_message = fmt::format("invalid replica dir '{}'", dir);
        return false;
    }

    replica_app_info rai(&ai);
    const auto ai_path = utils::filesystem::path_combine(dir, replica_app_info::kAppInfo);
    const auto err = rai.load(ai_path);
    if (ERR_OK != err) {
        hint_message = fmt::format("load app-info from '{}' failed, err = {}", ai_path, err);
        return false;
    }

    if (ai.app_type != app_type) {
        hint_message = fmt::format("unmatched app type '{}' for '{}'", ai.app_type, ai_path);
        return false;
    }

    if (pid.get_partition_index() >= ai.partition_count) {
        // Once the online partition split aborted, the partitions within the range of
        // [ai.partition_count, 2 * ai.partition_count) would become garbage.
        hint_message = fmt::format(
            "partition[{}], count={}, this replica may be partition split garbage partition, "
            "ignore it",
            pid,
            ai.partition_count);
        return false;
    }

    return true;
}

replica_ptr replica_stub::load_replica(dir_node *disk_node, const std::string &replica_dir)
{
    FAIL_POINT_INJECT_F("mock_replica_load",
                        [&](std::string_view) -> replica * { return nullptr; });

    app_info ai;
    gpid pid;
    std::string hint_message;
    if (!validate_replica_dir(replica_dir, ai, pid, hint_message)) {
        LOG_ERROR("invalid replica dir '{}', hint={}", replica_dir, hint_message);
        return nullptr;
    }

    // The replica's directory must exist when creating a replica.
    CHECK_EQ(disk_node->replica_dir(ai.app_type, pid), replica_dir);

    auto *rep = new replica(this, pid, ai, disk_node, false);
    const auto err = rep->initialize_on_load();
    if (err != ERR_OK) {
        LOG_ERROR("{}: load replica failed, tag={}, replica_dir={}, err={}",
                  rep->name(),
                  disk_node->tag,
                  replica_dir,
                  err);
        delete rep;
        rep = nullptr;

        // clear work on failure
        if (dsn::utils::filesystem::directory_exists(replica_dir)) {
            move_to_err_path(replica_dir, "load replica");
            METRIC_VAR_INCREMENT(moved_error_replicas);
            _fs_manager.remove_replica(pid);
        }

        return nullptr;
    }

    LOG_INFO("{}: load replica succeed, tag={}, replica_dir={}",
             rep->name(),
             disk_node->tag,
             replica_dir);
    return rep;
}

void replica_stub::clear_on_failure(replica *rep)
{
    const auto rep_dir = rep->dir();
    const auto pid = rep->get_gpid();

    rep->close();
    delete rep;
    rep = nullptr;

    // clear work on failure
    utils::filesystem::remove_path(rep_dir);
    _fs_manager.remove_replica(pid);
}

task_ptr replica_stub::begin_close_replica(replica_ptr r)
{
    CHECK(r->status() == partition_status::PS_ERROR ||
              r->status() == partition_status::PS_INACTIVE ||
              r->disk_migrator()->status() >= disk_migration_status::MOVED,
          "invalid state(partition_status={}, migration_status={}) when calling "
          "replica({}) close",
          enum_to_string(r->status()),
          enum_to_string(r->disk_migrator()->status()),
          r->name());

    gpid id = r->get_gpid();

    zauto_write_lock l(_replicas_lock);
    if (_replicas.erase(id) == 0) {
        return nullptr;
    }

    METRIC_VAR_DECREMENT(total_replicas);

    int delay_ms = 0;
    if (r->status() == partition_status::PS_INACTIVE) {
        delay_ms = FLAGS_gc_memory_replica_interval_ms;
        LOG_INFO("{}: delay {} milliseconds to close replica, status = PS_INACTIVE",
                 r->name(),
                 delay_ms);
    }

    app_info a_info = *(r->get_app_info());
    replica_info r_info;
    get_replica_info(r_info, r);
    task_ptr task = tasking::enqueue(
        LPC_CLOSE_REPLICA,
        &_tracker,
        [=]() { close_replica(r); },
        0,
        std::chrono::milliseconds(delay_ms));
    _closing_replicas[id] = std::make_tuple(task, r, std::move(a_info), std::move(r_info));
    METRIC_VAR_INCREMENT(closing_replicas);
    return task;
}

void replica_stub::close_replica(replica_ptr r)
{
    LOG_INFO("{}: start to close replica", r->name());

    gpid id = r->get_gpid();
    std::string name = r->name();

    r->close();

    {
        zauto_write_lock l(_replicas_lock);
        auto find = _closing_replicas.find(id);
        CHECK(find != _closing_replicas.end(), "replica {} is not in _closing_replicas", name);
        _closed_replicas.emplace(
            id, std::make_pair(std::get<2>(find->second), std::get<3>(find->second)));
        _closing_replicas.erase(find);
        METRIC_VAR_DECREMENT(closing_replicas);
    }

    _fs_manager.remove_replica(id);
    if (r->is_data_corrupted()) {
        move_to_err_path(r->dir(), "trash replica");
        METRIC_VAR_INCREMENT(moved_error_replicas);
    }

    LOG_INFO("{}: finish to close replica", name);
}

void replica_stub::notify_replica_state_update(const replica_configuration &config, bool is_closing)
{
    if (nullptr != _replica_state_subscriber) {
        if (_is_long_subscriber) {
            tasking::enqueue(
                LPC_REPLICA_STATE_CHANGE_NOTIFICATION,
                &_tracker,
                std::bind(_replica_state_subscriber, _primary_host_port, config, is_closing));
        } else {
            _replica_state_subscriber(_primary_host_port, config, is_closing);
        }
    }
}

void replica_stub::trigger_checkpoint(replica_ptr r, bool is_emergency)
{
    r->init_checkpoint(is_emergency);
}

void replica_stub::handle_log_failure(error_code err)
{
    LOG_ERROR("handle log failure: {}", err);
    CHECK(s_not_exit_on_log_failure, "");
}

void replica_stub::open_service()
{
    register_rpc_handler(RPC_CONFIG_PROPOSAL, "ProposeConfig", &replica_stub::on_config_proposal);
    register_rpc_handler(RPC_PREPARE, "prepare", &replica_stub::on_prepare);
    register_rpc_handler(RPC_LEARN, "Learn", &replica_stub::on_learn);
    register_rpc_handler_with_rpc_holder(RPC_LEARN_COMPLETION_NOTIFY,
                                         "LearnNotify",
                                         &replica_stub::on_learn_completion_notification);
    register_rpc_handler(RPC_LEARN_ADD_LEARNER, "LearnAdd", &replica_stub::on_add_learner);
    register_rpc_handler(RPC_REMOVE_REPLICA, "remove", &replica_stub::on_remove);
    register_rpc_handler_with_rpc_holder(
        RPC_GROUP_CHECK, "GroupCheck", &replica_stub::on_group_check);
    register_rpc_handler_with_rpc_holder(
        RPC_QUERY_PN_DECREE, "query_decree", &replica_stub::on_query_decree);
    register_rpc_handler_with_rpc_holder(
        RPC_QUERY_REPLICA_INFO, "query_replica_info", &replica_stub::on_query_replica_info);
    register_rpc_handler_with_rpc_holder(RPC_QUERY_LAST_CHECKPOINT_INFO,
                                         "query_last_checkpoint_info",
                                         &replica_stub::on_query_last_checkpoint);
    register_rpc_handler_with_rpc_holder(
        RPC_QUERY_DISK_INFO, "query_disk_info", &replica_stub::on_query_disk_info);
    register_rpc_handler_with_rpc_holder(
        RPC_REPLICA_DISK_MIGRATE, "disk_migrate_replica", &replica_stub::on_disk_migrate);
    register_rpc_handler_with_rpc_holder(
        RPC_QUERY_APP_INFO, "query_app_info", &replica_stub::on_query_app_info);
    register_rpc_handler_with_rpc_holder(RPC_SPLIT_UPDATE_CHILD_PARTITION_COUNT,
                                         "update_child_group_partition_count",
                                         &replica_stub::on_update_child_group_partition_count);
    register_rpc_handler_with_rpc_holder(RPC_SPLIT_NOTIFY_CATCH_UP,
                                         "child_notify_catch_up",
                                         &replica_stub::on_notify_primary_split_catch_up);
    register_rpc_handler_with_rpc_holder(RPC_BULK_LOAD, "bulk_load", &replica_stub::on_bulk_load);
    register_rpc_handler_with_rpc_holder(
        RPC_GROUP_BULK_LOAD, "group_bulk_load", &replica_stub::on_group_bulk_load);
    register_rpc_handler_with_rpc_holder(
        RPC_DETECT_HOTKEY, "detect_hotkey", &replica_stub::on_detect_hotkey);
    register_rpc_handler_with_rpc_holder(
        RPC_ADD_NEW_DISK, "add_new_disk", &replica_stub::on_add_new_disk);

    // nfs
    register_async_rpc_handler(dsn::service::RPC_NFS_COPY, "copy", &replica_stub::on_nfs_copy);
    register_async_rpc_handler(
        dsn::service::RPC_NFS_GET_FILE_SIZE, "get_file_size", &replica_stub::on_nfs_get_file_size);

    register_ctrl_command();
}

#if !defined(DSN_ENABLE_GPERF) && defined(DSN_USE_JEMALLOC)
void replica_stub::register_jemalloc_ctrl_command()
{
    _cmds.emplace_back(::dsn::command_manager::instance().register_single_command(
        "replica.dump-jemalloc-stats",
        "Dump stats of jemalloc",
        fmt::format("<{}> [buffer size]", kAllJeStatsTypesStr),
        [](const std::vector<std::string> &args) {
            if (args.empty()) {
                return std::string("invalid arguments");
            }

            auto type = enum_from_string(args[0].c_str(), je_stats_type::INVALID);
            if (type == je_stats_type::INVALID) {
                return std::string("invalid stats type");
            }

            std::string stats("\n");

            if (args.size() == 1) {
                dsn::je_dump_stats(type, stats);
                return stats;
            }

            uint64_t buf_sz;
            if (!dsn::buf2uint64(args[1], buf_sz)) {
                return std::string("invalid buffer size");
            }

            dsn::je_dump_stats(type, static_cast<size_t>(buf_sz), stats);
            return stats;
        }));
}
#endif

void replica_stub::register_ctrl_command()
{
    /// In simple_kv test, three replica apps are created, which means that three replica_stubs are
    /// initialized in simple_kv test. If we don't use std::call_once, these command are registered
    /// for three times. And in command_manager, one same command is not allowed to be registered
    /// more than twice times. That is why we use std::call_once here. Same situation in
    /// failure_detector::register_ctrl_commands and nfs_client_impl::register_cli_commands
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        _cmds.emplace_back(::dsn::command_manager::instance().register_single_command(
            "replica.kill_partition",
            "Kill partitions by (all, one app, one partition)",
            "[app_id [partition_index]]",
            [this](const std::vector<std::string> &args) {
                dsn::gpid pid;
                if (args.size() == 0) {
                    pid.set_app_id(-1);
                    pid.set_partition_index(-1);
                } else if (args.size() == 1) {
                    pid.set_app_id(atoi(args[0].c_str()));
                    pid.set_partition_index(-1);
                } else if (args.size() == 2) {
                    pid.set_app_id(atoi(args[0].c_str()));
                    pid.set_partition_index(atoi(args[1].c_str()));
                } else {
                    return std::string(ERR_INVALID_PARAMETERS.to_string());
                }
                dsn::error_code e = this->on_kill_replica(pid);
                return std::string(e.to_string());
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_bool_command(
            _deny_client, "replica.deny-client", "control if deny client read & write request"));

        _cmds.emplace_back(::dsn::command_manager::instance().register_bool_command(
            _verbose_client_log,
            "replica.verbose-client-log",
            "control if print verbose error log when reply read & write request"));

        _cmds.emplace_back(::dsn::command_manager::instance().register_bool_command(
            _verbose_commit_log,
            "replica.verbose-commit-log",
            "control if print verbose log when commit mutation"));

        _cmds.emplace_back(::dsn::command_manager::instance().register_single_command(
            "replica.trigger-checkpoint",
            "Trigger replicas to do checkpoint by app_id or app_id.partition_id",
            "[id1,id2,...]",
            [this](const std::vector<std::string> &args) {
                return exec_command_on_replica(args, true, [this](const replica_ptr &rep) {
                    tasking::enqueue(LPC_PER_REPLICA_CHECKPOINT_TIMER,
                                     rep->tracker(),
                                     std::bind(&replica_stub::trigger_checkpoint, this, rep, true),
                                     rep->get_gpid().thread_hash());
                    return std::string("triggered");
                });
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_single_command(
            "replica.query-compact",
            "Query full compact status on the underlying storage engine by app_id or "
            "app_id.partition_id",
            "[id1,id2,...]",
            [this](const std::vector<std::string> &args) {
                return exec_command_on_replica(args, true, [](const replica_ptr &rep) {
                    return rep->query_manual_compact_state();
                });
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_single_command(
            "replica.query-app-envs",
            "Query app envs on the underlying storage engine by app_id or app_id.partition_id",
            "[id1,id2,...]",
            [this](const std::vector<std::string> &args) {
                return exec_command_on_replica(args, true, [](const replica_ptr &rep) {
                    std::map<std::string, std::string> kv_map;
                    rep->query_app_envs(kv_map);
                    return dsn::utils::kv_map_to_string(kv_map, ',', '=');
                });
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_single_command(
            "replica.enable-plog-gc",
            "Enable plog garbage collection for replicas specified by comma-separated list "
            "of 'app_id' or 'app_id.partition_id', or all replicas for empty",
            "[id1,id2,...]",
            [this](const std::vector<std::string> &args) {
                return exec_command_on_replica(args, true, [](const replica_ptr &rep) {
                    rep->update_plog_gc_enabled(true);
                    return rep->get_plog_gc_enabled_message();
                });
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_single_command(
            "replica.disable-plog-gc",
            "Disable plog garbage collection for replicas specified by comma-separated list "
            "of 'app_id' or 'app_id.partition_id', or all replicas for empty",
            "[id1,id2,...]",
            [this](const std::vector<std::string> &args) {
                return exec_command_on_replica(args, true, [](const replica_ptr &rep) {
                    rep->update_plog_gc_enabled(false);
                    return rep->get_plog_gc_enabled_message();
                });
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_single_command(
            "replica.query-plog-gc-enabled-status",
            "Query if plog garbage collection is enabled or disabled for replicas specified by "
            "comma-separated list of 'app_id' or 'app_id.partition_id', or all replicas for empty",
            "[id1,id2,...]",
            [this](const std::vector<std::string> &args) {
                return exec_command_on_replica(args, true, [](const replica_ptr &rep) {
                    return rep->get_plog_gc_enabled_message();
                });
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_single_command(
            "replica.query-progress",
            "Query the progress of decrees, including both local writes and duplications for "
            "replicas specified by comma-separated list of 'app_id' or 'app_id.partition_id', "
            "or all replicas for empty",
            "[id1,id2,...]",
            [this](const std::vector<std::string> &args) {
                return exec_command_on_replica(args, true, [](const replica_ptr &rep) {
                    std::ostringstream out;
                    rapidjson::OStreamWrapper wrapper(out);
                    dsn::json::PrettyJsonWriter writer(wrapper);
                    rep->encode_progress(writer);
                    return out.str();
                });
            }));

#ifdef DSN_ENABLE_GPERF
        _cmds.emplace_back(::dsn::command_manager::instance().register_bool_command(
            _release_tcmalloc_memory,
            "replica.release-tcmalloc-memory",
            "control if try to release tcmalloc memory"));

        _cmds.emplace_back(::dsn::command_manager::instance().register_single_command(
            "replica.get-tcmalloc-status",
            "Get the status of tcmalloc",
            "",
            [](const std::vector<std::string> &args) {
                char buf[4096];
                MallocExtension::instance()->GetStats(buf, 4096);
                return std::string(buf);
            }));

        _cmds.emplace_back(::dsn::command_manager::instance().register_int_command(
            _mem_release_max_reserved_mem_percentage,
            FLAGS_mem_release_max_reserved_mem_percentage,
            "replica.mem-release-max-reserved-percentage",
            "control tcmalloc max reserved but not-used memory percentage",
            &check_mem_release_max_reserved_mem_percentage));

        _cmds.emplace_back(::dsn::command_manager::instance().register_single_command(
            "replica.release-all-reserved-memory",
            "Release tcmalloc all reserved-not-used memory back to operating system",
            "",
            [this](const std::vector<std::string> &args) {
                auto release_bytes = gc_tcmalloc_memory(true);
                return "OK, release_bytes=" + std::to_string(release_bytes);
            }));
#elif defined(DSN_USE_JEMALLOC)
        register_jemalloc_ctrl_command();
#endif
    });
}

std::string
replica_stub::exec_command_on_replica(const std::vector<std::string> &arg_str_list,
                                      bool allow_empty_args,
                                      std::function<std::string(const replica_ptr &)> func)
{
    static const std::string kInvalidArguments("invalid arguments");

    if (arg_str_list.empty() && !allow_empty_args) {
        return kInvalidArguments;
    }

    replica_map_by_gpid rs;
    {
        zauto_read_lock l(_replicas_lock);
        rs = _replicas;
    }

    std::set<gpid> required_ids;
    replica_map_by_gpid choosed_rs;
    if (!arg_str_list.empty()) {
        for (const auto &arg_str : arg_str_list) {
            std::vector<std::string> args;
            utils::split_args(arg_str.c_str(), args, ',');
            if (args.empty()) {
                return kInvalidArguments;
            }

            for (const std::string &arg : args) {
                if (arg.empty()) {
                    continue;
                }

                gpid id;
                if (id.parse_from(arg.c_str())) {
                    // Format: app_id.partition_index
                    required_ids.insert(id);
                    auto find = rs.find(id);
                    if (find != rs.end()) {
                        choosed_rs[id] = find->second;
                    }

                    continue;
                }

                // Must be app_id.
                int32_t app_id = 0;
                if (!buf2int32(arg, app_id)) {
                    return kInvalidArguments;
                }

                for (const auto &[_, rep] : rs) {
                    id = rep->get_gpid();
                    if (id.get_app_id() == app_id) {
                        choosed_rs[id] = rep;
                    }
                }
            }
        }
    } else {
        // all replicas
        choosed_rs = rs;
    }

    std::vector<task_ptr> tasks;
    ::dsn::zlock results_lock;
    std::map<gpid, std::pair<partition_status::type, std::string>> results; // id => status,result
    for (auto &kv : choosed_rs) {
        replica_ptr rep = kv.second;
        task_ptr tsk = tasking::enqueue(
            LPC_EXEC_COMMAND_ON_REPLICA,
            rep->tracker(),
            [rep, &func, &results_lock, &results]() {
                partition_status::type status = rep->status();
                if (status != partition_status::PS_PRIMARY &&
                    status != partition_status::PS_SECONDARY) {
                    return;
                }

                std::string result = func(rep);
                ::dsn::zauto_lock l(results_lock);
                auto &value = results[rep->get_gpid()];
                value.first = status;
                value.second = result;
            },
            rep->get_gpid().thread_hash());
        tasks.emplace_back(std::move(tsk));
    }

    for (auto &tsk : tasks) {
        tsk->wait();
    }

    int processed = results.size();
    int not_found = 0;
    for (auto &id : required_ids) {
        if (results.find(id) == results.end()) {
            auto &value = results[id];
            value.first = partition_status::PS_INVALID;
            value.second = "not found";
            not_found++;
        }
    }

    std::stringstream query_state;
    query_state << processed << " processed, " << not_found << " not found";
    for (auto &kv : results) {
        query_state << "\n    " << kv.first << "@" << _primary_host_port_cache;
        if (kv.second.first != partition_status::PS_INVALID)
            query_state << "@" << (kv.second.first == partition_status::PS_PRIMARY ? "P" : "S");
        query_state << " : " << kv.second.second;
    }

    return query_state.str();
}

void replica_stub::close()
{
    if (!_is_running) {
        return;
    }

    _tracker.cancel_outstanding_tasks();

    // this replica may not be opened
    // or is already closed by calling tool_app::stop_all_apps()
    // in this case, just return
    if (_cmds.empty()) {
        return;
    }
    _cmds.clear();

    if (_config_sync_timer_task != nullptr) {
        _config_sync_timer_task->cancel(true);
        _config_sync_timer_task = nullptr;
    }

    if (_duplication_sync_timer != nullptr) {
        _duplication_sync_timer->close();
        _duplication_sync_timer = nullptr;
    }

    if (_config_query_task != nullptr) {
        _config_query_task->cancel(true);
        _config_query_task = nullptr;
    }
    _state = NS_Disconnected;

    if (_disk_stat_timer_task != nullptr) {
        _disk_stat_timer_task->cancel(true);
        _disk_stat_timer_task = nullptr;
    }

    if (_replicas_stat_timer_task != nullptr) {
        _replicas_stat_timer_task->cancel(true);
        _replicas_stat_timer_task = nullptr;
    }

    if (_mem_release_timer_task != nullptr) {
        _mem_release_timer_task->cancel(true);
        _mem_release_timer_task = nullptr;
    }

    wait_closing_replicas_finished();

    {
        zauto_write_lock l(_replicas_lock);

        while (!_opening_replicas.empty()) {
            task_ptr task = _opening_replicas.begin()->second;
            _replicas_lock.unlock_write();

            task->cancel(true);

            METRIC_VAR_DECREMENT(opening_replicas);
            _replicas_lock.lock_write();
            _opening_replicas.erase(_opening_replicas.begin());
        }

        while (!_replicas.empty()) {
            _replicas.begin()->second->close();

            METRIC_VAR_DECREMENT(total_replicas);
            _replicas.erase(_replicas.begin());
        }
    }
    _is_running = false;
}

#ifdef DSN_ENABLE_GPERF
// Get tcmalloc numeric property (name is "prop") value.
// Return -1 if get property failed (property we used will be greater than zero)
// Properties can be found in 'gperftools/malloc_extension.h'
static int64_t get_tcmalloc_numeric_property(const char *prop)
{
    size_t value;
    if (!::MallocExtension::instance()->GetNumericProperty(prop, &value)) {
        LOG_ERROR("Failed to get tcmalloc property {}", prop);
        return -1;
    }
    return value;
}

uint64_t replica_stub::gc_tcmalloc_memory(bool release_all)
{
    if (!_release_tcmalloc_memory) {
        _is_releasing_memory.store(false);
        return 0;
    }

    if (_is_releasing_memory.load()) {
        LOG_WARNING("This node is releasing memory...");
        return 0;
    }

    _is_releasing_memory.store(true);

    int64_t total_allocated_bytes =
        get_tcmalloc_numeric_property("generic.current_allocated_bytes");
    int64_t reserved_bytes = get_tcmalloc_numeric_property("tcmalloc.pageheap_free_bytes");
    if (total_allocated_bytes == -1 || reserved_bytes == -1) {
        return 0;
    }

    int64_t max_reserved_bytes =
        release_all ? 0
                    : (total_allocated_bytes * _mem_release_max_reserved_mem_percentage / 100.0);
    if (reserved_bytes <= max_reserved_bytes) {
        return 0;
    }

    const int64_t expected_released_bytes = reserved_bytes - max_reserved_bytes;
    LOG_INFO("Memory release started, almost {} bytes will be released", expected_released_bytes);

    int64_t unreleased_bytes = expected_released_bytes;
    while (unreleased_bytes > 0) {
        // tcmalloc releasing memory will lock page heap, release 1MB at a time to avoid locking
        // page heap for long time
        static const int64_t kReleasedBytesEachTime = 1024 * 1024;
        ::MallocExtension::instance()->ReleaseToSystem(kReleasedBytesEachTime);
        unreleased_bytes -= kReleasedBytesEachTime;
    }
    METRIC_VAR_INCREMENT_BY(tcmalloc_released_bytes, expected_released_bytes);

    _is_releasing_memory.store(false);

    return expected_released_bytes;
}
#endif

//
// partition split
//
void replica_stub::create_child_replica(const host_port &primary_address,
                                        app_info app,
                                        ballot init_ballot,
                                        gpid child_gpid,
                                        gpid parent_gpid,
                                        const std::string &parent_dir)
{
    replica_ptr child_replica = create_child_replica_if_not_found(child_gpid, &app, parent_dir);
    if (child_replica != nullptr) {
        LOG_INFO("app({}), create child replica ({}) succeed", app.app_name, child_gpid);
        tasking::enqueue(LPC_PARTITION_SPLIT,
                         child_replica->tracker(),
                         std::bind(&replica_split_manager::child_init_replica,
                                   child_replica->get_split_manager(),
                                   parent_gpid,
                                   primary_address,
                                   init_ballot),
                         child_gpid.thread_hash());
    } else {
        LOG_WARNING("failed to create child replica ({}), ignore it and wait next run", child_gpid);
        split_replica_error_handler(
            parent_gpid,
            std::bind(&replica_split_manager::parent_cleanup_split_context, std::placeholders::_1));
    }
}

replica_ptr replica_stub::create_child_replica_if_not_found(gpid child_pid,
                                                            app_info *app,
                                                            const std::string &parent_dir)
{
    FAIL_POINT_INJECT_F(
        "replica_stub_create_child_replica_if_not_found", [=](std::string_view) -> replica_ptr {
            const auto dn =
                _fs_manager.create_child_replica_dir(app->app_type, child_pid, parent_dir);
            CHECK_NOTNULL(dn, "");
            auto *rep = new replica(this, child_pid, *app, dn, false);
            rep->_config.status = partition_status::PS_INACTIVE;
            _replicas.insert(replica_map_by_gpid::value_type(child_pid, rep));
            LOG_INFO("mock create_child_replica_if_not_found succeed");
            return rep;
        });

    zauto_write_lock l(_replicas_lock);

    const auto it = _replicas.find(child_pid);
    if (it != _replicas.end()) {
        return it->second;
    }

    if (_opening_replicas.find(child_pid) != _opening_replicas.end()) {
        LOG_WARNING("failed create child replica({}) because it is under open", child_pid);
        return nullptr;
    }

    if (_closing_replicas.find(child_pid) != _closing_replicas.end()) {
        LOG_WARNING("failed create child replica({}) because it is under close", child_pid);
        return nullptr;
    }

    replica *rep = new_replica(child_pid, *app, false, false, parent_dir);
    if (rep == nullptr) {
        return nullptr;
    }

    const auto pr = _replicas.insert(replica_map_by_gpid::value_type(child_pid, rep));
    CHECK(pr.second, "child replica {} has been existed", rep->name());
    METRIC_VAR_INCREMENT(total_replicas);
    _closed_replicas.erase(child_pid);

    return rep;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_stub::split_replica_error_handler(gpid pid, local_execution handler)
{
    split_replica_exec(LPC_PARTITION_SPLIT_ERROR, pid, handler);
}

// ThreadPool: THREAD_POOL_REPLICATION
dsn::error_code
replica_stub::split_replica_exec(dsn::task_code code, gpid pid, local_execution handler)
{
    FAIL_POINT_INJECT_F("replica_stub_split_replica_exec", [](std::string_view) { return ERR_OK; });
    replica_ptr replica = pid.get_app_id() == 0 ? nullptr : get_replica(pid);
    if (replica && handler) {
        tasking::enqueue(
            code,
            replica.get()->tracker(),
            [handler, replica]() { handler(replica->get_split_manager()); },
            pid.thread_hash());
        return ERR_OK;
    }
    LOG_WARNING("replica({}) is invalid", pid);
    return ERR_OBJECT_NOT_FOUND;
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_stub::on_notify_primary_split_catch_up(notify_catch_up_rpc rpc)
{
    const notify_catch_up_request &request = rpc.request();
    notify_cacth_up_response &response = rpc.response();
    replica_ptr replica = get_replica(request.parent_gpid);
    if (replica != nullptr) {
        replica->get_split_manager()->parent_handle_child_catch_up(request, response);
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

// ThreadPool: THREAD_POOL_REPLICATION
void replica_stub::on_update_child_group_partition_count(update_child_group_partition_count_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();
    replica_ptr replica = get_replica(request.child_pid);
    if (replica != nullptr) {
        replica->get_split_manager()->on_update_child_group_partition_count(request, response);
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::update_disk_holding_replicas()
{
    for (const auto &dn : _fs_manager.get_dir_nodes()) {
        dn->holding_primary_replicas.clear();
        dn->holding_secondary_replicas.clear();
        for (const auto &holding_replicas : dn->holding_replicas) {
            const auto &pids = holding_replicas.second;
            for (const auto &pid : pids) {
                const auto rep = get_replica(pid);
                if (rep == nullptr) {
                    continue;
                }
                if (rep->status() == partition_status::PS_PRIMARY) {
                    dn->holding_primary_replicas[holding_replicas.first].emplace(pid);
                } else if (rep->status() == partition_status::PS_SECONDARY) {
                    dn->holding_secondary_replicas[holding_replicas.first].emplace(pid);
                }
            }
        }
    }
}

void replica_stub::on_bulk_load(bulk_load_rpc rpc)
{
    const bulk_load_request &request = rpc.request();
    bulk_load_response &response = rpc.response();

    LOG_INFO("[{}@{}]: receive bulk load request", request.pid, _primary_host_port_cache);
    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->get_bulk_loader()->on_bulk_load(request, response);
    } else {
        LOG_ERROR("replica({}) is not existed", request.pid);
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::on_group_bulk_load(group_bulk_load_rpc rpc)
{
    const group_bulk_load_request &request = rpc.request();
    group_bulk_load_response &response = rpc.response();

    LOG_INFO("[{}@{}]: received group bulk load request, primary = {}, ballot = {}, "
             "meta_bulk_load_status = {}",
             request.config.pid,
             _primary_host_port_cache,
             FMT_HOST_PORT_AND_IP(request.config, primary),
             request.config.ballot,
             enum_to_string(request.meta_bulk_load_status));

    replica_ptr rep = get_replica(request.config.pid);
    if (rep != nullptr) {
        rep->get_bulk_loader()->on_group_bulk_load(request, response);
    } else {
        LOG_ERROR("replica({}) is not existed", request.config.pid);
        response.err = ERR_OBJECT_NOT_FOUND;
    }
}

void replica_stub::on_detect_hotkey(detect_hotkey_rpc rpc)
{
    const auto &request = rpc.request();
    auto &response = rpc.response();

    LOG_INFO("[{}@{}]: received detect hotkey request, hotkey_type = {}, detect_action = {}",
             request.pid,
             _primary_host_port_cache,
             enum_to_string(request.type),
             enum_to_string(request.action));

    replica_ptr rep = get_replica(request.pid);
    if (rep != nullptr) {
        rep->on_detect_hotkey(request, response);
    } else {
        response.err = ERR_OBJECT_NOT_FOUND;
        response.err_hint = fmt::format("not find the replica {} \n", request.pid);
    }
}

void replica_stub::query_app_data_version(
    int32_t app_id, /*pidx => data_version*/ std::unordered_map<int32_t, uint32_t> &version_map)
{
    zauto_read_lock l(_replicas_lock);
    for (const auto &kv : _replicas) {
        if (kv.first.get_app_id() == app_id) {
            replica_ptr rep = kv.second;
            if (rep != nullptr) {
                uint32_t data_version = rep->query_data_version();
                version_map[kv.first.get_partition_index()] = data_version;
            }
        }
    }
}

void replica_stub::query_app_manual_compact_status(
    int32_t app_id, std::unordered_map<gpid, manual_compaction_status::type> &status)
{
    zauto_read_lock l(_replicas_lock);
    for (auto it = _replicas.begin(); it != _replicas.end(); ++it) {
        if (it->first.get_app_id() == app_id) {
            status[it->first] = it->second->get_manual_compact_status();
        }
    }
}

void replica_stub::update_config(const std::string &name)
{
    // The new value has been validated and FLAGS_* has been updated, it's safety to use it
    // directly.
    UPDATE_CONFIG(_config_sync_timer_task->update_interval, config_sync_interval_ms, name);
}

void replica_stub::wait_closing_replicas_finished()
{
    zauto_write_lock l(_replicas_lock);
    while (!_closing_replicas.empty()) {
        auto task = std::get<0>(_closing_replicas.begin()->second);
        auto first_gpid = _closing_replicas.begin()->first;

        // TODO(yingchun): improve the code
        _replicas_lock.unlock_write();
        task->wait();
        _replicas_lock.lock_write();

        // task will automatically remove this replica from '_closing_replicas'
        if (!_closing_replicas.empty()) {
            CHECK_NE_MSG(first_gpid,
                         _closing_replicas.begin()->first,
                         "this replica '{}' should has been removed",
                         first_gpid);
        }
    }
}

} // namespace replication
} // namespace dsn
