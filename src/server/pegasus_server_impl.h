/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#pragma once

#include <gtest/gtest_prod.h>
#include <rocksdb/compression_type.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/table.h>
#include <rrdb/rrdb_types.h>
#include <stdint.h>
#include <atomic>
#include <chrono>
#include <deque>
#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "bulk_load_types.h"
#include "common/gpid.h"
#include "metadata_types.h"
#include "pegasus_manual_compact_service.h"
#include "pegasus_read_service.h"
#include "pegasus_scan_context.h"
#include "pegasus_utils.h"
#include "pegasus_value_schema.h"
#include "range_read_limiter.h"
#include "replica/replication_app_base.h"
#include "runtime/task/task.h"
#include "runtime/task/task_tracker.h"
#include "utils/error_code.h"
#include "utils/flags.h"
#include "utils/metrics.h"
#include "utils/rand.h"
#include "utils/synchronize.h"

DSN_DECLARE_uint64(rocksdb_abnormal_batch_get_bytes_threshold);
DSN_DECLARE_uint64(rocksdb_abnormal_batch_get_count_threshold);
DSN_DECLARE_uint64(rocksdb_abnormal_get_size_threshold);
DSN_DECLARE_uint64(rocksdb_abnormal_multi_get_iterate_count_threshold);
DSN_DECLARE_uint64(rocksdb_abnormal_multi_get_size_threshold);

namespace pegasus {
namespace server {
class KeyWithTTLCompactionFilterFactory;
} // namespace server
} // namespace pegasus
namespace rocksdb {
class Cache;
class ColumnFamilyHandle;
class DB;
class RateLimiter;
class Statistics;
class WriteBufferManager;
} // namespace rocksdb

namespace dsn {
class blob;
class message_ex;
class rpc_address;

namespace replication {
class detect_hotkey_request;
class detect_hotkey_response;
class learn_state;
class replica;
} // namespace replication

namespace utils {
class token_bucket_throttling_controller;
} // namespace utils
} // namespace dsn
typedef dsn::utils::token_bucket_throttling_controller throttling_controller;

namespace pegasus {
namespace server {

class capacity_unit_calculator;
class hotkey_collector;
class meta_store;
class pegasus_server_write;

enum class range_iteration_state
{
    kNormal = 1,
    kExpired,
    kFiltered,
    kHashInvalid
};

class pegasus_server_impl : public pegasus_read_service
{
public:
    static void register_service()
    {
        replication_app_base::register_storage_engine(
            "pegasus", replication_app_base::create<pegasus::server::pegasus_server_impl>);
        register_rpc_handlers();
    }
    explicit pegasus_server_impl(dsn::replication::replica *r);

    ~pegasus_server_impl() override;

    // the following methods may set physical error if internal error occurs
    void on_get(get_rpc rpc) override;
    void on_multi_get(multi_get_rpc rpc) override;
    void on_batch_get(batch_get_rpc rpc) override;
    void on_sortkey_count(sortkey_count_rpc rpc) override;
    void on_ttl(ttl_rpc rpc) override;
    void on_get_scanner(get_scanner_rpc rpc) override;
    void on_scan(scan_rpc rpc) override;
    void on_clear_scanner(const int64_t &args) override;

    // input:
    //  - argc = 0 : re-open the db
    //  - argc = 2n + 1, n >= 0; normal open the db
    // returns:
    //  - ERR_OK
    //  - ERR_FILE_OPERATION_FAILED
    //  - ERR_LOCAL_APP_FAILURE
    ::dsn::error_code start(int argc, char **argv) override;

    void cancel_background_work(bool wait);

    // returns:
    //  - ERR_OK
    //  - ERR_FILE_OPERATION_FAILED
    ::dsn::error_code stop(bool clear_state) override;

    /// Each of the write request (specifically, the rpc that's configured as write, see
    /// option `rpc_request_is_write_operation` in rDSN `task_spec`) will first be
    /// replicated to the replicas through the underlying PacificA protocol in rDSN, and
    /// after being committed, the mutation will be applied into rocksdb by this function.
    ///
    /// \see dsn::replication::replication_app_base::apply_mutation
    /// \inherit dsn::replication::replication_app_base
    int on_batched_write_requests(int64_t decree,
                                  uint64_t timestamp,
                                  dsn::message_ex **requests,
                                  int count) override;

    ::dsn::error_code prepare_get_checkpoint(dsn::blob &learn_req) override
    {
        return ::dsn::ERR_OK;
    }

    // returns:
    //  - ERR_OK: checkpoint succeed
    //  - ERR_WRONG_TIMING: another checkpoint is running now
    //  - ERR_LOCAL_APP_FAILURE: some internal failure
    //  - ERR_FILE_OPERATION_FAILED: some file failure
    // ATTENTION: make sure that no other threads is writing into the replica.
    ::dsn::error_code sync_checkpoint() override;

    // returns:
    //  - ERR_OK: checkpoint succeed
    //  - ERR_WRONG_TIMING: another checkpoint is running now
    //  - ERR_LOCAL_APP_FAILURE: some internal failure
    //  - ERR_FILE_OPERATION_FAILED: some file failure
    //  - ERR_TRY_AGAIN: flush memtable triggered, need try again later
    ::dsn::error_code async_checkpoint(bool flush_memtable) override;

    //
    // copy the latest checkpoint to checkpoint_dir, and the decree of the checkpoint
    // copied will be assigned to checkpoint_decree if checkpoint_decree is not null.
    // if checkpoint_dir already exist, this function will delete it first.
    //
    // must be thread safe
    // this method will not trigger flush(), just copy even if the app is empty.
    ::dsn::error_code copy_checkpoint_to_dir(const char *checkpoint_dir,
                                             /*output*/ int64_t *last_decree,
                                             bool flush_memtable = false) override;

    //
    // help function, just copy checkpoint to specified dir and ignore _is_checkpointing.
    // if checkpoint_dir already exist, this function will delete it first.
    ::dsn::error_code copy_checkpoint_to_dir_unsafe(const char *checkpoint_dir,
                                                    /**output*/ int64_t *checkpoint_decree,
                                                    bool flush_memtable = false);

    // get the last checkpoint
    // if succeed:
    //  - the checkpoint files path are put into "state.files"
    //  - the checkpoint_info are serialized into "state.meta"
    //  - the "state.from_decree_excluded" and "state.to_decree_excluded" are set properly
    // returns:
    //  - ERR_OK
    //  - ERR_OBJECT_NOT_FOUND
    //  - ERR_FILE_OPERATION_FAILED
    ::dsn::error_code get_checkpoint(int64_t learn_start,
                                     const dsn::blob &learn_request,
                                     dsn::replication::learn_state &state) override;

    // apply checkpoint, this will clear and recreate the db
    // if succeed:
    //  - last_committed_decree() == last_durable_decree()
    // returns:
    //  - ERR_OK
    //  - ERR_FILE_OPERATION_FAILED
    //  - error code of close()
    //  - error code of open()
    //  - error code of checkpoint()
    ::dsn::error_code storage_apply_checkpoint(chkpt_apply_mode mode,
                                               const dsn::replication::learn_state &state) override;

    int64_t last_durable_decree() const override { return _last_durable_decree.load(); }

    void update_app_envs(const std::map<std::string, std::string> &envs) override;

    void query_app_envs(/*out*/ std::map<std::string, std::string> &envs) override;

    void set_partition_version(int32_t partition_version) override;

    std::string dump_write_request(dsn::message_ex *request) override;

    // Not thread-safe
    void set_ingestion_status(dsn::replication::ingestion_status::type status) override;

    dsn::replication::ingestion_status::type get_ingestion_status() override
    {
        return _ingestion_status;
    }

private:
    friend class manual_compact_service_test;
    friend class pegasus_compression_options_test;
    friend class pegasus_server_impl_test;
    friend class hotkey_collector_test;
    FRIEND_TEST(pegasus_server_impl_test, default_data_version);
    FRIEND_TEST(pegasus_server_impl_test, test_open_db_with_latest_options);
    FRIEND_TEST(pegasus_server_impl_test, test_open_db_with_app_envs);
    FRIEND_TEST(pegasus_server_impl_test, test_stop_db_twice);
    FRIEND_TEST(pegasus_server_impl_test, test_update_user_specified_compaction);

    friend class pegasus_manual_compact_service;
    friend class pegasus_write_service;
    friend class rocksdb_wrapper;

    // parse checkpoint directories in the data dir
    // checkpoint directory format is: "checkpoint.{decree}"
    void parse_checkpoints();

    // garbage collection checkpoints
    // if force_reserve_one == true, then only reserve the last one checkpoint
    void gc_checkpoints(bool force_reserve_one = false);

    void set_last_durable_decree(int64_t decree) { _last_durable_decree.store(decree); }

    void append_key_value(std::vector<::dsn::apps::key_value> &kvs,
                          const rocksdb::Slice &key,
                          const rocksdb::Slice &value,
                          bool no_value,
                          bool request_expire_ts);

    range_iteration_state
    validate_key_value_for_scan(const rocksdb::Slice &key,
                                const rocksdb::Slice &value,
                                ::dsn::apps::filter_type::type hash_key_filter_type,
                                const ::dsn::blob &hash_key_filter_pattern,
                                ::dsn::apps::filter_type::type sort_key_filter_type,
                                const ::dsn::blob &sort_key_filter_pattern,
                                uint32_t epoch_now,
                                bool request_validate_hash);

    range_iteration_state
    append_key_value_for_multi_get(std::vector<::dsn::apps::key_value> &kvs,
                                   const rocksdb::Slice &key,
                                   const rocksdb::Slice &value,
                                   ::dsn::apps::filter_type::type sort_key_filter_type,
                                   const ::dsn::blob &sort_key_filter_pattern,
                                   uint32_t epoch_now,
                                   bool no_value);

    // return true if the filter type is supported
    bool is_filter_type_supported(::dsn::apps::filter_type::type filter_type)
    {
        return filter_type >= ::dsn::apps::filter_type::FT_NO_FILTER &&
               filter_type <= ::dsn::apps::filter_type::FT_MATCH_POSTFIX;
    }

    // return true if the data is valid for the filter
    bool validate_filter(::dsn::apps::filter_type::type filter_type,
                         const ::dsn::blob &filter_pattern,
                         const ::dsn::blob &value);

    void update_replica_rocksdb_statistics();

    static void update_server_rocksdb_statistics();

    // get the absolute path of restore directory and the flag whether force restore from env
    // return
    //      std::pair<std::string, bool>, pair.first is the path of the restore dir; pair.second is
    //      the flag that whether force restore
    std::pair<std::string, bool>
    get_restore_dir_from_env(const std::map<std::string, std::string> &env_kvs);

    void update_app_envs_before_open_db(const std::map<std::string, std::string> &envs);

    void update_usage_scenario(const std::map<std::string, std::string> &envs);

    void update_default_ttl(const std::map<std::string, std::string> &envs);

    void update_checkpoint_reserve(const std::map<std::string, std::string> &envs);

    void update_slow_query_threshold(const std::map<std::string, std::string> &envs);

    void update_rocksdb_iteration_threshold(const std::map<std::string, std::string> &envs);

    void update_rocksdb_block_cache_enabled(const std::map<std::string, std::string> &envs);

    void update_validate_partition_hash(const std::map<std::string, std::string> &envs);

    void update_user_specified_compaction(const std::map<std::string, std::string> &envs);

    void update_rocksdb_dynamic_options(const std::map<std::string, std::string> &envs);

    void set_rocksdb_options_before_creating(const std::map<std::string, std::string> &envs);

    void update_throttling_controller(const std::map<std::string, std::string> &envs);

    bool parse_allow_ingest_behind(const std::map<std::string, std::string> &envs);

    // return true if parse compression types 'config' success, otherwise return false.
    // 'compression_per_level' will not be changed if parse failed.
    bool parse_compression_types(const std::string &config,
                                 std::vector<rocksdb::CompressionType> &compression_per_level);

    bool compression_str_to_type(const std::string &compression_str,
                                 rocksdb::CompressionType &type);
    std::string compression_type_to_str(rocksdb::CompressionType type);

    // return finish time recorded in rocksdb
    uint64_t do_manual_compact(const rocksdb::CompactRangeOptions &options);

    // generate new checkpoint and remove old checkpoints, in order to release storage asap
    // return true if release succeed (new checkpointed generated).
    bool release_storage_after_manual_compact();

    std::string query_compact_state() const override;

    // return true if successfully changed
    bool set_usage_scenario(const std::string &usage_scenario);

    // recalculate option value if necessary
    void recalculate_data_cf_options(const rocksdb::ColumnFamilyOptions &cur_data_cf_opts);

    void reset_usage_scenario_options(const rocksdb::ColumnFamilyOptions &base_opts,
                                      rocksdb::ColumnFamilyOptions *target_opts);

    void reset_allow_ingest_behind_option(const rocksdb::DBOptions &base_db_opt,
                                          const std::map<std::string, std::string> &envs,
                                          rocksdb::DBOptions *target_db_opt);

    void reset_rocksdb_options(const rocksdb::ColumnFamilyOptions &base_cf_opts,
                               const rocksdb::DBOptions &base_db_opt,
                               const std::map<std::string, std::string> &envs,
                               rocksdb::ColumnFamilyOptions *target_cf_opts,
                               rocksdb::DBOptions *target_db_opt);

    // return true if successfully set
    bool set_options(const std::unordered_map<std::string, std::string> &new_options);

    // return random value in range of [0.75,1.25] * base_value
    uint64_t get_random_nearby(uint64_t base_value)
    {
        uint64_t gap = base_value / 4;
        return dsn::rand::next_u64(base_value - gap, base_value + gap);
    }

    // return true if value in range of [0.75, 1.25] * base_value
    bool check_value_if_nearby(uint64_t base_value, uint64_t check_value)
    {
        uint64_t gap = base_value / 4;
        uint64_t actual_gap =
            (base_value < check_value) ? check_value - base_value : base_value - check_value;
        return actual_gap <= gap;
    }

    // return true if expired
    bool check_if_record_expired(uint32_t epoch_now, rocksdb::Slice raw_value)
    {
        return pegasus::check_if_record_expired(
            _pegasus_data_version, epoch_now, utils::to_string_view(raw_value));
    }

    bool is_multi_get_abnormal(uint64_t time_used, uint64_t size, uint64_t iterate_count)
    {
        if (FLAGS_rocksdb_abnormal_multi_get_size_threshold > 0 &&
            size >= FLAGS_rocksdb_abnormal_multi_get_size_threshold) {
            return true;
        }
        if (FLAGS_rocksdb_abnormal_multi_get_iterate_count_threshold > 0 &&
            iterate_count >= FLAGS_rocksdb_abnormal_multi_get_iterate_count_threshold) {
            return true;
        }
        if (time_used >= _slow_query_threshold_ns) {
            return true;
        }

        return false;
    }

    bool is_batch_get_abnormal(uint64_t time_used, uint64_t size, uint64_t count)
    {
        if (FLAGS_rocksdb_abnormal_batch_get_bytes_threshold &&
            size >= FLAGS_rocksdb_abnormal_batch_get_bytes_threshold) {
            return true;
        }
        if (FLAGS_rocksdb_abnormal_batch_get_count_threshold &&
            count >= FLAGS_rocksdb_abnormal_batch_get_count_threshold) {
            return true;
        }
        if (time_used >= _slow_query_threshold_ns) {
            return true;
        }

        return false;
    }

    bool is_get_abnormal(uint64_t time_used, uint64_t value_size)
    {
        if (FLAGS_rocksdb_abnormal_get_size_threshold > 0 &&
            value_size >= FLAGS_rocksdb_abnormal_get_size_threshold) {
            return true;
        }
        if (time_used >= _slow_query_threshold_ns) {
            return true;
        }

        return false;
    }

    ::dsn::error_code
    check_column_families(const std::string &path, bool *missing_meta_cf, bool *miss_data_cf);

    void release_db();

    ::dsn::error_code flush_all_family_columns(bool wait);

    void on_detect_hotkey(const dsn::replication::detect_hotkey_request &req,
                          dsn::replication::detect_hotkey_response &resp) override;

    uint32_t query_data_version() const override;

    dsn::replication::manual_compaction_status::type query_compact_status() const override;

    // Log expired keys for verbose mode.
    void log_expired_data(const char *op,
                          const dsn::rpc_address &addr,
                          const dsn::blob &hash_key,
                          const dsn::blob &sort_key) const;
    void log_expired_data(const char *op, const dsn::rpc_address &addr, const dsn::blob &key) const;
    void
    log_expired_data(const char *op, const dsn::rpc_address &addr, const rocksdb::Slice &key) const;

private:
    static const std::chrono::seconds kServerStatUpdateTimeSec;
    static const std::string COMPRESSION_HEADER;

    dsn::gpid _gpid;
    std::string _primary_address;
    // slow query time threshold. exceed this threshold will be logged.
    uint64_t _slow_query_threshold_ns;

    range_read_limiter_options _rng_rd_opts;

    std::shared_ptr<KeyWithTTLCompactionFilterFactory> _key_ttl_compaction_filter_factory;
    std::shared_ptr<rocksdb::Statistics> _statistics;
    rocksdb::DBOptions _db_opts;
    // The value of option in data_cf according to conf template file config.ini
    rocksdb::ColumnFamilyOptions _data_cf_opts;
    // Dynamically calculate the value of current data_cf option according to the conf module file
    // and usage scenario
    rocksdb::ColumnFamilyOptions _table_data_cf_opts;
    rocksdb::BlockBasedTableOptions _tbl_opts;
    rocksdb::ColumnFamilyOptions _meta_cf_opts;
    rocksdb::ReadOptions _data_cf_rd_opts;
    std::string _usage_scenario;
    std::string _user_specified_compaction;
    // Whether it is necessary to update the current data_cf, it is required when opening the db at
    // the first time, but not later
    bool _table_data_cf_opts_recalculated;

    rocksdb::DB *_db;
    rocksdb::ColumnFamilyHandle *_data_cf;
    rocksdb::ColumnFamilyHandle *_meta_cf;
    static std::shared_ptr<rocksdb::Cache> _s_block_cache;
    static std::shared_ptr<rocksdb::WriteBufferManager> _s_write_buffer_manager;
    static std::shared_ptr<rocksdb::RateLimiter> _s_rate_limiter;
    static int64_t _rocksdb_limiter_last_total_through;
    volatile bool _is_open;
    uint32_t _pegasus_data_version;
    std::atomic<int64_t> _last_durable_decree;

    std::unique_ptr<meta_store> _meta_store;
    std::unique_ptr<capacity_unit_calculator> _cu_calculator;
    std::unique_ptr<pegasus_server_write> _server_write;

    uint32_t _checkpoint_reserve_min_count;
    uint32_t _checkpoint_reserve_time_seconds;
    std::atomic_bool _is_checkpointing;         // whether the db is doing checkpoint
    ::dsn::utils::ex_lock_nr _checkpoints_lock; // protected the following checkpoints vector
    std::deque<int64_t> _checkpoints;           // ordered checkpoints

    pegasus_context_cache _context_cache;

    ::dsn::task_ptr _update_replica_rdb_stat;
    static ::dsn::task_ptr _update_server_rdb_stat;

    pegasus_manual_compact_service _manual_compact_svc;

    std::atomic<int32_t> _partition_version;
    bool _validate_partition_hash{false};

    dsn::replication::ingestion_status::type _ingestion_status{
        dsn::replication::ingestion_status::IS_INVALID};

    dsn::task_tracker _tracker;

    std::shared_ptr<hotkey_collector> _read_hotkey_collector;
    std::shared_ptr<hotkey_collector> _write_hotkey_collector;

    std::shared_ptr<throttling_controller> _read_size_throttling_controller;

    METRIC_VAR_DECLARE_counter(get_requests);
    METRIC_VAR_DECLARE_counter(multi_get_requests);
    METRIC_VAR_DECLARE_counter(batch_get_requests);
    METRIC_VAR_DECLARE_counter(scan_requests);

    METRIC_VAR_DECLARE_percentile_int64(get_latency_ns);
    METRIC_VAR_DECLARE_percentile_int64(multi_get_latency_ns);
    METRIC_VAR_DECLARE_percentile_int64(batch_get_latency_ns);
    METRIC_VAR_DECLARE_percentile_int64(scan_latency_ns);

    METRIC_VAR_DECLARE_counter(read_expired_values);
    METRIC_VAR_DECLARE_counter(read_filtered_values);
    METRIC_VAR_DECLARE_counter(abnormal_read_requests);
    METRIC_VAR_DECLARE_counter(throttling_rejected_read_requests);

    // Server-level metrics for rocksdb.
    METRIC_VAR_DECLARE_gauge_int64(rdb_block_cache_mem_usage_bytes, static);
    METRIC_VAR_DECLARE_gauge_int64(rdb_write_rate_limiter_through_bytes_per_sec, static);

    // Replica-level metrics for rocksdb.
    METRIC_VAR_DECLARE_gauge_int64(rdb_total_sst_files);
    METRIC_VAR_DECLARE_gauge_int64(rdb_total_sst_size_mb);
    METRIC_VAR_DECLARE_gauge_int64(rdb_estimated_keys);

    METRIC_VAR_DECLARE_gauge_int64(rdb_index_and_filter_blocks_mem_usage_bytes);
    METRIC_VAR_DECLARE_gauge_int64(rdb_memtable_mem_usage_bytes);
    METRIC_VAR_DECLARE_gauge_int64(rdb_block_cache_hit_count);
    METRIC_VAR_DECLARE_gauge_int64(rdb_block_cache_total_count);
    METRIC_VAR_DECLARE_gauge_int64(rdb_memtable_hit_count);
    METRIC_VAR_DECLARE_gauge_int64(rdb_memtable_total_count);
    METRIC_VAR_DECLARE_gauge_int64(rdb_l0_hit_count);
    METRIC_VAR_DECLARE_gauge_int64(rdb_l1_hit_count);
    METRIC_VAR_DECLARE_gauge_int64(rdb_l2_and_up_hit_count);

    METRIC_VAR_DECLARE_gauge_int64(rdb_write_amplification);
    METRIC_VAR_DECLARE_gauge_int64(rdb_read_amplification);

    METRIC_VAR_DECLARE_gauge_int64(rdb_bloom_filter_seek_negatives);
    METRIC_VAR_DECLARE_gauge_int64(rdb_bloom_filter_seek_total);
    METRIC_VAR_DECLARE_gauge_int64(rdb_bloom_filter_point_lookup_negatives);
    METRIC_VAR_DECLARE_gauge_int64(rdb_bloom_filter_point_lookup_positives);
    METRIC_VAR_DECLARE_gauge_int64(rdb_bloom_filter_point_lookup_true_positives);
};

} // namespace server
} // namespace pegasus
