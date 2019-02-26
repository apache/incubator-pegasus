// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <vector>
#include <rocksdb/db.h>
#include <rocksdb/table.h>
#include <rocksdb/listener.h>
#include <rocksdb/options.h>
#include <dsn/perf_counter/perf_counter_wrapper.h>
#include <dsn/dist/replication/replication.codes.h>
#include <rrdb/rrdb_types.h>
#include <rrdb/rrdb.server.h>

#include "key_ttl_compaction_filter.h"
#include "pegasus_scan_context.h"
#include "pegasus_manual_compact_service.h"
#include "pegasus_write_service.h"

namespace pegasus {
namespace server {

class pegasus_server_write;

class pegasus_server_impl : public ::dsn::apps::rrdb_service
{
public:
    static void register_service()
    {
        replication_app_base::register_storage_engine(
            "pegasus", replication_app_base::create<pegasus::server::pegasus_server_impl>);
        register_rpc_handlers();
    }
    explicit pegasus_server_impl(dsn::replication::replica *r);

    virtual ~pegasus_server_impl() override;

    // the following methods may set physical error if internal error occurs
    virtual void on_get(const ::dsn::blob &key,
                        ::dsn::rpc_replier<::dsn::apps::read_response> &reply) override;
    virtual void on_multi_get(const ::dsn::apps::multi_get_request &args,
                              ::dsn::rpc_replier<::dsn::apps::multi_get_response> &reply) override;
    virtual void on_sortkey_count(const ::dsn::blob &args,
                                  ::dsn::rpc_replier<::dsn::apps::count_response> &reply) override;
    virtual void on_ttl(const ::dsn::blob &key,
                        ::dsn::rpc_replier<::dsn::apps::ttl_response> &reply) override;
    virtual void on_get_scanner(const ::dsn::apps::get_scanner_request &args,
                                ::dsn::rpc_replier<::dsn::apps::scan_response> &reply) override;
    virtual void on_scan(const ::dsn::apps::scan_request &args,
                         ::dsn::rpc_replier<::dsn::apps::scan_response> &reply) override;
    virtual void on_clear_scanner(const int64_t &args) override;

    // input:
    //  - argc = 0 : re-open the db
    //  - argc = 2n + 1, n >= 0; normal open the db
    // returns:
    //  - ERR_OK
    //  - ERR_FILE_OPERATION_FAILED
    //  - ERR_LOCAL_APP_FAILURE
    virtual ::dsn::error_code start(int argc, char **argv) override;

    virtual void cancel_background_work(bool wait) override;

    // returns:
    //  - ERR_OK
    //  - ERR_FILE_OPERATION_FAILED
    virtual ::dsn::error_code stop(bool clear_state) override;

    /// Each of the write request (specifically, the rpc that's configured as write, see
    /// option `rpc_request_is_write_operation` in rDSN `task_spec`) will first be
    /// replicated to the replicas through the underlying PacificA protocol in rDSN, and
    /// after being committed, the mutation will be applied into rocksdb by this function.
    ///
    /// \see dsn::replication::replication_app_base::apply_mutation
    /// \inherit dsn::replication::replication_app_base
    virtual int on_batched_write_requests(int64_t decree,
                                          uint64_t timestamp,
                                          dsn::message_ex **requests,
                                          int count) override;

    virtual ::dsn::error_code prepare_get_checkpoint(dsn::blob &learn_req) override
    {
        return ::dsn::ERR_OK;
    }

    // returns:
    //  - ERR_OK: checkpoint succeed
    //  - ERR_WRONG_TIMING: another checkpoint is running now
    //  - ERR_LOCAL_APP_FAILURE: some internal failure
    //  - ERR_FILE_OPERATION_FAILED: some file failure
    // ATTENTION: make sure that no other threads is writing into the replica.
    virtual ::dsn::error_code sync_checkpoint() override;

    // returns:
    //  - ERR_OK: checkpoint succeed
    //  - ERR_WRONG_TIMING: another checkpoint is running now
    //  - ERR_LOCAL_APP_FAILURE: some internal failure
    //  - ERR_FILE_OPERATION_FAILED: some file failure
    //  - ERR_TRY_AGAIN: flush memtable triggered, need try again later
    virtual ::dsn::error_code async_checkpoint(bool flush_memtable) override;

    //
    // copy the latest checkpoint to checkpoint_dir, and the decree of the checkpoint
    // copied will be assigned to checkpoint_decree if checkpoint_decree is not null.
    // if checkpoint_dir already exist, this function will delete it first.
    //
    // must be thread safe
    // this method will not trigger flush(), just copy even if the app is empty.
    virtual ::dsn::error_code copy_checkpoint_to_dir(const char *checkpoint_dir,
                                                     /*output*/ int64_t *last_decree) override;

    //
    // help function, just copy checkpoint to specified dir and ignore _is_checkpointing.
    // if checkpoint_dir already exist, this function will delete it first.
    ::dsn::error_code copy_checkpoint_to_dir_unsafe(const char *checkpoint_dir,
                                                    /**output*/ int64_t *checkpoint_decree);

    // get the last checkpoint
    // if succeed:
    //  - the checkpoint files path are put into "state.files"
    //  - the checkpoint_info are serialized into "state.meta"
    //  - the "state.from_decree_excluded" and "state.to_decree_excluded" are set properly
    // returns:
    //  - ERR_OK
    //  - ERR_OBJECT_NOT_FOUND
    //  - ERR_FILE_OPERATION_FAILED
    virtual ::dsn::error_code get_checkpoint(int64_t learn_start,
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
    virtual ::dsn::error_code
    storage_apply_checkpoint(chkpt_apply_mode mode,
                             const dsn::replication::learn_state &state) override;

    virtual int64_t last_durable_decree() const override { return _last_durable_decree.load(); }

    virtual int64_t last_flushed_decree() const override { return _db->GetLastFlushedDecree(); }

    virtual void update_app_envs(const std::map<std::string, std::string> &envs) override;

    virtual void query_app_envs(/*out*/ std::map<std::string, std::string> &envs) override;

private:
    friend class manual_compact_service_test;
    friend class pegasus_compression_options_test;

    friend class pegasus_manual_compact_service;
    friend class pegasus_write_service;

    // parse checkpoint directories in the data dir
    // checkpoint directory format is: "checkpoint.{decree}"
    void parse_checkpoints();

    // garbage collection checkpoints
    // if force_reserve_one == true, then only reserve the last one checkpoint
    void gc_checkpoints(bool force_reserve_one = false);

    void set_last_durable_decree(int64_t decree) { _last_durable_decree.store(decree); }

    // return 1 if value is appended
    // return 2 if value is expired
    // return 3 if value is filtered
    int append_key_value_for_scan(std::vector<::dsn::apps::key_value> &kvs,
                                  const rocksdb::Slice &key,
                                  const rocksdb::Slice &value,
                                  ::dsn::apps::filter_type::type hash_key_filter_type,
                                  const ::dsn::blob &hash_key_filter_pattern,
                                  ::dsn::apps::filter_type::type sort_key_filter_type,
                                  const ::dsn::blob &sort_key_filter_pattern,
                                  uint32_t epoch_now,
                                  bool no_value);

    // return 1 if value is appended
    // return 2 if value is expired
    // return 3 if value is filtered
    int append_key_value_for_multi_get(std::vector<::dsn::apps::key_value> &kvs,
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

    void update_usage_scenario(const std::map<std::string, std::string> &envs);

    void update_default_ttl(const std::map<std::string, std::string> &envs);

    void update_checkpoint_reserve(const std::map<std::string, std::string> &envs);

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

    // return true if successfully set
    bool set_options(const std::unordered_map<std::string, std::string> &new_options);

    // return random value in range of [0.75,1.25] * base_value
    uint64_t get_random_nearby(uint64_t base_value)
    {
        uint64_t gap = base_value / 4;
        return dsn::rand::next_u64(base_value - gap, base_value + gap);
    }

    // return true if expired
    bool check_if_record_expired(uint32_t epoch_now, rocksdb::Slice raw_value)
    {
        return pegasus::check_if_record_expired(
            _value_schema_version, epoch_now, utils::to_string_view(raw_value));
    }

private:
    static const std::string COMPRESSION_HEADER;

    dsn::gpid _gpid;
    std::string _primary_address;
    bool _verbose_log;
    uint64_t _abnormal_get_time_threshold_ns;
    uint64_t _abnormal_get_size_threshold;
    uint64_t _abnormal_multi_get_time_threshold_ns;
    uint64_t _abnormal_multi_get_size_threshold;
    uint64_t _abnormal_multi_get_iterate_count_threshold;

    std::shared_ptr<KeyWithTTLCompactionFilterFactory> _key_ttl_compaction_filter_factory;
    std::shared_ptr<rocksdb::Statistics> _statistics;
    rocksdb::BlockBasedTableOptions _tbl_opts;
    rocksdb::Options _db_opts;
    rocksdb::WriteOptions _wt_opts;
    rocksdb::ReadOptions _rd_opts;
    std::string _usage_scenario;

    rocksdb::DB *_db;
    static std::shared_ptr<rocksdb::Cache> _block_cache;
    volatile bool _is_open;
    uint32_t _value_schema_version;
    std::atomic<int64_t> _last_durable_decree;

    std::unique_ptr<pegasus_server_write> _server_write;

    uint32_t _checkpoint_reserve_min_count_in_config;
    uint32_t _checkpoint_reserve_time_seconds_in_config;
    uint32_t _checkpoint_reserve_min_count;
    uint32_t _checkpoint_reserve_time_seconds;
    std::atomic_bool _is_checkpointing;         // whether the db is doing checkpoint
    ::dsn::utils::ex_lock_nr _checkpoints_lock; // protected the following checkpoints vector
    std::deque<int64_t> _checkpoints;           // ordered checkpoints

    pegasus_context_cache _context_cache;

    std::chrono::seconds _update_rdb_stat_interval;
    ::dsn::task_ptr _update_replica_rdb_stat;
    static ::dsn::task_ptr _update_server_rdb_stat;

    pegasus_manual_compact_service _manual_compact_svc;

    dsn::task_tracker _tracker;

    // perf counters
    ::dsn::perf_counter_wrapper _pfc_get_qps;
    ::dsn::perf_counter_wrapper _pfc_multi_get_qps;
    ::dsn::perf_counter_wrapper _pfc_scan_qps;

    ::dsn::perf_counter_wrapper _pfc_get_latency;
    ::dsn::perf_counter_wrapper _pfc_multi_get_latency;
    ::dsn::perf_counter_wrapper _pfc_scan_latency;

    ::dsn::perf_counter_wrapper _pfc_recent_expire_count;
    ::dsn::perf_counter_wrapper _pfc_recent_filter_count;
    ::dsn::perf_counter_wrapper _pfc_recent_abnormal_count;

    // rocksdb internal statistics
    // server level
    static ::dsn::perf_counter_wrapper _pfc_rdb_block_cache_mem_usage;
    // replica level
    ::dsn::perf_counter_wrapper _pfc_rdb_sst_count;
    ::dsn::perf_counter_wrapper _pfc_rdb_sst_size;
    ::dsn::perf_counter_wrapper _pfc_rdb_block_cache_hit_count;
    ::dsn::perf_counter_wrapper _pfc_rdb_block_cache_total_count;
    ::dsn::perf_counter_wrapper _pfc_rdb_index_and_filter_blocks_mem_usage;
    ::dsn::perf_counter_wrapper _pfc_rdb_memtable_mem_usage;
};

} // namespace server
} // namespace pegasus
