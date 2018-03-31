// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "key_ttl_compaction_filter.h"
#include "pegasus_scan_context.h"
#include <rocksdb/db.h>
#include <rrdb/rrdb.server.h>
#include <vector>
#include <dsn/cpp/perf_counter_wrapper.h>
#include <dsn/dist/replication/replication.codes.h>

namespace pegasus {
namespace server {

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
    virtual ~pegasus_server_impl() {}

    // the following methods may set physical error if internal error occurs
    virtual void on_put(const ::dsn::apps::update_request &update,
                        ::dsn::rpc_replier<::dsn::apps::update_response> &reply) override;
    virtual void on_multi_put(const ::dsn::apps::multi_put_request &args,
                              ::dsn::rpc_replier<::dsn::apps::update_response> &reply) override;
    virtual void on_remove(const ::dsn::blob &key,
                           ::dsn::rpc_replier<::dsn::apps::update_response> &reply) override;
    virtual void
    on_multi_remove(const ::dsn::apps::multi_remove_request &args,
                    ::dsn::rpc_replier<::dsn::apps::multi_remove_response> &reply) override;
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

    // returns:
    //  - ERR_OK
    //  - ERR_FILE_OPERATION_FAILED
    virtual ::dsn::error_code stop(bool clear_state) override;

    virtual int on_batched_write_requests(int64_t decree,
                                          int64_t timestamp,
                                          dsn_message_t *requests,
                                          int count) override;

    virtual ::dsn::error_code prepare_get_checkpoint(dsn::blob &learn_req) override
    {
        return ::dsn::ERR_OK;
    }
    // returns:
    //  - ERR_OK
    //  - ERR_WRONG_TIMING
    //  - ERR_NO_NEED_OPERATE
    //  - ERR_LOCAL_APP_FAILURE
    //  - ERR_FILE_OPERATION_FAILED
    virtual ::dsn::error_code sync_checkpoint() override;

    // returns:
    //  - ERR_OK
    //  - ERR_WRONG_TIMING: is checkpointing now
    //  - ERR_NO_NEED_OPERATE: the checkpoint is fresh enough, no need to checkpoint
    //  - ERR_LOCAL_APP_FAILURE: some internal failure
    //  - ERR_FILE_OPERATION_FAILED: some file failure
    //  - ERR_TRY_AGAIN: need try again later
    virtual ::dsn::error_code async_checkpoint(bool is_emergency) override;

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

    virtual int64_t last_durable_decree() const { return _last_durable_decree.load(); }

private:
    // parse checkpoint directories in the data dir
    // checkpoint directory format is: "checkpoint.{decree}"
    void parse_checkpoints();

    // garbage collection checkpoints
    void gc_checkpoints();

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
    bool is_filter_type_supported(::dsn::apps::filter_type::type filter_type);

    // return true if the data is valid for the filter
    bool validate_filter(::dsn::apps::filter_type::type filter_type,
                         const ::dsn::blob &filter_pattern,
                         const ::dsn::blob &value);

    // statistic the sst file info for this replica. return (-1,-1) if failed.
    std::pair<int64_t, int64_t> statistic_sst_size();

    void updating_rocksdb_sstsize();

    virtual void manual_compact();

    // get the absolute path of restore directory and the flag whether force restore from env
    // return
    //      std::pair<std::string, bool>, pair.first is the path of the restore dir; pair.second is
    //      the flag that whether force restore
    std::pair<std::string, bool> get_restore_dir_from_env(int argc, char **argv);

private:
    dsn::gpid _gpid;
    std::string _primary_address;
    bool _verbose_log;
    uint64_t _abnormal_get_time_threshold_ns;
    uint64_t _abnormal_get_size_threshold;
    uint64_t _abnormal_multi_get_time_threshold_ns;
    uint64_t _abnormal_multi_get_size_threshold;
    uint64_t _abnormal_multi_get_iterate_count_threshold;

    KeyWithTTLCompactionFilter _key_ttl_compaction_filter;
    rocksdb::Options _db_opts;
    rocksdb::WriteOptions _wt_opts;
    rocksdb::ReadOptions _rd_opts;

    rocksdb::DB *_db;
    volatile bool _is_open;
    uint32_t _value_schema_version;
    std::atomic<int64_t> _last_durable_decree;

    rocksdb::WriteBatch _batch;
    std::vector<::dsn::rpc_replier<::dsn::apps::update_response>> _batch_repliers;
    std::vector<::dsn::perf_counter *> _batch_perfcounters;

    std::string _write_buf;
    std::vector<rocksdb::Slice> _write_slices;
    int _physical_error;

    uint32_t _checkpoint_reserve_min_count;
    uint32_t _checkpoint_reserve_time_seconds;
    std::atomic_bool _is_checkpointing;         // whether the db is doing checkpoint
    ::dsn::utils::ex_lock_nr _checkpoints_lock; // protected the following checkpoints vector
    std::deque<int64_t> _checkpoints;           // ordered checkpoints

    pegasus_context_cache _context_cache;

    uint32_t _updating_rocksdb_sstsize_interval_seconds;
    ::dsn::task_ptr _updating_task;

    // perf counters
    ::dsn::perf_counter_wrapper _pfc_get_qps;
    ::dsn::perf_counter_wrapper _pfc_multi_get_qps;
    ::dsn::perf_counter_wrapper _pfc_scan_qps;
    ::dsn::perf_counter_wrapper _pfc_put_qps;
    ::dsn::perf_counter_wrapper _pfc_multi_put_qps;
    ::dsn::perf_counter_wrapper _pfc_remove_qps;
    ::dsn::perf_counter_wrapper _pfc_multi_remove_qps;

    ::dsn::perf_counter_wrapper _pfc_get_latency;
    ::dsn::perf_counter_wrapper _pfc_multi_get_latency;
    ::dsn::perf_counter_wrapper _pfc_scan_latency;
    ::dsn::perf_counter_wrapper _pfc_put_latency;
    ::dsn::perf_counter_wrapper _pfc_multi_put_latency;
    ::dsn::perf_counter_wrapper _pfc_remove_latency;
    ::dsn::perf_counter_wrapper _pfc_multi_remove_latency;

    ::dsn::perf_counter_wrapper _pfc_recent_expire_count;
    ::dsn::perf_counter_wrapper _pfc_recent_filter_count;
    ::dsn::perf_counter_wrapper _pfc_recent_abnormal_count;
    ::dsn::perf_counter_wrapper _pfc_sst_count;
    ::dsn::perf_counter_wrapper _pfc_sst_size;
};
}
} // namespace
