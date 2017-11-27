// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "key_ttl_compaction_filter.h"
#include "pegasus_scan_context.h"
#include <rocksdb/db.h>
#include <rrdb/rrdb.server.h>
#include <dsn/cpp/replicated_service_app.h>
#include <vector>
#include <dsn/cpp/perf_counter_.h>
#include <dsn/dist/replication/replication.codes.h>

namespace pegasus {
namespace server {

class pegasus_server_impl : public ::dsn::apps::rrdb_service,
                            public ::dsn::replicated_service_app_type_1
{
public:
    explicit pegasus_server_impl(dsn_gpid gpid);
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

    // the following methods are for stateful apps with layer 2 support

    // returns:
    //  - ERR_OK
    //  - ERR_FILE_OPERATION_FAILED
    //  - ERR_LOCAL_APP_FAILURE
    virtual ::dsn::error_code start(int argc, char **argv) override;

    // returns:
    //  - ERR_OK
    //  - ERR_FILE_OPERATION_FAILED
    virtual ::dsn::error_code stop(bool clear_state) override;

    virtual void on_batched_write_requests(int64_t decree,
                                           int64_t timestamp,
                                           dsn_message_t *requests,
                                           int count) override;

    virtual int get_physical_error() override { return _physical_error; }

    // returns:
    //  - ERR_OK
    //  - ERR_WRONG_TIMING
    //  - ERR_NO_NEED_OPERATE
    //  - ERR_LOCAL_APP_FAILURE
    //  - ERR_FILE_OPERATION_FAILED
    virtual ::dsn::error_code sync_checkpoint(int64_t last_commit) override;

    // returns:
    //  - ERR_OK
    //  - ERR_WRONG_TIMING: is checkpointing now
    //  - ERR_NO_NEED_OPERATE: the checkpoint is fresh enough, no need to checkpoint
    //  - ERR_LOCAL_APP_FAILURE: some internal failure
    //  - ERR_FILE_OPERATION_FAILED: some file failure
    //  - ERR_TRY_AGAIN: need try again later
    virtual ::dsn::error_code async_checkpoint(int64_t last_commit, bool is_emergency) override;

    virtual int64_t get_last_checkpoint_decree() override { return last_durable_decree(); }

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
                                             int64_t local_commit,
                                             void *learn_request,
                                             int learn_request_size,
                                             app_learn_state &state) override;

    // apply checkpoint, this will clear and recreate the db
    // if succeed:
    //  - last_committed_decree() == last_durable_decree()
    // returns:
    //  - ERR_OK
    //  - ERR_FILE_OPERATION_FAILED
    //  - error code of close()
    //  - error code of open()
    //  - error code of checkpoint()
    virtual ::dsn::error_code apply_checkpoint(dsn_chkpt_apply_mode mode,
                                               int64_t local_commit,
                                               const dsn_app_learn_state &state) override;

private:
    // parse checkpoint directories in the data dir
    // checkpoint directory format is: "checkpoint.{decree}"
    void parse_checkpoints();

    // garbage collection checkpoints
    void gc_checkpoints();

    int64_t last_durable_decree() { return _last_durable_decree.load(); }
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

private:
    dsn_gpid _gpid;
    std::string _primary_address;
    std::string _replica_name;
    std::string _data_dir;
    bool _verbose_log;

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
    std::vector<dsn_handle_t> _batch_perfcounters;

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
    ::dsn::perf_counter_ _pfc_get_qps;
    ::dsn::perf_counter_ _pfc_multi_get_qps;
    ::dsn::perf_counter_ _pfc_scan_qps;
    ::dsn::perf_counter_ _pfc_put_qps;
    ::dsn::perf_counter_ _pfc_multi_put_qps;
    ::dsn::perf_counter_ _pfc_remove_qps;
    ::dsn::perf_counter_ _pfc_multi_remove_qps;

    ::dsn::perf_counter_ _pfc_get_latency;
    ::dsn::perf_counter_ _pfc_multi_get_latency;
    ::dsn::perf_counter_ _pfc_scan_latency;
    ::dsn::perf_counter_ _pfc_put_latency;
    ::dsn::perf_counter_ _pfc_multi_put_latency;
    ::dsn::perf_counter_ _pfc_remove_latency;
    ::dsn::perf_counter_ _pfc_multi_remove_latency;

    ::dsn::perf_counter_ _pfc_recent_expire_count;
    ::dsn::perf_counter_ _pfc_recent_filter_count;
    ::dsn::perf_counter_ _pfc_sst_count;
    ::dsn::perf_counter_ _pfc_sst_size;
};
}
} // namespace
