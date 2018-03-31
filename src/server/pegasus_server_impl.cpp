// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_server_impl.h"
#include <pegasus_key_schema.h>
#include <pegasus_value_schema.h>
#include <pegasus_utils.h>
#include <dsn/cpp/smart_pointers.h>
#include <dsn/utility/utils.h>
#include <dsn/utility/filesystem.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/table.h>
#include <boost/lexical_cast.hpp>
#include <algorithm>

namespace pegasus {
namespace server {

// Although we have removed the INCR operator, but we need reserve the code for compatibility
// reason,
// because there may be some mutation log entries which include the code. Even if these entries need
// not to be applied to rocksdb, they may be deserialized.
DEFINE_TASK_CODE_RPC(RPC_RRDB_RRDB_INCR, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

static std::string chkpt_get_dir_name(int64_t decree)
{
    char buffer[256];
    sprintf(buffer, "checkpoint.%" PRId64 "", decree);
    return std::string(buffer);
}

static bool chkpt_init_from_dir(const char *name, int64_t &decree)
{
    return 1 == sscanf(name, "checkpoint.%" PRId64 "", &decree) &&
           std::string(name) == chkpt_get_dir_name(decree);
}

pegasus_server_impl::pegasus_server_impl(dsn::replication::replica *r)
    : dsn::apps::rrdb_service(r),
      _db(nullptr),
      _is_open(false),
      _value_schema_version(0),
      _last_durable_decree(0),
      _physical_error(0),
      _is_checkpointing(false)
{
    _primary_address = dsn::rpc_address(dsn_primary_address()).to_string();
    _gpid = get_gpid();
    _verbose_log = dsn_config_get_value_bool("pegasus.server",
                                             "rocksdb_verbose_log",
                                             false,
                                             "print verbose log for debugging, default is false");
    _abnormal_get_time_threshold_ns = dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_abnormal_get_time_threshold_ns",
        0,
        "rocksdb_abnormal_get_time_threshold_ns, default is 0, means no check");
    _abnormal_get_size_threshold = dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_abnormal_get_size_threshold",
        0,
        "rocksdb_abnormal_get_size_threshold, default is 0, means no check");
    _abnormal_multi_get_time_threshold_ns = dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_abnormal_multi_get_time_threshold_ns",
        0,
        "rocksdb_abnormal_multi_get_time_threshold_ns, default is 0, means no check");
    _abnormal_multi_get_size_threshold = dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_abnormal_multi_get_size_threshold",
        0,
        "rocksdb_abnormal_multi_get_size_threshold, default is 0, means no check");
    _abnormal_multi_get_iterate_count_threshold = dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_abnormal_multi_get_iterate_count_threshold",
        0,
        "rocksdb_abnormal_multi_get_iterate_count_threshold, default is 0, means no check");

    // init db options

    // rocksdb default: snappy
    std::string compression_str = dsn_config_get_value_string(
        "pegasus.server",
        "rocksdb_compression_type",
        "none",
        "rocksdb options.compression, default none. Supported: snappy, none.");
    if (compression_str == "none") {
        _db_opts.compression = rocksdb::kNoCompression;
    } else if (compression_str == "snappy") {
        _db_opts.compression = rocksdb::kSnappyCompression;
    } else {
        dassert("unsupported compression type: %s", compression_str.c_str());
    }

    // rocksdb default: 4MB
    _db_opts.write_buffer_size =
        (size_t)dsn_config_get_value_uint64("pegasus.server",
                                            "rocksdb_write_buffer_size",
                                            64 * 1024 * 1024,
                                            "rocksdb options.write_buffer_size, default 64MB");

    // rocksdb default: 2
    _db_opts.max_write_buffer_number =
        (int)dsn_config_get_value_uint64("pegasus.server",
                                         "rocksdb_max_write_buffer_number",
                                         4,
                                         "rocksdb options.max_write_buffer_number, default 4");

    // rocksdb default: 1
    _db_opts.max_background_flushes =
        (int)dsn_config_get_value_uint64("pegasus.server",
                                         "rocksdb_max_background_flushes",
                                         1,
                                         "rocksdb options.max_background_flushes, default 1");

    // rocksdb default: 1
    _db_opts.max_background_compactions =
        (int)dsn_config_get_value_uint64("pegasus.server",
                                         "rocksdb_max_background_compactions",
                                         2,
                                         "rocksdb options.max_background_compactions, default 2");

    // rocksdb default: 7
    _db_opts.num_levels = dsn_config_get_value_uint64(
        "pegasus.server", "rocksdb_num_levels", 6, "rocksdb options.num_levels, default 6");

    // rocksdb default: 2MB
    _db_opts.target_file_size_base =
        dsn_config_get_value_uint64("pegasus.server",
                                    "rocksdb_target_file_size_base",
                                    64 * 1024 * 1024,
                                    "rocksdb options.target_file_size_base, default 64MB");

    // rocksdb default: 10MB
    _db_opts.max_bytes_for_level_base =
        dsn_config_get_value_uint64("pegasus.server",
                                    "rocksdb_max_bytes_for_level_base",
                                    10 * 64 * 1024 * 1024,
                                    "rocksdb options.max_bytes_for_level_base, default 640MB");

// Deprecated
//    // rocksdb default: 10
//    _db_opts.max_grandparent_overlap_factor = (int)dsn_config_get_value_uint64(
//        "pegasus.server",
//        "rocksdb_max_grandparent_overlap_factor",
//        10,
//        "rocksdb options.max_grandparent_overlap_factor, default 10");

    // rocksdb default: 4
    _db_opts.level0_file_num_compaction_trigger =
        (int)dsn_config_get_value_uint64("pegasus.server",
                                         "rocksdb_level0_file_num_compaction_trigger",
                                         10,
                                         "rocksdb options.level0_file_num_compaction_trigger, 10");

    // rocksdb default: 20
    _db_opts.level0_slowdown_writes_trigger = (int)dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_level0_slowdown_writes_trigger",
        30,
        "rocksdb options.level0_slowdown_writes_trigger, default 30");

    // rocksdb default: 24
    _db_opts.level0_stop_writes_trigger =
        (int)dsn_config_get_value_uint64("pegasus.server",
                                         "rocksdb_level0_stop_writes_trigger",
                                         60,
                                         "rocksdb options.level0_stop_writes_trigger, default 60");

    // disable table block cache, default: false
    if ((bool)dsn_config_get_value_bool(
            "pegasus.server",
            "rocksdb_disable_table_block_cache",
            false,
            "rocksdb options.disable_table_block_cache, default false")) {
        rocksdb::BlockBasedTableOptions table_options;
        table_options.no_block_cache = true;
        table_options.block_restart_interval = 4;
        _db_opts.table_factory.reset(NewBlockBasedTableFactory(table_options));
    }

    // disable write ahead logging as replication handles logging instead now
    _wt_opts.disableWAL = true;

    // get the checkpoint reserve options.
    _checkpoint_reserve_min_count = (uint32_t)dsn_config_get_value_uint64(
        "pegasus.server", "checkpoint_reserve_min_count", 3, "checkpoint_reserve_min_count");
    _checkpoint_reserve_time_seconds =
        (uint32_t)dsn_config_get_value_uint64("pegasus.server",
                                              "checkpoint_reserve_time_seconds",
                                              3600,
                                              "checkpoint_reserve_time_seconds");

    // get the _updating_sstsize_inteval_seconds.
    _updating_rocksdb_sstsize_interval_seconds =
        (uint32_t)dsn_config_get_value_uint64("pegasus.server",
                                              "updating_rocksdb_sstsize_interval_seconds",
                                              600,
                                              "updating_rocksdb_sstsize_interval_seconds");

    // TODO: move the qps/latency counters and it's statistics to replication_app_base layer
    char str_gpid[128], buf[256];
    snprintf(str_gpid, 128, "%d.%d", _gpid.get_app_id(), _gpid.get_partition_index());

    // register the perf counters
    snprintf(buf, 255, "get_qps@%s", str_gpid);
    _pfc_get_qps.init_app_counter(
        "app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of GET request");

    snprintf(buf, 255, "multi_get_qps@%s", str_gpid);
    _pfc_multi_get_qps.init_app_counter(
        "app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of MULTI_GET request");

    snprintf(buf, 255, "scan_qps@%s", str_gpid);
    _pfc_scan_qps.init_app_counter(
        "app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of SCAN request");

    snprintf(buf, 255, "put_qps@%s", str_gpid);
    _pfc_put_qps.init_app_counter(
        "app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of PUT request");

    snprintf(buf, 255, "multi_put_qps@%s", str_gpid);
    _pfc_multi_put_qps.init_app_counter(
        "app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of MULTI_PUT request");

    snprintf(buf, 255, "remove_qps@%s", str_gpid);
    _pfc_remove_qps.init_app_counter(
        "app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of REMOVE request");

    snprintf(buf, 255, "multi_remove_qps@%s", str_gpid);
    _pfc_multi_remove_qps.init_app_counter(
        "app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of MULTI_REMOVE request");

    snprintf(buf, 255, "get_latency@%s", str_gpid);
    _pfc_get_latency.init_app_counter("app.pegasus",
                                      buf,
                                      COUNTER_TYPE_NUMBER_PERCENTILES,
                                      "statistic the latency of GET request");

    snprintf(buf, 255, "multi_get_latency@%s", str_gpid);
    _pfc_multi_get_latency.init_app_counter("app.pegasus",
                                            buf,
                                            COUNTER_TYPE_NUMBER_PERCENTILES,
                                            "statistic the latency of MULTI_GET request");

    snprintf(buf, 255, "scan_latency@%s", str_gpid);
    _pfc_scan_latency.init_app_counter("app.pegasus",
                                       buf,
                                       COUNTER_TYPE_NUMBER_PERCENTILES,
                                       "statistic the latency of SCAN request");

    snprintf(buf, 255, "put_latency@%s", str_gpid);
    _pfc_put_latency.init_app_counter("app.pegasus",
                                      buf,
                                      COUNTER_TYPE_NUMBER_PERCENTILES,
                                      "statistic the latency of PUT request");

    snprintf(buf, 255, "multi_put_latency@%s", str_gpid);
    _pfc_multi_put_latency.init_app_counter("app.pegasus",
                                            buf,
                                            COUNTER_TYPE_NUMBER_PERCENTILES,
                                            "statistic the latency of MULTI_PUT request");

    snprintf(buf, 255, "remove_latency@%s", str_gpid);
    _pfc_remove_latency.init_app_counter("app.pegasus",
                                         buf,
                                         COUNTER_TYPE_NUMBER_PERCENTILES,
                                         "statistic the latency of REMOVE request");

    snprintf(buf, 255, "multi_remove_latency@%s", str_gpid);
    _pfc_multi_remove_latency.init_app_counter("app.pegasus",
                                               buf,
                                               COUNTER_TYPE_NUMBER_PERCENTILES,
                                               "statistic the latency of MULTI_REMOVE request");

    snprintf(buf, 255, "recent.expire.count@%s", str_gpid);
    _pfc_recent_expire_count.init_app_counter("app.pegasus",
                                              buf,
                                              COUNTER_TYPE_VOLATILE_NUMBER,
                                              "statistic the recent expired value read count");

    snprintf(buf, 255, "recent.filter.count@%s", str_gpid);
    _pfc_recent_filter_count.init_app_counter("app.pegasus",
                                              buf,
                                              COUNTER_TYPE_VOLATILE_NUMBER,
                                              "statistic the recent filtered value read count");

    snprintf(buf, 255, "recent.abnormal.count@%s", str_gpid);
    _pfc_recent_abnormal_count.init_app_counter("app.pegasus",
                                                buf,
                                                COUNTER_TYPE_VOLATILE_NUMBER,
                                                "statistic the recent abnormal read count");

    snprintf(buf, 255, "disk.storage.sst.count@%s", str_gpid);
    _pfc_sst_count.init_app_counter(
        "app.pegasus", buf, COUNTER_TYPE_NUMBER, "statistic the count of sstable files");

    snprintf(buf, 255, "disk.storage.sst(MB)@%s", str_gpid);
    _pfc_sst_size.init_app_counter(
        "app.pegasus", buf, COUNTER_TYPE_NUMBER, "statistic the size of sstable files");

    updating_rocksdb_sstsize();
}

void pegasus_server_impl::parse_checkpoints()
{
    std::vector<std::string> dirs;
    ::dsn::utils::filesystem::get_subdirectories(data_dir(), dirs, false);

    ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);

    _checkpoints.clear();
    for (auto &d : dirs) {
        int64_t ci;
        std::string d1 = d.substr(data_dir().size() + 1);
        if (chkpt_init_from_dir(d1.c_str(), ci)) {
            _checkpoints.push_back(ci);
        } else if (d1.find("checkpoint") != std::string::npos) {
            ddebug("%s: invalid checkpoint directory %s, remove it", replica_name(), d.c_str());
            ::dsn::utils::filesystem::remove_path(d);
            if (!::dsn::utils::filesystem::remove_path(d)) {
                derror(
                    "%s: remove invalid checkpoint directory %s failed", replica_name(), d.c_str());
            }
        }
    }

    if (!_checkpoints.empty()) {
        std::sort(_checkpoints.begin(), _checkpoints.end());
        set_last_durable_decree(_checkpoints.back());
    } else {
        set_last_durable_decree(0);
    }
}

void pegasus_server_impl::gc_checkpoints()
{
    std::deque<int64_t> temp_list;
    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);
        if (_checkpoints.size() <= _checkpoint_reserve_min_count)
            return;
        temp_list = _checkpoints;
    }

    // find the max checkpoint which can be deleted
    int64_t max_del_d = -1;
    uint64_t current_time = dsn_now_ms() / 1000;
    for (int i = 0; i < temp_list.size(); ++i) {
        if (i + _checkpoint_reserve_min_count >= temp_list.size())
            break;
        int64_t d = temp_list[i];
        // we check last write time of "CURRENT" instead of directory, because the directory's
        // last write time may be updated by previous incompleted garbage collection.
        auto cpt_dir = ::dsn::utils::filesystem::path_combine(data_dir(), chkpt_get_dir_name(d));
        auto current_file = ::dsn::utils::filesystem::path_combine(cpt_dir, "CURRENT");
        if (!::dsn::utils::filesystem::file_exists(current_file)) {
            max_del_d = d;
            continue;
        }
        time_t tm;
        if (!dsn::utils::filesystem::last_write_time(current_file, tm)) {
            dwarn("get last write time of file %s failed", current_file.c_str());
            break;
        }
        uint64_t last_write_time = (uint64_t)tm;
        if (last_write_time + _checkpoint_reserve_time_seconds >= current_time) {
            // not expired
            break;
        }
        max_del_d = d;
    }
    if (max_del_d == -1) {
        // no checkpoint to delete
        ddebug("%s: no checkpoint to garbage collection, checkpoints_count = %d",
               replica_name(),
               (int)temp_list.size());
        return;
    }
    std::list<int64_t> to_delete_list;
    int64_t min_d = 0;
    int64_t max_d = 0;
    int checkpoints_count = 0;
    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);
        int delete_max_index = -1;
        for (int i = 0; i < _checkpoints.size(); ++i) {
            int64_t del_d = _checkpoints[i];
            if (i + _checkpoint_reserve_min_count >= _checkpoints.size() || del_d > max_del_d)
                break;
            to_delete_list.push_back(del_d);
            delete_max_index = i;
        }
        if (delete_max_index >= 0) {
            _checkpoints.erase(_checkpoints.begin(), _checkpoints.begin() + delete_max_index + 1);
        }

        if (!_checkpoints.empty()) {
            min_d = _checkpoints.front();
            max_d = _checkpoints.back();
            checkpoints_count = _checkpoints.size();
        } else {
            min_d = 0;
            max_d = 0;
            checkpoints_count = 0;
        }
    }

    // do delete
    std::list<int64_t> put_back_list;
    for (auto &del_d : to_delete_list) {
        auto cpt_dir =
            ::dsn::utils::filesystem::path_combine(data_dir(), chkpt_get_dir_name(del_d));
        if (::dsn::utils::filesystem::directory_exists(cpt_dir)) {
            if (::dsn::utils::filesystem::remove_path(cpt_dir)) {
                ddebug("%s: checkpoint directory %s removed by garbage collection",
                       replica_name(),
                       cpt_dir.c_str());
            } else {
                derror("%s: checkpoint directory %s remove failed by garbage collection",
                       replica_name(),
                       cpt_dir.c_str());
                put_back_list.push_back(del_d);
            }
        } else {
            ddebug("%s: checkpoint directory %s does not exist, ignored by garbage collection",
                   replica_name(),
                   cpt_dir.c_str());
        }
    }

    // put back checkpoints which is not deleted, to make it delete again in the next gc time.
    // ATTENTION: the put back checkpoint may be incomplete, which will cause failure on load. But
    // it would not cause data lost, because incomplete checkpoint can not be loaded successfully.
    if (!put_back_list.empty()) {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);
        if (_checkpoints.empty() || put_back_list.back() < _checkpoints.front()) {
            // just insert to front will hold the order
            _checkpoints.insert(_checkpoints.begin(), put_back_list.begin(), put_back_list.end());
        } else {
            // need to re-sort
            _checkpoints.insert(_checkpoints.begin(), put_back_list.begin(), put_back_list.end());
            std::sort(_checkpoints.begin(), _checkpoints.end());
        }

        if (!_checkpoints.empty()) {
            min_d = _checkpoints.front();
            max_d = _checkpoints.back();
            checkpoints_count = _checkpoints.size();
        } else {
            min_d = 0;
            max_d = 0;
            checkpoints_count = 0;
        }
    }

    ddebug("%s: after checkpoint garbage collection, checkpoints_count = %d, "
           "min_checkpoint = %" PRId64 ", max_checkpoint = %" PRId64,
           replica_name(),
           checkpoints_count,
           min_d,
           max_d);
}

int pegasus_server_impl::on_batched_write_requests(int64_t decree,
                                                   int64_t timestamp,
                                                   dsn_message_t *requests,
                                                   int count)
{
    dassert(_is_open, "");
    dassert(requests != nullptr, "");
    uint64_t start_time = dsn_now_ns();

    _physical_error = 0;
    if (count == 1 &&
        ((::dsn::message_ex *)requests[0])->local_rpc_code ==
            ::dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
        _pfc_multi_put_qps->increment();
        dsn_message_t request = requests[0];

        ::dsn::apps::update_response resp;
        resp.app_id = _gpid.get_app_id();
        resp.partition_index = _gpid.get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        ::dsn::apps::multi_put_request update;
        ::dsn::unmarshall(request, update);

        if (update.kvs.empty()) {
            // invalid argument
            derror("%s: invalid argument for multi_put: decree = %" PRId64 ", error = empty kvs",
                   replica_name(),
                   decree);

            ::dsn::rpc_replier<::dsn::apps::update_response> replier(
                dsn_msg_create_response(request));
            if (!replier.is_empty()) {
                // an invalid operation shoundn't be added to latency calculation
                resp.error = rocksdb::Status::kInvalidArgument;
                replier(resp);
            }
            return 0;
        }

        for (auto &kv : update.kvs) {
            ::dsn::blob raw_key;
            pegasus_generate_key(raw_key, update.hash_key, kv.key);
            rocksdb::Slice slice_key(raw_key.data(), raw_key.length());
            rocksdb::SliceParts skey(&slice_key, 1);

            pegasus_generate_value(_value_schema_version,
                                   update.expire_ts_seconds,
                                   kv.value,
                                   _write_buf,
                                   _write_slices);
            rocksdb::SliceParts svalue(&_write_slices[0], _write_slices.size());

            _batch.Put(skey, svalue);
        }

        _wt_opts.given_decree = decree;
        rocksdb::Status status = _db->Write(_wt_opts, &_batch);
        if (!status.ok()) {
            derror("%s: rocksdb write failed for multi_put: decree = %" PRId64 ", error = %s",
                   replica_name(),
                   decree,
                   status.ToString().c_str());
            _physical_error = status.code();
        }

        ::dsn::rpc_replier<::dsn::apps::update_response> replier(dsn_msg_create_response(request));
        if (!replier.is_empty()) {
            _pfc_multi_put_latency->set(dsn_now_ns() - start_time);
            resp.error = status.code();
            replier(resp);
        }

        _batch.Clear();
        return _physical_error;
    } else if (count == 1 &&
               ((::dsn::message_ex *)requests[0])->local_rpc_code ==
                   ::dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE) {
        _pfc_multi_remove_qps->increment();
        dsn_message_t request = requests[0];

        ::dsn::apps::multi_remove_response resp;
        resp.app_id = _gpid.get_app_id();
        resp.partition_index = _gpid.get_partition_index();
        resp.decree = decree;
        resp.server = _primary_address;

        ::dsn::apps::multi_remove_request update;
        ::dsn::unmarshall(request, update);

        if (update.sort_keys.empty()) {
            // invalid argument
            derror("%s: invalid argument for multi_remove: decree = %" PRId64
                   ", error = empty sort keys",
                   replica_name(),
                   decree);

            ::dsn::rpc_replier<::dsn::apps::multi_remove_response> replier(
                dsn_msg_create_response(request));
            if (!replier.is_empty()) {
                // an invalid operation shoundn't be added to latency calculation
                resp.error = rocksdb::Status::kInvalidArgument;
                resp.count = 0;
                replier(resp);
            }
            return 0;
        }

        for (auto &sort_key : update.sort_keys) {
            ::dsn::blob raw_key;
            pegasus_generate_key(raw_key, update.hash_key, sort_key);
            _batch.Delete(rocksdb::Slice(raw_key.data(), raw_key.length()));
        }

        _wt_opts.given_decree = decree;
        rocksdb::Status status = _db->Write(_wt_opts, &_batch);
        if (!status.ok()) {
            derror("%s: rocksdb write failed for multi_remove: decree = %" PRId64 ", error = %s",
                   replica_name(),
                   decree,
                   status.ToString().c_str());
            _physical_error = status.code();
            resp.count = 0;
        } else {
            resp.count = update.sort_keys.size();
        }

        ::dsn::rpc_replier<::dsn::apps::multi_remove_response> replier(
            dsn_msg_create_response(request));
        if (!replier.is_empty()) {
            _pfc_multi_remove_latency->set(dsn_now_ns() - start_time);
            resp.error = status.code();
            replier(resp);
        }

        _batch.Clear();
        return _physical_error;
    } else {
        for (int i = 0; i < count; ++i) {
            dsn_message_t request = requests[i];
            dassert(request != nullptr, "");
            ::dsn::message_ex *msg = (::dsn::message_ex *)request;
            ::dsn::blob key;
            if (msg->local_rpc_code == ::dsn::apps::RPC_RRDB_RRDB_PUT) {
                _pfc_put_qps->increment();
                ::dsn::apps::update_request update;
                ::dsn::unmarshall(request, update);
                key = update.key;

                rocksdb::Slice slice_key(key.data(), key.length());
                rocksdb::SliceParts skey(&slice_key, 1);

                pegasus_generate_value(_value_schema_version,
                                       update.expire_ts_seconds,
                                       update.value,
                                       _write_buf,
                                       _write_slices);
                rocksdb::SliceParts svalue(&_write_slices[0], _write_slices.size());

                _batch.Put(skey, svalue);
                _batch_repliers.emplace_back(dsn_msg_create_response(request));
                _batch_perfcounters.push_back(_pfc_put_latency.get());
            } else if (msg->local_rpc_code == ::dsn::apps::RPC_RRDB_RRDB_REMOVE) {
                _pfc_remove_qps->increment();
                ::dsn::unmarshall(request, key);

                rocksdb::Slice skey(key.data(), key.length());
                _batch.Delete(skey);
                _batch_repliers.emplace_back(dsn_msg_create_response(request));
                _batch_perfcounters.push_back(_pfc_remove_latency.get());
            } else if (msg->local_rpc_code == ::dsn::apps::RPC_RRDB_RRDB_MULTI_PUT ||
                       msg->local_rpc_code == ::dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE) {
                dassert(false,
                        "rpc code not allow batch: %s",
                        ::dsn::task_code(msg->local_rpc_code).to_string());
            } else {
                dassert(false,
                        "rpc code not handled: %s",
                        ::dsn::task_code(msg->local_rpc_code).to_string());
            }

            if (msg->header->client.partition_hash != 0) {
                uint64_t partition_hash = pegasus_key_hash(key);
                dassert(msg->header->client.partition_hash == partition_hash,
                        "inconsistent partition hash");
                int thread_hash = _gpid.thread_hash();
                dassert(msg->header->client.thread_hash == thread_hash, "inconsistent thread hash");
            }

            if (_verbose_log) {
                ::dsn::blob hash_key, sort_key;
                pegasus_restore_key(key, hash_key, sort_key);
                ddebug("%s: rocksdb write: decree = %" PRId64
                       ", code = %s, hash_key = \"%s\", sort_key = \"%s\"",
                       replica_name(),
                       decree,
                       msg->local_rpc_code.to_string(),
                       ::pegasus::utils::c_escape_string(hash_key).c_str(),
                       ::pegasus::utils::c_escape_string(sort_key).c_str());
            }
        }
    }

    if (_batch.Count() == 0) {
        // write fake data
        rocksdb::Slice empty_key;
        rocksdb::SliceParts skey(&empty_key, 1);

        ::dsn::blob empty_value;
        pegasus_generate_value(_value_schema_version, 0, empty_value, _write_buf, _write_slices);
        rocksdb::SliceParts svalue(&_write_slices[0], _write_slices.size());

        _batch.Put(skey, svalue);
    }
    _wt_opts.given_decree = decree;
    auto status = _db->Write(_wt_opts, &_batch);
    if (!status.ok()) {
        derror("%s: rocksdb write failed: decree = %" PRId64 ", error = %s",
               replica_name(),
               decree,
               status.ToString().c_str());
        _physical_error = status.code();
    }

    ::dsn::apps::update_response resp;
    resp.error = status.code();
    resp.app_id = _gpid.get_app_id();
    resp.partition_index = _gpid.get_partition_index();
    resp.decree = decree;
    resp.server = _primary_address;

    dassert(_batch_repliers.size() == _batch_perfcounters.size(),
            "%s: repliers's size(%u) vs perfcounters's size(%u) not match",
            replica_name(),
            _batch_repliers.size(),
            _batch_perfcounters.size());
    uint64_t latency = dsn_now_ns() - start_time;
    for (unsigned int i = 0; i != _batch_repliers.size(); ++i) {
        if (!_batch_repliers[i].is_empty()) {
            _batch_perfcounters[i]->set(latency);
            _batch_repliers[i](resp);
        }
    }

    _batch.Clear();
    _batch_repliers.clear();
    _batch_perfcounters.clear();

    return _physical_error;
}

void pegasus_server_impl::on_put(const ::dsn::apps::update_request &update,
                                 ::dsn::rpc_replier<::dsn::apps::update_response> &reply)
{
    dassert(false, "not implemented");
}

void pegasus_server_impl::on_multi_put(const ::dsn::apps::multi_put_request &args,
                                       ::dsn::rpc_replier<::dsn::apps::update_response> &reply)
{
    dassert(false, "not implemented");
}

void pegasus_server_impl::on_remove(const ::dsn::blob &key,
                                    ::dsn::rpc_replier<::dsn::apps::update_response> &reply)
{
    dassert(false, "not implemented");
}

void pegasus_server_impl::on_multi_remove(
    const ::dsn::apps::multi_remove_request &args,
    ::dsn::rpc_replier<::dsn::apps::multi_remove_response> &reply)
{
    dassert(false, "not implemented");
}

void pegasus_server_impl::on_get(const ::dsn::blob &key,
                                 ::dsn::rpc_replier<::dsn::apps::read_response> &reply)
{
    dassert(_is_open, "");
    _pfc_get_qps->increment();
    uint64_t start_time = dsn_now_ns();

    ::dsn::apps::read_response resp;
    resp.app_id = _gpid.get_app_id();
    resp.partition_index = _gpid.get_partition_index();
    resp.server = _primary_address;

    rocksdb::Slice skey(key.data(), key.length());
    std::unique_ptr<std::string> value(new std::string());
    rocksdb::Status status = _db->Get(_rd_opts, skey, value.get());

    if (status.ok()) {
        uint32_t expire_ts = pegasus_extract_expire_ts(_value_schema_version, *value);
        if (expire_ts > 0 && expire_ts <= ::pegasus::utils::epoch_now()) {
            _pfc_recent_expire_count->increment();
            if (_verbose_log) {
                derror("%s: rocksdb data expired for get", replica_name());
            }
            status = rocksdb::Status::NotFound();
        }
    }

    if (!status.ok()) {
        if (_verbose_log) {
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(key, hash_key, sort_key);
            derror("%s: rocksdb get failed for get: "
                   "hash_key = \"%s\", sort_key = \"%s\", error = %s",
                   replica_name(),
                   ::pegasus::utils::c_escape_string(hash_key).c_str(),
                   ::pegasus::utils::c_escape_string(sort_key).c_str(),
                   status.ToString().c_str());
        } else if (!status.IsNotFound()) {
            derror("%s: rocksdb get failed for get: error = %s",
                   replica_name(),
                   status.ToString().c_str());
        }
    }

    if (_abnormal_get_time_threshold_ns || _abnormal_get_size_threshold) {
        uint64_t time_used = dsn_now_ns() - start_time;
        if ((_abnormal_get_time_threshold_ns && time_used >= _abnormal_get_time_threshold_ns) ||
            (_abnormal_get_size_threshold && value->size() >= _abnormal_get_size_threshold)) {
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(key, hash_key, sort_key);
            dwarn("%s: rocksdb abnormal get: "
                  "hash_key = \"%s\", sort_key = \"%s\", return = %s, "
                  "value_size = %d, time_used = %" PRIu64 " ns",
                  replica_name(),
                  ::pegasus::utils::c_escape_string(hash_key).c_str(),
                  ::pegasus::utils::c_escape_string(sort_key).c_str(),
                  status.ToString().c_str(),
                  (int)value->size(),
                  time_used);
            _pfc_recent_abnormal_count->increment();
        }
    }

    resp.error = status.code();
    if (status.ok()) {
        pegasus_extract_user_data(_value_schema_version, std::move(value), resp.value);
    }

    _pfc_get_latency->set(dsn_now_ns() - start_time);

    reply(resp);
}

void pegasus_server_impl::on_multi_get(const ::dsn::apps::multi_get_request &request,
                                       ::dsn::rpc_replier<::dsn::apps::multi_get_response> &reply)
{
    dassert(_is_open, "");
    _pfc_multi_get_qps->increment();
    uint64_t start_time = dsn_now_ns();

    ::dsn::apps::multi_get_response resp;
    resp.app_id = _gpid.get_app_id();
    resp.partition_index = _gpid.get_partition_index();
    resp.server = _primary_address;

    if (!is_filter_type_supported(request.sort_key_filter_type)) {
        derror("%s: filter type %d not supported for multi_get",
               replica_name(),
               request.sort_key_filter_type);
        resp.error = rocksdb::Status::kInvalidArgument;
        _pfc_multi_get_latency->set(dsn_now_ns() - start_time);
        reply(resp);
        return;
    }

    int32_t max_kv_count = request.max_kv_count > 0 ? request.max_kv_count : INT_MAX;
    int32_t max_kv_size = request.max_kv_size > 0 ? request.max_kv_size : INT_MAX;
    uint32_t epoch_now = ::pegasus::utils::epoch_now();
    int32_t count = 0;
    int64_t size = 0;
    int32_t iterate_count = 0;
    int32_t expire_count = 0;
    int32_t filter_count = 0;

    if (request.sort_keys.empty()) {
        ::dsn::blob range_start_key, range_stop_key;
        pegasus_generate_key(range_start_key, request.hash_key, request.start_sortkey);
        bool start_inclusive = request.start_inclusive;
        bool stop_inclusive;
        if (request.stop_sortkey.length() == 0) {
            pegasus_generate_next_blob(range_stop_key, request.hash_key);
            stop_inclusive = false;
        } else {
            pegasus_generate_key(range_stop_key, request.hash_key, request.stop_sortkey);
            stop_inclusive = request.stop_inclusive;
        }

        rocksdb::Slice start(range_start_key.data(), range_start_key.length());
        rocksdb::Slice stop(range_stop_key.data(), range_stop_key.length());

        // limit key range by prefix filter
        ::dsn::blob prefix_start_key, prefix_stop_key;
        if (request.sort_key_filter_type == ::dsn::apps::filter_type::FT_MATCH_PREFIX &&
            request.sort_key_filter_pattern.length() > 0) {
            pegasus_generate_key(
                prefix_start_key, request.hash_key, request.sort_key_filter_pattern);
            pegasus_generate_next_blob(
                prefix_stop_key, request.hash_key, request.sort_key_filter_pattern);

            rocksdb::Slice prefix_start(prefix_start_key.data(), prefix_start_key.length());
            if (prefix_start.compare(start) > 0) {
                start = prefix_start;
                start_inclusive = true;
            }

            rocksdb::Slice prefix_stop(prefix_stop_key.data(), prefix_stop_key.length());
            if (prefix_stop.compare(stop) <= 0) {
                stop = prefix_stop;
                stop_inclusive = false;
            }
        }

        // check if range is empty
        int c = start.compare(stop);
        if (c > 0 || (c == 0 && (!start_inclusive || !stop_inclusive))) {
            // empty sort key range
            if (_verbose_log) {
                dwarn("%s: empty sort key range for multi_get: hash_key = \"%s\", "
                      "start_sort_key = \"%s\" (%s), stop_sort_key = \"%s\" (%s), "
                      "sort_key_filter_type = %s, sort_key_filter_pattern = \"%s\", "
                      "final_start = \"%s\" (%s), final_stop = \"%s\" (%s)",
                      replica_name(),
                      ::pegasus::utils::c_escape_string(request.hash_key).c_str(),
                      ::pegasus::utils::c_escape_string(request.start_sortkey).c_str(),
                      request.start_inclusive ? "inclusive" : "exclusive",
                      ::pegasus::utils::c_escape_string(request.stop_sortkey).c_str(),
                      request.stop_inclusive ? "inclusive" : "exclusive",
                      ::dsn::apps::_filter_type_VALUES_TO_NAMES.find(request.sort_key_filter_type)
                          ->second,
                      ::pegasus::utils::c_escape_string(request.sort_key_filter_pattern).c_str(),
                      ::pegasus::utils::c_escape_string(start).c_str(),
                      start_inclusive ? "inclusive" : "exclusive",
                      ::pegasus::utils::c_escape_string(stop).c_str(),
                      stop_inclusive ? "inclusive" : "exclusive");
            }
            resp.error = rocksdb::Status::kOk;
            _pfc_multi_get_latency->set(dsn_now_ns() - start_time);
            reply(resp);
            return;
        }

        std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(_rd_opts));
        bool complete = false;
        if (!request.reverse) {
            it->Seek(start);
            bool first_exclusive = !start_inclusive;
            while (count < max_kv_count && size < max_kv_size && it->Valid()) {
                iterate_count++;

                // check stop sort key
                int c = it->key().compare(stop);
                if (c > 0 || (c == 0 && !stop_inclusive)) {
                    // out of range
                    complete = true;
                    break;
                }

                // check start sort key
                if (first_exclusive) {
                    first_exclusive = false;
                    if (it->key().compare(start) == 0) {
                        // discard start_sortkey
                        it->Next();
                        continue;
                    }
                }

                // extract value
                int r = append_key_value_for_multi_get(resp.kvs,
                                                       it->key(),
                                                       it->value(),
                                                       request.sort_key_filter_type,
                                                       request.sort_key_filter_pattern,
                                                       epoch_now,
                                                       request.no_value);
                if (r == 1) {
                    count++;
                    auto &kv = resp.kvs.back();
                    size += kv.key.length() + kv.value.length();
                } else if (r == 2) {
                    expire_count++;
                } else { // r == 3
                    filter_count++;
                }

                if (c == 0) {
                    // if arrived to the last position
                    complete = true;
                    break;
                }

                it->Next();
            }
        } else { // reverse
            it->SeekForPrev(stop);
            bool first_exclusive = !stop_inclusive;
            std::vector<::dsn::apps::key_value> reverse_kvs;
            while (count < max_kv_count && size < max_kv_size && it->Valid()) {
                iterate_count++;

                // check start sort key
                int c = it->key().compare(start);
                if (c < 0 || (c == 0 && !start_inclusive)) {
                    // out of range
                    complete = true;
                    break;
                }

                // check stop sort key
                if (first_exclusive) {
                    first_exclusive = false;
                    if (it->key().compare(stop) == 0) {
                        // discard stop_sortkey
                        it->Prev();
                        continue;
                    }
                }

                // extract value
                int r = append_key_value_for_multi_get(reverse_kvs,
                                                       it->key(),
                                                       it->value(),
                                                       request.sort_key_filter_type,
                                                       request.sort_key_filter_pattern,
                                                       epoch_now,
                                                       request.no_value);
                if (r == 1) {
                    count++;
                    auto &kv = reverse_kvs.back();
                    size += kv.key.length() + kv.value.length();
                } else if (r == 2) {
                    expire_count++;
                } else { // r == 3
                    filter_count++;
                }

                if (c == 0) {
                    // if arrived to the last position
                    complete = true;
                    break;
                }

                it->Prev();
            }

            if (it->status().ok() && !reverse_kvs.empty()) {
                // revert order to make resp.kvs ordered in sort_key
                resp.kvs.reserve(reverse_kvs.size());
                for (int i = reverse_kvs.size() - 1; i >= 0; i--) {
                    resp.kvs.emplace_back(std::move(reverse_kvs[i]));
                }
            }
        }

        resp.error = it->status().code();
        if (!it->status().ok()) {
            // error occur
            if (_verbose_log) {
                derror("%s: rocksdb scan failed for multi_get: hash_key = \"%s\", "
                       "reverse = %s, error = %s",
                       replica_name(),
                       ::pegasus::utils::c_escape_string(request.hash_key).c_str(),
                       request.reverse ? "true" : "false",
                       it->status().ToString().c_str());
            } else {
                derror("%s: rocksdb scan failed for multi_get: reverse = %s, error = %s",
                       replica_name(),
                       request.reverse ? "true" : "false",
                       it->status().ToString().c_str());
            }
            resp.kvs.clear();
        } else if (it->Valid() && !complete) {
            // scan not completed
            resp.error = rocksdb::Status::kIncomplete;
        }
    } else {
        bool error_occurred = false;
        rocksdb::Status final_status;
        bool exceed_limit = false;
        std::vector<::dsn::blob> keys_holder;
        std::vector<rocksdb::Slice> keys;
        std::vector<std::string> values;
        keys_holder.reserve(request.sort_keys.size());
        keys.reserve(request.sort_keys.size());
        for (auto &sort_key : request.sort_keys) {
            ::dsn::blob raw_key;
            pegasus_generate_key(raw_key, request.hash_key, sort_key);
            keys.emplace_back(raw_key.data(), raw_key.length());
            keys_holder.emplace_back(std::move(raw_key));
        }

        std::vector<rocksdb::Status> statuses = _db->MultiGet(_rd_opts, keys, &values);
        for (int i = 0; i < keys.size(); i++) {
            rocksdb::Status& status = statuses[i];
            std::string& value = values[i];
            // print log
            if (!status.ok()) {
                if (_verbose_log) {
                    derror("%s: rocksdb get failed for multi_get: "
                           "hash_key = \"%s\", sort_key = \"%s\", error = %s",
                           replica_name(),
                           ::pegasus::utils::c_escape_string(request.hash_key).c_str(),
                           ::pegasus::utils::c_escape_string(request.sort_keys[i]).c_str(),
                           status.ToString().c_str());
                } else if (!status.IsNotFound()) {
                    derror("%s: rocksdb get failed for multi_get: error = %s",
                           replica_name(),
                           status.ToString().c_str());
                }
            }
            // check ttl
            if (status.ok()) {
                uint32_t expire_ts = pegasus_extract_expire_ts(_value_schema_version, value);
                if (expire_ts > 0 && expire_ts <= epoch_now) {
                    expire_count++;
                    if (_verbose_log) {
                        derror("%s: rocksdb data expired for multi_get", replica_name());
                    }
                    status = rocksdb::Status::NotFound();
                }
            }
            // extract value
            if (status.ok()) {
                // check if exceed limit
                if (count >= max_kv_count || size >= max_kv_size) {
                    exceed_limit = true;
                    break;
                }
                ::dsn::apps::key_value kv;
                kv.key = request.sort_keys[i];
                if (!request.no_value) {
                    pegasus_extract_user_data(_value_schema_version,
                                              ::dsn::make_unique<std::string>(std::move(value)),
                                              kv.value);
                }
                count++;
                size += kv.key.length() + kv.value.length();
                resp.kvs.emplace_back(std::move(kv));
            }
            // if error occurred
            if (!status.ok() && !status.IsNotFound()) {
                error_occurred = true;
                final_status = status;
                break;
            }
        }

        if (error_occurred) {
            resp.error = final_status.code();
            resp.kvs.clear();
        } else if (exceed_limit) {
            resp.error = rocksdb::Status::kIncomplete;
        } else {
            resp.error = rocksdb::Status::kOk;
        }
    }

    if (_abnormal_multi_get_time_threshold_ns || _abnormal_multi_get_size_threshold ||
        _abnormal_multi_get_iterate_count_threshold) {
        uint64_t time_used = dsn_now_ns() - start_time;
        if ((_abnormal_multi_get_time_threshold_ns &&
             time_used >= _abnormal_multi_get_time_threshold_ns) ||
            (_abnormal_multi_get_size_threshold &&
             (uint64_t)size >= _abnormal_multi_get_size_threshold) ||
            (_abnormal_multi_get_iterate_count_threshold &&
             (uint64_t)iterate_count >= _abnormal_multi_get_iterate_count_threshold)) {
            dwarn("%s: rocksdb abnormal multi_get: hash_key = \"%s\", "
                  "result_count = %d, result_size = %" PRId64 ", iterate_count = %d, "
                  "expire_count = %d, filter_count = %d, time_used = %" PRIu64 " ns",
                  replica_name(),
                  ::pegasus::utils::c_escape_string(request.hash_key).c_str(),
                  count,
                  size,
                  iterate_count,
                  expire_count,
                  filter_count,
                  time_used);
            _pfc_recent_abnormal_count->increment();
        }
    }

    if (expire_count > 0) {
        _pfc_recent_expire_count->add(expire_count);
    }
    if (filter_count > 0) {
        _pfc_recent_filter_count->add(filter_count);
    }
    _pfc_multi_get_latency->set(dsn_now_ns() - start_time);

    reply(resp);
}

void pegasus_server_impl::on_sortkey_count(const ::dsn::blob &hash_key,
                                           ::dsn::rpc_replier<::dsn::apps::count_response> &reply)
{
    dassert(_is_open, "");

    ::dsn::apps::count_response resp;
    resp.app_id = _gpid.get_app_id();
    resp.partition_index = _gpid.get_partition_index();
    resp.server = _primary_address;

    // scan
    ::dsn::blob start_key, stop_key;
    pegasus_generate_key(start_key, hash_key, ::dsn::blob());
    pegasus_generate_next_blob(stop_key, hash_key);
    rocksdb::Slice start(start_key.data(), start_key.length());
    rocksdb::Slice stop(stop_key.data(), stop_key.length());
    rocksdb::ReadOptions options = _rd_opts;
    options.iterate_upper_bound = &stop;
    std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(options));
    it->Seek(start);
    resp.count = 0;
    uint32_t epoch_now = ::pegasus::utils::epoch_now();
    uint64_t expire_count = 0;
    while (it->Valid()) {
        uint32_t expire_ts = pegasus_extract_expire_ts(_value_schema_version, it->value());
        if (expire_ts > 0 && expire_ts <= epoch_now) {
            expire_count++;
            if (_verbose_log) {
                derror("%s: rocksdb data expired for sortkey_count", replica_name());
            }
        } else {
            resp.count++;
        }
        it->Next();
    }
    if (expire_count > 0) {
        _pfc_recent_expire_count->add(expire_count);
    }

    resp.error = it->status().code();
    if (!it->status().ok()) {
        // error occur
        if (_verbose_log) {
            derror("%s: rocksdb scan failed for sortkey_count: hash_key = \"%s\", error = %s",
                   replica_name(),
                   ::pegasus::utils::c_escape_string(hash_key).c_str(),
                   it->status().ToString().c_str());
        } else {
            derror("%s: rocksdb scan failed for sortkey_count: error = %s",
                   replica_name(),
                   it->status().ToString().c_str());
        }
        resp.count = 0;
    }

    reply(resp);
}

void pegasus_server_impl::on_ttl(const ::dsn::blob &key,
                                 ::dsn::rpc_replier<::dsn::apps::ttl_response> &reply)
{
    dassert(_is_open, "");

    ::dsn::apps::ttl_response resp;
    resp.app_id = _gpid.get_app_id();
    resp.partition_index = _gpid.get_partition_index();
    resp.server = _primary_address;

    rocksdb::Slice skey(key.data(), key.length());
    std::string value;
    rocksdb::Status status = _db->Get(_rd_opts, skey, &value);

    uint32_t expire_ts;
    uint32_t now_ts = ::pegasus::utils::epoch_now();
    if (status.ok()) {
        expire_ts = pegasus_extract_expire_ts(_value_schema_version, value);
        if (expire_ts > 0 && expire_ts <= now_ts) {
            _pfc_recent_expire_count->increment();
            if (_verbose_log) {
                derror("%s: rocksdb data expired for ttl", replica_name());
            }
            status = rocksdb::Status::NotFound();
        }
    }

    if (!status.ok()) {
        if (_verbose_log) {
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(key, hash_key, sort_key);
            derror("%s: rocksdb get failed for ttl: "
                   "hash_key = \"%s\", sort_key = \"%s\", error = %s",
                   replica_name(),
                   ::pegasus::utils::c_escape_string(hash_key).c_str(),
                   ::pegasus::utils::c_escape_string(sort_key).c_str(),
                   status.ToString().c_str());
        } else if (!status.IsNotFound()) {
            derror("%s: rocksdb get failed for ttl: error = %s",
                   replica_name(),
                   status.ToString().c_str());
        }
    }

    resp.error = status.code();
    if (status.ok()) {
        if (expire_ts > 0) {
            resp.ttl_seconds = expire_ts - now_ts;
        } else {
            // no ttl
            resp.ttl_seconds = -1;
        }
    }

    reply(resp);
}

DEFINE_TASK_CODE(LOCAL_PEGASUS_SERVER_DELAY, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)
void pegasus_server_impl::on_get_scanner(const ::dsn::apps::get_scanner_request &request,
                                         ::dsn::rpc_replier<::dsn::apps::scan_response> &reply)
{
    dassert(_is_open, "");
    _pfc_scan_qps->increment();
    uint64_t start_time = dsn_now_ns();

    ::dsn::apps::scan_response resp;
    resp.app_id = _gpid.get_app_id();
    resp.partition_index = _gpid.get_partition_index();
    resp.server = _primary_address;

    if (!is_filter_type_supported(request.hash_key_filter_type)) {
        derror("%s: filter type %d not supported for get_scanner",
               replica_name(),
               request.hash_key_filter_type);
        resp.error = rocksdb::Status::kInvalidArgument;
        reply(resp);
        return;
    }
    if (!is_filter_type_supported(request.sort_key_filter_type)) {
        derror("%s: filter type %d not supported for get_scanner",
               replica_name(),
               request.sort_key_filter_type);
        resp.error = rocksdb::Status::kInvalidArgument;
        reply(resp);
        return;
    }

    bool start_inclusive = request.start_inclusive;
    bool stop_inclusive = request.stop_inclusive;
    rocksdb::Slice start(request.start_key.data(), request.start_key.length());
    rocksdb::Slice stop(request.stop_key.data(), request.stop_key.length());

    // limit key range by prefix filter
    // because data is not ordered by hash key (hash key "aa" is greater than "b"),
    // so we can only limit the start range by hash key filter.
    ::dsn::blob prefix_start_key;
    if (request.hash_key_filter_type == ::dsn::apps::filter_type::FT_MATCH_PREFIX &&
        request.hash_key_filter_pattern.length() > 0) {
        pegasus_generate_key(prefix_start_key, request.hash_key_filter_pattern, ::dsn::blob());
        rocksdb::Slice prefix_start(prefix_start_key.data(), prefix_start_key.length());
        if (prefix_start.compare(start) > 0) {
            start = prefix_start;
            start_inclusive = true;
        }
    }

    // check if range is empty
    int c = start.compare(stop);
    if (c > 0 || (c == 0 && (!start_inclusive || !stop_inclusive))) {
        // empty key range
        if (_verbose_log) {
            dwarn("%s: empty key range for get_scanner: "
                  "start_key = \"%s\" (%s), stop_key = \"%s\" (%s)",
                  replica_name(),
                  ::pegasus::utils::c_escape_string(request.start_key).c_str(),
                  request.start_inclusive ? "inclusive" : "exclusive",
                  ::pegasus::utils::c_escape_string(request.stop_key).c_str(),
                  request.stop_inclusive ? "inclusive" : "exclusive");
        }
        resp.error = rocksdb::Status::kOk;
        _pfc_multi_get_latency->set(dsn_now_ns() - start_time);
        reply(resp);
        return;
    }

    std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(_rd_opts));
    it->Seek(start);
    bool complete = false;
    bool first_exclusive = !start_inclusive;
    uint32_t epoch_now = ::pegasus::utils::epoch_now();
    uint64_t expire_count = 0;
    uint64_t filter_count = 0;
    int32_t count = 0;
    resp.kvs.reserve(request.batch_size);
    while (count < request.batch_size && it->Valid()) {
        int c = it->key().compare(stop);
        if (c > 0 || (c == 0 && !stop_inclusive)) {
            // out of range
            complete = true;
            break;
        }

        if (first_exclusive) {
            first_exclusive = false;
            if (it->key().compare(start) == 0) {
                // discard start_sortkey
                it->Next();
                continue;
            }
        }

        int r = append_key_value_for_scan(resp.kvs,
                                          it->key(),
                                          it->value(),
                                          request.hash_key_filter_type,
                                          request.hash_key_filter_pattern,
                                          request.sort_key_filter_type,
                                          request.sort_key_filter_pattern,
                                          epoch_now,
                                          request.no_value);
        if (r == 1) {
            count++;
        } else if (r == 2) {
            expire_count++;
        } else { // r == 3
            filter_count++;
        }

        if (c == 0) {
            // seek to the last position
            complete = true;
            break;
        }

        it->Next();
    }

    resp.error = it->status().code();
    if (!it->status().ok()) {
        // error occur
        if (_verbose_log) {
            derror("%s: rocksdb scan failed for get_scanner: "
                   "start_key = \"%s\" (%s), stop_key = \"%s\" (%s), "
                   "batch_size = %d, read_count = %d, error = %s",
                   replica_name(),
                   ::pegasus::utils::c_escape_string(start).c_str(),
                   request.start_inclusive ? "inclusive" : "exclusive",
                   ::pegasus::utils::c_escape_string(stop).c_str(),
                   request.stop_inclusive ? "inclusive" : "exclusive",
                   request.batch_size,
                   count,
                   it->status().ToString().c_str());
        } else {
            derror("%s: rocksdb scan failed for get_scanner: error = %s",
                   replica_name(),
                   it->status().ToString().c_str());
        }
        resp.kvs.clear();
    } else if (it->Valid() && !complete) {
        // scan not completed
        std::unique_ptr<pegasus_scan_context> context(
            new pegasus_scan_context(std::move(it),
                                     std::string(stop.data(), stop.size()),
                                     request.stop_inclusive,
                                     request.hash_key_filter_type,
                                     std::string(request.hash_key_filter_pattern.data(),
                                                 request.hash_key_filter_pattern.length()),
                                     request.sort_key_filter_type,
                                     std::string(request.sort_key_filter_pattern.data(),
                                                 request.sort_key_filter_pattern.length()),
                                     request.batch_size,
                                     request.no_value));
        int64_t handle = _context_cache.put(std::move(context));
        resp.context_id = handle;
        // if the context is used, it will be fetched and re-put into cache,
        // which will change the handle,
        // then the delayed task will fetch null context by old handle, and do nothing.
        ::dsn::tasking::enqueue(LOCAL_PEGASUS_SERVER_DELAY,
                                this,
                                [this, handle]() { _context_cache.fetch(handle); },
                                0,
                                std::chrono::minutes(5));
    } else {
        // scan completed
        resp.context_id = pegasus::SCAN_CONTEXT_ID_COMPLETED;
    }

    if (expire_count > 0) {
        _pfc_recent_expire_count->add(expire_count);
    }
    if (filter_count > 0) {
        _pfc_recent_filter_count->add(filter_count);
    }

    _pfc_scan_latency->set(dsn_now_ns() - start_time);
    reply(resp);
}

void pegasus_server_impl::on_scan(const ::dsn::apps::scan_request &request,
                                  ::dsn::rpc_replier<::dsn::apps::scan_response> &reply)
{
    dassert(_is_open, "");
    _pfc_scan_qps->increment();
    uint64_t start_time = dsn_now_ns();

    ::dsn::apps::scan_response resp;
    resp.app_id = _gpid.get_app_id();
    resp.partition_index = _gpid.get_partition_index();
    resp.server = _primary_address;

    std::unique_ptr<pegasus_scan_context> context = _context_cache.fetch(request.context_id);
    if (context) {
        rocksdb::Iterator *it = context->iterator.get();
        int32_t batch_size = context->batch_size;
        const rocksdb::Slice &stop = context->stop;
        bool stop_inclusive = context->stop_inclusive;
        ::dsn::apps::filter_type::type hash_key_filter_type = context->hash_key_filter_type;
        const ::dsn::blob &hash_key_filter_pattern = context->hash_key_filter_pattern;
        ::dsn::apps::filter_type::type sort_key_filter_type = context->hash_key_filter_type;
        const ::dsn::blob &sort_key_filter_pattern = context->hash_key_filter_pattern;
        bool no_value = context->no_value;
        bool complete = false;
        uint32_t epoch_now = ::pegasus::utils::epoch_now();
        uint64_t expire_count = 0;
        uint64_t filter_count = 0;
        int32_t count = 0;

        while (count < batch_size && it->Valid()) {
            int c = it->key().compare(stop);
            if (c > 0 || (c == 0 && !stop_inclusive)) {
                // out of range
                complete = true;
                break;
            }

            int r = append_key_value_for_scan(resp.kvs,
                                              it->key(),
                                              it->value(),
                                              hash_key_filter_type,
                                              hash_key_filter_pattern,
                                              sort_key_filter_type,
                                              sort_key_filter_pattern,
                                              epoch_now,
                                              no_value);
            if (r == 1) {
                count++;
            } else if (r == 2) {
                expire_count++;
            } else { // r == 3
                filter_count++;
            }

            if (c == 0) {
                // seek to the last position
                complete = true;
                break;
            }

            it->Next();
        }

        resp.error = it->status().code();
        if (!it->status().ok()) {
            // error occur
            if (_verbose_log) {
                derror("%s: rocksdb scan failed for scan: "
                       "context_id= %" PRId64 ", stop_key = \"%s\" (%s), "
                       "batch_size = %d, read_count = %d, error = %s",
                       replica_name(),
                       request.context_id,
                       ::pegasus::utils::c_escape_string(stop).c_str(),
                       stop_inclusive ? "inclusive" : "exclusive",
                       batch_size,
                       count,
                       it->status().ToString().c_str());
            } else {
                derror("%s: rocksdb scan failed for scan: error = %s",
                       replica_name(),
                       it->status().ToString().c_str());
            }
            resp.kvs.clear();
        } else if (it->Valid() && !complete) {
            // scan not completed
            int64_t handle = _context_cache.put(std::move(context));
            resp.context_id = handle;
            ::dsn::tasking::enqueue(LOCAL_PEGASUS_SERVER_DELAY,
                                    this,
                                    [this, handle]() { _context_cache.fetch(handle); },
                                    0,
                                    std::chrono::minutes(5));
        } else {
            // scan completed
            resp.context_id = pegasus::SCAN_CONTEXT_ID_COMPLETED;
        }

        if (expire_count > 0) {
            _pfc_recent_expire_count->add(expire_count);
        }
        if (filter_count > 0) {
            _pfc_recent_filter_count->add(filter_count);
        }
    } else {
        resp.error = rocksdb::Status::Code::kNotFound;
    }

    _pfc_scan_latency->set(dsn_now_ns() - start_time);
    reply(resp);
}

void pegasus_server_impl::on_clear_scanner(const int64_t &args) { _context_cache.fetch(args); }

DEFINE_TASK_CODE(UPDATING_ROCKSDB_SSTSIZE, TASK_PRIORITY_COMMON, THREAD_POOL_REPLICATION_LONG)

::dsn::error_code pegasus_server_impl::start(int argc, char **argv)
{
    dassert(!_is_open, "");
    ddebug("%s: start to open app %s", replica_name(), data_dir().c_str());

    rocksdb::Options opts = _db_opts;
    opts.create_if_missing = true;
    opts.error_if_exists = false;
    opts.compaction_filter = &_key_ttl_compaction_filter;
    opts.default_value_schema_version = PEGASUS_VALUE_SCHEMA_MAX_VERSION;

    auto path = ::dsn::utils::filesystem::path_combine(data_dir(), "rdb");

    //
    // here, we must distinguish three cases, such as:
    //  case 1: we open the db that already exist
    //  case 2: we open a new db
    //  case 3: we restore the db base on old data
    //
    // if we want to restore the db base on old data, only all of the restore preconditions are
    // satisfied
    //      restore preconditions:
    //          1, rdb isn't exist
    //          2, we can parse restore info from app env, which is stored in argv
    //          3, restore_dir is exist
    //
    if (::dsn::utils::filesystem::path_exists(path)) {
        // only case 1
        ddebug("%s: rdb is already exist, path = %s", replica_name(), path.c_str());
    } else {
        std::pair<std::string, bool> restore_info = get_restore_dir_from_env(argc, argv);
        const std::string &restore_dir = restore_info.first;
        bool force_restore = restore_info.second;
        if (restore_dir.empty()) {
            // case 2
            if (force_restore) {
                derror("%s: try to restore, but we can't combine restore_dir from envs",
                       replica_name());
                return ::dsn::ERR_FILE_OPERATION_FAILED;
            } else {
                dinfo("%s: open a new db, path = %s", replica_name(), path.c_str());
            }
        } else {
            // case 3
            ddebug("%s: try to restore from restore_dir = %s", replica_name(), restore_dir.c_str());
            if (::dsn::utils::filesystem::directory_exists(restore_dir)) {
                // here, we just rename restore_dir to rdb, then continue the normal process
                if (::dsn::utils::filesystem::rename_path(restore_dir.c_str(), path.c_str())) {
                    ddebug("%s: rename restore_dir(%s) to rdb(%s) succeed",
                           replica_name(),
                           restore_dir.c_str(),
                           path.c_str());
                } else {
                    derror("%s: rename restore_dir(%s) to rdb(%s) failed",
                           replica_name(),
                           restore_dir.c_str(),
                           path.c_str());
                    return ::dsn::ERR_FILE_OPERATION_FAILED;
                }
            } else {
                if (force_restore) {
                    derror("%s: try to restore, but restore_dir isn't exist, restore_dir = %s",
                           replica_name(),
                           restore_dir.c_str());
                    return ::dsn::ERR_FILE_OPERATION_FAILED;
                } else {
                    dwarn(
                        "%s: try to restore and restore_dir(%s) isn't exist, but we don't force "
                        "it, the role of this replica must not primary, so we open a new db on the "
                        "path(%s)",
                        replica_name(),
                        restore_dir.c_str(),
                        path.c_str());
                }
            }
        }
    }

    ddebug("%s: start to open rocksDB's rdb(%s)", replica_name(), path.c_str());

    auto status = rocksdb::DB::Open(opts, path, &_db);
    if (status.ok()) {
        _value_schema_version = _db->GetValueSchemaVersion();
        if (_value_schema_version > PEGASUS_VALUE_SCHEMA_MAX_VERSION) {
            derror("%s: open app failed, unsupported value schema version %" PRIu32,
                   replica_name(),
                   _value_schema_version);
            delete _db;
            _db = nullptr;
            return ::dsn::ERR_LOCAL_APP_FAILURE;
        }

        // only enable filter after correct value_schema_version set
        _key_ttl_compaction_filter.SetValueSchemaVersion(_value_schema_version);
        _key_ttl_compaction_filter.EnableFilter();

        parse_checkpoints();

        int64_t ci = _db->GetLastFlushedDecree();
        if (ci != last_durable_decree()) {
            ddebug("%s: start to do async checkpoint, last_durable_decree = %" PRId64
                   ", last_flushed_decree = %" PRId64,
                   replica_name(),
                   last_durable_decree(),
                   ci);
            auto err = async_checkpoint(false);
            if (err != ::dsn::ERR_OK) {
                dwarn("%s: create checkpoint failed, error = %s, retry again",
                      replica_name(),
                      err.to_string());
                err = async_checkpoint(false);
                if (err != ::dsn::ERR_OK) {
                    derror("%s: create checkpoint failed, error = %s",
                           replica_name(),
                           err.to_string());
                    delete _db;
                    _db = nullptr;
                    return err;
                }
            }
            dassert(ci == last_durable_decree(),
                    "last durable decree mismatch after checkpoint: %" PRId64 " vs %" PRId64,
                    ci,
                    last_durable_decree());
        }

        ddebug("%s: open app succeed, value_schema_version = %" PRIu32
               ", last_durable_decree = %" PRId64 "",
               replica_name(),
               _value_schema_version,
               last_durable_decree());

        _is_open = true;

        dinfo("%s: start the updating sstsize timer task", replica_name());
        // using ::dsn::timer_task to updating the rocksdb sstsize.
        _updating_task = ::dsn::tasking::enqueue_timer(
            UPDATING_ROCKSDB_SSTSIZE,
            this,
            [this]() { this->updating_rocksdb_sstsize(); },
            std::chrono::seconds(_updating_rocksdb_sstsize_interval_seconds),
            0,
            std::chrono::seconds(30));

        return ::dsn::ERR_OK;
    } else {
        derror("%s: open app failed, error = %s", replica_name(), status.ToString().c_str());
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }
}

::dsn::error_code pegasus_server_impl::stop(bool clear_state)
{
    if (!_is_open) {
        dassert(_db == nullptr, "");
        dassert(!clear_state, "should not be here if do clear");
        return ::dsn::ERR_OK;
    }

    if (!clear_state) {
        rocksdb::FlushOptions options;
        options.wait = true;
        auto status = _db->Flush(options);
        if (!status.ok() && !status.IsNoNeedOperate()) {
            derror("%s: flush memtable on close failed: %s",
                   replica_name(),
                   status.ToString().c_str());
        }
    }

    _context_cache.clear();

    // when stop the, should stop the timer_task.
    if (_updating_task != nullptr)
        _updating_task->cancel(true);

    _is_open = false;
    delete _db;
    _db = nullptr;

    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);
        _checkpoints.clear();
        set_last_durable_decree(0);
    }

    if (clear_state) {
        if (!::dsn::utils::filesystem::remove_path(data_dir())) {
            derror(
                "%s: clear directory %s failed when stop app", replica_name(), data_dir().c_str());
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
        _pfc_sst_count->set(0);
        _pfc_sst_size->set(0);
    }

    ddebug(
        "%s: close app succeed, clear_state = %s", replica_name(), clear_state ? "true" : "false");
    return ::dsn::ERR_OK;
}

class CheckpointingTokenHelper
{
public:
    CheckpointingTokenHelper(std::atomic_bool &flag) : _flag(flag)
    {
        bool expected = false;
        _token_got = _flag.compare_exchange_strong(expected, true);
    }
    ~CheckpointingTokenHelper()
    {
        if (_token_got)
            _flag.store(false);
    }
    bool token_got() const { return _token_got; }
private:
    std::atomic_bool &_flag;
    bool _token_got;
};

::dsn::error_code pegasus_server_impl::sync_checkpoint()
{
    CheckpointingTokenHelper token_helper(_is_checkpointing);
    if (!token_helper.token_got())
        return ::dsn::ERR_WRONG_TIMING;

    int64_t last_commit = last_committed_decree();
    if (last_durable_decree() == last_commit)
        return ::dsn::ERR_NO_NEED_OPERATE;

    rocksdb::Checkpoint *chkpt = nullptr;
    auto status = rocksdb::Checkpoint::Create(_db, &chkpt);
    if (!status.ok()) {
        derror("%s: create Checkpoint object failed, error = %s",
               replica_name(),
               status.ToString().c_str());
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }

    auto dir = chkpt_get_dir_name(last_commit);
    auto chkpt_dir = ::dsn::utils::filesystem::path_combine(data_dir(), dir);
    if (::dsn::utils::filesystem::directory_exists(chkpt_dir)) {
        ddebug("%s: checkpoint directory %s already exist, remove it first",
               replica_name(),
               chkpt_dir.c_str());
        if (!::dsn::utils::filesystem::remove_path(chkpt_dir)) {
            derror(
                "%s: remove old checkpoint directory %s failed", replica_name(), chkpt_dir.c_str());
            delete chkpt;
            chkpt = nullptr;
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
    }

    status = chkpt->CreateCheckpoint(chkpt_dir);
    if (!status.ok()) {
        // sometimes checkpoint may fail, and try again will succeed
        derror("%s: create checkpoint failed, error = %s, try again",
               replica_name(),
               status.ToString().c_str());
        status = chkpt->CreateCheckpoint(chkpt_dir);
    }

    // destroy Checkpoint object
    delete chkpt;
    chkpt = nullptr;

    if (!status.ok()) {
        derror(
            "%s: create checkpoint failed, error = %s", replica_name(), status.ToString().c_str());
        ::dsn::utils::filesystem::remove_path(chkpt_dir);
        if (!::dsn::utils::filesystem::remove_path(chkpt_dir)) {
            derror("%s: remove damaged checkpoint directory %s failed",
                   replica_name(),
                   chkpt_dir.c_str());
        }
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }

    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);
        dassert(last_commit > last_durable_decree(),
                "%" PRId64 " VS %" PRId64 "",
                last_commit,
                last_durable_decree());
        if (!_checkpoints.empty()) {
            dassert(last_commit > _checkpoints.back(),
                    "%" PRId64 " VS %" PRId64 "",
                    last_commit,
                    _checkpoints.back());
        }
        _checkpoints.push_back(last_commit);
        set_last_durable_decree(_checkpoints.back());
    }

    ddebug("%s: sync create checkpoint succeed, last_durable_decree = %" PRId64 "",
           replica_name(),
           last_durable_decree());

    gc_checkpoints();

    return ::dsn::ERR_OK;
}

// Must be thread safe.
::dsn::error_code pegasus_server_impl::async_checkpoint(bool is_emergency)
{
    CheckpointingTokenHelper token_helper(_is_checkpointing);
    if (!token_helper.token_got())
        return ::dsn::ERR_WRONG_TIMING;

    // last_durable_decree must less than or equal to _db->GetLastFlushedDecree()
    // if last_durable_decree == _db->GetLastFlushedDecree(), we should flush it first
    int64_t last_flushed_decree = static_cast<int64_t>(_db->GetLastFlushedDecree());
    if (last_durable_decree() == last_flushed_decree) {
        if (is_emergency) {
            // trigger flushing memtable, but not wait
            rocksdb::FlushOptions options;
            options.wait = false;
            auto status = _db->Flush(options);
            if (status.ok()) {
                ddebug("%s: trigger flushing memtable succeed", replica_name());
                return ::dsn::ERR_TRY_AGAIN;
            } else if (status.IsNoNeedOperate()) {
                dwarn("%s: trigger flushing memtable failed, no memtable to flush", replica_name());
                return ::dsn::ERR_NO_NEED_OPERATE;
            } else {
                derror("%s: trigger flushing memtable failed, error = %s",
                       replica_name(),
                       status.ToString().c_str());
                return ::dsn::ERR_LOCAL_APP_FAILURE;
            }
        } else {
            return ::dsn::ERR_NO_NEED_OPERATE;
        }
    }

    dassert(last_durable_decree() < last_flushed_decree,
            "%" PRId64 " VS %" PRId64 "",
            last_durable_decree(),
            last_flushed_decree);

    char buf[256];
    sprintf(buf, "checkpoint.tmp.%" PRIu64 "", dsn_now_us());
    std::string tmp_dir = ::dsn::utils::filesystem::path_combine(data_dir(), buf);
    if (::dsn::utils::filesystem::directory_exists(tmp_dir)) {
        ddebug("%s: temporary checkpoint directory %s already exist, remove it first",
               replica_name(),
               tmp_dir.c_str());
        if (!::dsn::utils::filesystem::remove_path(tmp_dir)) {
            derror("%s: remove temporary checkpoint directory %s failed",
                   replica_name(),
                   tmp_dir.c_str());
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
    }

    int64_t checkpoint_decree = 0;
    ::dsn::error_code err = copy_checkpoint_to_dir_unsafe(tmp_dir.c_str(), &checkpoint_decree);
    if (err != ::dsn::ERR_OK) {
        derror("%s: call copy_checkpoint_to_dir_unsafe failed with err = %s",
               replica_name(),
               err.to_string());
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }

    auto chkpt_dir =
        ::dsn::utils::filesystem::path_combine(data_dir(), chkpt_get_dir_name(checkpoint_decree));
    if (::dsn::utils::filesystem::directory_exists(chkpt_dir)) {
        ddebug("%s: checkpoint directory %s already exist, remove it first",
               replica_name(),
               chkpt_dir.c_str());
        if (!::dsn::utils::filesystem::remove_path(chkpt_dir)) {
            derror(
                "%s: remove old checkpoint directory %s failed", replica_name(), chkpt_dir.c_str());
            if (!::dsn::utils::filesystem::remove_path(tmp_dir)) {
                derror("%s: remove temporary checkpoint directory %s failed",
                       replica_name(),
                       tmp_dir.c_str());
            }
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
    }

    if (!::dsn::utils::filesystem::rename_path(tmp_dir, chkpt_dir)) {
        derror("%s: rename checkpoint directory from %s to %s failed",
               replica_name(),
               tmp_dir.c_str(),
               chkpt_dir.c_str());
        if (!::dsn::utils::filesystem::remove_path(tmp_dir)) {
            derror("%s: remove temporary checkpoint directory %s failed",
                   replica_name(),
                   tmp_dir.c_str());
        }
        return ::dsn::ERR_FILE_OPERATION_FAILED;
    }

    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);
        dassert(checkpoint_decree > last_durable_decree(),
                "%" PRId64 " VS %" PRId64 "",
                checkpoint_decree,
                last_durable_decree());
        if (!_checkpoints.empty()) {
            dassert(checkpoint_decree > _checkpoints.back(),
                    "%" PRId64 " VS %" PRId64 "",
                    checkpoint_decree,
                    _checkpoints.back());
        }
        _checkpoints.push_back(checkpoint_decree);
        set_last_durable_decree(_checkpoints.back());
    }

    ddebug("%s: async create checkpoint succeed, last_durable_decree = %" PRId64 "",
           replica_name(),
           last_durable_decree());

    gc_checkpoints();

    return ::dsn::ERR_OK;
}

// Must be thread safe.
::dsn::error_code pegasus_server_impl::copy_checkpoint_to_dir(const char *checkpoint_dir,
                                                              /*output*/ int64_t *last_decree)
{
    CheckpointingTokenHelper token_helper(_is_checkpointing);
    if (!token_helper.token_got()) {
        return ::dsn::ERR_WRONG_TIMING;
    }

    return copy_checkpoint_to_dir_unsafe(checkpoint_dir, last_decree);
}

// not thread safe, should be protected by caller
::dsn::error_code pegasus_server_impl::copy_checkpoint_to_dir_unsafe(const char *checkpoint_dir,
                                                                     int64_t *checkpoint_decree)
{
    rocksdb::Checkpoint *chkpt = nullptr;
    rocksdb::Status status = rocksdb::Checkpoint::Create(_db, &chkpt);
    if (!status.ok()) {
        if (chkpt != nullptr)
            delete chkpt, chkpt = nullptr;
        derror("%s: create Checkpoint object failed, error = %s",
               replica_name(),
               status.ToString().c_str());
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }

    if (::dsn::utils::filesystem::directory_exists(checkpoint_dir)) {
        ddebug("%s: checkpoint directory %s is already exist, remove it first",
               replica_name(),
               checkpoint_dir);
        if (!::dsn::utils::filesystem::remove_path(checkpoint_dir)) {
            derror("%s: remove checkpoint directory %s failed", replica_name(), checkpoint_dir);
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
    }

    uint64_t ci = 0;
    status = chkpt->CreateCheckpointQuick(checkpoint_dir, &ci);
    delete chkpt, chkpt = nullptr;

    if (!status.ok()) {
        derror("%s: async create checkpoint failed, error = %s",
               replica_name(),
               status.ToString().c_str());
        if (!::dsn::utils::filesystem::remove_path(checkpoint_dir)) {
            derror("%s: remove checkpoint directory %s failed", replica_name(), checkpoint_dir);
        }
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }

    ddebug("%s: copy checkpoint to dir(%s) succeed, last_decree = %" PRId64 "",
           replica_name(),
           checkpoint_dir,
           ci);
    if (checkpoint_decree != nullptr) {
        *checkpoint_decree = static_cast<int64_t>(ci);
    }

    return ::dsn::ERR_OK;
}

::dsn::error_code pegasus_server_impl::get_checkpoint(int64_t learn_start,
                                                      const dsn::blob &learn_request,
                                                      dsn::replication::learn_state &state)
{
    dassert(_is_open, "");

    int64_t ci = last_durable_decree();
    if (ci == 0) {
        derror("%s: no checkpoint found", replica_name());
        return ::dsn::ERR_OBJECT_NOT_FOUND;
    }

    auto chkpt_dir = ::dsn::utils::filesystem::path_combine(data_dir(), chkpt_get_dir_name(ci));
    state.files.clear();
    if (!::dsn::utils::filesystem::get_subfiles(chkpt_dir, state.files, true)) {
        derror("%s: list files in checkpoint dir %s failed", replica_name(), chkpt_dir.c_str());
        return ::dsn::ERR_FILE_OPERATION_FAILED;
    }

    state.from_decree_excluded = 0;
    state.to_decree_included = ci;

    ddebug("%s: get checkpoint succeed, from_decree_excluded = 0, to_decree_included = %" PRId64 "",
           replica_name(),
           state.to_decree_included);
    return ::dsn::ERR_OK;
}

::dsn::error_code
pegasus_server_impl::storage_apply_checkpoint(chkpt_apply_mode mode,
                                              const dsn::replication::learn_state &state)
{
    ::dsn::error_code err;
    int64_t ci = state.to_decree_included;

    if (mode == chkpt_apply_mode::copy) {
        dassert(ci > last_durable_decree(),
                "state.to_decree_included(%" PRId64 ") <= last_durable_decree(%" PRId64 ")",
                ci,
                last_durable_decree());

        auto learn_dir = ::dsn::utils::filesystem::remove_file_name(state.files[0]);
        auto chkpt_dir = ::dsn::utils::filesystem::path_combine(data_dir(), chkpt_get_dir_name(ci));
        if (::dsn::utils::filesystem::rename_path(learn_dir, chkpt_dir)) {
            ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);
            dassert(ci > last_durable_decree(),
                    "%" PRId64 " VS %" PRId64 "",
                    ci,
                    last_durable_decree());
            _checkpoints.push_back(ci);
            if (!_checkpoints.empty()) {
                dassert(ci > _checkpoints.back(),
                        "%" PRId64 " VS %" PRId64 "",
                        ci,
                        _checkpoints.back());
            }
            set_last_durable_decree(ci);
            err = ::dsn::ERR_OK;
        } else {
            derror("%s: rename directory %s to %s failed",
                   replica_name(),
                   learn_dir.c_str(),
                   chkpt_dir.c_str());
            err = ::dsn::ERR_FILE_OPERATION_FAILED;
        }

        return err;
    }

    if (_is_open) {
        err = stop(true);
        if (err != ::dsn::ERR_OK) {
            derror("%s: close rocksdb %s failed, error = %s", replica_name(), err.to_string());
            return err;
        }
    }

    // clear data dir
    if (!::dsn::utils::filesystem::remove_path(data_dir())) {
        derror("%s: clear data directory %s failed", replica_name(), data_dir().c_str());
        return ::dsn::ERR_FILE_OPERATION_FAILED;
    }

    // reopen the db with the new checkpoint files
    if (state.files.size() > 0) {
        // create data dir
        if (!::dsn::utils::filesystem::create_directory(data_dir())) {
            derror("%s: create data directory %s failed", replica_name(), data_dir().c_str());
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }

        // move learned files from learn_dir to data_dir/rdb
        std::string learn_dir = ::dsn::utils::filesystem::remove_file_name(state.files[0]);
        std::string new_dir = ::dsn::utils::filesystem::path_combine(data_dir(), "rdb");
        if (!::dsn::utils::filesystem::rename_path(learn_dir, new_dir)) {
            derror("%s: rename directory %s to %s failed",
                   replica_name(),
                   learn_dir.c_str(),
                   new_dir.c_str());
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }

        err = start(0, nullptr);
    } else {
        ddebug("%s: apply empty checkpoint, create new rocksdb", replica_name());
        err = start(0, nullptr);
    }

    if (err != ::dsn::ERR_OK) {
        derror("%s: open rocksdb failed, error = %s", replica_name(), err.to_string());
        return err;
    }

    dassert(_is_open, "");
    dassert(ci == last_durable_decree(), "%" PRId64 " VS %" PRId64 "", ci, last_durable_decree());

    ddebug("%s: apply checkpoint succeed, last_durable_decree = %" PRId64,
           replica_name(),
           last_durable_decree());
    return ::dsn::ERR_OK;
}

bool pegasus_server_impl::is_filter_type_supported(::dsn::apps::filter_type::type filter_type)
{
    return filter_type >= ::dsn::apps::filter_type::FT_NO_FILTER &&
           filter_type <= ::dsn::apps::filter_type::FT_MATCH_POSTFIX;
}

bool pegasus_server_impl::validate_filter(::dsn::apps::filter_type::type filter_type,
                                          const ::dsn::blob &filter_pattern,
                                          const ::dsn::blob &value)
{
    if (filter_type == ::dsn::apps::filter_type::FT_NO_FILTER || filter_pattern.length() == 0)
        return true;
    if (value.length() < filter_pattern.length())
        return false;
    switch (filter_type) {
    case ::dsn::apps::filter_type::FT_MATCH_ANYWHERE: {
        // brute force search
        // TODO: improve it according to
        //   http://old.blog.phusion.nl/2010/12/06/efficient-substring-searching/
        const char *a1 = value.data();
        int l1 = value.length();
        const char *a2 = filter_pattern.data();
        int l2 = filter_pattern.length();
        for (int i = 0; i <= l1 - l2; ++i) {
            int j = 0;
            while (j < l2 && a1[i + j] == a2[j])
                ++j;
            if (j == l2)
                return true;
        }
        return false;
    }
    case ::dsn::apps::filter_type::FT_MATCH_PREFIX:
        return (memcmp(value.data(), filter_pattern.data(), filter_pattern.length()) == 0);
    case ::dsn::apps::filter_type::FT_MATCH_POSTFIX:
        return (memcmp(value.data() + value.length() - filter_pattern.length(),
                       filter_pattern.data(),
                       filter_pattern.length()) == 0);
    default:
        dassert(false, "unsupported filter type: %d", filter_type);
    }
    return false;
}

int pegasus_server_impl::append_key_value_for_scan(
    std::vector<::dsn::apps::key_value> &kvs,
    const rocksdb::Slice &key,
    const rocksdb::Slice &value,
    ::dsn::apps::filter_type::type hash_key_filter_type,
    const ::dsn::blob &hash_key_filter_pattern,
    ::dsn::apps::filter_type::type sort_key_filter_type,
    const ::dsn::blob &sort_key_filter_pattern,
    uint32_t epoch_now,
    bool no_value)
{
    uint32_t expire_ts = pegasus_extract_expire_ts(_value_schema_version, value);
    if (expire_ts > 0 && expire_ts <= epoch_now) {
        if (_verbose_log) {
            derror("%s: rocksdb data expired for scan", replica_name());
        }
        return 2;
    }

    ::dsn::apps::key_value kv;

    // extract raw key
    ::dsn::blob raw_key(key.data(), 0, key.size());
    if (hash_key_filter_type != ::dsn::apps::filter_type::FT_NO_FILTER ||
        sort_key_filter_type != ::dsn::apps::filter_type::FT_NO_FILTER) {
        ::dsn::blob hash_key, sort_key;
        pegasus_restore_key(raw_key, hash_key, sort_key);
        if (hash_key_filter_type != ::dsn::apps::filter_type::FT_NO_FILTER &&
            !validate_filter(hash_key_filter_type, hash_key_filter_pattern, hash_key)) {
            if (_verbose_log) {
                derror("%s: hash key filtered for scan", replica_name());
            }
            return 3;
        }
        if (sort_key_filter_type != ::dsn::apps::filter_type::FT_NO_FILTER &&
            !validate_filter(sort_key_filter_type, sort_key_filter_pattern, sort_key)) {
            if (_verbose_log) {
                derror("%s: sort key filtered for scan", replica_name());
            }
            return 3;
        }
    }
    std::shared_ptr<char> key_buf(::dsn::utils::make_shared_array<char>(raw_key.length()));
    ::memcpy(key_buf.get(), raw_key.data(), raw_key.length());
    kv.key.assign(std::move(key_buf), 0, raw_key.length());

    // extract value
    if (!no_value) {
        std::unique_ptr<std::string> value_buf(new std::string(value.data(), value.size()));
        pegasus_extract_user_data(_value_schema_version, std::move(value_buf), kv.value);
    }

    kvs.emplace_back(std::move(kv));
    return 1;
}

int pegasus_server_impl::append_key_value_for_multi_get(
    std::vector<::dsn::apps::key_value> &kvs,
    const rocksdb::Slice &key,
    const rocksdb::Slice &value,
    ::dsn::apps::filter_type::type sort_key_filter_type,
    const ::dsn::blob &sort_key_filter_pattern,
    uint32_t epoch_now,
    bool no_value)
{
    uint32_t expire_ts = pegasus_extract_expire_ts(_value_schema_version, value);
    if (expire_ts > 0 && expire_ts <= epoch_now) {
        if (_verbose_log) {
            derror("%s: rocksdb data expired for multi get", replica_name());
        }
        return 2;
    }

    ::dsn::apps::key_value kv;

    // extract sort_key
    ::dsn::blob raw_key(key.data(), 0, key.size());
    ::dsn::blob hash_key, sort_key;
    pegasus_restore_key(raw_key, hash_key, sort_key);
    if (sort_key_filter_type != ::dsn::apps::filter_type::FT_NO_FILTER &&
        !validate_filter(sort_key_filter_type, sort_key_filter_pattern, sort_key)) {
        if (_verbose_log) {
            derror("%s: sort key filtered for multi get", replica_name());
        }
        return 3;
    }
    std::shared_ptr<char> sort_key_buf(::dsn::utils::make_shared_array<char>(sort_key.length()));
    ::memcpy(sort_key_buf.get(), sort_key.data(), sort_key.length());
    kv.key.assign(std::move(sort_key_buf), 0, sort_key.length());

    // extract value
    if (!no_value) {
        std::unique_ptr<std::string> value_buf(new std::string(value.data(), value.size()));
        pegasus_extract_user_data(_value_schema_version, std::move(value_buf), kv.value);
    }

    kvs.emplace_back(std::move(kv));
    return 1;
}

// statistic the count and size of files of this type. return (-1,-1) if failed.
static std::pair<int64_t, int64_t> get_type_file_size(const std::string &path,
                                                      const std::string &type)
{
    std::vector<std::string> files;
    if (!::dsn::utils::filesystem::get_subfiles(path, files, false)) {
        dwarn("get subfiles of dir %s failed", path.c_str());
        return std::pair<int64_t, int64_t>(-1, -1);
    }
    int64_t res = 0;
    int64_t cnt = 0;
    for (auto &f : files) {
        if (f.length() > type.length() && f.substr(f.length() - type.length()) == type) {
            int64_t tsize = 0;
            if (::dsn::utils::filesystem::file_size(f, tsize)) {
                res += tsize;
                cnt++;
            } else {
                dwarn("get size of file %s failed", f.c_str());
                return std::pair<int64_t, int64_t>(-1, -1);
            }
        }
    }
    return std::pair<int64_t, int64_t>(cnt, res);
}

std::pair<int64_t, int64_t> pegasus_server_impl::statistic_sst_size()
{
    // dir = data_dir()/rdb
    return get_type_file_size(::dsn::utils::filesystem::path_combine(data_dir(), "rdb"), ".sst");
}

void pegasus_server_impl::updating_rocksdb_sstsize()
{
    std::pair<int64_t, int64_t> sst_size = statistic_sst_size();
    if (sst_size.first == -1) {
        dwarn("%s: statistic sst file size failed", replica_name());
    } else {
        int64_t sst_size_mb = sst_size.second / 1048576;
        ddebug("%s: statistic sst file size succeed, sst_count = %" PRId64 ", sst_size = %" PRId64
               "(%" PRId64 "MB)",
               replica_name(),
               sst_size.first,
               sst_size.second,
               sst_size_mb);
        _pfc_sst_count->set(sst_size.first);
        _pfc_sst_size->set(sst_size_mb);
    }
}

std::pair<std::string, bool> pegasus_server_impl::get_restore_dir_from_env(int argc, char **argv)
{
    std::pair<std::string, bool> res;
    res.first = std::string();
    res.second = false;
    if (argc <= 0 || ((argc - 1) % 2 != 0) || argv == nullptr) {
        derror("%s: parse restore info from env failed, because invalid argc = %d",
               replica_name(),
               argc);
        return res;
    }
    // env is compounded in replication_app_base::open() function
    std::map<std::string, std::string> env_kvs;
    int idx = 1;
    while (idx < argc) {
        std::string key = std::string(argv[idx++]);
        std::string value = std::string(argv[idx++]);
        env_kvs.insert(std::make_pair(key, value));
    }

    auto it = env_kvs.find("force_restore");
    if (it != env_kvs.end()) {
        res.second = true;
    }

    std::stringstream os;
    os << "restore.";
    // TODO: using cold_backup_constant to replace
    // Attention: "policy_name" and "backup_id" must equal to cold_backup_constant::POLICY_NAME
    //             and cold_backup_constant::BACKUP_ID
    it = env_kvs.find("policy_name");
    if (it != env_kvs.end()) {
        ddebug(
            "%s: find policy_name from env, policy_name = %s", replica_name(), it->second.c_str());
        os << it->second << ".";
    } else {
        return res;
    }
    it = env_kvs.find("backup_id");
    if (it != env_kvs.end()) {
        ddebug("%s: find backup_id from env, backup_id = %s", replica_name(), it->second.c_str());
        os << it->second;
    } else {
        return res;
    }
    std::string parent_dir = ::dsn::utils::filesystem::remove_file_name(data_dir());
    res.first = ::dsn::utils::filesystem::path_combine(parent_dir, os.str());
    return res;
}

void pegasus_server_impl::manual_compact()
{
    ddebug("%s: start to CompactRange", replica_name());
    rocksdb::CompactRangeOptions options;
    options.exclusive_manual_compaction = true;
    options.change_level = true;
    options.target_level = -1;
    options.bottommost_level_compaction = rocksdb::BottommostLevelCompaction::kForce;
    _db->CompactRange(options, nullptr, nullptr);
    ddebug("%s: CompactRange finished", replica_name());
}

}
} // namespace
