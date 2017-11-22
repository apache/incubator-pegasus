// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_server_impl.h"
#include <pegasus_key_schema.h>
#include <pegasus_value_schema.h>
#include <pegasus_utils.h>
#include <dsn/utility/utils.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/table.h>
#include <boost/lexical_cast.hpp>
#include <algorithm>

#ifdef __TITLE__
#undef __TITLE__
#endif
#define __TITLE__ "pegasus.server.impl"

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

pegasus_server_impl::pegasus_server_impl(dsn_gpid gpid)
    : ::dsn::replicated_service_app_type_1(gpid),
      _gpid(gpid),
      _db(nullptr),
      _is_open(false),
      _value_schema_version(0),
      _physical_error(0),
      _is_checkpointing(false)
{
    _primary_address = ::dsn::rpc_address(dsn_primary_address()).to_string();
    char buf[256];
    sprintf(buf, "%d.%d@%s", gpid.u.app_id, gpid.u.partition_index, _primary_address.c_str());
    _replica_name = buf;
    _data_dir = dsn_get_app_data_dir(gpid);

    _verbose_log = dsn_config_get_value_bool("pegasus.server",
                                             "rocksdb_verbose_log",
                                             false,
                                             "print verbose log for debugging, default is false");

    // init db options

    // rocksdb default: 4MB
    _db_opts.write_buffer_size =
        (size_t)dsn_config_get_value_uint64("pegasus.server",
                                            "rocksdb_write_buffer_size",
                                            16777216,
                                            "rocksdb options.write_buffer_size, default 16MB");

    // rocksdb default: 2
    _db_opts.max_write_buffer_number =
        (int)dsn_config_get_value_uint64("pegasus.server",
                                         "rocksdb_max_write_buffer_number",
                                         2,
                                         "rocksdb options.max_write_buffer_number, default 2");

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
                                    8388608,
                                    "rocksdb options.target_file_size_base, default 8MB");

    // rocksdb default: 10MB
    _db_opts.max_bytes_for_level_base =
        dsn_config_get_value_uint64("pegasus.server",
                                    "rocksdb_max_bytes_for_level_base",
                                    41943040,
                                    "rocksdb options.max_bytes_for_level_base, default 40MB");

    // rocksdb default: 10
    _db_opts.max_grandparent_overlap_factor = (int)dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_max_grandparent_overlap_factor",
        10,
        "rocksdb options.max_grandparent_overlap_factor, default 10");

    // rocksdb default: 4
    _db_opts.level0_file_num_compaction_trigger =
        (int)dsn_config_get_value_uint64("pegasus.server",
                                         "rocksdb_level0_file_num_compaction_trigger",
                                         4,
                                         "rocksdb options.level0_file_num_compaction_trigger, 4");

    // rocksdb default: 20
    _db_opts.level0_slowdown_writes_trigger = (int)dsn_config_get_value_uint64(
        "pegasus.server",
        "rocksdb_level0_slowdown_writes_trigger",
        20,
        "rocksdb options.level0_slowdown_writes_trigger, default 20");

    // rocksdb default: 24
    _db_opts.level0_stop_writes_trigger =
        (int)dsn_config_get_value_uint64("pegasus.server",
                                         "rocksdb_level0_stop_writes_trigger",
                                         24,
                                         "rocksdb options.level0_stop_writes_trigger, default 24");

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

    // register the perf counters
    snprintf(buf, 255, "get_qps@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_get_qps.init("app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of GET request");

    snprintf(buf, 255, "multi_get_qps@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_multi_get_qps.init(
        "app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of MULTI_GET request");

    snprintf(buf, 255, "scan_qps@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_scan_qps.init("app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of SCAN request");

    snprintf(buf, 255, "put_qps@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_put_qps.init("app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of PUT request");

    snprintf(buf, 255, "multi_put_qps@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_multi_put_qps.init(
        "app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of MULTI_PUT request");

    snprintf(buf, 255, "remove_qps@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_remove_qps.init(
        "app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of REMOVE request");

    snprintf(buf, 255, "multi_remove_qps@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_multi_remove_qps.init(
        "app.pegasus", buf, COUNTER_TYPE_RATE, "statistic the qps of MULTI_REMOVE request");

    snprintf(buf, 255, "get_latency@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_get_latency.init("app.pegasus",
                          buf,
                          COUNTER_TYPE_NUMBER_PERCENTILES,
                          "statistic the latency of GET request");

    snprintf(buf, 255, "multi_get_latency@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_multi_get_latency.init("app.pegasus",
                                buf,
                                COUNTER_TYPE_NUMBER_PERCENTILES,
                                "statistic the latency of MULTI_GET request");

    snprintf(buf, 255, "scan_latency@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_scan_latency.init("app.pegasus",
                           buf,
                           COUNTER_TYPE_NUMBER_PERCENTILES,
                           "statistic the latency of SCAN request");

    snprintf(buf, 255, "put_latency@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_put_latency.init("app.pegasus",
                          buf,
                          COUNTER_TYPE_NUMBER_PERCENTILES,
                          "statistic the latency of PUT request");

    snprintf(buf, 255, "multi_put_latency@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_multi_put_latency.init("app.pegasus",
                                buf,
                                COUNTER_TYPE_NUMBER_PERCENTILES,
                                "statistic the latency of MULTI_PUT request");

    snprintf(buf, 255, "remove_latency@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_remove_latency.init("app.pegasus",
                             buf,
                             COUNTER_TYPE_NUMBER_PERCENTILES,
                             "statistic the latency of REMOVE request");

    snprintf(buf, 255, "multi_remove_latency@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_multi_remove_latency.init("app.pegasus",
                                   buf,
                                   COUNTER_TYPE_NUMBER_PERCENTILES,
                                   "statistic the latency of MULTI_REMOVE request");

    snprintf(buf, 255, "recent.expire.count@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_recent_expire_count.init("app.pegasus",
                                  buf,
                                  COUNTER_TYPE_VOLATILE_NUMBER,
                                  "statistic the recent expired value read count");

    snprintf(buf, 255, "disk.storage.sst.count@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_sst_count.init(
        "app.pegasus", buf, COUNTER_TYPE_NUMBER, "statistic the count of sstable files");

    snprintf(buf, 255, "disk.storage.sst(MB)@%d.%d", gpid.u.app_id, gpid.u.partition_index);
    _pfc_sst_size.init(
        "app.pegasus", buf, COUNTER_TYPE_NUMBER, "statistic the size of sstable files");

    updating_rocksdb_sstsize();
}

void pegasus_server_impl::parse_checkpoints()
{
    std::vector<std::string> dirs;
    ::dsn::utils::filesystem::get_subdirectories(_data_dir, dirs, false);

    ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);

    _checkpoints.clear();
    for (auto &d : dirs) {
        int64_t ci;
        std::string d1 = d.substr(_data_dir.size() + 1);
        if (chkpt_init_from_dir(d1.c_str(), ci)) {
            _checkpoints.push_back(ci);
        } else if (d1.find("checkpoint") != std::string::npos) {
            ddebug(
                "%s: invalid checkpoint directory %s, remove it", _replica_name.c_str(), d.c_str());
            ::dsn::utils::filesystem::remove_path(d);
            if (!::dsn::utils::filesystem::remove_path(d)) {
                derror("%s: remove invalid checkpoint directory %s failed",
                       _replica_name.c_str(),
                       d.c_str());
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
        auto cpt_dir = ::dsn::utils::filesystem::path_combine(_data_dir, chkpt_get_dir_name(d));
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
               _replica_name.c_str(),
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
        if (delete_max_index > 0) {
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
        auto cpt_dir = ::dsn::utils::filesystem::path_combine(_data_dir, chkpt_get_dir_name(del_d));
        if (::dsn::utils::filesystem::directory_exists(cpt_dir)) {
            if (::dsn::utils::filesystem::remove_path(cpt_dir)) {
                ddebug("%s: checkpoint directory %s removed by garbage collection",
                       _replica_name.c_str(),
                       cpt_dir.c_str());
            } else {
                derror("%s: checkpoint directory %s remove failed by garbage collection",
                       _replica_name.c_str(),
                       cpt_dir.c_str());
                put_back_list.push_back(del_d);
            }
        } else {
            ddebug("%s: checkpoint directory %s does not exist, ignored by garbage collection",
                   _replica_name.c_str(),
                   cpt_dir.c_str());
        }
    }

    // put back checkpoints which is not deleted
    // ATTENTION: the put back checkpoint may be incomplete, which will cause failure on load.
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
           _replica_name.c_str(),
           checkpoints_count,
           min_d,
           max_d);
}

void pegasus_server_impl::on_batched_write_requests(int64_t decree,
                                                    int64_t timestamp,
                                                    dsn_message_t *requests,
                                                    int count)
{
    dassert(_is_open, "");
    dassert(requests != nullptr, "");
    uint64_t start_time = dsn_now_ns();

    if (count == 1 &&
        ((::dsn::message_ex *)requests[0])->local_rpc_code ==
            ::dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
        _pfc_multi_put_qps.increment();
        dsn_message_t request = requests[0];

        ::dsn::apps::update_response resp;
        resp.app_id = _gpid.u.app_id;
        resp.partition_index = _gpid.u.partition_index;
        resp.decree = decree;
        resp.server = _primary_address;

        ::dsn::apps::multi_put_request update;
        ::dsn::unmarshall(request, update);

        if (update.kvs.empty()) {
            // invalid argument
            derror("%s: invalid argument for multi_put: decree = %" PRId64 ", error = empty kvs",
                   _replica_name.c_str(),
                   decree);

            ::dsn::rpc_replier<::dsn::apps::update_response> replier(
                dsn_msg_create_response(request));
            if (!replier.is_empty()) {
                // an invalid operation shoundn't be added to latency calculation
                resp.error = rocksdb::Status::kInvalidArgument;
                replier(resp);
            }
            return;
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
                   _replica_name.c_str(),
                   decree,
                   status.ToString().c_str());
            _physical_error = status.code();
        }

        ::dsn::rpc_replier<::dsn::apps::update_response> replier(dsn_msg_create_response(request));
        if (!replier.is_empty()) {
            _pfc_multi_put_latency.set(dsn_now_ns() - start_time);
            resp.error = status.code();
            replier(resp);
        }

        _batch.Clear();
        return;
    } else if (count == 1 &&
               ((::dsn::message_ex *)requests[0])->local_rpc_code ==
                   ::dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE) {
        _pfc_multi_remove_qps.increment();
        dsn_message_t request = requests[0];

        ::dsn::apps::multi_remove_response resp;
        resp.app_id = _gpid.u.app_id;
        resp.partition_index = _gpid.u.partition_index;
        resp.decree = decree;
        resp.server = _primary_address;

        ::dsn::apps::multi_remove_request update;
        ::dsn::unmarshall(request, update);

        if (update.sort_keys.empty()) {
            // invalid argument
            derror("%s: invalid argument for multi_remove: decree = %" PRId64
                   ", error = empty sort keys",
                   _replica_name.c_str(),
                   decree);

            ::dsn::rpc_replier<::dsn::apps::multi_remove_response> replier(
                dsn_msg_create_response(request));
            if (!replier.is_empty()) {
                // an invalid operation shoundn't be added to latency calculation
                resp.error = rocksdb::Status::kInvalidArgument;
                resp.count = 0;
                replier(resp);
            }
            return;
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
                   _replica_name.c_str(),
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
            _pfc_multi_remove_latency.set(dsn_now_ns() - start_time);
            resp.error = status.code();
            replier(resp);
        }

        _batch.Clear();
        return;
    } else {
        for (int i = 0; i < count; ++i) {
            dsn_message_t request = requests[i];
            dassert(request != nullptr, "");
            ::dsn::message_ex *msg = (::dsn::message_ex *)request;
            ::dsn::blob key;
            if (msg->local_rpc_code == ::dsn::apps::RPC_RRDB_RRDB_PUT) {
                _pfc_put_qps.increment();
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
                _batch_perfcounters.push_back(_pfc_put_latency.get_handle());
            } else if (msg->local_rpc_code == ::dsn::apps::RPC_RRDB_RRDB_REMOVE) {
                _pfc_remove_qps.increment();
                ::dsn::unmarshall(request, key);

                rocksdb::Slice skey(key.data(), key.length());
                _batch.Delete(skey);
                _batch_repliers.emplace_back(dsn_msg_create_response(request));
                _batch_perfcounters.push_back(_pfc_remove_latency.get_handle());
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
                int thread_hash = dsn_gpid_to_thread_hash(_gpid);
                dassert(msg->header->client.thread_hash == thread_hash, "inconsistent thread hash");
            }

            if (_verbose_log) {
                ::dsn::blob hash_key, sort_key;
                pegasus_restore_key(key, hash_key, sort_key);
                ddebug("%s: rocksdb write: decree = %" PRId64
                       ", code = %s, hash_key = \"%.*s\", sort_key = \"%.*s\"",
                       _replica_name.c_str(),
                       decree,
                       dsn_task_code_to_string(msg->local_rpc_code),
                       hash_key.length(),
                       hash_key.data(),
                       sort_key.length(),
                       sort_key.data());
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
               _replica_name.c_str(),
               decree,
               status.ToString().c_str());
        _physical_error = status.code();
    }

    ::dsn::apps::update_response resp;
    resp.error = status.code();
    resp.app_id = _gpid.u.app_id;
    resp.partition_index = _gpid.u.partition_index;
    resp.decree = decree;
    resp.server = _primary_address;

    dassert(_batch_repliers.size() == _batch_perfcounters.size(),
            "%s: repliers's size(%u) vs perfcounters's size(%u) not match",
            _replica_name.c_str(),
            _batch_repliers.size(),
            _batch_perfcounters.size());
    uint64_t latency = dsn_now_ns() - start_time;
    for (unsigned int i = 0; i != _batch_repliers.size(); ++i) {
        if (!_batch_repliers[i].is_empty()) {
            dsn_perf_counter_set(_batch_perfcounters[i], latency);
            _batch_repliers[i](resp);
        }
    }

    _batch.Clear();
    _batch_repliers.clear();
    _batch_perfcounters.clear();
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
    _pfc_get_qps.increment();
    uint64_t start_time = dsn_now_ns();

    ::dsn::apps::read_response resp;
    auto id = get_gpid();
    resp.app_id = id.u.app_id;
    resp.partition_index = id.u.partition_index;
    resp.server = _primary_address;

    rocksdb::Slice skey(key.data(), key.length());
    std::unique_ptr<std::string> value(new std::string());
    rocksdb::Status status = _db->Get(_rd_opts, skey, value.get());

    if (status.ok()) {
        uint32_t expire_ts = pegasus_extract_expire_ts(_value_schema_version, *value);
        if (expire_ts > 0 && expire_ts <= ::pegasus::utils::epoch_now()) {
            _pfc_recent_expire_count.increment();
            if (_verbose_log) {
                derror("%s: rocksdb data expired", _replica_name.c_str());
            }
            status = rocksdb::Status::NotFound();
        }
    }

    if (!status.ok()) {
        if (_verbose_log) {
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(key, hash_key, sort_key);
            derror("%s: rocksdb get failed: hash_key = \"%.*s\", sort_key = \"%.*s\", error = %s",
                   _replica_name.c_str(),
                   hash_key.length(),
                   hash_key.data(),
                   sort_key.length(),
                   sort_key.data(),
                   status.ToString().c_str());
        } else if (!status.IsNotFound()) {
            derror("%s: rocksdb get failed: error = %s",
                   _replica_name.c_str(),
                   status.ToString().c_str());
        }
    }

    resp.error = status.code();
    if (status.ok()) {
        pegasus_extract_user_data(_value_schema_version, std::move(value), resp.value);
    }

    _pfc_get_latency.set(dsn_now_ns() - start_time);
    reply(resp);
}

void pegasus_server_impl::on_multi_get(const ::dsn::apps::multi_get_request &request,
                                       ::dsn::rpc_replier<::dsn::apps::multi_get_response> &reply)
{
    dassert(_is_open, "");
    _pfc_multi_get_qps.increment();
    uint64_t start_time = dsn_now_ns();

    ::dsn::apps::multi_get_response resp;
    auto id = get_gpid();
    resp.app_id = id.u.app_id;
    resp.partition_index = id.u.partition_index;
    resp.server = _primary_address;

    int32_t max_kv_count = request.max_kv_count > 0 ? request.max_kv_count : INT_MAX;
    int32_t max_kv_size = request.max_kv_size > 0 ? request.max_kv_size : INT_MAX;
    uint32_t epoch_now = ::pegasus::utils::epoch_now();
    uint64_t expire_count = 0;

    if (request.sort_keys.empty()) {
        // scan
        ::dsn::blob start_key, stop_key;
        pegasus_generate_key(start_key, request.hash_key, ::dsn::blob());
        pegasus_generate_next_blob(stop_key, request.hash_key);
        rocksdb::Slice start(start_key.data(), start_key.length());
        rocksdb::Slice stop(stop_key.data(), stop_key.length());
        rocksdb::ReadOptions options = _rd_opts;
        options.iterate_upper_bound = &stop;
        std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(options));
        it->Seek(start);

        int32_t count = 0;
        int32_t size = 0;
        while (count < max_kv_count && size < max_kv_size && it->Valid()) {
            if (append_key_value_for_multi_get(
                    resp.kvs, it->key(), it->value(), epoch_now, request.no_value)) {
                count++;
                auto &kv = resp.kvs.back();
                size += kv.key.length() + kv.value.length();
            } else {
                expire_count++;
            }
            it->Next();
        }

        resp.error = it->status().code();
        if (!it->status().ok()) {
            // error occur
            if (_verbose_log) {
                derror("%s: rocksdb scan failed for multi get: hash_key = \"%.*s\", error = %s",
                       _replica_name.c_str(),
                       request.hash_key.length(),
                       request.hash_key.data(),
                       it->status().ToString().c_str());
            } else {
                derror("%s: rocksdb scan failed for multi get: error = %s",
                       _replica_name.c_str(),
                       it->status().ToString().c_str());
            }
            resp.kvs.clear();
        } else if (it->Valid()) {
            // scan not completed
            resp.error = rocksdb::Status::kIncomplete;
        }
    } else {
        rocksdb::Status status;
        int32_t count = 0;
        int32_t size = 0;
        bool exceed_limit = false;
        bool error_occurred = false;
        for (auto &sort_key : request.sort_keys) {
            // if exceed limit
            if (count >= max_kv_count || size >= max_kv_size) {
                exceed_limit = true;
                break;
            }

            ::dsn::blob raw_key;
            pegasus_generate_key(raw_key, request.hash_key, sort_key);
            rocksdb::Slice skey(raw_key.data(), raw_key.length());
            std::unique_ptr<std::string> value(new std::string());
            status = _db->Get(_rd_opts, skey, value.get());

            // check ttl
            if (status.ok()) {
                uint32_t expire_ts = pegasus_extract_expire_ts(_value_schema_version, *value);
                if (expire_ts > 0 && expire_ts <= epoch_now) {
                    expire_count++;
                    if (_verbose_log) {
                        derror("%s: rocksdb data expired for multi get", _replica_name.c_str());
                    }
                    status = rocksdb::Status::NotFound();
                }
            }

            // print log
            if (!status.ok()) {
                if (_verbose_log) {
                    derror("%s: rocksdb get failed for multi get: hash_key = \"%.*s\", sort_key = "
                           "\"%.*s\", error = %s",
                           _replica_name.c_str(),
                           request.hash_key.length(),
                           request.hash_key.data(),
                           sort_key.length(),
                           sort_key.data(),
                           status.ToString().c_str());
                } else if (!status.IsNotFound()) {
                    derror("%s: rocksdb get failed for multi get: error = %s",
                           _replica_name.c_str(),
                           status.ToString().c_str());
                }
            }

            // extract value
            if (status.ok()) {
                ::dsn::apps::key_value kv;
                kv.key = sort_key;
                if (!request.no_value) {
                    pegasus_extract_user_data(_value_schema_version, std::move(value), kv.value);
                }
                resp.kvs.emplace_back(kv);
                count++;
                size += kv.key.length() + kv.value.length();
            }

            // if error occurred
            if (!status.ok() && !status.IsNotFound()) {
                error_occurred = true;
                break;
            }
        }

        if (error_occurred) {
            resp.error = status.code();
            resp.kvs.clear();
        } else if (exceed_limit) {
            resp.error = rocksdb::Status::kIncomplete;
        } else {
            resp.error = rocksdb::Status::kOk;
        }
    }

    if (expire_count > 0) {
        _pfc_recent_expire_count.add(expire_count);
    }
    _pfc_multi_get_latency.set(dsn_now_ns() - start_time);
    reply(resp);
}

void pegasus_server_impl::on_sortkey_count(const ::dsn::blob &hash_key,
                                           ::dsn::rpc_replier<::dsn::apps::count_response> &reply)
{
    dassert(_is_open, "");

    ::dsn::apps::count_response resp;
    auto id = get_gpid();
    resp.app_id = id.u.app_id;
    resp.partition_index = id.u.partition_index;
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
                derror("%s: rocksdb data expired for sortkey count", _replica_name.c_str());
            }
        } else {
            resp.count++;
        }
        it->Next();
    }
    if (expire_count > 0) {
        _pfc_recent_expire_count.add(expire_count);
    }

    resp.error = it->status().code();
    if (!it->status().ok()) {
        // error occur
        if (_verbose_log) {
            derror("%s: rocksdb scan failed for sortkey_count: hash_key = \"%.*s\", error = %s",
                   _replica_name.c_str(),
                   hash_key.length(),
                   hash_key.data(),
                   it->status().ToString().c_str());
        } else {
            derror("%s: rocksdb scan failed for sortkey_count: error = %s",
                   _replica_name.c_str(),
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
    auto id = get_gpid();
    resp.app_id = id.u.app_id;
    resp.partition_index = id.u.partition_index;
    resp.server = _primary_address;

    rocksdb::Slice skey(key.data(), key.length());
    std::string value;
    rocksdb::Status status = _db->Get(_rd_opts, skey, &value);

    uint32_t expire_ts;
    uint32_t now_ts = ::pegasus::utils::epoch_now();
    if (status.ok()) {
        expire_ts = pegasus_extract_expire_ts(_value_schema_version, value);
        if (expire_ts > 0 && expire_ts <= now_ts) {
            _pfc_recent_expire_count.increment();
            if (_verbose_log) {
                derror("%s: rocksdb data expired", _replica_name.c_str());
            }
            status = rocksdb::Status::NotFound();
        }
    }

    if (!status.ok()) {
        if (_verbose_log) {
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(key, hash_key, sort_key);
            derror("%s: rocksdb get failed: hash_key = \"%.*s\", sort_key = \"%.*s\", error = %s",
                   _replica_name.c_str(),
                   hash_key.length(),
                   hash_key.data(),
                   sort_key.length(),
                   sort_key.data(),
                   status.ToString().c_str());
        } else if (!status.IsNotFound()) {
            derror("%s: rocksdb get failed: error = %s",
                   _replica_name.c_str(),
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
void pegasus_server_impl::on_get_scanner(const ::dsn::apps::get_scanner_request &args,
                                         ::dsn::rpc_replier<::dsn::apps::scan_response> &reply)
{
    dassert(_is_open, "");

    ::dsn::apps::scan_response resp;
    auto id = get_gpid();
    resp.app_id = id.u.app_id;
    resp.partition_index = id.u.partition_index;
    resp.server = _primary_address;

    rocksdb::Slice start(args.start_key.data(), args.start_key.length());
    rocksdb::Slice stop(args.stop_key.data(), args.stop_key.length());
    resp.kvs.reserve(args.batch_size);

    std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(rocksdb::ReadOptions()));
    it->Seek(start);
    bool complete = false;
    bool exclusive = !args.start_inclusive;
    uint32_t epoch_now = ::pegasus::utils::epoch_now();
    uint64_t expire_count = 0;
    for (int i = 0; i < args.batch_size; i++, it->Next()) {
        if (!it->Valid() || it->key().compare(stop) >= 0) {
            if (!it->status().ok()) {
                // error occur
                if (_verbose_log) {
                    derror(
                        "%s: rocksdb get_scanner failed: start_key = %s (%s), stop_key = %s (%s), "
                        "batch_size = %d, read_count = %d, error = %s",
                        _replica_name.c_str(),
                        start.ToString(true).c_str(),
                        args.start_inclusive ? "inclusive" : "exclusive",
                        stop.ToString(true).c_str(),
                        args.stop_inclusive ? "inclusive" : "exclusive",
                        args.batch_size,
                        i,
                        it->status().ToString().c_str());
                } else {
                    derror("%s: rocksdb get_scanner failed: error = %s",
                           _replica_name.c_str(),
                           it->status().ToString().c_str());
                }
            } else if (args.stop_inclusive && it->Valid() && !it->key().compare(stop)) {
                if (!append_key_value_for_scan(resp.kvs, it->key(), it->value(), epoch_now)) {
                    expire_count++;
                }
            }
            complete = true;
            break;
        } else if (exclusive && i == 0) {
            exclusive = false;
            if (!it->key().compare(start)) {
                i--;
                continue;
            }
        }

        if (!append_key_value_for_scan(resp.kvs, it->key(), it->value(), epoch_now)) {
            expire_count++;
        }
    }
    if (expire_count > 0) {
        _pfc_recent_expire_count.add(expire_count);
    }
    resp.error = it->status().code();

    if (complete) {
        resp.context_id = pegasus::SCAN_CONTEXT_ID_COMPLETED;
    } else {
        std::unique_ptr<pegasus_scan_context> context(
            new pegasus_scan_context(std::move(it),
                                     std::string(stop.data(), stop.size()),
                                     args.stop_inclusive,
                                     args.batch_size));
        int64_t handle = _context_cache.put(std::move(context));
        resp.context_id = handle;
        // if the context is used, it will be fetched and re-put into cache, which will change the
        // handle,
        // then the delayed task will fetch null context by old handle, and do nothing.
        ::dsn::tasking::enqueue(LOCAL_PEGASUS_SERVER_DELAY,
                                this,
                                [this, handle]() { _context_cache.fetch(handle); },
                                0,
                                std::chrono::minutes(5));
    }

    reply(resp);
}

void pegasus_server_impl::on_scan(const ::dsn::apps::scan_request &args,
                                  ::dsn::rpc_replier<::dsn::apps::scan_response> &reply)
{
    dassert(_is_open, "");
    _pfc_scan_qps.increment();
    uint64_t start_time = dsn_now_ns();

    ::dsn::apps::scan_response resp;
    auto id = get_gpid();
    resp.app_id = id.u.app_id;
    resp.partition_index = id.u.partition_index;
    resp.server = _primary_address;

    std::unique_ptr<pegasus_scan_context> context = _context_cache.fetch(args.context_id);
    if (context) {
        rocksdb::Iterator *it = context->iterator.get();
        int32_t batch_size = context->batch_size;
        const rocksdb::Slice &stop = context->stop;
        bool complete = false;
        uint32_t epoch_now = ::pegasus::utils::epoch_now();
        uint64_t expire_count = 0;
        for (int i = 0; i < batch_size; i++, it->Next()) {
            if (!it->Valid() || it->key().compare(stop) >= 0) {
                if (!it->status().ok()) {
                    // error occur
                    if (_verbose_log) {
                        derror("%s: rocksdb on_scan failed: context_id= %lld, stop_key = %s (%s),, "
                               "batch_size = %d, read_count = %d, error = %s",
                               _replica_name.c_str(),
                               args.context_id,
                               stop.ToString(true).c_str(),
                               context->stop_inclusive ? "inclusive" : "exclusieve",
                               context->batch_size,
                               i,
                               it->status().ToString().c_str());
                    } else {
                        derror("%s: rocksdb get_scanner failed: error = %s",
                               _replica_name.c_str(),
                               it->status().ToString().c_str());
                    }
                } else if (context->stop_inclusive && it->Valid() && !it->key().compare(stop)) {
                    if (!append_key_value_for_scan(resp.kvs, it->key(), it->value(), epoch_now)) {
                        expire_count++;
                    }
                }
                complete = true;
                break;
            }
            if (!append_key_value_for_scan(resp.kvs, it->key(), it->value(), epoch_now)) {
                expire_count++;
            }
        }
        if (expire_count > 0) {
            _pfc_recent_expire_count.add(expire_count);
        }
        if (complete) {
            resp.context_id = pegasus::SCAN_CONTEXT_ID_COMPLETED;
        } else {
            int64_t handle = _context_cache.put(std::move(context));
            resp.context_id = handle;
            ::dsn::tasking::enqueue(LOCAL_PEGASUS_SERVER_DELAY,
                                    this,
                                    [this, handle]() { _context_cache.fetch(handle); },
                                    0,
                                    std::chrono::minutes(5));
        }
        resp.error = rocksdb::Status::Code::kOk;
    } else {
        resp.error = rocksdb::Status::Code::kNotFound;
    }

    _pfc_scan_latency.set(dsn_now_ns() - start_time);
    reply(resp);
}

void pegasus_server_impl::on_clear_scanner(const int64_t &args) { _context_cache.fetch(args); }

DEFINE_TASK_CODE(UPDATING_ROCKSDB_SSTSIZE, TASK_PRIORITY_COMMON, THREAD_POOL_REPLICATION_LONG)

::dsn::error_code pegasus_server_impl::start(int argc, char **argv)
{
    dassert(!_is_open, "");
    ddebug("%s: start to open app %s", _replica_name.c_str(), _data_dir.c_str());

    rocksdb::Options opts = _db_opts;
    opts.create_if_missing = true;
    opts.error_if_exists = false;
    opts.compaction_filter = &_key_ttl_compaction_filter;
    opts.default_value_schema_version = PEGASUS_VALUE_SCHEMA_MAX_VERSION;

    auto path = ::dsn::utils::filesystem::path_combine(_data_dir, "rdb");
    auto status = rocksdb::DB::Open(opts, path, &_db);
    if (status.ok()) {
        _value_schema_version = _db->GetValueSchemaVersion();
        if (_value_schema_version > PEGASUS_VALUE_SCHEMA_MAX_VERSION) {
            derror("%s: open app failed, unsupported value schema version %" PRIu32,
                   _replica_name.c_str(),
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
                   _replica_name.c_str(),
                   last_durable_decree(),
                   ci);
            auto err = async_checkpoint(ci, false);
            if (err != ::dsn::ERR_OK) {
                derror("%s: create checkpoint failed, error = %s",
                       _replica_name.c_str(),
                       err.to_string());
                delete _db;
                _db = nullptr;
                return err;
            }
            dassert(ci == last_durable_decree(),
                    "last durable decree mismatch after checkpoint: %" PRId64 " vs %" PRId64,
                    ci,
                    last_durable_decree());
        }

        ddebug("%s: open app succeed, value_schema_version = %" PRIu32
               ", last_durable_decree = %" PRId64 "",
               _replica_name.c_str(),
               _value_schema_version,
               last_durable_decree());

        _is_open = true;

        open_service(get_gpid());

        dinfo("%s: start the updating sstsize timer task", _replica_name.c_str());
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
        derror("%s: open app failed, error = %s", _replica_name.c_str(), status.ToString().c_str());
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

    close_service(get_gpid());

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
        if (!::dsn::utils::filesystem::remove_path(_data_dir)) {
            derror("%s: clear directory %s failed when stop app",
                   _replica_name.c_str(),
                   _data_dir.c_str());
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
        _pfc_sst_count.set(0);
        _pfc_sst_size.set(0);
    }

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

::dsn::error_code pegasus_server_impl::sync_checkpoint(int64_t last_commit)
{
    CheckpointingTokenHelper token_helper(_is_checkpointing);
    if (!token_helper.token_got())
        return ::dsn::ERR_WRONG_TIMING;

    if (last_durable_decree() == last_commit)
        return ::dsn::ERR_NO_NEED_OPERATE;

    rocksdb::Checkpoint *chkpt = nullptr;
    auto status = rocksdb::Checkpoint::Create(_db, &chkpt);
    if (!status.ok()) {
        derror("%s: create Checkpoint object failed, error = %s",
               _replica_name.c_str(),
               status.ToString().c_str());
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }

    auto dir = chkpt_get_dir_name(last_commit);
    auto chkpt_dir = ::dsn::utils::filesystem::path_combine(_data_dir, dir);
    if (::dsn::utils::filesystem::directory_exists(chkpt_dir)) {
        ddebug("%s: checkpoint directory %s already exist, remove it first",
               _replica_name.c_str(),
               chkpt_dir.c_str());
        if (!::dsn::utils::filesystem::remove_path(chkpt_dir)) {
            derror("%s: remove old checkpoint directory %s failed",
                   _replica_name.c_str(),
                   chkpt_dir.c_str());
            delete chkpt;
            chkpt = nullptr;
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
    }

    status = chkpt->CreateCheckpoint(chkpt_dir);
    if (!status.ok()) {
        // sometimes checkpoint may fail, and try again will succeed
        derror("%s: create checkpoint failed, error = %s, try again",
               _replica_name.c_str(),
               status.ToString().c_str());
        status = chkpt->CreateCheckpoint(chkpt_dir);
    }

    // destroy Checkpoint object
    delete chkpt;
    chkpt = nullptr;

    if (!status.ok()) {
        derror("%s: create checkpoint failed, error = %s",
               _replica_name.c_str(),
               status.ToString().c_str());
        ::dsn::utils::filesystem::remove_path(chkpt_dir);
        if (!::dsn::utils::filesystem::remove_path(chkpt_dir)) {
            derror("%s: remove damaged checkpoint directory %s failed",
                   _replica_name.c_str(),
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
           _replica_name.c_str(),
           last_durable_decree());

    gc_checkpoints();

    return ::dsn::ERR_OK;
}

// Must be thread safe.
::dsn::error_code pegasus_server_impl::async_checkpoint(int64_t /*last_commit*/, bool is_emergency)
{
    CheckpointingTokenHelper token_helper(_is_checkpointing);
    if (!token_helper.token_got())
        return ::dsn::ERR_WRONG_TIMING;

    if (last_durable_decree() == _db->GetLastFlushedDecree()) {
        if (is_emergency) {
            // trigger flushing memtable, but not wait
            rocksdb::FlushOptions options;
            options.wait = false;
            auto status = _db->Flush(options);
            if (status.ok()) {
                ddebug("%s: trigger flushing memtable succeed", _replica_name.c_str());
                return ::dsn::ERR_TRY_AGAIN;
            } else if (status.IsNoNeedOperate()) {
                dwarn("%s: trigger flushing memtable failed, no memtable to flush",
                      _replica_name.c_str());
                return ::dsn::ERR_NO_NEED_OPERATE;
            } else {
                derror("%s: trigger flushing memtable failed, error = %s",
                       _replica_name.c_str(),
                       status.ToString().c_str());
                return ::dsn::ERR_LOCAL_APP_FAILURE;
            }
        } else {
            return ::dsn::ERR_NO_NEED_OPERATE;
        }
    }

    rocksdb::Checkpoint *chkpt = nullptr;
    auto status = rocksdb::Checkpoint::Create(_db, &chkpt);
    if (!status.ok()) {
        derror("%s: create Checkpoint object failed, error = %s",
               _replica_name.c_str(),
               status.ToString().c_str());
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }

    char buf[256];
    sprintf(buf, "checkpoint.tmp.%" PRIu64 "", dsn_now_us());
    std::string tmp_dir = ::dsn::utils::filesystem::path_combine(_data_dir, buf);
    if (::dsn::utils::filesystem::directory_exists(tmp_dir)) {
        ddebug("%s: temporary checkpoint directory %s already exist, remove it first",
               _replica_name.c_str(),
               tmp_dir.c_str());
        if (!::dsn::utils::filesystem::remove_path(tmp_dir)) {
            derror("%s: remove temporary checkpoint directory %s failed",
                   _replica_name.c_str(),
                   tmp_dir.c_str());
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
    }

    uint64_t ci = last_durable_decree();
    status = chkpt->CreateCheckpointQuick(tmp_dir, &ci);
    delete chkpt;
    chkpt = nullptr;
    if (!status.ok()) {
        derror("%s: async create checkpoint failed, error = %s",
               _replica_name.c_str(),
               status.ToString().c_str());
        if (!::dsn::utils::filesystem::remove_path(tmp_dir)) {
            derror("%s: remove temporary checkpoint directory %s failed",
                   _replica_name.c_str(),
                   tmp_dir.c_str());
        }
        return status.IsNoNeedOperate() ? ::dsn::ERR_NO_NEED_OPERATE : ::dsn::ERR_LOCAL_APP_FAILURE;
    }

    auto chkpt_dir = ::dsn::utils::filesystem::path_combine(_data_dir, chkpt_get_dir_name(ci));
    if (::dsn::utils::filesystem::directory_exists(chkpt_dir)) {
        ddebug("%s: checkpoint directory %s already exist, remove it first",
               _replica_name.c_str(),
               chkpt_dir.c_str());
        if (!::dsn::utils::filesystem::remove_path(chkpt_dir)) {
            derror("%s: remove old checkpoint directory %s failed",
                   _replica_name.c_str(),
                   chkpt_dir.c_str());
            if (!::dsn::utils::filesystem::remove_path(tmp_dir)) {
                derror("%s: remove temporary checkpoint directory %s failed",
                       _replica_name.c_str(),
                       tmp_dir.c_str());
            }
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
    }

    if (!::dsn::utils::filesystem::rename_path(tmp_dir, chkpt_dir)) {
        derror("%s: rename checkpoint directory from %s to %s failed",
               _replica_name.c_str(),
               tmp_dir.c_str(),
               chkpt_dir.c_str());
        if (!::dsn::utils::filesystem::remove_path(tmp_dir)) {
            derror("%s: remove temporary checkpoint directory %s failed",
                   _replica_name.c_str(),
                   tmp_dir.c_str());
        }
        return ::dsn::ERR_FILE_OPERATION_FAILED;
    }

    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);
        dassert(
            ci > last_durable_decree(), "%" PRId64 " VS %" PRId64 "", ci, last_durable_decree());
        if (!_checkpoints.empty()) {
            dassert(
                ci > _checkpoints.back(), "%" PRId64 " VS %" PRId64 "", ci, _checkpoints.back());
        }
        _checkpoints.push_back(ci);
        set_last_durable_decree(_checkpoints.back());
    }

    ddebug("%s: async create checkpoint succeed, last_durable_decree = %" PRId64 "",
           _replica_name.c_str(),
           last_durable_decree());

    gc_checkpoints();

    return ::dsn::ERR_OK;
}

::dsn::error_code pegasus_server_impl::get_checkpoint(int64_t learn_start,
                                                      int64_t local_commit,
                                                      void *learn_request,
                                                      int learn_request_size,
                                                      app_learn_state &state)
{
    dassert(_is_open, "");

    int64_t ci = last_durable_decree();
    if (ci == 0) {
        derror("%s: no checkpoint found", _replica_name.c_str());
        return ::dsn::ERR_OBJECT_NOT_FOUND;
    }

    auto chkpt_dir = ::dsn::utils::filesystem::path_combine(_data_dir, chkpt_get_dir_name(ci));
    state.files.clear();
    if (!::dsn::utils::filesystem::get_subfiles(chkpt_dir, state.files, true)) {
        derror(
            "%s: list files in checkpoint dir %s failed", _replica_name.c_str(), chkpt_dir.c_str());
        return ::dsn::ERR_FILE_OPERATION_FAILED;
    }

    state.from_decree_excluded = 0;
    state.to_decree_included = ci;

    ddebug("%s: get checkpoint succeed, from_decree_excluded = 0, to_decree_included = %" PRId64 "",
           _replica_name.c_str(),
           state.to_decree_included);
    return ::dsn::ERR_OK;
}

::dsn::error_code pegasus_server_impl::apply_checkpoint(dsn_chkpt_apply_mode mode,
                                                        int64_t local_commit,
                                                        const dsn_app_learn_state &state)
{
    ::dsn::error_code err;
    int64_t ci = state.to_decree_included;

    if (mode == DSN_CHKPT_COPY) {
        dassert(ci > last_durable_decree(),
                "state.to_decree_included(%" PRId64 ") <= last_durable_decree(%" PRId64 ")",
                ci,
                last_durable_decree());

        auto learn_dir = ::dsn::utils::filesystem::remove_file_name(state.files[0]);
        auto chkpt_dir = ::dsn::utils::filesystem::path_combine(_data_dir, chkpt_get_dir_name(ci));
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
                   _replica_name.c_str(),
                   learn_dir.c_str(),
                   chkpt_dir.c_str());
            err = ::dsn::ERR_FILE_OPERATION_FAILED;
        }

        return err;
    }

    if (_is_open) {
        err = stop(true);
        if (err != ::dsn::ERR_OK) {
            derror(
                "%s: close rocksdb %s failed, error = %s", _replica_name.c_str(), err.to_string());
            return err;
        }
    }

    // clear data dir
    if (!::dsn::utils::filesystem::remove_path(_data_dir)) {
        derror("%s: clear data directory %s failed", _replica_name.c_str(), _data_dir.c_str());
        return ::dsn::ERR_FILE_OPERATION_FAILED;
    }

    // reopen the db with the new checkpoint files
    if (state.file_state_count > 0) {
        // create data dir
        if (!::dsn::utils::filesystem::create_directory(_data_dir)) {
            derror("%s: create data directory %s failed", _replica_name.c_str(), _data_dir.c_str());
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }

        // move learned files from learn_dir to data_dir/rdb
        std::string learn_dir = ::dsn::utils::filesystem::remove_file_name(state.files[0]);
        std::string new_dir = ::dsn::utils::filesystem::path_combine(_data_dir, "rdb");
        if (!::dsn::utils::filesystem::rename_path(learn_dir, new_dir)) {
            derror("%s: rename directory %s to %s failed",
                   _replica_name.c_str(),
                   learn_dir.c_str(),
                   new_dir.c_str());
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }

        err = start(0, nullptr);
    } else {
        ddebug("%s: apply empty checkpoint, create new rocksdb", _replica_name.c_str());
        err = start(0, nullptr);
    }

    if (err != ::dsn::ERR_OK) {
        derror("%s: open rocksdb failed, error = %s", _replica_name.c_str(), err.to_string());
        return err;
    }

    dassert(_is_open, "");
    dassert(ci == last_durable_decree(), "%" PRId64 " VS %" PRId64 "", ci, last_durable_decree());

    ddebug("%s: apply checkpoint succeed, last_durable_decree = %" PRId64,
           _replica_name.c_str(),
           last_durable_decree());
    return ::dsn::ERR_OK;
}

bool pegasus_server_impl::append_key_value_for_scan(std::vector<::dsn::apps::key_value> &kvs,
                                                    const rocksdb::Slice &key,
                                                    const rocksdb::Slice &value,
                                                    uint32_t epoch_now)
{
    uint32_t expire_ts = pegasus_extract_expire_ts(_value_schema_version, value);
    if (expire_ts > 0 && expire_ts <= epoch_now) {
        if (_verbose_log) {
            derror("%s: rocksdb data expired for scan", _replica_name.c_str());
        }
        return false;
    }

    ::dsn::apps::key_value kv;

    // extract raw key
    std::shared_ptr<char> key_buf(::dsn::make_shared_array<char>(key.size()));
    ::memcpy(key_buf.get(), key.data(), key.size());
    kv.key.assign(std::move(key_buf), 0, key.size());

    // extract value
    std::unique_ptr<std::string> value_buf(new std::string(value.data(), value.size()));
    pegasus_extract_user_data(_value_schema_version, std::move(value_buf), kv.value);

    kvs.emplace_back(kv);
    return true;
}

bool pegasus_server_impl::append_key_value_for_multi_get(std::vector<::dsn::apps::key_value> &kvs,
                                                         const rocksdb::Slice &key,
                                                         const rocksdb::Slice &value,
                                                         uint32_t epoch_now,
                                                         bool no_value)
{
    uint32_t expire_ts = pegasus_extract_expire_ts(_value_schema_version, value);
    if (expire_ts > 0 && expire_ts <= epoch_now) {
        if (_verbose_log) {
            derror("%s: rocksdb data expired for multi get", _replica_name.c_str());
        }
        return false;
    }

    ::dsn::apps::key_value kv;

    // extract sort_key
    ::dsn::blob raw_key(key.data(), 0, key.size());
    ::dsn::blob hash_key, sort_key;
    pegasus_restore_key(raw_key, hash_key, sort_key);
    std::shared_ptr<char> sort_key_buf(::dsn::make_shared_array<char>(sort_key.length()));
    ::memcpy(sort_key_buf.get(), sort_key.data(), sort_key.length());
    kv.key.assign(std::move(sort_key_buf), 0, sort_key.length());

    // extract value
    if (!no_value) {
        std::unique_ptr<std::string> value_buf(new std::string(value.data(), value.size()));
        pegasus_extract_user_data(_value_schema_version, std::move(value_buf), kv.value);
    }

    kvs.emplace_back(kv);
    return true;
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
    // dir = _data_dir/rdb
    return get_type_file_size(::dsn::utils::filesystem::path_combine(_data_dir, "rdb"), ".sst");
}

void pegasus_server_impl::updating_rocksdb_sstsize()
{
    std::pair<int64_t, int64_t> sst_size = statistic_sst_size();
    if (sst_size.first == -1) {
        dwarn("%s: statistic sst file size failed", _replica_name.c_str());
    } else {
        int64_t sst_size_mb = sst_size.second / 1048576;
        ddebug("%s: statistic sst file size succeed, sst_count = %" PRId64 ", sst_size = %" PRId64
               "(%" PRId64 "MB)",
               _replica_name.c_str(),
               sst_size.first,
               sst_size.second,
               sst_size_mb);
        _pfc_sst_count.set(sst_size.first);
        _pfc_sst_size.set(sst_size_mb);
    }
}
}
} // namespace
