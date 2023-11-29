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

#include "pegasus_server_impl.h"

#include <fmt/core.h>
#include <inttypes.h>
#include <limits.h>
#include <rocksdb/advanced_cache.h>
#include <rocksdb/convenience.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/rate_limiter.h>
#include <rocksdb/statistics.h>
#include <rocksdb/status.h>
#include <rocksdb/utilities/checkpoint.h>
#include <rocksdb/utilities/options_util.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h> // IWYU pragma: keep
#include <algorithm>
#include <cstdint>
#include <functional>
#include <limits>
#include <list>
#include <mutex>
#include <ostream>
#include <set>

#include "base/idl_utils.h" // IWYU pragma: keep
#include "base/pegasus_key_schema.h"
#include "base/pegasus_utils.h"
#include "base/pegasus_value_schema.h"
#include "capacity_unit_calculator.h"
#include "common/replication.codes.h"
#include "common/replication_enums.h"
#include "consensus_types.h"
#include "dsn.layer2_types.h"
#include "hotkey_collector.h"
#include "meta_store.h"
#include "pegasus_const.h"
#include "pegasus_rpc_types.h"
#include "pegasus_server_write.h"
#include "perf_counter/perf_counter.h"
#include "replica_admin_types.h"
#include "rrdb/rrdb.code.definition.h"
#include "rrdb/rrdb_types.h"
#include "runtime/api_layer1.h"
#include "runtime/rpc/rpc_address.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/task/async_calls.h"
#include "runtime/task/task_code.h"
#include "server/key_ttl_compaction_filter.h"
#include "server/pegasus_manual_compact_service.h"
#include "server/pegasus_read_service.h"
#include "server/pegasus_scan_context.h"
#include "server/range_read_limiter.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/chrono_literals.h"
#include "utils/defer.h"
#include "utils/env.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"
#include "utils/string_conv.h"
#include "absl/strings/string_view.h"
#include "utils/strings.h"
#include "utils/threadpool_code.h"
#include "utils/token_bucket_throttling_controller.h"
#include "utils/utils.h"

namespace rocksdb {
class WriteBufferManager;
} // namespace rocksdb

using namespace dsn::literals::chrono_literals;

namespace pegasus {
namespace server {

DEFINE_TASK_CODE(LPC_PEGASUS_SERVER_DELAY, TASK_PRIORITY_COMMON, ::dsn::THREAD_POOL_DEFAULT)

DSN_DECLARE_int32(read_amp_bytes_per_bit);
DSN_DECLARE_uint32(checkpoint_reserve_min_count);
DSN_DECLARE_uint32(checkpoint_reserve_time_seconds);
DSN_DECLARE_uint64(rocksdb_iteration_threshold_time_ms);
DSN_DECLARE_uint64(rocksdb_slow_query_threshold_ns);

DSN_DEFINE_bool(pegasus.server,
                rocksdb_verbose_log,
                false,
                "whether to print verbose log for debugging");
DSN_DEFINE_int32(pegasus.server,
                 hotkey_analyse_time_interval_s,
                 10,
                 "hotkey analyse interval in seconds");
DSN_DEFINE_int32(pegasus.server,
                 update_rdb_stat_interval,
                 60,
                 "The interval seconds to update RocksDB statistics, in seconds.");
DSN_DEFINE_int32(pegasus.server,
                 inject_read_error_for_test,
                 0,
                 "Which error code to inject in read path, 0 means no error. Only for test.");
DSN_TAG_VARIABLE(inject_read_error_for_test, FT_MUTABLE);

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

std::shared_ptr<rocksdb::RateLimiter> pegasus_server_impl::_s_rate_limiter;
int64_t pegasus_server_impl::_rocksdb_limiter_last_total_through;
std::shared_ptr<rocksdb::Cache> pegasus_server_impl::_s_block_cache;
std::shared_ptr<rocksdb::WriteBufferManager> pegasus_server_impl::_s_write_buffer_manager;
::dsn::task_ptr pegasus_server_impl::_update_server_rdb_stat;
::dsn::perf_counter_wrapper pegasus_server_impl::_pfc_rdb_block_cache_mem_usage;
::dsn::perf_counter_wrapper pegasus_server_impl::_pfc_rdb_write_limiter_rate_bytes;
const std::string pegasus_server_impl::COMPRESSION_HEADER = "per_level:";
const std::string pegasus_server_impl::DATA_COLUMN_FAMILY_NAME = "default";
const std::string pegasus_server_impl::META_COLUMN_FAMILY_NAME = "pegasus_meta_cf";
const std::chrono::seconds pegasus_server_impl::kServerStatUpdateTimeSec = std::chrono::seconds(10);

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
            LOG_INFO_PREFIX("invalid checkpoint directory {}, remove it", d);
            ::dsn::utils::filesystem::remove_path(d);
            if (!::dsn::utils::filesystem::remove_path(d)) {
                LOG_ERROR_PREFIX("remove invalid checkpoint directory {} failed", d);
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

pegasus_server_impl::~pegasus_server_impl()
{
    if (_is_open) {
        CHECK_NOTNULL(_db, "");
        release_db();
    }
}

void pegasus_server_impl::gc_checkpoints(bool force_reserve_one)
{
    int min_count = force_reserve_one ? 1 : _checkpoint_reserve_min_count;
    uint64_t reserve_time = force_reserve_one ? 0 : _checkpoint_reserve_time_seconds;
    std::deque<int64_t> temp_list;
    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);
        if (_checkpoints.size() <= min_count)
            return;
        temp_list = _checkpoints;
    }

    // find the max checkpoint which can be deleted
    int64_t max_del_d = -1;
    uint64_t current_time = dsn_now_ms() / 1000;
    for (int i = 0; i < temp_list.size(); ++i) {
        if (i + min_count >= temp_list.size())
            break;
        int64_t d = temp_list[i];
        if (reserve_time > 0) {
            // we check last write time of "CURRENT" instead of directory, because the directory's
            // last write time may be updated by previous incompleted garbage collection.
            auto cpt_dir =
                ::dsn::utils::filesystem::path_combine(data_dir(), chkpt_get_dir_name(d));
            auto current_file = ::dsn::utils::filesystem::path_combine(cpt_dir, "CURRENT");
            if (!::dsn::utils::filesystem::file_exists(current_file)) {
                max_del_d = d;
                continue;
            }
            time_t tm;
            if (!dsn::utils::filesystem::last_write_time(current_file, tm)) {
                LOG_WARNING("get last write time of file {} failed", current_file);
                break;
            }
            auto last_write_time = (uint64_t)tm;
            if (last_write_time + reserve_time >= current_time) {
                // not expired
                break;
            }
        }
        max_del_d = d;
    }
    if (max_del_d == -1) {
        // no checkpoint to delete
        LOG_INFO_PREFIX("no checkpoint to garbage collection, checkpoints_count = {}",
                        temp_list.size());
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
            if (i + min_count >= _checkpoints.size() || del_d > max_del_d)
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
                LOG_INFO_PREFIX("checkpoint directory {} removed by garbage collection", cpt_dir);
            } else {
                LOG_ERROR_PREFIX("checkpoint directory {} remove failed by garbage collection",
                                 cpt_dir);
                put_back_list.push_back(del_d);
            }
        } else {
            LOG_INFO_PREFIX("checkpoint directory {} does not exist, ignored by garbage collection",
                            cpt_dir);
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

    LOG_INFO_PREFIX("after checkpoint garbage collection, checkpoints_count = {}, min_checkpoint = "
                    "{}, max_checkpoint = {}",
                    checkpoints_count,
                    min_d,
                    max_d);
}

int pegasus_server_impl::on_batched_write_requests(int64_t decree,
                                                   uint64_t timestamp,
                                                   dsn::message_ex **requests,
                                                   int count)
{
    CHECK(_is_open, "");
    CHECK_NOTNULL(requests, "");

    return _server_write->on_batched_write_requests(requests, count, decree, timestamp);
}

void pegasus_server_impl::on_get(get_rpc rpc)
{
    CHECK(_is_open, "");
    _pfc_get_qps->increment();
    uint64_t start_time = dsn_now_ns();

    const auto &key = rpc.request();
    auto &resp = rpc.response();
    resp.app_id = _gpid.get_app_id();
    resp.partition_index = _gpid.get_partition_index();
    resp.server = _primary_address;

    if (dsn_unlikely(FLAGS_inject_read_error_for_test != rocksdb::Status::kOk)) {
        resp.error = FLAGS_inject_read_error_for_test;
        return;
    }

    if (!_read_size_throttling_controller->available()) {
        rpc.error() = dsn::ERR_BUSY;
        _counter_recent_read_throttling_reject_count->increment();
        return;
    }

    rocksdb::Slice skey(key.data(), key.length());
    std::string value;
    rocksdb::Status status = _db->Get(_data_cf_rd_opts, _data_cf, skey, &value);

    if (status.ok()) {
        if (check_if_record_expired(utils::epoch_now(), value)) {
            _pfc_recent_expire_count->increment();
            if (FLAGS_rocksdb_verbose_log) {
                LOG_ERROR_PREFIX("rocksdb data expired for get from {}", rpc.remote_address());
            }
            status = rocksdb::Status::NotFound();
        }
    }

    if (!status.ok()) {
        if (FLAGS_rocksdb_verbose_log) {
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(key, hash_key, sort_key);
            LOG_ERROR_PREFIX("rocksdb get failed for get from {}: hash_key = \"{}\", sort_key = "
                             "\"{}\", error = {}",
                             rpc.remote_address(),
                             ::pegasus::utils::c_escape_sensitive_string(hash_key),
                             ::pegasus::utils::c_escape_sensitive_string(sort_key),
                             status.ToString());
        } else if (!status.IsNotFound()) {
            LOG_ERROR_PREFIX("rocksdb get failed for get from {}: error = {}",
                             rpc.remote_address(),
                             status.ToString());
        }
    }

#ifdef PEGASUS_UNIT_TEST
    // sleep 10ms for unit test,
    // so when we set slow_query_threshold <= 10ms, it will be a slow query
    usleep(10 * 1000);
#endif

    uint64_t time_used = dsn_now_ns() - start_time;
    if (is_get_abnormal(time_used, value.size())) {
        ::dsn::blob hash_key, sort_key;
        pegasus_restore_key(key, hash_key, sort_key);
        LOG_WARNING_PREFIX("rocksdb abnormal get from {}: "
                           "hash_key = {}, sort_key = {}, return = {}, "
                           "value_size = {}, time_used = {} ns",
                           rpc.remote_address(),
                           ::pegasus::utils::c_escape_sensitive_string(hash_key),
                           ::pegasus::utils::c_escape_sensitive_string(sort_key),
                           status.ToString(),
                           value.size(),
                           time_used);
        _pfc_recent_abnormal_count->increment();
    }

    resp.error = status.code();
    if (status.ok()) {
        pegasus_extract_user_data(_pegasus_data_version, std::move(value), resp.value);
    }

    _cu_calculator->add_get_cu(rpc.dsn_request(), resp.error, key, resp.value);
    _pfc_get_latency->set(dsn_now_ns() - start_time);
}

void pegasus_server_impl::on_multi_get(multi_get_rpc rpc)
{
    CHECK(_is_open, "");
    _pfc_multi_get_qps->increment();
    uint64_t start_time = dsn_now_ns();

    const auto &request = rpc.request();
    dsn::message_ex *req = rpc.dsn_request();
    auto &resp = rpc.response();
    resp.app_id = _gpid.get_app_id();
    resp.partition_index = _gpid.get_partition_index();
    resp.server = _primary_address;

    if (!_read_size_throttling_controller->available()) {
        rpc.error() = dsn::ERR_BUSY;
        _counter_recent_read_throttling_reject_count->increment();
        return;
    }

    if (!is_filter_type_supported(request.sort_key_filter_type)) {
        LOG_ERROR_PREFIX(
            "invalid argument for multi_get from {}: sort key filter type {} not supported",
            rpc.remote_address(),
            request.sort_key_filter_type);
        resp.error = rocksdb::Status::kInvalidArgument;
        _cu_calculator->add_multi_get_cu(req, resp.error, request.hash_key, resp.kvs);
        _pfc_multi_get_latency->set(dsn_now_ns() - start_time);
        return;
    }

    uint32_t max_kv_count = _rng_rd_opts.multi_get_max_iteration_count;
    uint32_t max_iteration_count = _rng_rd_opts.multi_get_max_iteration_count;
    if (request.max_kv_count > 0 && request.max_kv_count < max_kv_count) {
        max_kv_count = request.max_kv_count;
    }

    int32_t max_kv_size = request.max_kv_size > 0 ? request.max_kv_size : INT_MAX;
    int32_t max_iteration_size_config = _rng_rd_opts.multi_get_max_iteration_size > 0
                                            ? _rng_rd_opts.multi_get_max_iteration_size
                                            : INT_MAX;
    int32_t max_iteration_size = std::min(max_kv_size, max_iteration_size_config);

    uint32_t epoch_now = ::pegasus::utils::epoch_now();
    int32_t count = 0;
    int64_t size = 0;
    int32_t iteration_count = 0;
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
            if (FLAGS_rocksdb_verbose_log) {
                LOG_WARNING_PREFIX(
                    "empty sort key range for multi_get from {}: hash_key = \"{}\", start_sort_key "
                    "= \"{}\" ({}), stop_sort_key = \"{}\" ({}), sort_key_filter_type = {}, "
                    "sort_key_filter_pattern = \"{}\", final_start = \"{}\" ({}), final_stop = "
                    "\"{}\" ({})",
                    rpc.remote_address(),
                    ::pegasus::utils::c_escape_sensitive_string(request.hash_key),
                    ::pegasus::utils::c_escape_sensitive_string(request.start_sortkey),
                    request.start_inclusive ? "inclusive" : "exclusive",
                    ::pegasus::utils::c_escape_sensitive_string(request.stop_sortkey),
                    request.stop_inclusive ? "inclusive" : "exclusive",
                    ::dsn::apps::_filter_type_VALUES_TO_NAMES.find(request.sort_key_filter_type)
                        ->second,
                    ::pegasus::utils::c_escape_sensitive_string(request.sort_key_filter_pattern),
                    ::pegasus::utils::c_escape_sensitive_string(start),
                    start_inclusive ? "inclusive" : "exclusive",
                    ::pegasus::utils::c_escape_sensitive_string(stop),
                    stop_inclusive ? "inclusive" : "exclusive");
            }
            resp.error = rocksdb::Status::kOk;
            _cu_calculator->add_multi_get_cu(req, resp.error, request.hash_key, resp.kvs);
            _pfc_multi_get_latency->set(dsn_now_ns() - start_time);

            return;
        }

        std::unique_ptr<rocksdb::Iterator> it;
        bool complete = false;

        std::unique_ptr<range_read_limiter> limiter =
            std::make_unique<range_read_limiter>(max_iteration_count,
                                                 max_iteration_size,
                                                 _rng_rd_opts.rocksdb_iteration_threshold_time_ms);

        if (!request.reverse) {
            it.reset(_db->NewIterator(_data_cf_rd_opts, _data_cf));
            it->Seek(start);
            bool first_exclusive = !start_inclusive;
            while (count < max_kv_count && limiter->valid() && it->Valid()) {
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

                limiter->add_count();

                // extract value
                auto state = append_key_value_for_multi_get(resp.kvs,
                                                            it->key(),
                                                            it->value(),
                                                            request.sort_key_filter_type,
                                                            request.sort_key_filter_pattern,
                                                            epoch_now,
                                                            request.no_value);

                switch (state) {
                case range_iteration_state::kNormal: {
                    count++;
                    auto &kv = resp.kvs.back();
                    uint64_t kv_size = kv.key.length() + kv.value.length();
                    size += kv_size;
                    limiter->add_size(kv_size);
                } break;
                case range_iteration_state::kExpired:
                    expire_count++;
                    break;
                case range_iteration_state::kFiltered:
                    filter_count++;
                    break;
                default:
                    break;
                }

                if (c == 0) {
                    // if arrived to the last position
                    complete = true;
                    break;
                }

                it->Next();
            }
        } else { // reverse
            rocksdb::ReadOptions rd_opts(_data_cf_rd_opts);
            if (_data_cf_opts.prefix_extractor) {
                // NOTE: Prefix bloom filter is not supported in reverse seek mode (see
                // https://github.com/facebook/rocksdb/wiki/Prefix-Seek-API-Changes#limitation for
                // more details), and we have to do total order seek on rocksdb which might be worse
                // performance. However we consider that reverse scan is a rare use case, and if
                // your workload has many reverse scans, you'd better use 'common' bloom filter (by
                // set [pegasus.server]rocksdb_filter_type to 'common').
                rd_opts.total_order_seek = true;
                rd_opts.prefix_same_as_start = false;
            }
            it.reset(_db->NewIterator(rd_opts, _data_cf));
            it->SeekForPrev(stop);
            bool first_exclusive = !stop_inclusive;
            std::vector<::dsn::apps::key_value> reverse_kvs;
            while (count < max_kv_count && limiter->valid() && it->Valid()) {
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

                limiter->add_count();

                // extract value
                auto state = append_key_value_for_multi_get(reverse_kvs,
                                                            it->key(),
                                                            it->value(),
                                                            request.sort_key_filter_type,
                                                            request.sort_key_filter_pattern,
                                                            epoch_now,
                                                            request.no_value);
                switch (state) {
                case range_iteration_state::kNormal: {
                    count++;
                    auto &kv = reverse_kvs.back();
                    uint64_t kv_size = kv.key.length() + kv.value.length();
                    size += kv_size;
                    limiter->add_size(kv_size);
                } break;
                case range_iteration_state::kExpired:
                    expire_count++;
                    break;
                case range_iteration_state::kFiltered:
                    filter_count++;
                    break;
                default:
                    break;
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

        iteration_count = limiter->get_iteration_count();
        resp.error = it->status().code();
        if (!it->status().ok()) {
            // error occur
            if (FLAGS_rocksdb_verbose_log) {
                LOG_ERROR_PREFIX("rocksdb scan failed for multi_get from {}: hash_key = \"{}\", "
                                 "reverse = {}, error = {}",
                                 rpc.remote_address(),
                                 ::pegasus::utils::c_escape_sensitive_string(request.hash_key),
                                 request.reverse ? "true" : "false",
                                 it->status().ToString());
            } else {
                LOG_ERROR_PREFIX(
                    "rocksdb scan failed for multi_get from {}: reverse = {}, error = {}",
                    rpc.remote_address(),
                    request.reverse ? "true" : "false",
                    it->status().ToString());
            }
            resp.kvs.clear();
        } else if (it->Valid() && !complete) {
            // scan not completed
            resp.error = rocksdb::Status::kIncomplete;
            if (limiter->exceed_limit()) {
                LOG_WARNING_PREFIX(
                    "rocksdb abnormal scan from {}: time_used({}ns) VS time_threshold({}ns)",
                    rpc.remote_address(),
                    limiter->duration_time(),
                    limiter->max_duration_time());
            }
        }
    } else { // condition: !request.sort_keys.empty()
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

        std::vector<rocksdb::Status> statuses = _db->MultiGet(_data_cf_rd_opts, keys, &values);
        for (int i = 0; i < keys.size(); i++) {
            rocksdb::Status &status = statuses[i];
            std::string &value = values[i];
            // print log
            if (!status.ok()) {
                if (FLAGS_rocksdb_verbose_log) {
                    LOG_ERROR_PREFIX(
                        "rocksdb get failed for multi_get from {}: hash_key = \"{}\", "
                        "sort_key = \"{}\", error = {}",
                        rpc.remote_address(),
                        ::pegasus::utils::c_escape_sensitive_string(request.hash_key),
                        ::pegasus::utils::c_escape_sensitive_string(request.sort_keys[i]),
                        status.ToString());
                } else if (!status.IsNotFound()) {
                    LOG_ERROR_PREFIX("rocksdb get failed for multi_get from {}: error = {}",
                                     rpc.remote_address(),
                                     status.ToString());
                }
            }
            // check ttl
            if (status.ok()) {
                uint32_t expire_ts = pegasus_extract_expire_ts(_pegasus_data_version, value);
                if (expire_ts > 0 && expire_ts <= epoch_now) {
                    expire_count++;
                    if (FLAGS_rocksdb_verbose_log) {
                        LOG_ERROR_PREFIX("rocksdb data expired for multi_get from {}",
                                         rpc.remote_address());
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
                    pegasus_extract_user_data(_pegasus_data_version, std::move(value), kv.value);
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

#ifdef PEGASUS_UNIT_TEST
    // sleep 10ms for unit test
    usleep(10 * 1000);
#endif

    uint64_t time_used = dsn_now_ns() - start_time;
    if (is_multi_get_abnormal(time_used, size, iteration_count)) {
        LOG_WARNING_PREFIX(
            "rocksdb abnormal multi_get from {}: hash_key = {}, "
            "start_sort_key = {} ({}), stop_sort_key = {} ({}), "
            "sort_key_filter_type = {}, sort_key_filter_pattern = {}, "
            "max_kv_count = {}, max_kv_size = {}, reverse = {}, "
            "result_count = {}, result_size = {}, iteration_count = {}, "
            "expire_count = {}, filter_count = {}, time_used = {} ns",
            rpc.remote_address(),
            ::pegasus::utils::c_escape_sensitive_string(request.hash_key),
            ::pegasus::utils::c_escape_sensitive_string(request.start_sortkey),
            request.start_inclusive ? "inclusive" : "exclusive",
            ::pegasus::utils::c_escape_sensitive_string(request.stop_sortkey),
            request.stop_inclusive ? "inclusive" : "exclusive",
            ::dsn::apps::_filter_type_VALUES_TO_NAMES.find(request.sort_key_filter_type)->second,
            ::pegasus::utils::c_escape_sensitive_string(request.sort_key_filter_pattern),
            request.max_kv_count,
            request.max_kv_size,
            request.reverse ? "true" : "false",
            count,
            size,
            iteration_count,
            expire_count,
            filter_count,
            time_used);
        _pfc_recent_abnormal_count->increment();
    }

    if (expire_count > 0) {
        _pfc_recent_expire_count->add(expire_count);
    }
    if (filter_count > 0) {
        _pfc_recent_filter_count->add(filter_count);
    }

    _cu_calculator->add_multi_get_cu(req, resp.error, request.hash_key, resp.kvs);
    _pfc_multi_get_latency->set(dsn_now_ns() - start_time);
}

void pegasus_server_impl::on_batch_get(batch_get_rpc rpc)
{
    CHECK(_is_open, "");
    _pfc_batch_get_qps->increment();
    int64_t start_time = dsn_now_ns();

    auto &response = rpc.response();
    response.app_id = _gpid.get_app_id();
    response.partition_index = _gpid.get_partition_index();
    response.server = _primary_address;

    if (!_read_size_throttling_controller->available()) {
        rpc.error() = dsn::ERR_BUSY;
        _counter_recent_read_throttling_reject_count->increment();
        return;
    }

    const auto &request = rpc.request();
    if (request.keys.empty()) {
        response.error = rocksdb::Status::kInvalidArgument;
        LOG_ERROR_PREFIX("Invalid argument for batch_get from {}: 'keys' field in request is empty",
                         rpc.remote_address().to_string());
        _cu_calculator->add_batch_get_cu(rpc.dsn_request(), response.error, response.data);
        _pfc_batch_get_latency->set(dsn_now_ns() - start_time);
        return;
    }

    std::vector<rocksdb::Slice> keys;
    keys.reserve(request.keys.size());
    std::vector<::dsn::blob> keys_holder;
    keys_holder.reserve(request.keys.size());
    for (const auto &key : request.keys) {
        dsn::blob raw_key;
        pegasus_generate_key(raw_key, key.hash_key, key.sort_key);
        keys.emplace_back(rocksdb::Slice(raw_key.data(), raw_key.length()));
        keys_holder.emplace_back(std::move(raw_key));
    }

    rocksdb::Status final_status;
    bool error_occurred = false;
    int64_t total_data_size = 0;
    uint32_t epoch_now = pegasus::utils::epoch_now();
    std::vector<std::string> values;
    std::vector<rocksdb::Status> statuses = _db->MultiGet(_data_cf_rd_opts, keys, &values);
    response.data.reserve(request.keys.size());
    for (int i = 0; i < keys.size(); i++) {
        const auto &status = statuses[i];
        if (status.IsNotFound()) {
            continue;
        }

        const ::dsn::blob &hash_key = request.keys[i].hash_key;
        const ::dsn::blob &sort_key = request.keys[i].sort_key;
        std::string &value = values[i];

        if (dsn_likely(status.ok())) {
            if (check_if_record_expired(epoch_now, value)) {
                if (FLAGS_rocksdb_verbose_log) {
                    LOG_ERROR_PREFIX(
                        "rocksdb data expired for batch_get from {}, hash_key = {}, sort_key = {}",
                        rpc.remote_address().to_string(),
                        pegasus::utils::c_escape_sensitive_string(hash_key),
                        pegasus::utils::c_escape_sensitive_string(sort_key));
                }
                continue;
            }

            dsn::blob real_value;
            pegasus_extract_user_data(_pegasus_data_version, std::move(value), real_value);
            dsn::apps::full_data current_data;
            current_data.hash_key = hash_key;
            current_data.sort_key = sort_key;
            current_data.value = std::move(real_value);
            total_data_size += current_data.value.size();
            response.data.emplace_back(std::move(current_data));
        } else {
            if (FLAGS_rocksdb_verbose_log) {
                LOG_ERROR_PREFIX(
                    "rocksdb get failed for batch_get from {}:  error = {}, key size = {}",
                    rpc.remote_address().to_string(),
                    status.ToString(),
                    request.keys.size());
            } else {
                LOG_ERROR_PREFIX("rocksdb get failed for batch_get from {}: error = {}",
                                 rpc.remote_address().to_string(),
                                 status.ToString());
            }

            error_occurred = true;
            final_status = status;
            break;
        }
    }

    if (error_occurred) {
        response.error = final_status.code();
        response.data.clear();
    } else {
        response.error = rocksdb::Status::kOk;
    }

    int64_t time_used = dsn_now_ns() - start_time;
    if (is_batch_get_abnormal(time_used, total_data_size, request.keys.size())) {
        LOG_WARNING_PREFIX(
            "rocksdb abnormal batch_get from {}: total data size = {}, row count = {}, "
            "time_used = {} us",
            rpc.remote_address(),
            total_data_size,
            request.keys.size(),
            time_used / 1000);
        _pfc_recent_abnormal_count->increment();
    }

    _cu_calculator->add_batch_get_cu(rpc.dsn_request(), response.error, response.data);
    _pfc_batch_get_latency->set(time_used);
}

void pegasus_server_impl::on_sortkey_count(sortkey_count_rpc rpc)
{
    CHECK(_is_open, "");

    _pfc_scan_qps->increment();
    uint64_t start_time = dsn_now_ns();

    const auto &hash_key = rpc.request();
    auto &resp = rpc.response();
    resp.app_id = _gpid.get_app_id();
    resp.partition_index = _gpid.get_partition_index();
    resp.server = _primary_address;
    if (!_read_size_throttling_controller->available()) {
        rpc.error() = dsn::ERR_BUSY;
        _counter_recent_read_throttling_reject_count->increment();
        return;
    }

    // scan
    ::dsn::blob start_key, stop_key;
    pegasus_generate_key(start_key, hash_key, ::dsn::blob());
    pegasus_generate_next_blob(stop_key, hash_key);
    rocksdb::Slice start(start_key.data(), start_key.length());
    rocksdb::Slice stop(stop_key.data(), stop_key.length());
    rocksdb::ReadOptions options = _data_cf_rd_opts;
    options.iterate_upper_bound = &stop;
    std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(options, _data_cf));
    it->Seek(start);
    resp.count = 0;
    uint32_t epoch_now = ::pegasus::utils::epoch_now();
    uint64_t expire_count = 0;

    std::unique_ptr<range_read_limiter> limiter =
        std::make_unique<range_read_limiter>(_rng_rd_opts.rocksdb_max_iteration_count,
                                             0,
                                             _rng_rd_opts.rocksdb_iteration_threshold_time_ms);
    while (limiter->time_check() && it->Valid()) {
        limiter->add_count();

        if (check_if_record_expired(epoch_now, it->value())) {
            expire_count++;
            if (FLAGS_rocksdb_verbose_log) {
                LOG_ERROR_PREFIX("rocksdb data expired for sortkey_count from {}",
                                 rpc.remote_address());
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
        if (FLAGS_rocksdb_verbose_log) {
            LOG_ERROR_PREFIX(
                "rocksdb scan failed for sortkey_count from {}: hash_key = \"{}\", error = {}",
                rpc.remote_address(),
                ::pegasus::utils::c_escape_sensitive_string(hash_key),
                it->status().ToString());
        } else {
            LOG_ERROR_PREFIX("rocksdb scan failed for sortkey_count from {}: error = {}",
                             rpc.remote_address(),
                             it->status().ToString());
        }
        resp.count = 0;
    } else if (limiter->exceed_limit()) {
        LOG_WARNING_PREFIX("rocksdb abnormal scan from {}: time_used({}ns) VS time_threshold({}ns)",
                           rpc.remote_address(),
                           limiter->duration_time(),
                           limiter->max_duration_time());
        resp.count = -1;
    }

    _cu_calculator->add_sortkey_count_cu(rpc.dsn_request(), resp.error, hash_key);
    _pfc_scan_latency->set(dsn_now_ns() - start_time);
}

void pegasus_server_impl::on_ttl(ttl_rpc rpc)
{
    CHECK(_is_open, "");

    const auto &key = rpc.request();
    auto &resp = rpc.response();
    resp.app_id = _gpid.get_app_id();
    resp.partition_index = _gpid.get_partition_index();
    resp.server = _primary_address;

    if (!_read_size_throttling_controller->available()) {
        rpc.error() = dsn::ERR_BUSY;
        _counter_recent_read_throttling_reject_count->increment();
        return;
    }

    rocksdb::Slice skey(key.data(), key.length());
    std::string value;
    rocksdb::Status status = _db->Get(_data_cf_rd_opts, _data_cf, skey, &value);

    uint32_t expire_ts = 0;
    uint32_t now_ts = ::pegasus::utils::epoch_now();
    if (status.ok()) {
        expire_ts = pegasus_extract_expire_ts(_pegasus_data_version, value);
        if (check_if_ts_expired(now_ts, expire_ts)) {
            _pfc_recent_expire_count->increment();
            if (FLAGS_rocksdb_verbose_log) {
                LOG_ERROR_PREFIX("rocksdb data expired for ttl from {}", rpc.remote_address());
            }
            status = rocksdb::Status::NotFound();
        }
    }

    if (!status.ok()) {
        if (FLAGS_rocksdb_verbose_log) {
            ::dsn::blob hash_key, sort_key;
            pegasus_restore_key(key, hash_key, sort_key);
            LOG_ERROR_PREFIX("rocksdb get failed for ttl from {}: hash_key = \"{}\", sort_key = "
                             "\"{}\", error = {}",
                             rpc.remote_address(),
                             ::pegasus::utils::c_escape_sensitive_string(hash_key),
                             ::pegasus::utils::c_escape_sensitive_string(sort_key),
                             status.ToString());
        } else if (!status.IsNotFound()) {
            LOG_ERROR_PREFIX("rocksdb get failed for ttl from {}: error = {}",
                             rpc.remote_address(),
                             status.ToString());
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

    _cu_calculator->add_ttl_cu(rpc.dsn_request(), resp.error, key);
}

void pegasus_server_impl::on_get_scanner(get_scanner_rpc rpc)
{
    CHECK(_is_open, "");
    _pfc_scan_qps->increment();
    uint64_t start_time = dsn_now_ns();

    const auto &request = rpc.request();
    dsn::message_ex *req = rpc.dsn_request();
    auto &resp = rpc.response();
    resp.app_id = _gpid.get_app_id();
    resp.partition_index = _gpid.get_partition_index();
    resp.server = _primary_address;

    if (!_read_size_throttling_controller->available()) {
        rpc.error() = dsn::ERR_BUSY;
        _counter_recent_read_throttling_reject_count->increment();
        return;
    }

    if (!is_filter_type_supported(request.hash_key_filter_type)) {
        LOG_ERROR_PREFIX(
            "invalid argument for get_scanner from {}: hash key filter type {} not supported",
            rpc.remote_address(),
            request.hash_key_filter_type);
        resp.error = rocksdb::Status::kInvalidArgument;
        _cu_calculator->add_scan_cu(req, resp.error, resp.kvs);
        _pfc_scan_latency->set(dsn_now_ns() - start_time);

        return;
    }
    if (!is_filter_type_supported(request.sort_key_filter_type)) {
        LOG_ERROR_PREFIX(
            "invalid argument for get_scanner from {}: sort key filter type {} not supported",
            rpc.remote_address(),
            request.sort_key_filter_type);
        resp.error = rocksdb::Status::kInvalidArgument;
        _cu_calculator->add_scan_cu(req, resp.error, resp.kvs);
        _pfc_scan_latency->set(dsn_now_ns() - start_time);

        return;
    }

    rocksdb::ReadOptions rd_opts(_data_cf_rd_opts);
    if (_data_cf_opts.prefix_extractor) {
        ::dsn::blob start_hash_key, tmp;
        pegasus_restore_key(request.start_key, start_hash_key, tmp);
        if (start_hash_key.size() == 0 || request.full_scan) {
            // hash_key is not passed, only happened when do full scan (scanners got by
            // get_unordered_scanners) on a partition, we have to do total order seek on rocksDB.
            rd_opts.total_order_seek = true;
            rd_opts.prefix_same_as_start = false;
        }
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
            // Now 'start' is generated by 'request.hash_key_filter_pattern', it may be not a real
            // hashkey, we should not seek this prefix by prefix bloom filter. However, it only
            // happen when do full scan (scanners got by get_unordered_scanners), in which case the
            // following flags has been updated.
            CHECK(!_data_cf_opts.prefix_extractor || rd_opts.total_order_seek, "Invalid option");
            CHECK(!_data_cf_opts.prefix_extractor || !rd_opts.prefix_same_as_start,
                  "Invalid option");
        }
    }

    // check if range is empty
    int c = start.compare(stop);
    if (c > 0 || (c == 0 && (!start_inclusive || !stop_inclusive))) {
        // empty key range
        if (FLAGS_rocksdb_verbose_log) {
            LOG_WARNING_PREFIX("empty key range for get_scanner from {}: start_key = \"{}\" ({}), "
                               "stop_key = \"{}\" ({})",
                               rpc.remote_address(),
                               ::pegasus::utils::c_escape_sensitive_string(request.start_key),
                               request.start_inclusive ? "inclusive" : "exclusive",
                               ::pegasus::utils::c_escape_sensitive_string(request.stop_key),
                               request.stop_inclusive ? "inclusive" : "exclusive");
        }
        resp.error = rocksdb::Status::kOk;
        _cu_calculator->add_scan_cu(req, resp.error, resp.kvs);
        _pfc_scan_latency->set(dsn_now_ns() - start_time);

        return;
    }

    std::unique_ptr<rocksdb::Iterator> it(_db->NewIterator(rd_opts, _data_cf));
    it->Seek(start);
    bool complete = false;
    bool first_exclusive = !start_inclusive;
    uint32_t epoch_now = ::pegasus::utils::epoch_now();
    uint64_t expire_count = 0;
    uint64_t filter_count = 0;
    int32_t count = 0;

    uint32_t batch_count = _rng_rd_opts.rocksdb_max_iteration_count;
    if (request.batch_size > 0 && request.batch_size < batch_count) {
        batch_count = request.batch_size;
    }
    resp.kvs.reserve(batch_count);

    bool return_expire_ts = request.__isset.return_expire_ts ? request.return_expire_ts : false;
    bool only_return_count = request.__isset.only_return_count ? request.only_return_count : false;

    std::unique_ptr<range_read_limiter> limiter =
        std::make_unique<range_read_limiter>(_rng_rd_opts.rocksdb_max_iteration_count,
                                             0,
                                             _rng_rd_opts.rocksdb_iteration_threshold_time_ms);

    while (count < batch_count && limiter->valid() && it->Valid()) {
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

        limiter->add_count();

        auto state = validate_key_value_for_scan(
            it->key(),
            it->value(),
            request.hash_key_filter_type,
            request.hash_key_filter_pattern,
            request.sort_key_filter_type,
            request.sort_key_filter_pattern,
            epoch_now,
            request.__isset.validate_partition_hash ? request.validate_partition_hash : true);

        switch (state) {
        case range_iteration_state::kNormal:
            count++;
            if (!only_return_count) {
                append_key_value(
                    resp.kvs, it->key(), it->value(), request.no_value, return_expire_ts);
            }
            break;
        case range_iteration_state::kExpired:
            expire_count++;
            break;
        case range_iteration_state::kFiltered:
            filter_count++;
            break;
        default:
            break;
        }

        if (c == 0) {
            // seek to the last position
            complete = true;
            break;
        }

        it->Next();
    }
    if (only_return_count) {
        resp.__set_kv_count(count);
    }

    // check iteration time whether exceed limit
    if (!complete) {
        limiter->time_check_after_incomplete_scan();
    }

    resp.error = it->status().code();
    if (!it->status().ok()) {
        // error occur
        if (FLAGS_rocksdb_verbose_log) {
            LOG_ERROR_PREFIX("rocksdb scan failed for get_scanner from {}: start_key = \"{}\" "
                             "({}), stop_key = \"{}\" ({}), batch_size = {}, read_count = {}, "
                             "error = {}",
                             rpc.remote_address(),
                             ::pegasus::utils::c_escape_sensitive_string(start),
                             request.start_inclusive ? "inclusive" : "exclusive",
                             ::pegasus::utils::c_escape_sensitive_string(stop),
                             request.stop_inclusive ? "inclusive" : "exclusive",
                             batch_count,
                             count,
                             it->status().ToString());
        } else {
            LOG_ERROR_PREFIX("rocksdb scan failed for get_scanner from {}: error = {}",
                             rpc.remote_address(),
                             it->status().ToString());
        }
        resp.kvs.clear();
    } else if (limiter->exceed_limit()) {
        // scan exceed limit time
        resp.error = rocksdb::Status::kIncomplete;
        LOG_WARNING_PREFIX("rocksdb abnormal scan from {}: batch_count={}, time_used_ns({}) VS "
                           "time_threshold_ns({})",
                           rpc.remote_address(),
                           batch_count,
                           limiter->duration_time(),
                           limiter->max_duration_time());
    } else if (it->Valid() && !complete) {
        // scan not completed
        std::unique_ptr<pegasus_scan_context> context(new pegasus_scan_context(
            std::move(it),
            std::string(stop.data(), stop.size()),
            request.stop_inclusive,
            request.hash_key_filter_type,
            std::string(request.hash_key_filter_pattern.data(),
                        request.hash_key_filter_pattern.length()),
            request.sort_key_filter_type,
            std::string(request.sort_key_filter_pattern.data(),
                        request.sort_key_filter_pattern.length()),
            batch_count,
            request.no_value,
            request.__isset.validate_partition_hash ? request.validate_partition_hash : true,
            return_expire_ts,
            only_return_count));
        int64_t handle = _context_cache.put(std::move(context));
        resp.context_id = handle;
        // if the context is used, it will be fetched and re-put into cache,
        // which will change the handle,
        // then the delayed task will fetch null context by old handle, and do nothing.
        ::dsn::tasking::enqueue(LPC_PEGASUS_SERVER_DELAY,
                                &_tracker,
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

    _cu_calculator->add_scan_cu(req, resp.error, resp.kvs);
    _pfc_scan_latency->set(dsn_now_ns() - start_time);
}

void pegasus_server_impl::on_scan(scan_rpc rpc)
{
    CHECK(_is_open, "");
    _pfc_scan_qps->increment();
    uint64_t start_time = dsn_now_ns();
    const auto &request = rpc.request();
    dsn::message_ex *req = rpc.dsn_request();
    auto &resp = rpc.response();
    resp.app_id = _gpid.get_app_id();
    resp.partition_index = _gpid.get_partition_index();
    resp.server = _primary_address;

    if (!_read_size_throttling_controller->available()) {
        rpc.error() = dsn::ERR_BUSY;
        _counter_recent_read_throttling_reject_count->increment();
        return;
    }

    std::unique_ptr<pegasus_scan_context> context = _context_cache.fetch(request.context_id);
    if (context) {
        rocksdb::Iterator *it = context->iterator.get();
        const rocksdb::Slice &stop = context->stop;
        bool stop_inclusive = context->stop_inclusive;
        ::dsn::apps::filter_type::type hash_key_filter_type = context->hash_key_filter_type;
        const ::dsn::blob &hash_key_filter_pattern = context->hash_key_filter_pattern;
        ::dsn::apps::filter_type::type sort_key_filter_type = context->sort_key_filter_type;
        const ::dsn::blob &sort_key_filter_pattern = context->sort_key_filter_pattern;
        bool no_value = context->no_value;
        bool validate_hash = context->validate_partition_hash;
        bool return_expire_ts = context->return_expire_ts;
        bool complete = false;
        uint32_t epoch_now = ::pegasus::utils::epoch_now();
        uint64_t expire_count = 0;
        uint64_t filter_count = 0;
        int32_t count = 0;

        uint32_t batch_count = _rng_rd_opts.rocksdb_max_iteration_count;
        if (context->batch_size > 0 && context->batch_size < batch_count) {
            batch_count = context->batch_size;
        }

        std::unique_ptr<range_read_limiter> limiter = std::make_unique<range_read_limiter>(
            batch_count, 0, _rng_rd_opts.rocksdb_iteration_threshold_time_ms);

        while (count < batch_count && limiter->valid() && it->Valid()) {
            int c = it->key().compare(stop);
            if (c > 0 || (c == 0 && !stop_inclusive)) {
                // out of range
                complete = true;
                break;
            }

            limiter->add_count();

            auto state = validate_key_value_for_scan(it->key(),
                                                     it->value(),
                                                     hash_key_filter_type,
                                                     hash_key_filter_pattern,
                                                     sort_key_filter_type,
                                                     sort_key_filter_pattern,
                                                     epoch_now,
                                                     validate_hash);

            switch (state) {
            case range_iteration_state::kNormal:
                count++;
                if (!context->only_return_count) {
                    append_key_value(resp.kvs, it->key(), it->value(), no_value, return_expire_ts);
                }
                break;
            case range_iteration_state::kExpired:
                expire_count++;
                break;
            case range_iteration_state::kFiltered:
                filter_count++;
                break;
            default:
                break;
            }

            if (c == 0) {
                // seek to the last position
                complete = true;
                break;
            }

            it->Next();
        }

        if (context->only_return_count) {
            resp.__set_kv_count(count);
        }

        // check iteration time whether exceed limit
        if (!complete) {
            limiter->time_check_after_incomplete_scan();
        }

        resp.error = it->status().code();
        if (!it->status().ok()) {
            // error occur
            if (FLAGS_rocksdb_verbose_log) {
                LOG_ERROR_PREFIX("rocksdb scan failed for scan from {}: context_id= {}, stop_key = "
                                 "\"{}\" ({}), batch_size = {}, read_count = {}, error = {}",
                                 rpc.remote_address(),
                                 request.context_id,
                                 ::pegasus::utils::c_escape_sensitive_string(stop),
                                 stop_inclusive ? "inclusive" : "exclusive",
                                 batch_count,
                                 count,
                                 it->status().ToString());
            } else {
                LOG_ERROR_PREFIX("rocksdb scan failed for scan from {}: error = {}",
                                 rpc.remote_address(),
                                 it->status().ToString());
            }
            resp.kvs.clear();
        } else if (limiter->exceed_limit()) {
            // scan exceed limit time
            resp.error = rocksdb::Status::kIncomplete;
            LOG_WARNING_PREFIX("rocksdb abnormal scan from {}: batch_count={}, time_used({}ns) VS "
                               "time_threshold({}ns)",
                               rpc.remote_address(),
                               batch_count,
                               limiter->duration_time(),
                               limiter->max_duration_time());
        } else if (it->Valid() && !complete) {
            // scan not completed
            int64_t handle = _context_cache.put(std::move(context));
            resp.context_id = handle;
            ::dsn::tasking::enqueue(LPC_PEGASUS_SERVER_DELAY,
                                    &_tracker,
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

    _cu_calculator->add_scan_cu(req, resp.error, resp.kvs);
    _pfc_scan_latency->set(dsn_now_ns() - start_time);
}

void pegasus_server_impl::on_clear_scanner(const int64_t &args) { _context_cache.fetch(args); }

dsn::error_code pegasus_server_impl::start(int argc, char **argv)
{
    CHECK_PREFIX_MSG(!_is_open, "replica is already opened");
    LOG_INFO_PREFIX("start to open app {}", data_dir());

    // parse envs for parameters
    // envs is compounded in replication_app_base::open() function
    std::map<std::string, std::string> envs;
    if (argc > 0) {
        if ((argc - 1) % 2 != 0) {
            LOG_ERROR_PREFIX("parse envs failed, invalid argc = {}", argc);
            return dsn::ERR_INVALID_PARAMETERS;
        }
        if (argv == nullptr) {
            LOG_ERROR_PREFIX("parse envs failed, invalid argv = nullptr");
            return dsn::ERR_INVALID_PARAMETERS;
        }
        int idx = 1;
        while (idx < argc) {
            const char *key = argv[idx++];
            const char *value = argv[idx++];
            envs.emplace(key, value);
        }
    }
    // Update all envs before opening db, ensure all envs are effective for the newly opened db.
    update_app_envs_before_open_db(envs);

    // TODO(yingchun): refactor the following code
    //
    // here, we must distinguish three cases, such as:
    //  case 1: we open the db that already exist
    //  case 2: we load duplication data base checkpoint from master
    //  case 3: we open a new db
    //  case 4: we restore the db base on old data
    //
    // if we want to restore the db base on old data, only all of the restore preconditions are
    // satisfied
    //      restore preconditions:
    //          1, rdb isn't exist
    //          2, we can parse restore info from app env, which is stored in argv
    //          3, restore_dir is exist
    //
    bool db_exist = true;
    auto rdb_path = dsn::utils::filesystem::path_combine(data_dir(), "rdb");
    auto duplication_path = duplication_dir();
    if (dsn::utils::filesystem::path_exists(rdb_path)) {
        // only case 1
        LOG_INFO_PREFIX("rdb is already exist, path = {}", rdb_path);
    } else {
        // case 2
        if (dsn::utils::filesystem::path_exists(duplication_path) && is_duplication_follower()) {
            if (!dsn::utils::filesystem::rename_path(duplication_path, rdb_path)) {
                LOG_ERROR_PREFIX(
                    "load duplication data from {} to {} failed", duplication_path, rdb_path);
                return dsn::ERR_FILE_OPERATION_FAILED;
            }
        } else {
            std::pair<std::string, bool> restore_info = get_restore_dir_from_env(envs);
            const std::string &restore_dir = restore_info.first;
            bool force_restore = restore_info.second;
            if (restore_dir.empty()) {
                // case 3
                if (force_restore) {
                    LOG_ERROR_PREFIX("try to restore, but we can't combine restore_dir from envs");
                    return dsn::ERR_FILE_OPERATION_FAILED;
                } else {
                    db_exist = false;
                    LOG_DEBUG_PREFIX("open a new db, path = {}", rdb_path);
                }
            } else {
                // case 4
                LOG_INFO_PREFIX("try to restore from restore_dir = {}", restore_dir);
                if (dsn::utils::filesystem::directory_exists(restore_dir)) {
                    // here, we just rename restore_dir to rdb, then continue the normal process
                    if (dsn::utils::filesystem::rename_path(restore_dir, rdb_path)) {
                        LOG_INFO_PREFIX(
                            "rename restore_dir({}) to rdb({}) succeed", restore_dir, rdb_path);
                    } else {
                        LOG_ERROR_PREFIX(
                            "rename restore_dir({}) to rdb({}) failed", restore_dir, rdb_path);
                        return dsn::ERR_FILE_OPERATION_FAILED;
                    }
                } else {
                    if (force_restore) {
                        LOG_ERROR_PREFIX(
                            "try to restore, but restore_dir isn't exist, restore_dir = {}",
                            restore_dir);
                        return dsn::ERR_FILE_OPERATION_FAILED;
                    } else {
                        db_exist = false;
                        LOG_WARNING_PREFIX(
                            "try to restore and restore_dir({}) isn't exist, but we don't force "
                            "it, the role of this replica must not primary, so we open a new db on "
                            "the "
                            "path({})",
                            restore_dir,
                            rdb_path);
                    }
                }
            }
        }
    }

    LOG_INFO_PREFIX("start to open rocksDB's rdb({})", rdb_path);

    // Here we create a `_table_data_cf_opts` because we don't want to modify `_data_cf_opts`, which
    // will be used elsewhere.
    _table_data_cf_opts = _data_cf_opts;
    _table_data_cf_opts_recalculated = false;
    bool has_incompatible_db_options = false;
    if (db_exist) {
        // When DB exists, meta CF and data CF must be present.
        bool missing_meta_cf = true;
        bool missing_data_cf = true;
        auto ec = check_column_families(rdb_path, &missing_meta_cf, &missing_data_cf);
        if (ec != dsn::ERR_OK) {
            LOG_ERROR_PREFIX("check column families failed");
            return ec;
        }
        CHECK_PREFIX_MSG(!missing_meta_cf, "You must upgrade Pegasus server from 2.0");
        CHECK_PREFIX_MSG(!missing_data_cf, "Missing data column family");

        // Load latest options from option file stored in the db directory.
        rocksdb::DBOptions loaded_db_opt;
        std::vector<rocksdb::ColumnFamilyDescriptor> loaded_cf_descs;
        rocksdb::ColumnFamilyOptions loaded_data_cf_opts;
        rocksdb::ConfigOptions config_options;
        // Set `ignore_unknown_options` true for forward compatibility.
        config_options.ignore_unknown_options = true;
        config_options.env = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive);
        auto status =
            rocksdb::LoadLatestOptions(config_options, rdb_path, &loaded_db_opt, &loaded_cf_descs);
        if (!status.ok()) {
            // Here we ignore an invalid argument error related to `pegasus_data_version` and
            // `pegasus_data` options, which were used in old version rocksdbs (before 2.1.0).
            if (status.code() != rocksdb::Status::kInvalidArgument ||
                status.ToString().find("pegasus_data") == std::string::npos) {
                LOG_ERROR_PREFIX("load latest option file failed: {}.", status.ToString());
                return dsn::ERR_LOCAL_APP_FAILURE;
            }
            has_incompatible_db_options = true;
            LOG_WARNING_PREFIX(
                "The latest option file has incompatible db options: {}, use default "
                "options to open db.",
                status.ToString());
        }

        if (!has_incompatible_db_options) {
            for (int i = 0; i < loaded_cf_descs.size(); ++i) {
                if (loaded_cf_descs[i].name == DATA_COLUMN_FAMILY_NAME) {
                    loaded_data_cf_opts = loaded_cf_descs[i].options;
                }
            }
            // Reset usage scenario related options according to loaded_data_cf_opts.
            // We don't use `loaded_data_cf_opts` directly because pointer-typed options will
            // only be initialized with default values when calling 'LoadLatestOptions', see
            // 'rocksdb/utilities/options_util.h'.
            reset_rocksdb_options(
                loaded_data_cf_opts, loaded_db_opt, envs, &_table_data_cf_opts, &_db_opts);
        }
    } else {
        // When create new DB, we have to create a new column family to store meta data (meta column
        // family).
        _db_opts.create_missing_column_families = true;
        _db_opts.allow_ingest_behind = parse_allow_ingest_behind(envs);
    }

    std::vector<rocksdb::ColumnFamilyDescriptor> column_families(
        {{DATA_COLUMN_FAMILY_NAME, _table_data_cf_opts}, {META_COLUMN_FAMILY_NAME, _meta_cf_opts}});
    rocksdb::ConfigOptions config_options;
    config_options.ignore_unknown_options = true;
    config_options.ignore_unsupported_options = true;
    config_options.sanity_level =
        rocksdb::ConfigOptions::SanityLevel::kSanityLevelLooselyCompatible;
    config_options.env = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive);
    auto s =
        rocksdb::CheckOptionsCompatibility(config_options, rdb_path, _db_opts, column_families);
    if (!s.ok() && !s.IsNotFound() && !has_incompatible_db_options) {
        LOG_ERROR_PREFIX("rocksdb::CheckOptionsCompatibility failed, error = {}", s.ToString());
        return dsn::ERR_LOCAL_APP_FAILURE;
    }
    std::vector<rocksdb::ColumnFamilyHandle *> handles_opened;
    auto status = rocksdb::DB::Open(_db_opts, rdb_path, column_families, &handles_opened, &_db);
    if (!status.ok()) {
        LOG_ERROR_PREFIX("rocksdb::DB::Open failed, error = {}", status.ToString());
        return dsn::ERR_LOCAL_APP_FAILURE;
    }
    CHECK_EQ_PREFIX(2, handles_opened.size());
    CHECK_EQ_PREFIX(handles_opened[0]->GetName(), DATA_COLUMN_FAMILY_NAME);
    CHECK_EQ_PREFIX(handles_opened[1]->GetName(), META_COLUMN_FAMILY_NAME);
    _data_cf = handles_opened[0];
    _meta_cf = handles_opened[1];

    // Create _meta_store which provide Pegasus meta data read and write.
    _meta_store = std::make_unique<meta_store>(this, _db, _meta_cf);

    if (db_exist) {
        auto cleanup = dsn::defer([this]() { release_db(); });
        uint64_t decree = 0;
        LOG_AND_RETURN_NOT_OK(ERROR_PREFIX,
                              _meta_store->get_last_flushed_decree(&decree),
                              "get_last_flushed_decree failed");
        _last_committed_decree.store(static_cast<int64_t>(decree));
        LOG_AND_RETURN_NOT_OK(ERROR_PREFIX,
                              _meta_store->get_data_version(&_pegasus_data_version),
                              "get_data_version failed");
        _usage_scenario = _meta_store->get_usage_scenario();
        uint64_t last_manual_compact_finish_time = 0;
        LOG_AND_RETURN_NOT_OK(
            ERROR_PREFIX,
            _meta_store->get_last_manual_compact_finish_time(&last_manual_compact_finish_time),
            "get_last_manual_compact_finish_time failed");
        LOG_AND_RETURN_NOT_TRUE(ERROR_PREFIX,
                                _pegasus_data_version <= PEGASUS_DATA_VERSION_MAX,
                                dsn::ERR_LOCAL_APP_FAILURE,
                                "open app failed, unsupported data version {}",
                                _pegasus_data_version);
        // update last manual compact finish timestamp
        _manual_compact_svc.init_last_finish_time_ms(last_manual_compact_finish_time);
        cleanup.cancel();
    } else {
        // Write initial meta data to meta CF and flush when create new DB.
        _meta_store->set_data_version(PEGASUS_DATA_VERSION_MAX);
        _meta_store->set_last_flushed_decree(0);
        _meta_store->set_last_manual_compact_finish_time(0);
        flush_all_family_columns(true);
    }

    // only enable filter after correct pegasus_data_version set
    _key_ttl_compaction_filter_factory->SetPegasusDataVersion(_pegasus_data_version);
    _key_ttl_compaction_filter_factory->SetPartitionIndex(_gpid.get_partition_index());
    _key_ttl_compaction_filter_factory->SetPartitionVersion(_gpid.get_partition_index() - 1);
    _key_ttl_compaction_filter_factory->EnableFilter();

    parse_checkpoints();

    // checkpoint if necessary to make last_durable_decree() fresh.
    // only need async checkpoint because we sure that memtable is empty now.
    int64_t last_flushed = static_cast<int64_t>(_last_committed_decree);
    if (last_flushed != last_durable_decree()) {
        LOG_INFO_PREFIX(
            "start to do async checkpoint, last_durable_decree = {}, last_flushed_decree = {}",
            last_durable_decree(),
            last_flushed);
        auto err = async_checkpoint(false);
        if (err != dsn::ERR_OK) {
            LOG_ERROR_PREFIX("create checkpoint failed, error = {}", err.to_string());
            release_db();
            return err;
        }
        CHECK_EQ_PREFIX(last_flushed, last_durable_decree());
    }

    LOG_INFO_PREFIX("open app succeed, pegasus_data_version = {}, last_durable_decree = {}",
                    _pegasus_data_version,
                    last_durable_decree());

    _is_open = true;

    if (!db_exist) {
        // When create a new db, update usage scenario according to app envs.
        update_usage_scenario(envs);
    }

    LOG_DEBUG_PREFIX("start the update replica-level rocksdb statistics timer task");
    _update_replica_rdb_stat =
        dsn::tasking::enqueue_timer(LPC_REPLICATION_LONG_COMMON,
                                    &_tracker,
                                    [this]() { this->update_replica_rocksdb_statistics(); },
                                    std::chrono::seconds(FLAGS_update_rdb_stat_interval));

    // These counters are singletons on this server shared by all replicas, their metrics update
    // task should be scheduled once an interval on the server view.
    static std::once_flag flag;
    std::call_once(flag, [&]() {
        // The timer task will always running even though there is no replicas
        CHECK_NE(kServerStatUpdateTimeSec.count(), 0);
        _update_server_rdb_stat = dsn::tasking::enqueue_timer(
            LPC_REPLICATION_LONG_COMMON,
            nullptr, // TODO: the tracker is nullptr, we will fix it later
            []() { update_server_rocksdb_statistics(); },
            kServerStatUpdateTimeSec);
    });

    // initialize cu calculator and write service after server being initialized.
    _cu_calculator = std::make_unique<capacity_unit_calculator>(
        this, _read_hotkey_collector, _write_hotkey_collector, _read_size_throttling_controller);
    _server_write = std::make_unique<pegasus_server_write>(this);

    dsn::tasking::enqueue_timer(LPC_ANALYZE_HOTKEY,
                                &_tracker,
                                [this]() { _read_hotkey_collector->analyse_data(); },
                                std::chrono::seconds(FLAGS_hotkey_analyse_time_interval_s));

    dsn::tasking::enqueue_timer(LPC_ANALYZE_HOTKEY,
                                &_tracker,
                                [this]() { _write_hotkey_collector->analyse_data(); },
                                std::chrono::seconds(FLAGS_hotkey_analyse_time_interval_s));

    return dsn::ERR_OK;
}

void pegasus_server_impl::cancel_background_work(bool wait)
{
    if (_is_open) {
        CHECK_NOTNULL(_db, "");
        rocksdb::CancelAllBackgroundWork(_db, wait);
    }
}

::dsn::error_code pegasus_server_impl::stop(bool clear_state)
{
    if (!_is_open) {
        CHECK(_db == nullptr, "");
        CHECK(!clear_state, "should not be here if do clear");
        return ::dsn::ERR_OK;
    }

    if (!clear_state) {
        flush_all_family_columns(true);
    }

    cancel_background_work(true);

    // stop all tracked tasks when pegasus server is stopped.
    if (_update_replica_rdb_stat != nullptr) {
        _update_replica_rdb_stat->cancel(true);
        _update_replica_rdb_stat = nullptr;
    }
    if (_update_server_rdb_stat != nullptr) {
        _update_server_rdb_stat->cancel(true);
        _update_server_rdb_stat = nullptr;
    }
    _tracker.cancel_outstanding_tasks();

    _context_cache.clear();

    _is_open = false;
    release_db();

    std::deque<int64_t> reserved_checkpoints;
    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);
        std::swap(reserved_checkpoints, _checkpoints);
        set_last_durable_decree(0);
    }

    if (clear_state) {
        // when clean the data dir, please clean the checkpoints first.
        // otherwise, if the "rdb" is removed but the checkpoints remains,
        // the storage engine can't be opened again
        for (auto iter = reserved_checkpoints.begin(); iter != reserved_checkpoints.end(); ++iter) {
            std::string chkpt_path =
                dsn::utils::filesystem::path_combine(data_dir(), chkpt_get_dir_name(*iter));
            if (!dsn::utils::filesystem::remove_path(chkpt_path)) {
                LOG_ERROR_PREFIX("rmdir {} failed when stop app", chkpt_path);
            }
        }
        if (!dsn::utils::filesystem::remove_path(data_dir())) {
            LOG_ERROR_PREFIX("rmdir {} failed when stop app", data_dir());
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
        _pfc_rdb_sst_count->set(0);
        _pfc_rdb_sst_size->set(0);
        _pfc_rdb_block_cache_hit_count->set(0);
        _pfc_rdb_block_cache_total_count->set(0);
        _pfc_rdb_block_cache_mem_usage->set(0);
        _pfc_rdb_index_and_filter_blocks_mem_usage->set(0);
        _pfc_rdb_memtable_mem_usage->set(0);
    }

    LOG_INFO_PREFIX("close app succeed, clear_state = {}", clear_state ? "true" : "false");
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

    int64_t last_durable = last_durable_decree();
    int64_t last_commit = last_committed_decree();
    CHECK_LE_PREFIX(last_durable, last_commit);

    // case 1: last_durable == last_commit
    // no need to do checkpoint
    if (last_durable == last_commit) {
        LOG_INFO_PREFIX(
            "no need to do checkpoint because last_durable_decree = last_committed_decree = {}",
            last_durable);
        return ::dsn::ERR_OK;
    }

    // case 2: last_durable < last_commit
    // need to do checkpoint
    rocksdb::Checkpoint *chkpt_raw = nullptr;
    auto status = rocksdb::Checkpoint::Create(_db, &chkpt_raw);
    if (!status.ok()) {
        LOG_ERROR_PREFIX("create Checkpoint object failed, error = {}", status.ToString());
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }
    std::unique_ptr<rocksdb::Checkpoint> chkpt(chkpt_raw);

    auto dir = chkpt_get_dir_name(last_commit);
    auto checkpoint_dir = ::dsn::utils::filesystem::path_combine(data_dir(), dir);
    if (::dsn::utils::filesystem::directory_exists(checkpoint_dir)) {
        LOG_INFO_PREFIX("checkpoint directory {} is already existed, remove it first",
                        checkpoint_dir);
        if (!::dsn::utils::filesystem::remove_path(checkpoint_dir)) {
            LOG_ERROR_PREFIX("remove checkpoint directory {} failed", checkpoint_dir);
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
    }

    // log_size_for_flush = 0 means always flush memtable before recording the live files
    status = chkpt->CreateCheckpoint(checkpoint_dir, 0 /* log_size_for_flush */);
    if (!status.ok()) {
        // sometimes checkpoint may fail, and try again will succeed
        LOG_ERROR_PREFIX("CreateCheckpoint failed, error = {}, try again", status.ToString());
        // TODO(yingchun): fail and return
        status = chkpt->CreateCheckpoint(checkpoint_dir, 0);
    }

    if (!status.ok()) {
        LOG_ERROR_PREFIX("CreateCheckpoint failed, error = {}", status.ToString());
        if (!::dsn::utils::filesystem::remove_path(checkpoint_dir)) {
            LOG_ERROR_PREFIX("remove checkpoint directory {} failed", checkpoint_dir);
        }
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }

    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);
        CHECK_GT_PREFIX(last_commit, last_durable_decree());
        uint64_t last_flushed = 0;
        CHECK_OK_PREFIX(_meta_store->get_last_flushed_decree(&last_flushed));
        CHECK_EQ_PREFIX(last_commit, last_flushed);
        if (!_checkpoints.empty()) {
            CHECK_GT_PREFIX(last_commit, _checkpoints.back());
        }
        _checkpoints.push_back(last_commit);
        set_last_durable_decree(_checkpoints.back());
    }

    LOG_INFO_PREFIX("sync create checkpoint succeed, last_durable_decree = {}",
                    last_durable_decree());

    gc_checkpoints();

    return ::dsn::ERR_OK;
}

// Must be thread safe.
::dsn::error_code pegasus_server_impl::async_checkpoint(bool flush_memtable)
{
    CheckpointingTokenHelper token_helper(_is_checkpointing);
    if (!token_helper.token_got())
        return ::dsn::ERR_WRONG_TIMING;

    int64_t last_durable = last_durable_decree();
    uint64_t last_flushed = 0;
    CHECK_OK_PREFIX(_meta_store->get_last_flushed_decree(&last_flushed));
    int64_t last_commit = last_committed_decree();

    CHECK_LE_PREFIX(last_durable, last_flushed);
    CHECK_LE_PREFIX(last_flushed, last_commit);

    // case 1: last_durable == last_flushed == last_commit
    // no need to do checkpoint
    if (last_durable == last_commit) {
        CHECK_EQ_PREFIX(last_durable, last_flushed);
        CHECK_EQ_PREFIX(last_flushed, last_commit);
        LOG_INFO_PREFIX(
            "no need to checkpoint because last_durable_decree = last_committed_decree = {}",
            last_durable);
        return ::dsn::ERR_OK;
    }

    // case 2: last_durable == last_flushed < last_commit
    // no need to do checkpoint, but need to flush memtable if required
    if (last_durable == last_flushed) {
        CHECK_LT_PREFIX(last_flushed, last_commit);
        if (!flush_memtable) {
            // no flush required
            return ::dsn::ERR_OK;
        }

        // flush required, but not wait
        if (::dsn::ERR_OK == flush_all_family_columns(false)) {
            LOG_INFO_PREFIX("trigger flushing memtable succeed");
            return ::dsn::ERR_TRY_AGAIN;
        } else {
            LOG_ERROR_PREFIX("trigger flushing memtable failed");
            return ::dsn::ERR_LOCAL_APP_FAILURE;
        }
    }

    // case 3: last_durable < last_flushed <= last_commit
    // need to do checkpoint
    CHECK_LT_PREFIX(last_durable, last_flushed);

    std::string tmp_dir = ::dsn::utils::filesystem::path_combine(
        data_dir(), std::string("checkpoint.tmp.") + std::to_string(dsn_now_us()));
    if (::dsn::utils::filesystem::directory_exists(tmp_dir)) {
        LOG_INFO_PREFIX("temporary checkpoint directory {} is already existed, remove it first",
                        tmp_dir);
        if (!::dsn::utils::filesystem::remove_path(tmp_dir)) {
            LOG_ERROR_PREFIX("remove temporary checkpoint directory {} failed", tmp_dir);
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
    }

    int64_t checkpoint_decree = 0;
    ::dsn::error_code err =
        copy_checkpoint_to_dir_unsafe(tmp_dir.c_str(), &checkpoint_decree, flush_memtable);
    if (err != ::dsn::ERR_OK) {
        LOG_ERROR_PREFIX("copy_checkpoint_to_dir_unsafe failed with err = {}", err.to_string());
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }

    auto checkpoint_dir =
        ::dsn::utils::filesystem::path_combine(data_dir(), chkpt_get_dir_name(checkpoint_decree));
    if (::dsn::utils::filesystem::directory_exists(checkpoint_dir)) {
        LOG_INFO_PREFIX("checkpoint directory {} is already existed, remove it first",
                        checkpoint_dir);
        if (!::dsn::utils::filesystem::remove_path(checkpoint_dir)) {
            LOG_ERROR_PREFIX("remove old checkpoint directory {} failed", checkpoint_dir);
            if (!::dsn::utils::filesystem::remove_path(tmp_dir)) {
                LOG_ERROR_PREFIX("remove temporary checkpoint directory {} failed", tmp_dir);
            }
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
    }

    if (!::dsn::utils::filesystem::rename_path(tmp_dir, checkpoint_dir)) {
        LOG_ERROR_PREFIX(
            "rename checkpoint directory from {} to {} failed", tmp_dir, checkpoint_dir);
        if (!::dsn::utils::filesystem::remove_path(tmp_dir)) {
            LOG_ERROR_PREFIX("remove temporary checkpoint directory {} failed", tmp_dir);
        }
        return ::dsn::ERR_FILE_OPERATION_FAILED;
    }

    {
        ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);
        CHECK_GT_PREFIX(checkpoint_decree, last_durable_decree());
        if (!_checkpoints.empty()) {
            CHECK_GT_PREFIX(checkpoint_decree, _checkpoints.back());
        }
        _checkpoints.push_back(checkpoint_decree);
        set_last_durable_decree(_checkpoints.back());
    }

    LOG_INFO_PREFIX("async create checkpoint succeed, last_durable_decree = {}",
                    last_durable_decree());

    gc_checkpoints();

    return ::dsn::ERR_OK;
}

// Must be thread safe.
::dsn::error_code pegasus_server_impl::copy_checkpoint_to_dir(const char *checkpoint_dir,
                                                              /*output*/ int64_t *last_decree,
                                                              bool flush_memtable)
{
    CheckpointingTokenHelper token_helper(_is_checkpointing);
    if (!token_helper.token_got()) {
        return ::dsn::ERR_WRONG_TIMING;
    }

    return copy_checkpoint_to_dir_unsafe(checkpoint_dir, last_decree, flush_memtable);
}

// not thread safe, should be protected by caller
::dsn::error_code pegasus_server_impl::copy_checkpoint_to_dir_unsafe(const char *checkpoint_dir,
                                                                     int64_t *checkpoint_decree,
                                                                     bool flush_memtable)
{
    rocksdb::Checkpoint *chkpt_raw = nullptr;
    auto status = rocksdb::Checkpoint::Create(_db, &chkpt_raw);
    if (!status.ok()) {
        LOG_ERROR_PREFIX("create Checkpoint object failed, error = {}", status.ToString());
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }
    std::unique_ptr<rocksdb::Checkpoint> chkpt(chkpt_raw);

    if (::dsn::utils::filesystem::directory_exists(checkpoint_dir)) {
        LOG_INFO_PREFIX("checkpoint directory {} is already existed, remove it first",
                        checkpoint_dir);
        if (!::dsn::utils::filesystem::remove_path(checkpoint_dir)) {
            LOG_ERROR_PREFIX("remove checkpoint directory {} failed", checkpoint_dir);
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }
    }

    // CreateCheckpoint() will not flush memtable when log_size_for_flush = max
    status = chkpt->CreateCheckpoint(checkpoint_dir,
                                     flush_memtable ? 0 : std::numeric_limits<uint64_t>::max());
    if (!status.ok()) {
        LOG_ERROR_PREFIX("CreateCheckpoint failed, error = {}", status.ToString());
        if (!::dsn::utils::filesystem::remove_path(checkpoint_dir)) {
            LOG_ERROR_PREFIX("remove checkpoint directory {} failed", checkpoint_dir);
        }
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }
    LOG_INFO_PREFIX("copy checkpoint to dir({}) succeed", checkpoint_dir);

    if (checkpoint_decree != nullptr) {
        rocksdb::DB *snapshot_db = nullptr;
        std::vector<rocksdb::ColumnFamilyHandle *> handles_opened;
        auto cleanup = [&](bool remove_checkpoint) {
            if (remove_checkpoint && !::dsn::utils::filesystem::remove_path(checkpoint_dir)) {
                LOG_ERROR_PREFIX("remove checkpoint directory {} failed", checkpoint_dir);
            }
            if (snapshot_db) {
                for (auto handle : handles_opened) {
                    if (handle) {
                        snapshot_db->DestroyColumnFamilyHandle(handle);
                        handle = nullptr;
                    }
                }
                delete snapshot_db;
                snapshot_db = nullptr;
            }
        };

        // Because of RocksDB's restriction, we have to to open default column family even though
        // not use it
        std::vector<rocksdb::ColumnFamilyDescriptor> column_families(
            {{DATA_COLUMN_FAMILY_NAME, rocksdb::ColumnFamilyOptions()},
             {META_COLUMN_FAMILY_NAME, rocksdb::ColumnFamilyOptions()}});
        status = rocksdb::DB::OpenForReadOnly(
            _db_opts, checkpoint_dir, column_families, &handles_opened, &snapshot_db);
        if (!status.ok()) {
            LOG_ERROR_PREFIX(
                "OpenForReadOnly from {} failed, error = {}", checkpoint_dir, status.ToString());
            snapshot_db = nullptr;
            cleanup(true);
            return ::dsn::ERR_LOCAL_APP_FAILURE;
        }
        CHECK_EQ_PREFIX(handles_opened.size(), 2);
        CHECK_EQ_PREFIX(handles_opened[1]->GetName(), META_COLUMN_FAMILY_NAME);
        uint64_t last_flushed_decree =
            _meta_store->get_decree_from_readonly_db(snapshot_db, handles_opened[1]);
        *checkpoint_decree = last_flushed_decree;

        cleanup(false);
    }

    return ::dsn::ERR_OK;
}

::dsn::error_code pegasus_server_impl::get_checkpoint(int64_t learn_start,
                                                      const dsn::blob &learn_request,
                                                      dsn::replication::learn_state &state)
{
    CHECK(_is_open, "");

    int64_t ci = last_durable_decree();
    if (ci == 0) {
        LOG_ERROR_PREFIX("no checkpoint found");
        return ::dsn::ERR_OBJECT_NOT_FOUND;
    }

    auto chkpt_dir = ::dsn::utils::filesystem::path_combine(data_dir(), chkpt_get_dir_name(ci));
    state.files.clear();
    if (!::dsn::utils::filesystem::get_subfiles(chkpt_dir, state.files, true)) {
        LOG_ERROR_PREFIX("list files in checkpoint dir {} failed", chkpt_dir);
        return ::dsn::ERR_FILE_OPERATION_FAILED;
    }

    state.from_decree_excluded = 0;
    state.to_decree_included = ci;

    LOG_INFO_PREFIX("get checkpoint succeed, from_decree_excluded = 0, to_decree_included = {}",
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
        CHECK_GT(ci, last_durable_decree());

        auto learn_dir = ::dsn::utils::filesystem::remove_file_name(state.files[0]);
        auto chkpt_dir = ::dsn::utils::filesystem::path_combine(data_dir(), chkpt_get_dir_name(ci));
        if (::dsn::utils::filesystem::rename_path(learn_dir, chkpt_dir)) {
            ::dsn::utils::auto_lock<::dsn::utils::ex_lock_nr> l(_checkpoints_lock);
            CHECK_GT(ci, last_durable_decree());
            _checkpoints.push_back(ci);
            if (!_checkpoints.empty()) {
                CHECK_GT(ci, _checkpoints.back());
            }
            set_last_durable_decree(ci);
            err = ::dsn::ERR_OK;
        } else {
            LOG_ERROR_PREFIX("rename directory {} to {} failed", learn_dir, chkpt_dir);
            err = ::dsn::ERR_FILE_OPERATION_FAILED;
        }

        return err;
    }

    if (_is_open) {
        err = stop(true);
        if (err != ::dsn::ERR_OK) {
            LOG_ERROR_PREFIX("close rocksdb failed, error = {}", err);
            return err;
        }
    }

    // clear data dir
    if (!::dsn::utils::filesystem::remove_path(data_dir())) {
        LOG_ERROR_PREFIX("clear data directory {} failed", data_dir());
        return ::dsn::ERR_FILE_OPERATION_FAILED;
    }

    // reopen the db with the new checkpoint files
    if (state.files.size() > 0) {
        // create data dir
        if (!::dsn::utils::filesystem::create_directory(data_dir())) {
            LOG_ERROR_PREFIX("create data directory {} failed", data_dir());
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }

        // move learned files from learn_dir to data_dir/rdb
        std::string learn_dir = ::dsn::utils::filesystem::remove_file_name(state.files[0]);
        std::string new_dir = ::dsn::utils::filesystem::path_combine(data_dir(), "rdb");
        if (!::dsn::utils::filesystem::rename_path(learn_dir, new_dir)) {
            LOG_ERROR_PREFIX("rename directory {} to {} failed", learn_dir, new_dir);
            return ::dsn::ERR_FILE_OPERATION_FAILED;
        }

        err = start(0, nullptr);
    } else {
        LOG_INFO_PREFIX("apply empty checkpoint, create new rocksdb");
        err = start(0, nullptr);
    }

    if (err != ::dsn::ERR_OK) {
        LOG_ERROR_PREFIX("open rocksdb failed, error = {}", err);
        return err;
    }

    CHECK(_is_open, "");
    CHECK_EQ(ci, last_durable_decree());

    LOG_INFO_PREFIX("apply checkpoint succeed, last_durable_decree = {}", last_durable_decree());
    return ::dsn::ERR_OK;
}

bool pegasus_server_impl::validate_filter(::dsn::apps::filter_type::type filter_type,
                                          const ::dsn::blob &filter_pattern,
                                          const ::dsn::blob &value)
{
    switch (filter_type) {
    case ::dsn::apps::filter_type::FT_NO_FILTER:
        return true;
    case ::dsn::apps::filter_type::FT_MATCH_ANYWHERE:
    case ::dsn::apps::filter_type::FT_MATCH_PREFIX:
    case ::dsn::apps::filter_type::FT_MATCH_POSTFIX: {
        if (filter_pattern.length() == 0)
            return true;
        if (value.length() < filter_pattern.length())
            return false;
        if (filter_type == ::dsn::apps::filter_type::FT_MATCH_ANYWHERE) {
            return value.to_string_view().find(filter_pattern.to_string_view()) !=
                   absl::string_view::npos;
        } else if (filter_type == ::dsn::apps::filter_type::FT_MATCH_PREFIX) {
            return dsn::utils::mequals(
                value.data(), filter_pattern.data(), filter_pattern.length());
        } else { // filter_type == ::dsn::apps::filter_type::FT_MATCH_POSTFIX
            return dsn::utils::mequals(value.data() + value.length() - filter_pattern.length(),
                                       filter_pattern.data(),
                                       filter_pattern.length());
        }
    }
    default:
        CHECK(false, "unsupported filter type: {}", filter_type);
    }
    return false;
}

range_iteration_state pegasus_server_impl::validate_key_value_for_scan(
    const rocksdb::Slice &key,
    const rocksdb::Slice &value,
    ::dsn::apps::filter_type::type hash_key_filter_type,
    const ::dsn::blob &hash_key_filter_pattern,
    ::dsn::apps::filter_type::type sort_key_filter_type,
    const ::dsn::blob &sort_key_filter_pattern,
    uint32_t epoch_now,
    bool request_validate_hash)
{
    if (check_if_record_expired(epoch_now, value)) {
        if (FLAGS_rocksdb_verbose_log) {
            LOG_ERROR_PREFIX("rocksdb data expired for scan");
        }
        return range_iteration_state::kExpired;
    }

    if (request_validate_hash && _validate_partition_hash) {
        if (_partition_version < 0 || _gpid.get_partition_index() > _partition_version ||
            !check_pegasus_key_hash(key, _gpid.get_partition_index(), _partition_version)) {
            if (FLAGS_rocksdb_verbose_log) {
                LOG_ERROR_PREFIX("not serve hash key while scan");
            }
            return range_iteration_state::kHashInvalid;
        }
    }

    // extract raw key
    ::dsn::blob raw_key(key.data(), 0, key.size());
    if (hash_key_filter_type != ::dsn::apps::filter_type::FT_NO_FILTER ||
        sort_key_filter_type != ::dsn::apps::filter_type::FT_NO_FILTER) {
        ::dsn::blob hash_key, sort_key;
        pegasus_restore_key(raw_key, hash_key, sort_key);
        if (hash_key_filter_type != ::dsn::apps::filter_type::FT_NO_FILTER &&
            !validate_filter(hash_key_filter_type, hash_key_filter_pattern, hash_key)) {
            if (FLAGS_rocksdb_verbose_log) {
                LOG_ERROR_PREFIX("hash key filtered for scan");
            }
            return range_iteration_state::kFiltered;
        }
        if (sort_key_filter_type != ::dsn::apps::filter_type::FT_NO_FILTER &&
            !validate_filter(sort_key_filter_type, sort_key_filter_pattern, sort_key)) {
            if (FLAGS_rocksdb_verbose_log) {
                LOG_ERROR_PREFIX("sort key filtered for scan");
            }
            return range_iteration_state::kFiltered;
        }
    }

    return range_iteration_state::kNormal;
}

void pegasus_server_impl::append_key_value(std::vector<::dsn::apps::key_value> &kvs,
                                           const rocksdb::Slice &key,
                                           const rocksdb::Slice &value,
                                           bool no_value,
                                           bool request_expire_ts)
{
    ::dsn::apps::key_value kv;
    ::dsn::blob raw_key(key.data(), 0, key.size());
    std::shared_ptr<char> key_buf(::dsn::utils::make_shared_array<char>(raw_key.length()));
    ::memcpy(key_buf.get(), raw_key.data(), raw_key.length());
    kv.key.assign(std::move(key_buf), 0, raw_key.length());

    // extract expire ts if necessary
    if (request_expire_ts) {
        auto expire_ts_seconds =
            pegasus_extract_expire_ts(_pegasus_data_version, utils::to_string_view(value));
        kv.__set_expire_ts_seconds(static_cast<int32_t>(expire_ts_seconds));
    }

    // extract value
    if (!no_value) {
        std::string value_buf(value.data(), value.size());
        pegasus_extract_user_data(_pegasus_data_version, std::move(value_buf), kv.value);
    }

    kvs.emplace_back(std::move(kv));
}

range_iteration_state pegasus_server_impl::append_key_value_for_multi_get(
    std::vector<::dsn::apps::key_value> &kvs,
    const rocksdb::Slice &key,
    const rocksdb::Slice &value,
    ::dsn::apps::filter_type::type sort_key_filter_type,
    const ::dsn::blob &sort_key_filter_pattern,
    uint32_t epoch_now,
    bool no_value)
{
    if (check_if_record_expired(epoch_now, value)) {
        if (FLAGS_rocksdb_verbose_log) {
            LOG_ERROR_PREFIX("rocksdb data expired for multi get");
        }
        return range_iteration_state::kExpired;
    }

    ::dsn::apps::key_value kv;

    // extract sort_key
    ::dsn::blob raw_key(key.data(), 0, key.size());
    ::dsn::blob hash_key, sort_key;
    pegasus_restore_key(raw_key, hash_key, sort_key);

    if (sort_key_filter_type != ::dsn::apps::filter_type::FT_NO_FILTER &&
        !validate_filter(sort_key_filter_type, sort_key_filter_pattern, sort_key)) {
        if (FLAGS_rocksdb_verbose_log) {
            LOG_ERROR_PREFIX("sort key filtered for multi get");
        }
        return range_iteration_state::kFiltered;
    }
    std::shared_ptr<char> sort_key_buf(::dsn::utils::make_shared_array<char>(sort_key.length()));
    ::memcpy(sort_key_buf.get(), sort_key.data(), sort_key.length());
    kv.key.assign(std::move(sort_key_buf), 0, sort_key.length());

    // extract value
    if (!no_value) {
        std::string value_buf(value.data(), value.size());
        pegasus_extract_user_data(_pegasus_data_version, std::move(value_buf), kv.value);
    }

    kvs.emplace_back(std::move(kv));
    return range_iteration_state::kNormal;
}

void pegasus_server_impl::update_replica_rocksdb_statistics()
{
    std::string str_val;
    uint64_t val = 0;

    // Update _pfc_rdb_sst_count
    for (int i = 0; i < _data_cf_opts.num_levels; ++i) {
        int cur_level_count = 0;
        if (_db->GetProperty(rocksdb::DB::Properties::kNumFilesAtLevelPrefix + std::to_string(i),
                             &str_val) &&
            dsn::buf2int32(str_val, cur_level_count)) {
            val += cur_level_count;
        }
    }
    _pfc_rdb_sst_count->set(val);
    LOG_DEBUG_PREFIX("_pfc_rdb_sst_count: {}", val);

    // Update _pfc_rdb_sst_size
    if (_db->GetProperty(_data_cf, rocksdb::DB::Properties::kTotalSstFilesSize, &str_val) &&
        dsn::buf2uint64(str_val, val)) {
        static uint64_t bytes_per_mb = 1U << 20U;
        _pfc_rdb_sst_size->set(val / bytes_per_mb);
        LOG_DEBUG_PREFIX("_pfc_rdb_sst_size: {} bytes", val);
    }

    // Update _pfc_rdb_write_amplification
    std::map<std::string, std::string> props;
    if (_db->GetMapProperty(_data_cf, "rocksdb.cfstats", &props)) {
        auto write_amplification_iter = props.find("compaction.Sum.WriteAmp");
        auto write_amplification = write_amplification_iter == props.end()
                                       ? 1
                                       : std::stod(write_amplification_iter->second);
        _pfc_rdb_write_amplification->set(write_amplification);
        LOG_DEBUG_PREFIX("_pfc_rdb_write_amplification: {}", write_amplification);
    }

    // Update _pfc_rdb_index_and_filter_blocks_mem_usage
    if (_db->GetProperty(_data_cf, rocksdb::DB::Properties::kEstimateTableReadersMem, &str_val) &&
        dsn::buf2uint64(str_val, val)) {
        _pfc_rdb_index_and_filter_blocks_mem_usage->set(val);
        LOG_DEBUG_PREFIX("_pfc_rdb_index_and_filter_blocks_mem_usage: {} bytes", val);
    }

    // Update _pfc_rdb_memtable_mem_usage
    if (_db->GetProperty(_data_cf, rocksdb::DB::Properties::kCurSizeAllMemTables, &str_val) &&
        dsn::buf2uint64(str_val, val)) {
        _pfc_rdb_memtable_mem_usage->set(val);
        LOG_DEBUG_PREFIX("_pfc_rdb_memtable_mem_usage: {} bytes", val);
    }

    // Update _pfc_rdb_estimate_num_keys
    // NOTE: for the same n kv pairs, kEstimateNumKeys will be counted n times, you need compaction
    // to remove duplicate
    if (_db->GetProperty(_data_cf, rocksdb::DB::Properties::kEstimateNumKeys, &str_val) &&
        dsn::buf2uint64(str_val, val)) {
        _pfc_rdb_estimate_num_keys->set(val);
        LOG_DEBUG_PREFIX("_pfc_rdb_estimate_num_keys: {}", val);
    }

    // the follow stats is related to `read`, so only primary need update it，ignore
    // `backup-request` case
    if (!is_primary()) {
        return;
    }

    // Update _pfc_rdb_read_amplification
    if (FLAGS_read_amp_bytes_per_bit > 0) {
        auto estimate_useful_bytes =
            _statistics->getTickerCount(rocksdb::READ_AMP_ESTIMATE_USEFUL_BYTES);
        if (estimate_useful_bytes) {
            auto read_amplification =
                _statistics->getTickerCount(rocksdb::READ_AMP_TOTAL_READ_BYTES) /
                estimate_useful_bytes;
            _pfc_rdb_read_amplification->set(read_amplification);
            LOG_DEBUG_PREFIX("_pfc_rdb_read_amplification: {}", read_amplification);
        }
    }

    // Update _pfc_rdb_bf_seek_negatives
    auto bf_seek_negatives = _statistics->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_USEFUL);
    _pfc_rdb_bf_seek_negatives->set(bf_seek_negatives);
    LOG_DEBUG_PREFIX("_pfc_rdb_bf_seek_negatives: {}", bf_seek_negatives);

    // Update _pfc_rdb_bf_seek_total
    auto bf_seek_total = _statistics->getTickerCount(rocksdb::BLOOM_FILTER_PREFIX_CHECKED);
    _pfc_rdb_bf_seek_total->set(bf_seek_total);
    LOG_DEBUG_PREFIX("_pfc_rdb_bf_seek_total: {}", bf_seek_total);

    // Update _pfc_rdb_bf_point_positive_true
    auto bf_point_positive_true =
        _statistics->getTickerCount(rocksdb::BLOOM_FILTER_FULL_TRUE_POSITIVE);
    _pfc_rdb_bf_point_positive_true->set(bf_point_positive_true);
    LOG_DEBUG_PREFIX("_pfc_rdb_bf_point_positive_true: {}", bf_point_positive_true);

    // Update _pfc_rdb_bf_point_positive_total
    auto bf_point_positive_total = _statistics->getTickerCount(rocksdb::BLOOM_FILTER_FULL_POSITIVE);
    _pfc_rdb_bf_point_positive_total->set(bf_point_positive_total);
    LOG_DEBUG_PREFIX("_pfc_rdb_bf_point_positive_total: {}", bf_point_positive_total);

    // Update _pfc_rdb_bf_point_negatives
    auto bf_point_negatives = _statistics->getTickerCount(rocksdb::BLOOM_FILTER_USEFUL);
    _pfc_rdb_bf_point_negatives->set(bf_point_negatives);
    LOG_DEBUG_PREFIX("_pfc_rdb_bf_point_negatives: {}", bf_point_negatives);

    // Update _pfc_rdb_block_cache_hit_count and _pfc_rdb_block_cache_total_count
    auto block_cache_hit = _statistics->getTickerCount(rocksdb::BLOCK_CACHE_HIT);
    _pfc_rdb_block_cache_hit_count->set(block_cache_hit);
    LOG_DEBUG_PREFIX("_pfc_rdb_block_cache_hit_count: {}", block_cache_hit);

    auto block_cache_miss = _statistics->getTickerCount(rocksdb::BLOCK_CACHE_MISS);
    auto block_cache_total = block_cache_hit + block_cache_miss;
    _pfc_rdb_block_cache_total_count->set(block_cache_total);
    LOG_DEBUG_PREFIX("_pfc_rdb_block_cache_total_count: {}", block_cache_total);

    // update block memtable/l0/l1/l2andup hit rate under block cache up level
    auto memtable_hit_count = _statistics->getTickerCount(rocksdb::MEMTABLE_HIT);
    _pfc_rdb_memtable_hit_count->set(memtable_hit_count);
    LOG_DEBUG_PREFIX("_pfc_rdb_memtable_hit_count: {}", memtable_hit_count);

    auto memtable_miss_count = _statistics->getTickerCount(rocksdb::MEMTABLE_MISS);
    auto memtable_total = memtable_hit_count + memtable_miss_count;
    _pfc_rdb_memtable_total_count->set(memtable_total);
    LOG_DEBUG_PREFIX("_pfc_rdb_memtable_total_count: {}", memtable_total);

    auto l0_hit_count = _statistics->getTickerCount(rocksdb::GET_HIT_L0);
    _pfc_rdb_l0_hit_count->set(l0_hit_count);
    LOG_DEBUG_PREFIX("_pfc_rdb_l0_hit_count: {}", l0_hit_count);

    auto l1_hit_count = _statistics->getTickerCount(rocksdb::GET_HIT_L1);
    _pfc_rdb_l1_hit_count->set(l1_hit_count);
    LOG_DEBUG_PREFIX("_pfc_rdb_l1_hit_count: {}", l1_hit_count);

    auto l2andup_hit_count = _statistics->getTickerCount(rocksdb::GET_HIT_L2_AND_UP);
    _pfc_rdb_l2andup_hit_count->set(l2andup_hit_count);
    LOG_DEBUG_PREFIX("_pfc_rdb_l2andup_hit_count: {}", l2andup_hit_count);
}

void pegasus_server_impl::update_server_rocksdb_statistics()
{
    // Update _pfc_rdb_block_cache_mem_usage
    if (_s_block_cache) {
        uint64_t val = _s_block_cache->GetUsage();
        _pfc_rdb_block_cache_mem_usage->set(val);
    }

    // Update _pfc_rdb_write_limiter_rate_bytes
    if (_s_rate_limiter) {
        uint64_t current_total_through = _s_rate_limiter->GetTotalBytesThrough();
        uint64_t through_bytes_per_sec =
            (current_total_through - _rocksdb_limiter_last_total_through) /
            kServerStatUpdateTimeSec.count();
        _pfc_rdb_write_limiter_rate_bytes->set(through_bytes_per_sec);
        _rocksdb_limiter_last_total_through = current_total_through;
    }
}

std::pair<std::string, bool>
pegasus_server_impl::get_restore_dir_from_env(const std::map<std::string, std::string> &env_kvs)
{
    std::pair<std::string, bool> res;
    std::stringstream os;
    os << "restore.";

    auto it = env_kvs.find(ROCKSDB_ENV_RESTORE_FORCE_RESTORE);
    if (it != env_kvs.end()) {
        LOG_INFO_PREFIX("found {} in envs", ROCKSDB_ENV_RESTORE_FORCE_RESTORE);
        res.second = true;
    }

    it = env_kvs.find(ROCKSDB_ENV_RESTORE_POLICY_NAME);
    if (it != env_kvs.end()) {
        LOG_INFO_PREFIX("found {} in envs: {}", ROCKSDB_ENV_RESTORE_POLICY_NAME, it->second);
        os << it->second << ".";
    } else {
        return res;
    }

    it = env_kvs.find(ROCKSDB_ENV_RESTORE_BACKUP_ID);
    if (it != env_kvs.end()) {
        LOG_INFO_PREFIX("found {} in envs: {}", ROCKSDB_ENV_RESTORE_BACKUP_ID, it->second);
        os << it->second;
    } else {
        return res;
    }

    std::string parent_dir = ::dsn::utils::filesystem::remove_file_name(data_dir());
    res.first = ::dsn::utils::filesystem::path_combine(parent_dir, os.str());
    return res;
}

void pegasus_server_impl::update_rocksdb_dynamic_options(
    const std::map<std::string, std::string> &envs)
{
    if (envs.empty()) {
        return;
    }

    std::unordered_map<std::string, std::string> new_options;
    for (const auto &option : ROCKSDB_DYNAMIC_OPTIONS) {
        const auto &find = envs.find(option);
        if (find == envs.end()) {
            continue;
        }

        std::vector<std::string> args;
        // split_args example: Parse "write_buffer_size" from "rocksdb.write_buffer_size"
        dsn::utils::split_args(option.c_str(), args, '.');
        CHECK_EQ(args.size(), 2);
        new_options[args[1]] = find->second;
    }

    // doing set option
    if (!new_options.empty() && set_options(new_options)) {
        LOG_INFO("Set rocksdb dynamic options success");
    }
}

void pegasus_server_impl::set_rocksdb_options_before_creating(
    const std::map<std::string, std::string> &envs)
{
    if (envs.empty()) {
        return;
    }

    for (const auto &option : pegasus::ROCKSDB_STATIC_OPTIONS) {
        const auto &find = envs.find(option);
        if (find == envs.end()) {
            continue;
        }

        const auto &setter = cf_opts_setters.find(option);
        CHECK_TRUE(setter != cf_opts_setters.end());
        if (setter->second(find->second, _data_cf_opts)) {
            LOG_INFO_PREFIX("Set {} \"{}\" succeed", find->first, find->second);
        }
    }

    for (const auto &option : pegasus::ROCKSDB_DYNAMIC_OPTIONS) {
        const auto &find = envs.find(option);
        if (find == envs.end()) {
            continue;
        }

        const auto &setter = cf_opts_setters.find(option);
        CHECK_TRUE(setter != cf_opts_setters.end());
        if (setter->second(find->second, _data_cf_opts)) {
            LOG_INFO_PREFIX("Set {} \"{}\" succeed", find->first, find->second);
        }
    }
}

void pegasus_server_impl::update_app_envs(const std::map<std::string, std::string> &envs)
{
    update_usage_scenario(envs);
    update_default_ttl(envs);
    update_checkpoint_reserve(envs);
    update_slow_query_threshold(envs);
    update_rocksdb_iteration_threshold(envs);
    update_validate_partition_hash(envs);
    update_user_specified_compaction(envs);
    _manual_compact_svc.start_manual_compact_if_needed(envs);

    update_throttling_controller(envs);
    update_rocksdb_dynamic_options(envs);
}

void pegasus_server_impl::update_app_envs_before_open_db(
    const std::map<std::string, std::string> &envs)
{
    // we do not update usage scenario because it depends on opened db.
    update_default_ttl(envs);
    update_checkpoint_reserve(envs);
    update_slow_query_threshold(envs);
    update_rocksdb_iteration_threshold(envs);
    update_validate_partition_hash(envs);
    update_user_specified_compaction(envs);
    _manual_compact_svc.start_manual_compact_if_needed(envs);
    set_rocksdb_options_before_creating(envs);
}

void pegasus_server_impl::query_app_envs(/*out*/ std::map<std::string, std::string> &envs)
{
    envs[ROCKSDB_ENV_USAGE_SCENARIO_KEY] = _usage_scenario;
    // write_buffer_size involves random values (refer to pegasus_server_impl::set_usage_scenario),
    // so it can only be taken from _data_cf_opts
    envs[ROCKSDB_WRITE_BUFFER_SIZE] = std::to_string(_data_cf_opts.write_buffer_size);

    // Get Data ColumnFamilyOptions directly from _data_cf
    rocksdb::ColumnFamilyDescriptor desc;
    CHECK_TRUE(_data_cf->GetDescriptor(&desc).ok());
    for (const auto &option : pegasus::ROCKSDB_STATIC_OPTIONS) {
        auto getter = cf_opts_getters.find(option);
        CHECK_TRUE(getter != cf_opts_getters.end());
        std::string option_val;
        getter->second(desc.options, option_val);
        envs[option] = option_val;
    }
    for (const auto &option : pegasus::ROCKSDB_DYNAMIC_OPTIONS) {
        if (option.compare(ROCKSDB_WRITE_BUFFER_SIZE) == 0) {
            continue;
        }
        auto getter = cf_opts_getters.find(option);
        CHECK_TRUE(getter != cf_opts_getters.end());
        std::string option_val;
        getter->second(desc.options, option_val);
        envs[option] = option_val;
    }
}

void pegasus_server_impl::update_usage_scenario(const std::map<std::string, std::string> &envs)
{
    // update usage scenario
    // if not specified, default is normal
    auto find = envs.find(ROCKSDB_ENV_USAGE_SCENARIO_KEY);
    std::string new_usage_scenario =
        (find != envs.end() ? find->second : ROCKSDB_ENV_USAGE_SCENARIO_NORMAL);
    if (new_usage_scenario != _usage_scenario) {
        std::string old_usage_scenario = _usage_scenario;
        if (set_usage_scenario(new_usage_scenario)) {
            LOG_INFO_PREFIX("update app env[{}] from \"{}\" to \"{}\" succeed",
                            ROCKSDB_ENV_USAGE_SCENARIO_KEY,
                            old_usage_scenario,
                            new_usage_scenario);
        } else {
            LOG_ERROR_PREFIX("update app env[{}] from \"{}\" to \"{}\" failed",
                             ROCKSDB_ENV_USAGE_SCENARIO_KEY,
                             old_usage_scenario,
                             new_usage_scenario);
        }
    } else {
        // When an old db is opened and the rocksDB related configs in server config.ini has been
        // changed, the options related to usage scenario need to be recalculated with new values.
        recalculate_data_cf_options(_table_data_cf_opts);
    }
}

void pegasus_server_impl::update_default_ttl(const std::map<std::string, std::string> &envs)
{
    auto find = envs.find(TABLE_LEVEL_DEFAULT_TTL);
    if (find != envs.end()) {
        int32_t ttl = 0;
        if (!dsn::buf2int32(find->second, ttl) || ttl < 0) {
            LOG_ERROR_PREFIX("{}={} is invalid.", find->first, find->second);
            return;
        }
        _server_write->set_default_ttl(static_cast<uint32_t>(ttl));
        _key_ttl_compaction_filter_factory->SetDefaultTTL(static_cast<uint32_t>(ttl));
    }
}

// TODO(yingchun): change by http
void pegasus_server_impl::update_checkpoint_reserve(const std::map<std::string, std::string> &envs)
{
    int32_t count = FLAGS_checkpoint_reserve_min_count;
    int32_t time = FLAGS_checkpoint_reserve_time_seconds;

    auto find = envs.find(ROCKDB_CHECKPOINT_RESERVE_MIN_COUNT);
    if (find != envs.end()) {
        if (!dsn::buf2int32(find->second, count) || count <= 0) {
            LOG_ERROR_PREFIX("{}={} is invalid.", find->first, find->second);
            return;
        }
    }
    find = envs.find(ROCKDB_CHECKPOINT_RESERVE_TIME_SECONDS);
    if (find != envs.end()) {
        if (!dsn::buf2int32(find->second, time) || time < 0) {
            LOG_ERROR_PREFIX("{}={} is invalid.", find->first, find->second);
            return;
        }
    }

    if (count != _checkpoint_reserve_min_count) {
        LOG_INFO_PREFIX("update app env[{}] from \"{}\" to \"{}\" succeed",
                        ROCKDB_CHECKPOINT_RESERVE_MIN_COUNT,
                        _checkpoint_reserve_min_count,
                        count);
        _checkpoint_reserve_min_count = count;
    }
    if (time != _checkpoint_reserve_time_seconds) {
        LOG_INFO_PREFIX("update app env[{}] from \"{}\" to \"{}\" succeed",
                        ROCKDB_CHECKPOINT_RESERVE_TIME_SECONDS,
                        _checkpoint_reserve_time_seconds,
                        time);
        _checkpoint_reserve_time_seconds = time;
    }
}

void pegasus_server_impl::update_throttling_controller(
    const std::map<std::string, std::string> &envs)
{
    bool throttling_changed = false;
    std::string old_throttling;
    std::string parse_error;
    auto find = envs.find(READ_SIZE_THROTTLING);
    if (find != envs.end()) {
        if (!_read_size_throttling_controller->parse_from_env(find->second,
                                                              get_app_info()->partition_count,
                                                              parse_error,
                                                              throttling_changed,
                                                              old_throttling)) {
            LOG_WARNING_PREFIX("parse env failed, key = \"{}\", value = \"{}\", error = \"{}\"",
                               READ_SIZE_THROTTLING,
                               find->second,
                               parse_error);
            // reset if parse failed
            _read_size_throttling_controller->reset(throttling_changed, old_throttling);
        }
    } else {
        // reset if env not found
        _read_size_throttling_controller->reset(throttling_changed, old_throttling);
    }
    if (throttling_changed) {
        LOG_INFO_PREFIX("switch {} from \"{}\" to \"{}\"",
                        READ_SIZE_THROTTLING,
                        old_throttling,
                        _read_size_throttling_controller->env_value());
    }
}

void pegasus_server_impl::update_slow_query_threshold(
    const std::map<std::string, std::string> &envs)
{
    uint64_t threshold_ns = FLAGS_rocksdb_slow_query_threshold_ns;
    auto find = envs.find(ROCKSDB_ENV_SLOW_QUERY_THRESHOLD);
    if (find != envs.end()) {
        // get slow query from env(the unit of slow query from env is ms)
        uint64_t threshold_ms;
        if (!dsn::buf2uint64(find->second, threshold_ms) || threshold_ms <= 0) {
            LOG_ERROR_PREFIX("{}={} is invalid.", find->first, find->second);
            return;
        }
        threshold_ns = threshold_ms * 1e6;
    }

    // check if they are changed
    if (_slow_query_threshold_ns != threshold_ns) {
        LOG_INFO_PREFIX("update app env[{}] from \"{}\" to \"{}\" succeed",
                        ROCKSDB_ENV_SLOW_QUERY_THRESHOLD,
                        _slow_query_threshold_ns,
                        threshold_ns);
        _slow_query_threshold_ns = threshold_ns;
    }
}

void pegasus_server_impl::update_rocksdb_iteration_threshold(
    const std::map<std::string, std::string> &envs)
{
    uint64_t threshold_ms = FLAGS_rocksdb_iteration_threshold_time_ms;
    auto find = envs.find(ROCKSDB_ITERATION_THRESHOLD_TIME_MS);
    if (find != envs.end()) {
        // the unit of iteration threshold from env is ms
        if (!dsn::buf2uint64(find->second, threshold_ms) || threshold_ms < 0) {
            LOG_ERROR_PREFIX("{}={} is invalid.", find->first, find->second);
            return;
        }
    }

    if (_rng_rd_opts.rocksdb_iteration_threshold_time_ms != threshold_ms) {
        LOG_INFO_PREFIX("update app env[{}] from \"{}\" to \"{}\" succeed",
                        ROCKSDB_ITERATION_THRESHOLD_TIME_MS,
                        _rng_rd_opts.rocksdb_iteration_threshold_time_ms,
                        threshold_ms);
        _rng_rd_opts.rocksdb_iteration_threshold_time_ms = threshold_ms;
    }
}

void pegasus_server_impl::update_rocksdb_block_cache_enabled(
    const std::map<std::string, std::string> &envs)
{
    // default of ReadOptions:fill_cache is true
    bool cache_enabled = true;
    auto find = envs.find(ROCKSDB_BLOCK_CACHE_ENABLED);
    if (find != envs.end()) {
        if (!dsn::buf2bool(find->second, cache_enabled)) {
            LOG_ERROR_PREFIX("{}={} is invalid.", find->first, find->second);
            return;
        }
    }

    if (_data_cf_rd_opts.fill_cache != cache_enabled) {
        LOG_INFO_PREFIX("update app env[{}] from \"{}\" to \"{}\" succeed",
                        ROCKSDB_BLOCK_CACHE_ENABLED,
                        _data_cf_rd_opts.fill_cache,
                        cache_enabled);
        _data_cf_rd_opts.fill_cache = cache_enabled;
    }
}

void pegasus_server_impl::update_validate_partition_hash(
    const std::map<std::string, std::string> &envs)
{
    bool new_value = false;
    auto iter = envs.find(SPLIT_VALIDATE_PARTITION_HASH);
    if (iter != envs.end()) {
        if (!dsn::buf2bool(iter->second, new_value)) {
            LOG_ERROR_PREFIX("{}={} is invalid.", iter->first, iter->second);
            return;
        }
    }
    if (new_value != _validate_partition_hash) {
        LOG_INFO_PREFIX(
            "update '_validate_partition_hash' from {} to {}", _validate_partition_hash, new_value);
        _validate_partition_hash = new_value;
        _key_ttl_compaction_filter_factory->SetValidatePartitionHash(_validate_partition_hash);
    }
}

void pegasus_server_impl::update_user_specified_compaction(
    const std::map<std::string, std::string> &envs)
{
    auto iter = envs.find(USER_SPECIFIED_COMPACTION);
    if (dsn_unlikely(iter == envs.end() && _user_specified_compaction != "")) {
        LOG_INFO_PREFIX("clear user specified compaction coz it was deleted");
        _key_ttl_compaction_filter_factory->clear_user_specified_ops();
        _user_specified_compaction = "";
        return;
    }
    if (dsn_unlikely(iter != envs.end() && iter->second != _user_specified_compaction)) {
        LOG_INFO_PREFIX("update user specified compaction coz it was changed");
        _key_ttl_compaction_filter_factory->extract_user_specified_ops(iter->second);
        _user_specified_compaction = iter->second;
        return;
    }
}

bool pegasus_server_impl::parse_allow_ingest_behind(const std::map<std::string, std::string> &envs)
{
    bool allow_ingest_behind = false;
    const auto &iter = envs.find(ROCKSDB_ALLOW_INGEST_BEHIND);
    if (iter == envs.end()) {
        return allow_ingest_behind;
    }
    if (!dsn::buf2bool(iter->second, allow_ingest_behind)) {
        LOG_WARNING_PREFIX(
            "{}={} is invalid, set allow_ingest_behind = false", iter->first, iter->second);
        return false;
    }
    LOG_INFO_PREFIX("update allow_ingest_behind = {}", allow_ingest_behind);
    return allow_ingest_behind;
}

bool pegasus_server_impl::parse_compression_types(
    const std::string &config, std::vector<rocksdb::CompressionType> &compression_per_level)
{
    std::vector<rocksdb::CompressionType> tmp(_data_cf_opts.num_levels, rocksdb::kNoCompression);
    size_t i = config.find(COMPRESSION_HEADER);
    if (i != std::string::npos) {
        // New compression config style.
        // 'per_level:[none|snappy|zstd|lz4],[none|snappy|zstd|lz4],...' for each level 0,1,...
        // The last compression type will be used for levels not specified in the list.
        std::vector<std::string> compression_types;
        dsn::utils::split_args(
            config.substr(COMPRESSION_HEADER.length()).c_str(), compression_types, ',');
        rocksdb::CompressionType last_type = rocksdb::kNoCompression;
        for (int i = 0; i < _data_cf_opts.num_levels; ++i) {
            if (i < compression_types.size()) {
                if (!compression_str_to_type(compression_types[i], last_type)) {
                    return false;
                }
            }
            tmp[i] = last_type;
        }
    } else {
        // Old compression config style.
        // '[none|snappy|zstd|lz4]' for all level 2 and higher levels
        rocksdb::CompressionType compression;
        if (!compression_str_to_type(config, compression)) {
            return false;
        }
        if (compression != rocksdb::kNoCompression) {
            // only compress levels >= 2
            // refer to ColumnFamilyOptions::OptimizeLevelStyleCompaction()
            for (int i = 0; i < _data_cf_opts.num_levels; ++i) {
                if (i >= 2) {
                    tmp[i] = compression;
                }
            }
        }
    }

    compression_per_level = tmp;
    return true;
}

bool pegasus_server_impl::compression_str_to_type(const std::string &compression_str,
                                                  rocksdb::CompressionType &type)
{
    if (compression_str == "none") {
        type = rocksdb::kNoCompression;
    } else if (compression_str == "snappy") {
        type = rocksdb::kSnappyCompression;
    } else if (compression_str == "lz4") {
        type = rocksdb::kLZ4Compression;
    } else if (compression_str == "zstd") {
        type = rocksdb::kZSTD;
    } else {
        LOG_ERROR_PREFIX("Unsupported compression type: {}.", compression_str);
        return false;
    }
    return true;
}

std::string pegasus_server_impl::compression_type_to_str(rocksdb::CompressionType type)
{
    switch (type) {
    case rocksdb::kNoCompression:
        return "none";
    case rocksdb::kSnappyCompression:
        return "snappy";
    case rocksdb::kLZ4Compression:
        return "lz4";
    case rocksdb::kZSTD:
        return "zstd";
    default:
        LOG_ERROR_PREFIX("Unsupported compression type: {}.", static_cast<int>(type));
        return "<unsupported>";
    }
}

bool pegasus_server_impl::set_usage_scenario(const std::string &usage_scenario)
{
    if (usage_scenario == _usage_scenario)
        return false;
    std::string old_usage_scenario = _usage_scenario;
    std::unordered_map<std::string, std::string> new_options;
    if (usage_scenario == ROCKSDB_ENV_USAGE_SCENARIO_NORMAL ||
        usage_scenario == ROCKSDB_ENV_USAGE_SCENARIO_PREFER_WRITE) {
        if (_usage_scenario == ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD) {
            // old usage scenario is bulk load, reset first
            new_options["level0_file_num_compaction_trigger"] =
                std::to_string(_data_cf_opts.level0_file_num_compaction_trigger);
            new_options["level0_slowdown_writes_trigger"] =
                std::to_string(_data_cf_opts.level0_slowdown_writes_trigger);
            new_options["level0_stop_writes_trigger"] =
                std::to_string(_data_cf_opts.level0_stop_writes_trigger);
            new_options["soft_pending_compaction_bytes_limit"] =
                std::to_string(_data_cf_opts.soft_pending_compaction_bytes_limit);
            new_options["hard_pending_compaction_bytes_limit"] =
                std::to_string(_data_cf_opts.hard_pending_compaction_bytes_limit);
            new_options["disable_auto_compactions"] = "false";
            new_options["max_compaction_bytes"] =
                std::to_string(_data_cf_opts.max_compaction_bytes);
            new_options["write_buffer_size"] = std::to_string(_data_cf_opts.write_buffer_size);
            new_options["max_write_buffer_number"] =
                std::to_string(_data_cf_opts.max_write_buffer_number);
        }

        if (usage_scenario == ROCKSDB_ENV_USAGE_SCENARIO_NORMAL) {
            new_options["write_buffer_size"] =
                std::to_string(get_random_nearby(_data_cf_opts.write_buffer_size));
            new_options["level0_file_num_compaction_trigger"] =
                std::to_string(_data_cf_opts.level0_file_num_compaction_trigger);
        } else { // ROCKSDB_ENV_USAGE_SCENARIO_PREFER_WRITE
            uint64_t buffer_size = dsn::rand::next_u64(_data_cf_opts.write_buffer_size,
                                                       _data_cf_opts.write_buffer_size * 2);
            new_options["write_buffer_size"] = std::to_string(buffer_size);
            uint64_t max_size = get_random_nearby(_data_cf_opts.max_bytes_for_level_base);
            new_options["level0_file_num_compaction_trigger"] =
                std::to_string(std::max<uint64_t>(4UL, max_size / buffer_size));
        }
    } else if (usage_scenario == ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD) {
        // refer to Options::PrepareForBulkLoad()
        new_options["level0_file_num_compaction_trigger"] = "1000000000";
        new_options["level0_slowdown_writes_trigger"] = "1000000000";
        new_options["level0_stop_writes_trigger"] = "1000000000";
        new_options["soft_pending_compaction_bytes_limit"] = "0";
        new_options["hard_pending_compaction_bytes_limit"] = "0";
        new_options["disable_auto_compactions"] = "true";
        new_options["max_compaction_bytes"] = std::to_string(static_cast<uint64_t>(1) << 60);
        new_options["write_buffer_size"] =
            std::to_string(get_random_nearby(_data_cf_opts.write_buffer_size * 4));
        new_options["max_write_buffer_number"] =
            std::to_string(std::max(_data_cf_opts.max_write_buffer_number, 6));
    } else {
        LOG_ERROR_PREFIX("invalid usage scenario: {}", usage_scenario);
        return false;
    }
    if (set_options(new_options)) {
        _meta_store->set_usage_scenario(usage_scenario);
        _usage_scenario = usage_scenario;
        LOG_INFO_PREFIX(
            "set usage scenario from \"{}\" to \"{}\" succeed", old_usage_scenario, usage_scenario);
        return true;
    } else {
        LOG_ERROR_PREFIX(
            "set usage scenario from \"{}\" to \"{}\" failed", old_usage_scenario, usage_scenario);
        return false;
    }
}

void pegasus_server_impl::reset_rocksdb_options(const rocksdb::ColumnFamilyOptions &base_cf_opts,
                                                const rocksdb::DBOptions &base_db_opt,
                                                const std::map<std::string, std::string> &envs,
                                                rocksdb::ColumnFamilyOptions *target_cf_opts,
                                                rocksdb::DBOptions *target_db_opt)
{
    LOG_INFO_PREFIX("Reset rocksdb envs options");
    // Reset rocksdb option includes two aspects:
    // 1. Set usage_scenario related rocksdb options
    // 2. Rocksdb option set in app envs, consists of ROCKSDB_DYNAMIC_OPTIONS and
    // ROCKSDB_STATIC_OPTIONS

    // aspect 1:
    reset_usage_scenario_options(base_cf_opts, target_cf_opts);

    // aspect 2:
    target_cf_opts->num_levels = base_cf_opts.num_levels;
    target_cf_opts->write_buffer_size = base_cf_opts.write_buffer_size;

    reset_allow_ingest_behind_option(base_db_opt, envs, target_db_opt);
}

void pegasus_server_impl::reset_usage_scenario_options(
    const rocksdb::ColumnFamilyOptions &base_opts, rocksdb::ColumnFamilyOptions *target_opts)
{
    // reset usage scenario related options, refer to options set in 'set_usage_scenario'
    // function.
    target_opts->level0_file_num_compaction_trigger = base_opts.level0_file_num_compaction_trigger;
    target_opts->level0_slowdown_writes_trigger = base_opts.level0_slowdown_writes_trigger;
    target_opts->level0_stop_writes_trigger = base_opts.level0_stop_writes_trigger;
    target_opts->soft_pending_compaction_bytes_limit =
        base_opts.soft_pending_compaction_bytes_limit;
    target_opts->hard_pending_compaction_bytes_limit =
        base_opts.hard_pending_compaction_bytes_limit;
    target_opts->disable_auto_compactions = base_opts.disable_auto_compactions;
    target_opts->max_compaction_bytes = base_opts.max_compaction_bytes;
    target_opts->write_buffer_size = base_opts.write_buffer_size;
    target_opts->max_write_buffer_number = base_opts.max_write_buffer_number;
}

void pegasus_server_impl::reset_allow_ingest_behind_option(
    const rocksdb::DBOptions &base_db_opt,
    const std::map<std::string, std::string> &envs,
    rocksdb::DBOptions *target_db_opt)
{
    if (envs.empty()) {
        // for reopen db during load balance learning
        target_db_opt->allow_ingest_behind = base_db_opt.allow_ingest_behind;
    } else {
        target_db_opt->allow_ingest_behind = parse_allow_ingest_behind(envs);
    }
}

void pegasus_server_impl::recalculate_data_cf_options(
    const rocksdb::ColumnFamilyOptions &cur_data_cf_opts)
{
#define UPDATE_NUMBER_OPTION_IF_NEEDED(option, value)                                              \
    do {                                                                                           \
        auto _v = (value);                                                                         \
        if (_v != cur_data_cf_opts.option) {                                                       \
            new_options[#option] = std::to_string(_v);                                             \
        }                                                                                          \
    } while (0)

#define UPDATE_BOOL_OPTION_IF_NEEDED(option, value)                                                \
    do {                                                                                           \
        auto _v = (value);                                                                         \
        if (_v != cur_data_cf_opts.option) {                                                       \
            if (_v) {                                                                              \
                new_options[#option] = "true";                                                     \
            } else {                                                                               \
                new_options[#option] = "false";                                                    \
            }                                                                                      \
        }                                                                                          \
    } while (0)

#define UPDATE_OPTION_IF_NOT_NEARBY(option, value)                                                 \
    do {                                                                                           \
        auto _v = (value);                                                                         \
        if (!check_value_if_nearby(_v, cur_data_cf_opts.option)) {                                 \
            new_options[#option] = std::to_string(get_random_nearby(_v));                          \
        }                                                                                          \
    } while (0)

#define UPDATE_OPTION_IF_NEEDED(option) UPDATE_NUMBER_OPTION_IF_NEEDED(option, _data_cf_opts.option)

    if (_table_data_cf_opts_recalculated) {
        return;
    }
    std::unordered_map<std::string, std::string> new_options;
    if (ROCKSDB_ENV_USAGE_SCENARIO_NORMAL == _usage_scenario ||
        ROCKSDB_ENV_USAGE_SCENARIO_PREFER_WRITE == _usage_scenario) {
        if (ROCKSDB_ENV_USAGE_SCENARIO_NORMAL == _usage_scenario) {
            UPDATE_OPTION_IF_NOT_NEARBY(write_buffer_size, _data_cf_opts.write_buffer_size);
            UPDATE_OPTION_IF_NEEDED(level0_file_num_compaction_trigger);
        } else {
            uint64_t buffer_size = dsn::rand::next_u64(_data_cf_opts.write_buffer_size,
                                                       _data_cf_opts.write_buffer_size * 2);
            if (cur_data_cf_opts.write_buffer_size < _data_cf_opts.write_buffer_size ||
                cur_data_cf_opts.write_buffer_size > _data_cf_opts.write_buffer_size * 2) {
                new_options["write_buffer_size"] = std::to_string(buffer_size);
                uint64_t max_size = get_random_nearby(_data_cf_opts.max_bytes_for_level_base);
                new_options["level0_file_num_compaction_trigger"] =
                    std::to_string(std::max<uint64_t>(4UL, max_size / buffer_size));
            } else if (!check_value_if_nearby(_data_cf_opts.max_bytes_for_level_base,
                                              cur_data_cf_opts.max_bytes_for_level_base)) {
                uint64_t max_size = get_random_nearby(_data_cf_opts.max_bytes_for_level_base);
                new_options["level0_file_num_compaction_trigger"] =
                    std::to_string(std::max<uint64_t>(4UL, max_size / buffer_size));
            }
        }
        UPDATE_OPTION_IF_NEEDED(level0_slowdown_writes_trigger);
        UPDATE_OPTION_IF_NEEDED(level0_stop_writes_trigger);
        UPDATE_OPTION_IF_NEEDED(soft_pending_compaction_bytes_limit);
        UPDATE_OPTION_IF_NEEDED(hard_pending_compaction_bytes_limit);
        UPDATE_BOOL_OPTION_IF_NEEDED(disable_auto_compactions, false);
        UPDATE_OPTION_IF_NEEDED(max_compaction_bytes);
        UPDATE_OPTION_IF_NEEDED(max_write_buffer_number);
    } else {
        // ROCKSDB_ENV_USAGE_SCENARIO_BULK_LOAD
        UPDATE_NUMBER_OPTION_IF_NEEDED(level0_file_num_compaction_trigger, 1000000000);
        UPDATE_NUMBER_OPTION_IF_NEEDED(level0_slowdown_writes_trigger, 1000000000);
        UPDATE_NUMBER_OPTION_IF_NEEDED(level0_stop_writes_trigger, 1000000000);
        UPDATE_NUMBER_OPTION_IF_NEEDED(soft_pending_compaction_bytes_limit, 0);
        UPDATE_NUMBER_OPTION_IF_NEEDED(hard_pending_compaction_bytes_limit, 0);
        UPDATE_BOOL_OPTION_IF_NEEDED(disable_auto_compactions, true);
        UPDATE_NUMBER_OPTION_IF_NEEDED(max_compaction_bytes, static_cast<uint64_t>(1) << 60);
        UPDATE_OPTION_IF_NOT_NEARBY(write_buffer_size, _data_cf_opts.write_buffer_size * 4);
        UPDATE_NUMBER_OPTION_IF_NEEDED(max_write_buffer_number,
                                       std::max(_data_cf_opts.max_write_buffer_number, 6));
    }
    if (new_options.size() > 0) {
        if (set_options(new_options)) {
            LOG_INFO_PREFIX("recalculate the value of the options related to usage scenario \"{}\"",
                            _usage_scenario);
        }
    }
    _table_data_cf_opts_recalculated = true;
#undef UPDATE_OPTION_IF_NEEDED
#undef UPDATE_BOOL_OPTION_IF_NEEDED
#undef UPDATE_NUMBER_OPTION_IF_NEEDED
}

bool pegasus_server_impl::set_options(
    const std::unordered_map<std::string, std::string> &new_options)
{
    if (!_is_open) {
        LOG_WARNING_PREFIX("set_options failed, db is not open");
        return false;
    }

    std::ostringstream oss;
    int i = 0;
    for (auto &kv : new_options) {
        if (i > 0)
            oss << ",";
        oss << kv.first << "=" << kv.second;
        i++;
    }
    rocksdb::Status status = _db->SetOptions(_data_cf, new_options);
    if (status == rocksdb::Status::OK()) {
        LOG_INFO_PREFIX("rocksdb set options returns {}: {}", status.ToString(), oss.str());
        return true;
    } else {
        LOG_ERROR_PREFIX("rocksdb set options returns {}: {}", status.ToString(), oss.str());
        return false;
    }
}

::dsn::error_code pegasus_server_impl::check_column_families(const std::string &path,
                                                             bool *missing_meta_cf,
                                                             bool *missing_data_cf)
{
    *missing_meta_cf = true;
    *missing_data_cf = true;
    std::vector<std::string> column_families;
    auto s = rocksdb::DB::ListColumnFamilies(_db_opts, path, &column_families);
    if (!s.ok()) {
        LOG_ERROR_PREFIX("rocksdb::DB::ListColumnFamilies failed, error = {}", s.ToString());
        if (s.IsCorruption() &&
            s.ToString().find("VersionEdit: unknown tag") != std::string::npos) {
            LOG_ERROR_PREFIX("there are some unknown tags in MANIFEST, make sure you are upgrade "
                             "from Pegasus 2.1 or higher version");
        }
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }

    if (column_families.empty()) {
        LOG_ERROR_PREFIX("column families are empty");
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }

    for (const auto &column_family : column_families) {
        if (column_family == META_COLUMN_FAMILY_NAME) {
            *missing_meta_cf = false;
        } else if (column_family == DATA_COLUMN_FAMILY_NAME) {
            *missing_data_cf = false;
        } else {
            LOG_ERROR_PREFIX("unknown column family name: {}", column_family);
            return ::dsn::ERR_LOCAL_APP_FAILURE;
        }
    }
    return ::dsn::ERR_OK;
}

uint64_t pegasus_server_impl::do_manual_compact(const rocksdb::CompactRangeOptions &options)
{
    // wait flush before compact to make all data compacted.
    uint64_t start_time = dsn_now_ms();
    flush_all_family_columns(true);
    LOG_INFO_PREFIX("finish flush_all_family_columns, time_used = {} ms",
                    dsn_now_ms() - start_time);

    // do compact
    LOG_INFO_PREFIX(
        "start CompactRange, target_level = {}, bottommost_level_compaction = {}",
        options.target_level,
        options.bottommost_level_compaction == rocksdb::BottommostLevelCompaction::kForce ? "force"
                                                                                          : "skip");
    start_time = dsn_now_ms();
    auto status = _db->CompactRange(options, _data_cf, nullptr, nullptr);
    auto end_time = dsn_now_ms();
    LOG_INFO_PREFIX("finish CompactRange, status = {}, time_used = {}ms",
                    status.ToString(),
                    end_time - start_time);
    _meta_store->set_last_manual_compact_finish_time(end_time);
    // generate new checkpoint and remove old checkpoints, in order to release storage asap
    if (!release_storage_after_manual_compact()) {
        // it is possible that the new checkpoint is not generated, if there was no data
        // written into rocksdb when doing manual compact.
        // we will try to generate it again, and it will probably succeed because at least some
        // empty data is written into rocksdb by periodic group check.
        LOG_INFO_PREFIX("release storage failed after manual compact, will retry after 5 minutes");
        ::dsn::tasking::enqueue(LPC_PEGASUS_SERVER_DELAY,
                                &_tracker,
                                [this]() {
                                    LOG_INFO_PREFIX("retry release storage after manual compact");
                                    release_storage_after_manual_compact();
                                },
                                0,
                                std::chrono::minutes(5));
    }

    // update rocksdb statistics immediately
    update_replica_rocksdb_statistics();

    uint64_t last_manual_compact_finish_time = 0;
    CHECK_OK_PREFIX(
        _meta_store->get_last_manual_compact_finish_time(&last_manual_compact_finish_time));
    return last_manual_compact_finish_time;
}

bool pegasus_server_impl::release_storage_after_manual_compact()
{
    int64_t old_last_durable = last_durable_decree();

    // wait flush before async checkpoint to make all data compacted
    uint64_t start_time = dsn_now_ms();
    flush_all_family_columns(true);
    LOG_INFO_PREFIX("finish flush_all_family_columns, time_used = {} ms",
                    dsn_now_ms() - start_time);

    // async checkpoint
    LOG_INFO_PREFIX("start async_checkpoint");
    start_time = dsn_now_ms();
    ::dsn::error_code err = async_checkpoint(false);
    LOG_INFO_PREFIX(
        "finish async_checkpoint, return = {}, time_used = {}ms", err, dsn_now_ms() - start_time);

    // gc checkpoints
    LOG_INFO_PREFIX("start gc_checkpoints");
    start_time = dsn_now_ms();
    gc_checkpoints(true);
    LOG_INFO_PREFIX("finish gc_checkpoints, time_used = {}ms", dsn_now_ms() - start_time);

    uint64_t new_last_durable = 0;
    CHECK_OK_PREFIX(_meta_store->get_last_flushed_decree(&new_last_durable));
    if (new_last_durable > old_last_durable) {
        LOG_INFO_PREFIX("release storage succeed, last_durable_decree changed from {} to {}",
                        old_last_durable,
                        new_last_durable);
        return true;
    } else {
        LOG_INFO_PREFIX("release storage failed, last_durable_decree remains {}", new_last_durable);
        return false;
    }
}

std::string pegasus_server_impl::query_compact_state() const
{
    return _manual_compact_svc.query_compact_state();
}

void pegasus_server_impl::set_partition_version(int32_t partition_version)
{
    int32_t old_partition_version = _partition_version.exchange(partition_version);
    LOG_INFO_PREFIX(
        "update partition version from {} to {}", old_partition_version, partition_version);
    _key_ttl_compaction_filter_factory->SetPartitionVersion(partition_version);
}

::dsn::error_code pegasus_server_impl::flush_all_family_columns(bool wait)
{
    rocksdb::FlushOptions options;
    options.wait = wait;
    rocksdb::Status status = _db->Flush(options, {_meta_cf, _data_cf});
    if (!status.ok()) {
        LOG_ERROR_PREFIX("flush failed, error = {}", status.ToString());
        return ::dsn::ERR_LOCAL_APP_FAILURE;
    }
    return ::dsn::ERR_OK;
}

void pegasus_server_impl::release_db()
{
    if (_db) {
        CHECK_NOTNULL_PREFIX(_data_cf);
        CHECK_NOTNULL_PREFIX(_meta_cf);
        _db->DestroyColumnFamilyHandle(_data_cf);
        _data_cf = nullptr;
        _db->DestroyColumnFamilyHandle(_meta_cf);
        _meta_cf = nullptr;
        delete _db;
        _db = nullptr;
    }
}

std::string pegasus_server_impl::dump_write_request(dsn::message_ex *request)
{
    dsn::task_code rpc_code(request->rpc_code());
    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_PUT) {
        auto put = put_rpc(request).request();
        ::dsn::blob hash_key, sort_key;
        pegasus_restore_key(put.key, hash_key, sort_key);
        return fmt::format("put: hash_key={}, sort_key={}",
                           pegasus::utils::c_escape_sensitive_string(hash_key),
                           pegasus::utils::c_escape_sensitive_string(sort_key));
    }

    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_MULTI_PUT) {
        auto multi_put = multi_put_rpc(request).request();
        return fmt::format("multi_put: hash_key={}, multi_put_count={}",
                           pegasus::utils::c_escape_sensitive_string(multi_put.hash_key),
                           multi_put.kvs.size());
    }

    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_SET) {
        auto check_and_set = check_and_set_rpc(request).request();
        return fmt::format("check_and_set: hash_key={}, check_sort_key={}, set_sort_key={}",
                           pegasus::utils::c_escape_sensitive_string(check_and_set.hash_key),
                           pegasus::utils::c_escape_sensitive_string(check_and_set.check_sort_key),
                           pegasus::utils::c_escape_sensitive_string(check_and_set.set_sort_key));
    }

    if (rpc_code == dsn::apps::RPC_RRDB_RRDB_CHECK_AND_MUTATE) {
        auto check_and_mutate = check_and_mutate_rpc(request).request();
        return fmt::format(
            "check_and_mutate: hash_key={}, check_sort_key={}, set_value_count={}",
            pegasus::utils::c_escape_sensitive_string(check_and_mutate.hash_key),
            pegasus::utils::c_escape_sensitive_string(check_and_mutate.check_sort_key),
            check_and_mutate.mutate_list.size());
    }

    return "default";
}

void pegasus_server_impl::set_ingestion_status(dsn::replication::ingestion_status::type status)
{
    LOG_INFO_PREFIX("ingestion status from {} to {}",
                    dsn::enum_to_string(_ingestion_status),
                    dsn::enum_to_string(status));
    _ingestion_status = status;
}

void pegasus_server_impl::on_detect_hotkey(const dsn::replication::detect_hotkey_request &req,
                                           dsn::replication::detect_hotkey_response &resp)
{

    if (dsn_unlikely(req.action != dsn::replication::detect_action::START &&
                     req.action != dsn::replication::detect_action::STOP &&
                     req.action != dsn::replication::detect_action::QUERY)) {
        resp.err = dsn::ERR_INVALID_PARAMETERS;
        resp.__set_err_hint("invalid detect_action");
        return;
    }

    if (dsn_unlikely(req.type != dsn::replication::hotkey_type::READ &&
                     req.type != dsn::replication::hotkey_type::WRITE)) {
        resp.err = dsn::ERR_INVALID_PARAMETERS;
        resp.__set_err_hint("invalid hotkey_type");
        return;
    }

    auto collector = req.type == dsn::replication::hotkey_type::READ ? _read_hotkey_collector
                                                                     : _write_hotkey_collector;
    collector->handle_rpc(req, resp);
}

uint32_t pegasus_server_impl::query_data_version() const { return _pegasus_data_version; }

dsn::replication::manual_compaction_status::type pegasus_server_impl::query_compact_status() const
{
    return _manual_compact_svc.query_compact_status();
}

} // namespace server
} // namespace pegasus
