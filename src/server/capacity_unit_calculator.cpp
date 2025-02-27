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

#include "capacity_unit_calculator.h"

#include <rocksdb/status.h>
#include <sys/param.h>
#include <cmath>
#include <string_view>

#include "hotkey_collector.h"
#include "rpc/rpc_message.h"
#include "rrdb/rrdb_types.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/flags.h"
#include "utils/fmt_logging.h"
#include "utils/token_bucket_throttling_controller.h"

METRIC_DEFINE_counter(replica,
                      read_capacity_units,
                      dsn::metric_unit::kCapacityUnits,
                      "The number of capacity units for read requests");

METRIC_DEFINE_counter(replica,
                      write_capacity_units,
                      dsn::metric_unit::kCapacityUnits,
                      "The number of capacity units for write requests");

METRIC_DEFINE_counter(replica,
                      get_bytes,
                      dsn::metric_unit::kBytes,
                      "The number of bytes for GET requests");

METRIC_DEFINE_counter(replica,
                      multi_get_bytes,
                      dsn::metric_unit::kBytes,
                      "The number of bytes for MULTI_GET requests");

METRIC_DEFINE_counter(replica,
                      batch_get_bytes,
                      dsn::metric_unit::kBytes,
                      "The number of bytes for BATCH_GET requests");

METRIC_DEFINE_counter(replica,
                      scan_bytes,
                      dsn::metric_unit::kBytes,
                      "The number of bytes for SCAN requests");

METRIC_DEFINE_counter(replica,
                      put_bytes,
                      dsn::metric_unit::kBytes,
                      "The number of bytes for PUT requests");

METRIC_DEFINE_counter(replica,
                      multi_put_bytes,
                      dsn::metric_unit::kBytes,
                      "The number of bytes for MULTI_PUT requests");

METRIC_DEFINE_counter(replica,
                      check_and_set_bytes,
                      dsn::metric_unit::kBytes,
                      "The number of bytes for CHECK_AND_SET requests");

METRIC_DEFINE_counter(replica,
                      check_and_mutate_bytes,
                      dsn::metric_unit::kBytes,
                      "The number of bytes for CHECK_AND_MUTATE requests");

METRIC_DEFINE_counter(replica,
                      backup_request_bytes,
                      dsn::metric_unit::kBytes,
                      "The number of bytes for backup requests");

DSN_DEFINE_uint64(pegasus.server,
                  perf_counter_read_capacity_unit_size,
                  4 * 1024,
                  "capacity unit size of read requests, default 4KB");
DSN_DEFINE_validator(perf_counter_read_capacity_unit_size,
                     [](const uint64_t value) -> bool { return powerof2(value); });

DSN_DEFINE_uint64(pegasus.server,
                  perf_counter_write_capacity_unit_size,
                  4 * 1024,
                  "capacity unit size of write requests, default 4KB");
DSN_DEFINE_validator(perf_counter_write_capacity_unit_size,
                     [](const uint64_t value) -> bool { return powerof2(value); });

namespace pegasus {
namespace server {

capacity_unit_calculator::capacity_unit_calculator(
    replica_base *r,
    std::shared_ptr<hotkey_collector> read_hotkey_collector,
    std::shared_ptr<hotkey_collector> write_hotkey_collector,
    std::shared_ptr<throttling_controller> read_size_throttling_controller)
    : replica_base(r),
      METRIC_VAR_INIT_replica(read_capacity_units),
      METRIC_VAR_INIT_replica(write_capacity_units),
      METRIC_VAR_INIT_replica(get_bytes),
      METRIC_VAR_INIT_replica(multi_get_bytes),
      METRIC_VAR_INIT_replica(batch_get_bytes),
      METRIC_VAR_INIT_replica(scan_bytes),
      METRIC_VAR_INIT_replica(put_bytes),
      METRIC_VAR_INIT_replica(multi_put_bytes),
      METRIC_VAR_INIT_replica(check_and_set_bytes),
      METRIC_VAR_INIT_replica(check_and_mutate_bytes),
      METRIC_VAR_INIT_replica(backup_request_bytes),
      _read_hotkey_collector(read_hotkey_collector),
      _write_hotkey_collector(write_hotkey_collector),
      _read_size_throttling_controller(read_size_throttling_controller)
{
    CHECK(_read_hotkey_collector, "read hotkey collector is a nullptr");
    CHECK(_write_hotkey_collector, "write hotkey collector is a nullptr");
    CHECK(_read_size_throttling_controller, "_read_size_throttling_controller is a nullptr");

    _log_read_cu_size = log(FLAGS_perf_counter_read_capacity_unit_size) / log(2);
    _log_write_cu_size = log(FLAGS_perf_counter_write_capacity_unit_size) / log(2);
}

int64_t capacity_unit_calculator::add_read_cu(int64_t read_data_size)
{
    int64_t read_cu =
        read_data_size > 0
            ? (read_data_size + FLAGS_perf_counter_read_capacity_unit_size - 1) >> _log_read_cu_size
            : 1;
    METRIC_VAR_INCREMENT_BY(read_capacity_units, read_cu);
    _read_size_throttling_controller->consume_token(read_data_size);
    return read_cu;
}

int64_t capacity_unit_calculator::add_write_cu(int64_t write_data_size)
{
    int64_t write_cu = write_data_size > 0
                           ? (write_data_size + FLAGS_perf_counter_write_capacity_unit_size - 1) >>
                                 _log_write_cu_size
                           : 1;
    METRIC_VAR_INCREMENT_BY(write_capacity_units, write_cu);
    return write_cu;
}

void capacity_unit_calculator::add_get_cu(dsn::message_ex *req,
                                          int32_t status,
                                          const dsn::blob &key,
                                          const dsn::blob &value)
{
    auto total_size = key.size() + value.size();
    METRIC_VAR_INCREMENT_BY(get_bytes, total_size);
    add_backup_request_bytes(req, total_size);
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kNotFound) {
        return;
    }

    if (status == rocksdb::Status::kNotFound) {
        add_read_cu(1);
        _read_hotkey_collector->capture_raw_key(key, 1);
        return;
    }
    add_read_cu(key.size() + value.size());
    _read_hotkey_collector->capture_raw_key(key, 1);
}

void capacity_unit_calculator::add_multi_get_cu(dsn::message_ex *req,
                                                int32_t status,
                                                const dsn::blob &hash_key,
                                                const std::vector<::dsn::apps::key_value> &kvs)
{
    int64_t data_size = 0;
    int64_t multi_get_bytes = 0;
    for (const auto &kv : kvs) {
        multi_get_bytes += kv.key.size() + kv.value.size();
        data_size += hash_key.size() + kv.key.size() + kv.value.size();
    }
    auto total_size = hash_key.size() + multi_get_bytes;
    METRIC_VAR_INCREMENT_BY(multi_get_bytes, total_size);
    add_backup_request_bytes(req, total_size);

    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kNotFound &&
        status != rocksdb::Status::kIncomplete && status != rocksdb::Status::kInvalidArgument) {
        return;
    }

    uint64_t key_count = kvs.size();
    if (status == rocksdb::Status::kNotFound) {
        add_read_cu(1);
        _read_hotkey_collector->capture_hash_key(hash_key, key_count);
        return;
    }
    add_read_cu(data_size);
    _read_hotkey_collector->capture_hash_key(hash_key, key_count);
}

void capacity_unit_calculator::add_batch_get_cu(dsn::message_ex *req,
                                                int32_t status,
                                                const std::vector<::dsn::apps::full_data> &datas)
{
    int64_t data_size = 0;
    for (const auto &data : datas) {
        data_size += data.hash_key.size() + data.sort_key.size() + data.value.size();
        _read_hotkey_collector->capture_hash_key(data.hash_key, 1);
    }

    METRIC_VAR_INCREMENT_BY(batch_get_bytes, data_size);
    add_backup_request_bytes(req, data_size);

    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kNotFound &&
        status != rocksdb::Status::kIncomplete && status != rocksdb::Status::kInvalidArgument) {
        return;
    }

    if (status == rocksdb::Status::kNotFound) {
        add_read_cu(1);
        return;
    }

    add_read_cu(data_size);
}

void capacity_unit_calculator::add_scan_cu(dsn::message_ex *req,
                                           int32_t status,
                                           const std::vector<::dsn::apps::key_value> &kvs)
{
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kNotFound &&
        status != rocksdb::Status::kIncomplete && status != rocksdb::Status::kInvalidArgument) {
        return;
    }

    if (status == rocksdb::Status::kNotFound) {
        add_read_cu(1);
        return;
    }

    // TODO: (Tangyanzhao) hotkey detect in scan
    int64_t data_size = 0;
    for (const auto &kv : kvs) {
        data_size += kv.key.size() + kv.value.size();
    }
    add_read_cu(data_size);
    METRIC_VAR_INCREMENT_BY(scan_bytes, data_size);
    add_backup_request_bytes(req, data_size);
}

void capacity_unit_calculator::add_sortkey_count_cu(dsn::message_ex *req,
                                                    int32_t status,
                                                    const dsn::blob &hash_key)
{
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kNotFound) {
        return;
    }
    add_read_cu(1);
    add_backup_request_bytes(req, 1);
    _read_hotkey_collector->capture_hash_key(hash_key, 1);
}

void capacity_unit_calculator::add_ttl_cu(dsn::message_ex *req,
                                          int32_t status,
                                          const dsn::blob &key)
{
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kNotFound) {
        return;
    }
    add_read_cu(1);
    add_backup_request_bytes(req, 1);
    _read_hotkey_collector->capture_raw_key(key, 1);
}

void capacity_unit_calculator::add_put_cu(int32_t status,
                                          const dsn::blob &key,
                                          const dsn::blob &value)
{
    METRIC_VAR_INCREMENT_BY(put_bytes, key.size() + value.size());
    if (status != rocksdb::Status::kOk) {
        return;
    }
    add_write_cu(key.size() + value.size());
    _write_hotkey_collector->capture_raw_key(key, 1);
}

void capacity_unit_calculator::add_remove_cu(int32_t status, const dsn::blob &key)
{
    if (status != rocksdb::Status::kOk) {
        return;
    }
    add_write_cu(key.size());
    _write_hotkey_collector->capture_raw_key(key, 1);
}

void capacity_unit_calculator::add_multi_put_cu(int32_t status,
                                                const dsn::blob &hash_key,
                                                const std::vector<::dsn::apps::key_value> &kvs)
{
    int64_t data_size = 0;
    int64_t multi_put_bytes = 0;
    for (const auto &kv : kvs) {
        multi_put_bytes += kv.key.size() + kv.value.size();
        data_size += hash_key.size() + kv.key.size() + kv.value.size();
    }
    METRIC_VAR_INCREMENT_BY(multi_put_bytes, hash_key.size() + multi_put_bytes);
    uint64_t key_count = kvs.size();
    _write_hotkey_collector->capture_hash_key(hash_key, key_count);

    if (status != rocksdb::Status::kOk) {
        return;
    }
    add_write_cu(data_size);
}

void capacity_unit_calculator::add_multi_remove_cu(int32_t status,
                                                   const dsn::blob &hash_key,
                                                   const std::vector<::dsn::blob> &sort_keys)
{
    if (status != rocksdb::Status::kOk) {
        return;
    }

    int64_t data_size = 0;
    for (const auto &sort_key : sort_keys) {
        data_size += hash_key.size() + sort_key.size();
    }
    uint64_t key_count = sort_keys.size();
    _write_hotkey_collector->capture_hash_key(hash_key, key_count);
    add_write_cu(data_size);
}

void capacity_unit_calculator::add_incr_cu(int32_t status, const dsn::blob &key)
{
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kInvalidArgument) {
        return;
    }
    if (status == rocksdb::Status::kOk) {
        add_write_cu(1);
        _write_hotkey_collector->capture_raw_key(key, 1);
    }
    add_read_cu(1);
    _read_hotkey_collector->capture_raw_key(key, 1);
}

void capacity_unit_calculator::add_check_and_set_cu(int32_t status,
                                                    const dsn::blob &hash_key,
                                                    const dsn::blob &check_sort_key,
                                                    const dsn::blob &set_sort_key,
                                                    const dsn::blob &value)
{

    METRIC_VAR_INCREMENT_BY(check_and_set_bytes,
                            hash_key.size() + check_sort_key.size() + set_sort_key.size() +
                                value.size());
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kInvalidArgument &&
        status != rocksdb::Status::kTryAgain) {
        return;
    }

    if (status == rocksdb::Status::kOk) {
        add_write_cu(hash_key.size() + set_sort_key.size() + value.size());
        _write_hotkey_collector->capture_hash_key(hash_key, 1);
    }
    add_read_cu(hash_key.size() + check_sort_key.size());
    _read_hotkey_collector->capture_hash_key(hash_key, 1);
}

void capacity_unit_calculator::add_check_and_mutate_cu(
    int32_t status,
    const dsn::blob &hash_key,
    const dsn::blob &check_sort_key,
    const std::vector<::dsn::apps::mutate> &mutate_list)
{
    int64_t data_size = 0;
    int64_t check_and_mutate_bytes = 0;
    for (const auto &m : mutate_list) {
        check_and_mutate_bytes += m.sort_key.size() + m.value.size();
        data_size += hash_key.size() + m.sort_key.size() + m.value.size();
    }
    METRIC_VAR_INCREMENT_BY(check_and_mutate_bytes,
                            hash_key.size() + check_sort_key.size() + check_and_mutate_bytes);

    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kInvalidArgument &&
        status != rocksdb::Status::kTryAgain) {
        return;
    }
    uint64_t key_count = mutate_list.size();
    if (status == rocksdb::Status::kOk) {
        add_write_cu(data_size);
        _write_hotkey_collector->capture_hash_key(hash_key, key_count);
    }
    add_read_cu(hash_key.size() + check_sort_key.size());
    _read_hotkey_collector->capture_hash_key(hash_key, 1);
}

void capacity_unit_calculator::add_backup_request_bytes(dsn::message_ex *req, int64_t bytes)
{
    if (req->is_backup_request()) {
        METRIC_VAR_INCREMENT_BY(backup_request_bytes, bytes);
    }
}

} // namespace server
} // namespace pegasus
