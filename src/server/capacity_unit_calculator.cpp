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

#include "utils/config_api.h"
#include "utils/token_bucket_throttling_controller.h"
#include <rocksdb/status.h>
#include "hotkey_collector.h"
#include "utils/fmt_logging.h"

namespace pegasus {
namespace server {

capacity_unit_calculator::capacity_unit_calculator(
    replica_base *r,
    std::shared_ptr<hotkey_collector> read_hotkey_collector,
    std::shared_ptr<hotkey_collector> write_hotkey_collector,
    std::shared_ptr<throttling_controller> read_size_throttling_controller)
    : replica_base(r),
      _read_hotkey_collector(read_hotkey_collector),
      _write_hotkey_collector(write_hotkey_collector),
      _read_size_throttling_controller(read_size_throttling_controller)
{
    CHECK(_read_hotkey_collector, "read hotkey collector is a nullptr");
    CHECK(_write_hotkey_collector, "write hotkey collector is a nullptr");
    CHECK(_read_size_throttling_controller, "_read_size_throttling_controller is a nullptr");

    _read_capacity_unit_size =
        dsn_config_get_value_uint64("pegasus.server",
                                    "perf_counter_read_capacity_unit_size",
                                    4 * 1024,
                                    "capacity unit size of read requests, default 4KB");
    _write_capacity_unit_size =
        dsn_config_get_value_uint64("pegasus.server",
                                    "perf_counter_write_capacity_unit_size",
                                    4 * 1024,
                                    "capacity unit size of write requests, default 4KB");
    CHECK(powerof2(_read_capacity_unit_size),
          "'perf_counter_read_capacity_unit_size' must be a power of 2");
    CHECK(powerof2(_write_capacity_unit_size),
          "'perf_counter_write_capacity_unit_size' must be a power of 2");
    _log_read_cu_size = log(_read_capacity_unit_size) / log(2);
    _log_write_cu_size = log(_write_capacity_unit_size) / log(2);

    std::string str_gpid = r->get_gpid().to_string();
    char name[256];
    snprintf(name, 255, "recent.read.cu@%s", str_gpid.c_str());
    _pfc_recent_read_cu.init_app_counter("app.pegasus",
                                         name,
                                         COUNTER_TYPE_VOLATILE_NUMBER,
                                         "statistic the recent read capacity units");
    snprintf(name, 255, "recent.write.cu@%s", str_gpid.c_str());
    _pfc_recent_write_cu.init_app_counter("app.pegasus",
                                          name,
                                          COUNTER_TYPE_VOLATILE_NUMBER,
                                          "statistic the recent write capacity units");

    snprintf(name, 255, "get_bytes@%s", str_gpid.c_str());
    _pfc_get_bytes.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the get bytes");

    snprintf(name, 255, "multi_get_bytes@%s", str_gpid.c_str());
    _pfc_multi_get_bytes.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the multi get bytes");

    snprintf(name, 255, "batch_get_bytes@%s", str_gpid.c_str());
    _pfc_batch_get_bytes.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the batch get bytes");

    snprintf(name, 255, "scan_bytes@%s", str_gpid.c_str());
    _pfc_scan_bytes.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the scan bytes");

    snprintf(name, 255, "put_bytes@%s", str_gpid.c_str());
    _pfc_put_bytes.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the put bytes");

    snprintf(name, 255, "multi_put_bytes@%s", str_gpid.c_str());
    _pfc_multi_put_bytes.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the multi put bytes");

    snprintf(name, 255, "check_and_set_bytes@%s", str_gpid.c_str());
    _pfc_check_and_set_bytes.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the check and set bytes");

    snprintf(name, 255, "check_and_mutate_bytes@%s", str_gpid.c_str());
    _pfc_check_and_mutate_bytes.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the check and mutate bytes");

    snprintf(name, 255, "backup_request_bytes@%s", str_gpid.c_str());
    _pfc_backup_request_bytes.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_RATE, "statistic the backup request bytes");
}

int64_t capacity_unit_calculator::add_read_cu(int64_t read_data_size)
{
    int64_t read_cu = read_data_size > 0
                          ? (read_data_size + _read_capacity_unit_size - 1) >> _log_read_cu_size
                          : 1;
    _pfc_recent_read_cu->add(read_cu);
    _read_size_throttling_controller->consume_token(read_data_size);
    return read_cu;
}

int64_t capacity_unit_calculator::add_write_cu(int64_t write_data_size)
{
    int64_t write_cu = write_data_size > 0
                           ? (write_data_size + _write_capacity_unit_size - 1) >> _log_write_cu_size
                           : 1;
    _pfc_recent_write_cu->add(write_cu);
    return write_cu;
}

void capacity_unit_calculator::add_get_cu(dsn::message_ex *req,
                                          int32_t status,
                                          const dsn::blob &key,
                                          const dsn::blob &value)
{
    auto total_size = key.size() + value.size();
    _pfc_get_bytes->add(total_size);
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
    _pfc_multi_get_bytes->add(total_size);
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

    _pfc_batch_get_bytes->add(data_size);
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
    _pfc_scan_bytes->add(data_size);
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
    _pfc_put_bytes->add(key.size() + value.size());
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
    _pfc_multi_put_bytes->add(hash_key.size() + multi_put_bytes);
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

    _pfc_check_and_set_bytes->add(hash_key.size() + check_sort_key.size() + set_sort_key.size() +
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
    _pfc_check_and_mutate_bytes->add(hash_key.size() + check_sort_key.size() +
                                     check_and_mutate_bytes);

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
        _pfc_backup_request_bytes->add(bytes);
    }
}

} // namespace server
} // namespace pegasus
