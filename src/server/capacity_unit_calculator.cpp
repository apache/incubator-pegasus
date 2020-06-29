// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "capacity_unit_calculator.h"
#include <dsn/utility/config_api.h>
#include <rocksdb/status.h>

namespace pegasus {
namespace server {

capacity_unit_calculator::capacity_unit_calculator(
    replica_base *r,
    std::shared_ptr<hotkey_collector> read_hotkey_collector,
    std::shared_ptr<hotkey_collector> write_hotkey_collector)
    : replica_base(r),
      _read_hotkey_collector(read_hotkey_collector),
      _write_hotkey_collector(write_hotkey_collector)
{
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
    dassert(powerof2(_read_capacity_unit_size),
            "'perf_counter_read_capacity_unit_size' must be a power of 2");
    dassert(powerof2(_write_capacity_unit_size),
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
}

int64_t capacity_unit_calculator::add_read_cu(int64_t read_data_size)
{
    int64_t read_cu = read_data_size > 0
                          ? (read_data_size + _read_capacity_unit_size - 1) >> _log_read_cu_size
                          : 1;
    _pfc_recent_read_cu->add(read_cu);
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

void capacity_unit_calculator::add_get_cu(int32_t status,
                                          const dsn::blob &key,
                                          const dsn::blob &value)
{
    _pfc_get_bytes->add(key.size() + value.size());
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kNotFound) {
        return;
    }
    if (status == rocksdb::Status::kNotFound) {
        _read_hotkey_collector->capture_raw_key(key, key.size() + value.size());
        add_read_cu(1);
        return;
    }
    _read_hotkey_collector->capture_raw_key(key, key.size() + value.size());
    add_read_cu(key.size() + value.size());
}

void capacity_unit_calculator::add_multi_get_cu(int32_t status,
                                                const dsn::blob &hash_key,
                                                const std::vector<::dsn::apps::key_value> &kvs)
{
    int64_t data_size = 0;
    int64_t multi_get_bytes = 0;
    for (const auto &kv : kvs) {
        multi_get_bytes += kv.key.size() + kv.value.size();
        data_size += hash_key.size() + kv.key.size() + kv.value.size();
    }
    _pfc_multi_get_bytes->add(hash_key.size() + multi_get_bytes);

    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kNotFound &&
        status != rocksdb::Status::kIncomplete && status != rocksdb::Status::kInvalidArgument) {
        return;
    }
    if (status == rocksdb::Status::kNotFound) {
        _read_hotkey_collector->capture_hash_key(hash_key, 1);
        add_read_cu(1);
        return;
    }
    _read_hotkey_collector->capture_hash_key(hash_key, data_size);
    add_read_cu(data_size);
}

void capacity_unit_calculator::add_scan_cu(int32_t status,
                                           const std::vector<::dsn::apps::key_value> &kvs,
                                           const dsn::blob &hash_key_filter_pattern)
{
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kNotFound &&
        status != rocksdb::Status::kIncomplete && status != rocksdb::Status::kInvalidArgument) {
        return;
    }

    if (status == rocksdb::Status::kNotFound) {
        _read_hotkey_collector->capture_hash_key(hash_key_filter_pattern, 1);
        add_read_cu(1);
        return;
    }

    int64_t data_size = 0;
    for (const auto &kv : kvs) {
        data_size += kv.key.size() + kv.value.size();
    }
    _read_hotkey_collector->capture_hash_key(hash_key_filter_pattern, data_size);
    add_read_cu(data_size);
    _pfc_scan_bytes->add(data_size);
}

void capacity_unit_calculator::add_sortkey_count_cu(int32_t status)
{
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kNotFound) {
        return;
    }
    add_read_cu(1);
}

void capacity_unit_calculator::add_ttl_cu(int32_t status)
{
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kNotFound) {
        return;
    }
    add_read_cu(1);
}

void capacity_unit_calculator::add_put_cu(int32_t status,
                                          const dsn::blob &key,
                                          const dsn::blob &value)
{
    _pfc_put_bytes->add(key.size() + value.size());
    if (status != rocksdb::Status::kOk) {
        return;
    }
    _write_hotkey_collector->capture_raw_key(key, key.size() + value.size());
    add_write_cu(key.size() + value.size());
}

void capacity_unit_calculator::add_remove_cu(int32_t status, const dsn::blob &key)
{
    if (status != rocksdb::Status::kOk) {
        return;
    }
    _write_hotkey_collector->capture_raw_key(key, key.size());
    add_write_cu(key.size());
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
    if (status != rocksdb::Status::kOk) {
        return;
    }
    _write_hotkey_collector->capture_hash_key(hash_key, data_size);
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
    _write_hotkey_collector->capture_hash_key(hash_key, data_size);
    add_write_cu(data_size);
}

void capacity_unit_calculator::add_incr_cu(int32_t status)
{
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kInvalidArgument) {
        return;
    }
    if (status == rocksdb::Status::kOk) {
        add_write_cu(1);
    }
    add_read_cu(1);
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
        _write_hotkey_collector->capture_hash_key(
            hash_key, hash_key.size() + set_sort_key.size() + value.size());
        add_write_cu(hash_key.size() + set_sort_key.size() + value.size());
    }
    add_read_cu(hash_key.size() + check_sort_key.size());
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

    if (status == rocksdb::Status::kOk) {
        _write_hotkey_collector->capture_hash_key(hash_key, data_size);
        add_write_cu(data_size);
    }
    add_read_cu(hash_key.size() + check_sort_key.size());
}

} // namespace server
} // namespace pegasus
