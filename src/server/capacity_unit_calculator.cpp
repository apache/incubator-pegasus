// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "capacity_unit_calculator.h"
#include <dsn/utility/config_api.h>
#include <rocksdb/status.h>

namespace pegasus {
namespace server {

capacity_unit_calculator::capacity_unit_calculator(replica_base *r) : replica_base(r)
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

    snprintf(name, 255, "recent_get_throughput@%s", str_gpid.c_str());
    _pfc_recent_get_throughput.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_VOLATILE_NUMBER, "statistic the recent get throughput");
    snprintf(name, 255, "recent_multi_get_throughput@%s", str_gpid.c_str());
    _pfc_recent_multi_get_throughput.init_app_counter("app.pegasus",
                                                      name,
                                                      COUNTER_TYPE_VOLATILE_NUMBER,
                                                      "statistic the recent multi get throughput");
    snprintf(name, 255, "recent_scan_throughput@%s", str_gpid.c_str());
    _pfc_recent_scan_throughput.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_VOLATILE_NUMBER, "statistic the recent scan throughput");
    snprintf(name, 255, "recent_sortkey_count_throughput@%s", str_gpid.c_str());
    _pfc_recent_sortkey_count_throughput.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_VOLATILE_NUMBER,
        "statistic the recent sortkey count throughput");
    snprintf(name, 255, "recent_ttl_throughput@%s", str_gpid.c_str());
    _pfc_recent_ttl_throughput.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_VOLATILE_NUMBER, "statistic the recent ttl throughput");

    snprintf(name, 255, "recent_put_throughput@%s", str_gpid.c_str());
    _pfc_recent_put_throughput.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_VOLATILE_NUMBER, "statistic the recent put throughput");
    snprintf(name, 255, "recent_remove_throughput@%s", str_gpid.c_str());
    _pfc_recent_remove_throughput.init_app_counter("app.pegasus",
                                                   name,
                                                   COUNTER_TYPE_VOLATILE_NUMBER,
                                                   "statistic the recent remove throughput");
    snprintf(name, 255, "recent_multi_put_throughput@%s", str_gpid.c_str());
    _pfc_recent_multi_put_throughput.init_app_counter("app.pegasus",
                                                      name,
                                                      COUNTER_TYPE_VOLATILE_NUMBER,
                                                      "statistic the recent multi put throughput");
    snprintf(name, 255, "recent_multi_remove_throughput@%s", str_gpid.c_str());
    _pfc_recent_multi_remove_throughput.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_VOLATILE_NUMBER,
        "statistic the recent multi remove throughput");
    snprintf(name, 255, "recent_incr_throughput@%s", str_gpid.c_str());
    _pfc_recent_incr_throughput.init_app_counter(
        "app.pegasus", name, COUNTER_TYPE_VOLATILE_NUMBER, "statistic the recent incr throughput");
    snprintf(name, 255, "recent_check_and_set_throughput@%s", str_gpid.c_str());
    _pfc_recent_check_and_set_throughput.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_VOLATILE_NUMBER,
        "statistic the recent check and set throughput");
    snprintf(name, 255, "recent_check_and_mutate_throughput@%s", str_gpid.c_str());
    _pfc_recent_check_and_mutate_throughput.init_app_counter(
        "app.pegasus",
        name,
        COUNTER_TYPE_VOLATILE_NUMBER,
        "statistic the recent check and mutate throughput");
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
    if (status != rocksdb::Status::kOk) {
        _pfc_recent_get_throughput->add(key.size());
        return;
    }
    if (status != rocksdb::Status::kNotFound) {
        add_read_cu(kNotFound);
        _pfc_recent_get_throughput->add(key.size());
        return;
    }
    int64_t data_size = key.size() + value.size();
    add_read_cu(data_size);
    _pfc_recent_get_throughput->add(data_size);
}

void capacity_unit_calculator::add_multi_get_cu(int32_t status,
                                                const dsn::blob &hash_key,
                                                const std::vector<::dsn::apps::key_value> &kvs)
{
    int64_t sort_key_data_size = 0;
    for (const auto &kv : kvs) {
        sort_key_data_size += kv.key.size();
    }
    int64_t data_size = hash_key.size() + sort_key_data_size;
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kIncomplete &&
        status != rocksdb::Status::kInvalidArgument) {
        _pfc_recent_multi_get_throughput->add(data_size);
        return;
    }

    if (status != rocksdb::Status::kNotFound) {
        add_read_cu(kNotFound);
        _pfc_recent_multi_get_throughput->add(data_size);
        return;
    }

    for (const auto &kv : kvs) {
        data_size += kv.value.size();
    }
    add_read_cu(data_size);
    _pfc_recent_multi_get_throughput->add(data_size);
}

void capacity_unit_calculator::add_scan_cu(int32_t status,
                                           const std::vector<::dsn::apps::key_value> &kvs)
{
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kIncomplete &&
        status != rocksdb::Status::kInvalidArgument) {
        return;
    }

    if (status != rocksdb::Status::kNotFound) {
        add_read_cu(kNotFound);
        return;
    }

    int64_t data_size = 0;
    for (const auto &kv : kvs) {
        data_size += kv.key.size() + kv.value.size();
    }
    add_read_cu(data_size);
    _pfc_recent_scan_throughput->add(data_size);
}

void capacity_unit_calculator::add_sortkey_count_cu(int32_t status, const dsn::blob &hash_key)
{
    if (status != rocksdb::Status::kOk) {
        _pfc_recent_sortkey_count_throughput->add(hash_key.size());
        return;
    }

    if (status != rocksdb::Status::kNotFound) {
        add_read_cu(kNotFound);
        _pfc_recent_multi_get_throughput->add(hash_key.size());
        return;
    }

    int64_t data_size = hash_key.size() + 1;
    add_read_cu(data_size);
    _pfc_recent_sortkey_count_throughput->add(data_size);
}

void capacity_unit_calculator::add_ttl_cu(int32_t status, const dsn::blob &key)
{
    if (status != rocksdb::Status::kOk) {
        _pfc_recent_sortkey_count_throughput->add(key.size());
        return;
    }

    if (status != rocksdb::Status::kNotFound) {
        add_read_cu(kNotFound);
        _pfc_recent_multi_get_throughput->add(key.size());
        return;
    }

    int64_t data_size = key.size() + 1;
    add_read_cu(data_size);
    _pfc_recent_ttl_throughput->add(data_size);
}

void capacity_unit_calculator::add_put_cu(int32_t status,
                                          const dsn::blob &key,
                                          const dsn::blob &value)
{
    if (status != rocksdb::Status::kOk) {
        _pfc_recent_put_throughput->add(key.size());
        return;
    }
    int64_t data_size = key.size() + value.size();
    add_write_cu(data_size);
    _pfc_recent_put_throughput->add(data_size);
}

void capacity_unit_calculator::add_remove_cu(int32_t status, const dsn::blob &key)
{
    if (status != rocksdb::Status::kOk) {
        _pfc_recent_put_throughput->add(key.size());
        return;
    }
    add_write_cu(key.size());
    _pfc_recent_remove_throughput->add(key.size());
}

void capacity_unit_calculator::add_multi_put_cu(int32_t status,
                                                const dsn::blob &hash_key,
                                                const std::vector<::dsn::apps::key_value> &kvs)
{
    int64_t kv_data_size = 0;
    for (const auto &kv : kvs) {
        kv_data_size += kv.key.size() + kv.value.size();
    }
    int64_t data_size = hash_key.size() + kv_data_size;
    if (status != rocksdb::Status::kOk) {
        _pfc_recent_multi_put_throughput->add(data_size);
        return;
    }

    add_write_cu(data_size);
    _pfc_recent_multi_put_throughput->add(data_size);
}

void capacity_unit_calculator::add_multi_remove_cu(int32_t status,
                                                   const dsn::blob &hash_key,
                                                   const std::vector<::dsn::blob> &sort_keys)
{
    int64_t sort_key_data_size = 0;
    for (const auto &sort_key : sort_keys) {
        sort_key_data_size += sort_key.size();
    }
    int64_t data_size = hash_key.size() + sort_key_data_size;
    if (status != rocksdb::Status::kOk) {
        _pfc_recent_multi_remove_throughput->add(data_size);
        return;
    }

    add_write_cu(data_size);
    _pfc_recent_multi_remove_throughput->add(data_size);
}

void capacity_unit_calculator::add_incr_cu(int32_t status, const dsn::blob &hash_key)
{
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kInvalidArgument) {
        _pfc_recent_put_throughput->add(hash_key.size());
        return;
    }
    int64_t data_size = 0;
    if (status == rocksdb::Status::kOk) {
        data_size = hash_key.size() + 1;
        add_write_cu(data_size);
        _pfc_recent_incr_throughput->add(data_size);
    }
    add_read_cu(hash_key.size());
}

void capacity_unit_calculator::add_check_and_set_cu(int32_t status,
                                                    const dsn::blob &hash_key,
                                                    const dsn::blob &check_sort_key,
                                                    const dsn::blob &set_sort_key,
                                                    const dsn::blob &value)
{
    int64_t data_size = hash_key.size() + set_sort_key.size() + value.size();
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kInvalidArgument &&
        status != rocksdb::Status::kTryAgain) {
        _pfc_recent_check_and_set_throughput->add(data_size);
        return;
    }
    if (status == rocksdb::Status::kOk) {
        add_write_cu(data_size);
    }
    add_read_cu(hash_key.size() + check_sort_key.size());
    _pfc_recent_check_and_set_throughput->add(data_size);
}

void capacity_unit_calculator::add_check_and_mutate_cu(
    int32_t status,
    const dsn::blob &hash_key,
    const dsn::blob &check_sort_key,
    const std::vector<::dsn::apps::mutate> &mutate_list)
{
    int64_t kv_data_size = 0;
    for (const auto &m : mutate_list) {
        kv_data_size += m.sort_key.size() + m.value.size();
    }
    int64_t data_size = hash_key.size() + kv_data_size;
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kInvalidArgument &&
        status != rocksdb::Status::kTryAgain) {
        _pfc_recent_check_and_mutate_throughput->add(data_size);
        return;
    }
    if (status == rocksdb::Status::kOk) {
        add_write_cu(data_size);
    }
    add_read_cu(hash_key.size() + check_sort_key.size());
    _pfc_recent_check_and_mutate_throughput->add(data_size);
}

} // namespace server
} // namespace pegasus
