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
    if (status == rocksdb::Status::kNotFound) {
        add_read_cu(kNotFound);
        return;
    }

    if (status != rocksdb::Status::kOk) {
        return;
    }

    add_read_cu(key.size() + value.size());
}

void capacity_unit_calculator::add_multi_get_cu(int32_t status,
                                                const dsn::blob &hash_key,
                                                const std::vector<::dsn::apps::key_value> &kvs)
{
    if (status == rocksdb::Status::kNotFound) {
        add_read_cu(kNotFound);
        return;
    }

    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kIncomplete &&
        status != rocksdb::Status::kInvalidArgument) {
        return;
    }

    int64_t data_size = 0;
    for (const auto &kv : kvs) {
        data_size += kv.key.size() + kv.value.size();
    }
    data_size = data_size + hash_key.size();
    add_read_cu(data_size);
}

void capacity_unit_calculator::add_scan_cu(int32_t status,
                                           const std::vector<::dsn::apps::key_value> &kvs)
{
    if (status == rocksdb::Status::kNotFound) {
        add_read_cu(kNotFound);
        return;
    }

    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kIncomplete &&
        status != rocksdb::Status::kInvalidArgument) {
        return;
    }

    int64_t data_size = 0;
    for (const auto &kv : kvs) {
        data_size += kv.key.size() + kv.value.size();
    }
    add_read_cu(data_size);
}

void capacity_unit_calculator::add_sortkey_count_cu(int32_t status, const dsn::blob &hash_key)
{
    if (status == rocksdb::Status::kNotFound) {
        add_read_cu(kNotFound);
        return;
    }

    if (status != rocksdb::Status::kOk) {
        return;
    }

    add_read_cu(hash_key.size());
}

void capacity_unit_calculator::add_ttl_cu(int32_t status, const dsn::blob &key)
{
    if (status == rocksdb::Status::kNotFound) {
        add_read_cu(kNotFound);
        return;
    }

    if (status != rocksdb::Status::kOk) {
        return;
    }

    add_read_cu(key.size());
}

void capacity_unit_calculator::add_put_cu(int32_t status,
                                          const dsn::blob &key,
                                          const dsn::blob &value)
{
    if (status != rocksdb::Status::kOk) {
        return;
    }
    add_write_cu(key.size() + value.size());
}

void capacity_unit_calculator::add_remove_cu(int32_t status, const dsn::blob &key)
{
    if (status != rocksdb::Status::kOk) {
        return;
    }
    add_write_cu(key.size());
}

void capacity_unit_calculator::add_multi_put_cu(int32_t status,
                                                const dsn::blob &hash_key,
                                                const std::vector<::dsn::apps::key_value> &kvs)
{
    if (status != rocksdb::Status::kOk) {
        return;
    }

    int64_t data_size = 0;
    for (const auto &kv : kvs) {
        data_size += kv.key.size() + kv.value.size();
    }
    data_size = data_size + hash_key.size();

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
        data_size += sort_key.size();
    }
    data_size = data_size + hash_key.size();

    add_write_cu(data_size);
}

void capacity_unit_calculator::add_incr_cu(int32_t status, const dsn::blob &key)
{
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kInvalidArgument) {
        return;
    }

    if (status == rocksdb::Status::kOk) {
        add_write_cu(key.size());
    }
    add_read_cu(key.size());
}

void capacity_unit_calculator::add_check_and_set_cu(int32_t status,
                                                    const dsn::blob &hash_key,
                                                    const dsn::blob &check_sort_key,
                                                    const dsn::blob &set_sort_key,
                                                    const dsn::blob &value)
{
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kInvalidArgument &&
        status != rocksdb::Status::kTryAgain) {
        return;
    }

    if (status == rocksdb::Status::kOk) {
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
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kInvalidArgument &&
        status != rocksdb::Status::kTryAgain) {
        return;
    }

    int64_t data_size = 0;
    for (const auto &m : mutate_list) {
        data_size += m.sort_key.size() + m.value.size();
    }
    data_size = data_size + hash_key.size();

    if (status == rocksdb::Status::kOk) {
        add_write_cu(data_size);
    }
    add_read_cu(hash_key.size() + check_sort_key.size());
}

} // namespace server
} // namespace pegasus
