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

void capacity_unit_calculator::add_read_cu(int64_t read_data_size)
{
    int64_t read_cu = (read_data_size + _read_capacity_unit_size - 1) >> _log_read_cu_size;
    _pfc_recent_read_cu->add(read_cu);
}

void capacity_unit_calculator::add_write_cu(int64_t write_data_size)
{
    int64_t write_cu = (write_data_size + _write_capacity_unit_size - 1) >> _log_write_cu_size;
    _pfc_recent_write_cu->add(write_cu);
}

void capacity_unit_calculator::add_cu(int32_t status,
                                      int64_t read_data_size,
                                      int64_t write_data_size)
{
    if (status == rocksdb::Status::kOk) {
        add_read_cu(read_data_size);
        add_write_cu(write_data_size);
    } else if (status == rocksdb::Status::kNotFound ||
               status == rocksdb::Status::kInvalidArgument ||
               status == rocksdb::Status::kTryAgain) {
        add_read_cu(1);
    }
}

void capacity_unit_calculator::batched_add_read(int32_t status,
                                                std::vector<::dsn::apps::key_value> kvs)
{
    if (status != rocksdb::Status::kOk && status != rocksdb::Status::kIncomplete) {
        return;
    }
    int64_t data_size = 0;
    for (const auto &kv : kvs) {
        data_size += kv.key.size() + kv.value.size();
    }
    add_read_cu(data_size);
}

void capacity_unit_calculator::multi_put_add_write(int32_t status,
                                                   std::vector<::dsn::apps::key_value> kvs)
{
    if (status != rocksdb::Status::kOk) {
        return;
    }
    int64_t data_size = 0;
    for (const auto &kv : kvs) {
        data_size += kv.key.size() + kv.value.size();
    }
    add_write_cu(data_size);
}

void capacity_unit_calculator::multi_remove_add_write(int32_t status,
                                                      std::vector<::dsn::blob> sort_keys)
{
    if (status != rocksdb::Status::kOk) {
        return;
    }
    int64_t data_size = 0;
    for (const auto &sort_key : sort_keys) {
        data_size += sort_key.size();
    }
    add_write_cu(data_size);
}

void capacity_unit_calculator::cam_add_cu(int32_t status,
                                          std::vector<::dsn::apps::mutate> mutate_list)
{
    int64_t write_data_size = 0;
    for (const auto &m : mutate_list) {
        write_data_size += m.sort_key.size() + m.value.size();
    }
    add_cu(status, 1, write_data_size);
}

} // namespace server
} // namespace pegasus
