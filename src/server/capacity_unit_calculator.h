// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <dsn/dist/replication/replica_base.h>
#include <dsn/perf_counter/perf_counter_wrapper.h>
#include <rrdb/rrdb_types.h>

namespace pegasus {
namespace server {

class capacity_unit_calculator : public dsn::replication::replica_base
{
public:
    explicit capacity_unit_calculator(replica_base *r);

    void add_cu(int32_t status, int64_t read_data_size, int64_t write_data_size);
    void batched_add_read(int32_t status, std::vector<::dsn::apps::key_value> kvs);
    void multi_put_add_write(int32_t status, std::vector<::dsn::apps::key_value> kvs);
    void multi_remove_add_write(int32_t status, std::vector<::dsn::blob> sort_keys);
    void cam_add_cu(int32_t status, std::vector<::dsn::apps::mutate> mutate_list);

private:
    void add_read_cu(int64_t read_data_size);
    void add_write_cu(int64_t write_data_size);

    uint64_t _read_capacity_unit_size;
    uint64_t _write_capacity_unit_size;
    uint32_t _log_read_cu_size;
    uint32_t _log_write_cu_size;

    ::dsn::perf_counter_wrapper _pfc_recent_read_cu;
    ::dsn::perf_counter_wrapper _pfc_recent_write_cu;
};

} // namespace server
} // namespace pegasus
