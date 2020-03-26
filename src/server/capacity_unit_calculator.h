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

    void add_get_cu(int32_t status, const dsn::blob &key, const dsn::blob &value);
    void add_multi_get_cu(int32_t status,
                          const dsn::blob &hash_key,
                          const std::vector<::dsn::apps::key_value> &kvs);
    void add_scan_cu(int32_t status, const std::vector<::dsn::apps::key_value> &kvs);
    void add_sortkey_count_cu(int32_t status, const dsn::blob &hash_key);
    void add_ttl_cu(int32_t status, const dsn::blob &key);

    void add_put_cu(int32_t status, const dsn::blob &key, const dsn::blob &value);
    void add_remove_cu(int32_t status, const dsn::blob &key);
    void add_multi_put_cu(int32_t status,
                          const dsn::blob &hash_key,
                          const std::vector<::dsn::apps::key_value> &kvs);
    void add_multi_remove_cu(int32_t status,
                             const dsn::blob &hash_key,
                             const std::vector<::dsn::blob> &sort_keys);
    void add_incr_cu(int32_t status, const dsn::blob &key);
    void add_check_and_set_cu(int32_t status,
                              const dsn::blob &hash_key,
                              const dsn::blob &check_sort_key,
                              const dsn::blob &set_sort_key,
                              const dsn::blob &value);
    void add_check_and_mutate_cu(int32_t status,
                                 const dsn::blob &hash_key,
                                 const dsn::blob &check_sort_key,
                                 const std::vector<::dsn::apps::mutate> &mutate_list);

protected:
    friend class capacity_unit_calculator_test;

#ifdef PEGASUS_UNIT_TEST
    virtual int64_t add_read_cu(int64_t read_data_size);
    virtual int64_t add_write_cu(int64_t write_data_size);
#else
    int64_t add_read_cu(int64_t read_data_size);
    int64_t add_write_cu(int64_t write_data_size);
#endif

private:
    // add_read_cu(kNotFound) or add_write_cu(kNotFound) mean the cu = 1, see the two
    // functions definition.
    const int kNotFound = 0;

    uint64_t _read_capacity_unit_size;
    uint64_t _write_capacity_unit_size;
    uint32_t _log_read_cu_size;
    uint32_t _log_write_cu_size;

    ::dsn::perf_counter_wrapper _pfc_recent_read_cu;
    ::dsn::perf_counter_wrapper _pfc_recent_write_cu;

    ::dsn::perf_counter_wrapper _pfc_recent_get_throughput;
    ::dsn::perf_counter_wrapper _pfc_recent_multi_get_throughput;
    ::dsn::perf_counter_wrapper _pfc_recent_scan_throughput;
    ::dsn::perf_counter_wrapper _pfc_recent_put_throughput;
    ::dsn::perf_counter_wrapper _pfc_recent_remove_throughput;
    ::dsn::perf_counter_wrapper _pfc_recent_multi_put_throughput;
    ::dsn::perf_counter_wrapper _pfc_recent_multi_remove_throughput;
    ::dsn::perf_counter_wrapper _pfc_recent_incr_throughput;
    ::dsn::perf_counter_wrapper _pfc_recent_check_and_set_throughput;
    ::dsn::perf_counter_wrapper _pfc_recent_check_and_mutate_throughput;
};

} // namespace server
} // namespace pegasus
