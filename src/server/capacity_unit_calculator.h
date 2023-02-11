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

#pragma once

#include <stdint.h>
#include <memory>
#include <vector>

#include "perf_counter/perf_counter_wrapper.h"
#include "replica/replica_base.h"

namespace dsn {
class blob;
class message_ex;
namespace apps {
class full_data;
class key_value;
class mutate;
} // namespace apps

namespace utils {
class token_bucket_throttling_controller;
} // namespace utils
} // namespace dsn
typedef dsn::utils::token_bucket_throttling_controller throttling_controller;

namespace pegasus {
namespace server {

class hotkey_collector;

class capacity_unit_calculator : public dsn::replication::replica_base
{
public:
    capacity_unit_calculator(
        replica_base *r,
        std::shared_ptr<hotkey_collector> read_hotkey_collector,
        std::shared_ptr<hotkey_collector> write_hotkey_collector,
        std::shared_ptr<throttling_controller> _read_size_throttling_controller);

    virtual ~capacity_unit_calculator() = default;

    void
    add_get_cu(dsn::message_ex *req, int32_t status, const dsn::blob &key, const dsn::blob &value);
    void add_multi_get_cu(dsn::message_ex *req,
                          int32_t status,
                          const dsn::blob &hash_key,
                          const std::vector<::dsn::apps::key_value> &kvs);
    void add_batch_get_cu(dsn::message_ex *req,
                          int32_t status,
                          const std::vector<::dsn::apps::full_data> &rows);
    void add_scan_cu(dsn::message_ex *req,
                     int32_t status,
                     const std::vector<::dsn::apps::key_value> &kvs);
    void add_sortkey_count_cu(dsn::message_ex *req, int32_t status, const dsn::blob &hash_key);
    void add_ttl_cu(dsn::message_ex *req, int32_t status, const dsn::blob &key);

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
    virtual void add_backup_request_bytes(dsn::message_ex *req, int64_t bytes);
#else
    int64_t add_read_cu(int64_t read_data_size);
    int64_t add_write_cu(int64_t write_data_size);
    void add_backup_request_bytes(dsn::message_ex *req, int64_t bytes);
#endif

private:
    uint32_t _log_read_cu_size;
    uint32_t _log_write_cu_size;

    ::dsn::perf_counter_wrapper _pfc_recent_read_cu;
    ::dsn::perf_counter_wrapper _pfc_recent_write_cu;

    ::dsn::perf_counter_wrapper _pfc_get_bytes;
    ::dsn::perf_counter_wrapper _pfc_multi_get_bytes;
    ::dsn::perf_counter_wrapper _pfc_batch_get_bytes;
    ::dsn::perf_counter_wrapper _pfc_scan_bytes;
    ::dsn::perf_counter_wrapper _pfc_put_bytes;
    ::dsn::perf_counter_wrapper _pfc_multi_put_bytes;
    ::dsn::perf_counter_wrapper _pfc_check_and_set_bytes;
    ::dsn::perf_counter_wrapper _pfc_check_and_mutate_bytes;
    ::dsn::perf_counter_wrapper _pfc_backup_request_bytes;

    /*
        hotkey capturing weight rules:
            add_get_cu: whether find the key or not, weight = 1(read_collector),
            add_multi_get_cu: weight = returned sortkey count(read_collector),
            add_scan_cu : not capture now,
            add_sortkey_count_cu: weight = 1(read_collector),
            add_ttl_cu: weight = 1(read_collector),
            add_put_cu: weight = 1(write_collector),
            add_remove_cu: weight = 1(write_collector),
            add_multi_put_cu: weight = returned sortkey count(write_collector),
            add_multi_remove_cu: weight = returned sortkey count(write_collector),
            add_incr_cu: if find the key, weight = 1(write_collector),
                         else weight = 1(read_collector)
            add_check_and_set_cu: if find the key, weight = 1(write_collector),
                         else weight = 1(read_collector)
            add_check_and_mutate_cu: if find the key, weight = mutate_list size
                                     else weight = 1
    */
    std::shared_ptr<hotkey_collector> _read_hotkey_collector;
    std::shared_ptr<hotkey_collector> _write_hotkey_collector;

    std::shared_ptr<throttling_controller> _read_size_throttling_controller;
};

} // namespace server
} // namespace pegasus
