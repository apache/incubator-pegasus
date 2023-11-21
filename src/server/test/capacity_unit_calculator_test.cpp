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

#include <fmt/core.h>
#include <rocksdb/status.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/replication.codes.h"
#include "gtest/gtest.h"
#include "pegasus_key_schema.h"
#include "pegasus_server_test_base.h"
#include "replica_admin_types.h"
#include "rrdb/rrdb_types.h"
#include "runtime/rpc/rpc_message.h"
#include "server/capacity_unit_calculator.h"
#include "server/hotkey_collector.h"
#include "utils/autoref_ptr.h"
#include "utils/blob.h"
#include "utils/flags.h"
#include "utils/token_bucket_throttling_controller.h"

namespace dsn {
namespace replication {
struct replica_base;
} // namespace replication
} // namespace dsn

namespace pegasus {
namespace server {

DSN_DECLARE_uint64(perf_counter_read_capacity_unit_size);
DSN_DECLARE_uint64(perf_counter_write_capacity_unit_size);

class mock_capacity_unit_calculator : public capacity_unit_calculator
{
public:
    int64_t add_read_cu(int64_t read_data_size) override
    {
        read_cu += capacity_unit_calculator::add_read_cu(read_data_size);
        return read_cu;
    }

    int64_t add_write_cu(int64_t write_data_size) override
    {
        write_cu += capacity_unit_calculator::add_write_cu(write_data_size);
        return write_cu;
    }

    void add_backup_request_bytes(dsn::message_ex *req, int64_t bytes)
    {
        if (req->is_backup_request()) {
            backup_request_bytes += bytes;
        }
    }

    explicit mock_capacity_unit_calculator(dsn::replication::replica_base *r)
        : capacity_unit_calculator(
              r,
              std::make_shared<hotkey_collector>(dsn::replication::hotkey_type::READ, r),
              std::make_shared<hotkey_collector>(dsn::replication::hotkey_type::WRITE, r),
              std::make_shared<dsn::utils::token_bucket_throttling_controller>())
    {
    }

    void reset()
    {
        write_cu = 0;
        read_cu = 0;
        backup_request_bytes = 0;
    }

    int64_t write_cu{0};
    int64_t read_cu{0};
    uint64_t backup_request_bytes{0};
};

static constexpr int MAX_ROCKSDB_STATUS_CODE = 13;

class capacity_unit_calculator_test : public pegasus_server_test_base
{
protected:
    std::unique_ptr<mock_capacity_unit_calculator> _cal;

public:
    dsn::blob key, hash_key;

    capacity_unit_calculator_test() : pegasus_server_test_base()
    {
        _cal = std::make_unique<mock_capacity_unit_calculator>(_server.get());
        pegasus_generate_key(key, dsn::blob::create_from_bytes("h"), dsn::blob());
        hash_key = dsn::blob::create_from_bytes("key");
    }

    void test_init()
    {
        ASSERT_EQ(FLAGS_perf_counter_read_capacity_unit_size, 4096);
        ASSERT_EQ(FLAGS_perf_counter_write_capacity_unit_size, 4096);

        ASSERT_EQ(_cal->_log_read_cu_size, 12);
        ASSERT_EQ(_cal->_log_write_cu_size, 12);
    }

    void generate_n_kvs(int n, std::vector<::dsn::apps::key_value> &kvs)
    {
        std::vector<::dsn::apps::key_value> tmp_kvs;
        for (int i = 0; i < n; i++) {
            dsn::apps::key_value kv;
            kv.key = dsn::blob::create_from_bytes("key_" + std::to_string(i));
            kv.value = dsn::blob::create_from_bytes("value_" + std::to_string(i));
            tmp_kvs.emplace_back(kv);
        }
        kvs = std::move(tmp_kvs);
    }

    void generate_n_keys(int n, std::vector<::dsn::blob> &keys)
    {
        std::vector<::dsn::blob> tmp_keys;
        for (int i = 0; i < n; i++) {
            tmp_keys.emplace_back(dsn::blob::create_from_bytes("key_" + std::to_string(i)));
        }
        keys = std::move(tmp_keys);
    }

    void generate_n_mutates(int n, std::vector<::dsn::apps::mutate> &mutates)
    {
        std::vector<::dsn::apps::mutate> tmp_mutates;
        for (int i = 0; i < n; i++) {
            dsn::apps::mutate m;
            m.sort_key = dsn::blob::create_from_bytes("key_" + std::to_string(i));
            m.value = dsn::blob::create_from_bytes("value_" + std::to_string(i));
            tmp_mutates.emplace_back(m);
        }
        mutates = std::move(tmp_mutates);
    }
};

INSTANTIATE_TEST_CASE_P(, capacity_unit_calculator_test, ::testing::Values(false, true));

TEST_P(capacity_unit_calculator_test, init) { test_init(); }

TEST_P(capacity_unit_calculator_test, get)
{
    dsn::message_ptr msg = dsn::message_ex::create_request(RPC_TEST, static_cast<int>(1000), 1, 1);
    msg->header->context.u.is_backup_request = false;

    // value < 4KB
    _cal->add_get_cu(msg, rocksdb::Status::kOk, key, dsn::blob::create_from_bytes("value"));
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    // value = 4KB
    _cal->add_get_cu(
        msg, rocksdb::Status::kOk, key, dsn::blob::create_from_bytes(std::string(4093, ' ')));
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    // value > 4KB
    _cal->add_get_cu(
        msg, rocksdb::Status::kOk, key, dsn::blob::create_from_bytes(std::string(4097, ' ')));
    ASSERT_EQ(_cal->read_cu, 2);
    _cal->reset();

    // value > 8KB
    _cal->add_get_cu(msg,
                     rocksdb::Status::kOk,
                     key,
                     dsn::blob::create_from_bytes(std::string(4096 * 2 + 1, ' ')));
    ASSERT_EQ(_cal->read_cu, 3);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();

    _cal->add_get_cu(msg, rocksdb::Status::kNotFound, key, dsn::blob());
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_get_cu(msg, rocksdb::Status::kCorruption, key, dsn::blob());
    ASSERT_EQ(_cal->read_cu, 0);
    _cal->reset();
}

TEST_P(capacity_unit_calculator_test, multi_get)
{
    dsn::message_ptr msg = dsn::message_ex::create_request(RPC_TEST, static_cast<int>(1000), 1, 1);
    msg->header->context.u.is_backup_request = false;

    std::vector<::dsn::apps::key_value> kvs;

    generate_n_kvs(100, kvs);
    _cal->add_multi_get_cu(msg, rocksdb::Status::kIncomplete, hash_key, kvs);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    generate_n_kvs(500, kvs);
    _cal->add_multi_get_cu(msg, rocksdb::Status::kOk, hash_key, kvs);
    ASSERT_GT(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();

    kvs.clear();
    _cal->add_multi_get_cu(msg, rocksdb::Status::kNotFound, hash_key, kvs);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_multi_get_cu(msg, rocksdb::Status::kInvalidArgument, hash_key, kvs);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_multi_get_cu(msg, rocksdb::Status::kCorruption, hash_key, kvs);
    ASSERT_EQ(_cal->read_cu, 0);
    _cal->reset();
}

TEST_P(capacity_unit_calculator_test, scan)
{
    dsn::message_ptr msg = dsn::message_ex::create_request(RPC_TEST, static_cast<int>(1000), 1, 1);
    msg->header->context.u.is_backup_request = false;
    std::vector<::dsn::apps::key_value> kvs;

    generate_n_kvs(100, kvs);
    _cal->add_scan_cu(msg, rocksdb::Status::kIncomplete, kvs);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    generate_n_kvs(500, kvs);
    _cal->add_scan_cu(msg, rocksdb::Status::kIncomplete, kvs);
    ASSERT_GT(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_scan_cu(msg, rocksdb::Status::kOk, kvs);
    ASSERT_GT(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();

    kvs.clear();
    _cal->add_scan_cu(msg, rocksdb::Status::kInvalidArgument, kvs);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_scan_cu(msg, rocksdb::Status::kNotFound, kvs);
    ASSERT_EQ(_cal->read_cu, 1);
    _cal->reset();

    _cal->add_scan_cu(msg, rocksdb::Status::kCorruption, kvs);
    ASSERT_EQ(_cal->read_cu, 0);
    _cal->reset();
}

TEST_P(capacity_unit_calculator_test, sortkey_count)
{
    dsn::message_ptr msg = dsn::message_ex::create_request(RPC_TEST, static_cast<int>(1000), 1, 1);
    msg->header->context.u.is_backup_request = false;
    for (int i = 0; i < MAX_ROCKSDB_STATUS_CODE; i++) {
        _cal->add_sortkey_count_cu(msg, i, hash_key);
        if (i == rocksdb::Status::kOk || i == rocksdb::Status::kNotFound) {
            ASSERT_EQ(_cal->read_cu, 1);
        } else {
            ASSERT_EQ(_cal->read_cu, 0);
        }
        ASSERT_EQ(_cal->write_cu, 0);
        _cal->reset();
    }
}

TEST_P(capacity_unit_calculator_test, ttl)
{
    dsn::message_ptr msg = dsn::message_ex::create_request(RPC_TEST, static_cast<int>(1000), 1, 1);
    msg->header->context.u.is_backup_request = false;
    for (int i = 0; i < MAX_ROCKSDB_STATUS_CODE; i++) {
        _cal->add_ttl_cu(msg, i, key);
        if (i == rocksdb::Status::kOk || i == rocksdb::Status::kNotFound) {
            ASSERT_EQ(_cal->read_cu, 1);
        } else {
            ASSERT_EQ(_cal->read_cu, 0);
        }
        ASSERT_EQ(_cal->write_cu, 0);
        _cal->reset();
    }
}

TEST_P(capacity_unit_calculator_test, put)
{
    for (int i = 0; i < MAX_ROCKSDB_STATUS_CODE; i++) {
        _cal->add_put_cu(i, key, dsn::blob::create_from_bytes(std::string(4097, ' ')));
        if (i == rocksdb::Status::kOk) {
            ASSERT_EQ(_cal->write_cu, 2);
        } else {
            ASSERT_EQ(_cal->write_cu, 0);
        }
        ASSERT_EQ(_cal->read_cu, 0);
        _cal->reset();
    }
}

TEST_P(capacity_unit_calculator_test, remove)
{
    for (int i = 0; i < MAX_ROCKSDB_STATUS_CODE; i++) {
        _cal->add_remove_cu(i, key);
        if (i == rocksdb::Status::kOk) {
            ASSERT_EQ(_cal->write_cu, 1);
        } else {
            ASSERT_EQ(_cal->write_cu, 0);
        }
        ASSERT_EQ(_cal->read_cu, 0);
        _cal->reset();
    }
}

TEST_P(capacity_unit_calculator_test, multi_put)
{
    std::vector<::dsn::apps::key_value> kvs;

    generate_n_kvs(100, kvs);
    _cal->add_multi_put_cu(rocksdb::Status::kOk, hash_key, kvs);
    ASSERT_EQ(_cal->write_cu, 1);
    _cal->reset();

    generate_n_kvs(500, kvs);
    for (int i = 0; i < MAX_ROCKSDB_STATUS_CODE; i++) {
        _cal->add_multi_put_cu(i, hash_key, kvs);
        if (i == rocksdb::Status::kOk) {
            ASSERT_GT(_cal->write_cu, 1);
        } else {
            ASSERT_EQ(_cal->write_cu, 0);
        }
        ASSERT_EQ(_cal->read_cu, 0);
        _cal->reset();
    }
}

TEST_P(capacity_unit_calculator_test, multi_remove)
{
    std::vector<::dsn::blob> keys;

    generate_n_keys(100, keys);
    _cal->add_multi_remove_cu(rocksdb::Status::kOk, hash_key, keys);
    ASSERT_EQ(_cal->write_cu, 1);
    _cal->reset();

    generate_n_keys(1000, keys);
    for (int i = 0; i < MAX_ROCKSDB_STATUS_CODE; i++) {
        _cal->add_multi_remove_cu(i, hash_key, keys);
        if (i == rocksdb::Status::kOk) {
            ASSERT_GT(_cal->write_cu, 1);
        } else {
            ASSERT_EQ(_cal->write_cu, 0);
        }
        ASSERT_EQ(_cal->read_cu, 0);
        _cal->reset();
    }
}

TEST_P(capacity_unit_calculator_test, incr)
{
    for (int i = 0; i < MAX_ROCKSDB_STATUS_CODE; i++) {
        _cal->add_incr_cu(i, key);
        if (i == rocksdb::Status::kOk) {
            ASSERT_EQ(_cal->read_cu, 1);
            ASSERT_EQ(_cal->write_cu, 1);
        } else if (i == rocksdb::Status::kInvalidArgument) {
            ASSERT_EQ(_cal->read_cu, 1);
            ASSERT_EQ(_cal->write_cu, 0);
        } else {
            ASSERT_EQ(_cal->write_cu, 0);
            ASSERT_EQ(_cal->read_cu, 0);
        }
        _cal->reset();
    }
}

TEST_P(capacity_unit_calculator_test, check_and_set)
{
    dsn::blob cas_hash_key = dsn::blob::create_from_bytes("hash_key");
    dsn::blob check_sort_key = dsn::blob::create_from_bytes("check_sort_key");
    dsn::blob set_sort_key = dsn::blob::create_from_bytes("set_sort_key");
    dsn::blob value = dsn::blob::create_from_bytes("value");

    _cal->add_check_and_set_cu(
        rocksdb::Status::kOk, cas_hash_key, check_sort_key, set_sort_key, value);
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 1);
    _cal->reset();

    _cal->add_check_and_set_cu(
        rocksdb::Status::kInvalidArgument, cas_hash_key, check_sort_key, set_sort_key, value);
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();

    _cal->add_check_and_set_cu(
        rocksdb::Status::kTryAgain, cas_hash_key, check_sort_key, set_sort_key, value);
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();

    _cal->add_check_and_set_cu(
        rocksdb::Status::kCorruption, cas_hash_key, check_sort_key, set_sort_key, value);
    ASSERT_EQ(_cal->read_cu, 0);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();
}

TEST_P(capacity_unit_calculator_test, check_and_mutate)
{
    dsn::blob cam_hash_key = dsn::blob::create_from_bytes("hash_key");
    dsn::blob check_sort_key = dsn::blob::create_from_bytes("check_sort_key");
    std::vector<::dsn::apps::mutate> mutate_list;

    generate_n_mutates(100, mutate_list);
    _cal->add_check_and_mutate_cu(rocksdb::Status::kOk, cam_hash_key, check_sort_key, mutate_list);
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 1);
    _cal->reset();

    generate_n_mutates(1000, mutate_list);
    _cal->add_check_and_mutate_cu(rocksdb::Status::kOk, cam_hash_key, check_sort_key, mutate_list);
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_GT(_cal->write_cu, 1);
    _cal->reset();

    _cal->add_check_and_mutate_cu(
        rocksdb::Status::kInvalidArgument, cam_hash_key, check_sort_key, mutate_list);
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();

    _cal->add_check_and_mutate_cu(
        rocksdb::Status::kTryAgain, cam_hash_key, check_sort_key, mutate_list);
    ASSERT_EQ(_cal->read_cu, 1);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();

    _cal->add_check_and_mutate_cu(
        rocksdb::Status::kCorruption, cam_hash_key, check_sort_key, mutate_list);
    ASSERT_EQ(_cal->read_cu, 0);
    ASSERT_EQ(_cal->write_cu, 0);
    _cal->reset();
}

TEST_P(capacity_unit_calculator_test, backup_request_bytes)
{
    dsn::message_ptr msg = dsn::message_ex::create_request(RPC_TEST, static_cast<int>(1000), 1, 1);

    msg->header->context.u.is_backup_request = false;
    dsn::blob value = dsn::blob::create_from_bytes("value");
    _cal->add_get_cu(msg, rocksdb::Status::kOk, key, value);
    ASSERT_EQ(_cal->backup_request_bytes, 0);
    _cal->reset();

    msg->header->context.u.is_backup_request = true;
    value = dsn::blob::create_from_bytes("value");
    _cal->add_get_cu(msg, rocksdb::Status::kOk, key, value);
    ASSERT_EQ(_cal->backup_request_bytes, key.size() + value.size());
    _cal->reset();

    std::vector<::dsn::apps::key_value> kvs;
    generate_n_kvs(100, kvs);
    uint64_t total_size = 0;
    for (const auto &kv : kvs) {
        total_size += kv.key.size() + kv.value.size();
    }

    msg->header->context.u.is_backup_request = false;
    _cal->add_multi_get_cu(msg, rocksdb::Status::kOk, hash_key, kvs);
    ASSERT_EQ(_cal->backup_request_bytes, 0);
    _cal->reset();

    msg->header->context.u.is_backup_request = true;
    _cal->add_multi_get_cu(msg, rocksdb::Status::kOk, hash_key, kvs);
    ASSERT_EQ(_cal->backup_request_bytes, total_size + hash_key.size());
    _cal->reset();

    msg->header->context.u.is_backup_request = false;
    _cal->add_scan_cu(msg, rocksdb::Status::kOk, kvs);
    ASSERT_EQ(_cal->backup_request_bytes, 0);
    _cal->reset();

    msg->header->context.u.is_backup_request = true;
    _cal->add_scan_cu(msg, rocksdb::Status::kOk, kvs);
    ASSERT_EQ(_cal->backup_request_bytes, total_size);
    _cal->reset();

    msg->header->context.u.is_backup_request = false;
    _cal->add_sortkey_count_cu(msg, rocksdb::Status::kOk, hash_key);
    ASSERT_EQ(_cal->backup_request_bytes, 0);
    _cal->reset();

    msg->header->context.u.is_backup_request = true;
    _cal->add_sortkey_count_cu(msg, rocksdb::Status::kOk, hash_key);
    ASSERT_EQ(_cal->backup_request_bytes, 1);
    _cal->reset();

    msg->header->context.u.is_backup_request = false;
    _cal->add_ttl_cu(msg, rocksdb::Status::kOk, key);
    ASSERT_EQ(_cal->backup_request_bytes, 0);
    _cal->reset();

    msg->header->context.u.is_backup_request = true;
    _cal->add_ttl_cu(msg, rocksdb::Status::kOk, key);
    ASSERT_EQ(_cal->backup_request_bytes, 1);
    _cal->reset();
}

} // namespace server
} // namespace pegasus
