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
#include <rocksdb/write_batch.h>
#include <stdint.h>
#include <array>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/pegasus_key_schema.h"
#include "base/pegasus_value_schema.h"
#include "common/gpid.h"
#include "duplication_internal_types.h"
#include "gtest/gtest.h"
#include "message_utils.h"
#include "pegasus_server_test_base.h"
#include "rpc/rpc_message.h"
#include "rrdb/rrdb.code.definition.h"
#include "rrdb/rrdb_types.h"
#include "runtime/message_utils.h"
#include "server/pegasus_server_write.h"
#include "server/pegasus_write_service.h"
#include "server/pegasus_write_service_impl.h"
#include "server/rocksdb_wrapper.h"
#include "task/task_code.h"
#include "utils/blob.h"
#include "utils/fail_point.h"
#include "utils/string_conv.h"

namespace pegasus::server {

class pegasus_write_service_test : public pegasus_server_test_base
{
protected:
    pegasus_write_service *_write_svc{nullptr};
    std::unique_ptr<pegasus_server_write> _server_write;

public:
    pegasus_write_service_test() = default;

    void SetUp() override
    {
        start();
        _server_write = std::make_unique<pegasus_server_write>(_server);
        _write_svc = _server_write->_write_svc.get();
    }

    void test_multi_put()
    {
        dsn::fail::setup();

        dsn::apps::multi_put_request request;
        dsn::apps::update_response response;

        int64_t decree = 10;
        std::string hash_key = "hash_key";

        // alarm for empty request
        request.hash_key = dsn::blob(hash_key.data(), 0, hash_key.size());
        auto ctx = db_write_context::create(decree, 1000);
        int err = _write_svc->multi_put(ctx, request, response);
        ASSERT_EQ(err, 0);
        verify_response(response, rocksdb::Status::kInvalidArgument, decree);

        constexpr int kv_num = 100;
        std::string sort_key[kv_num];
        std::string value[kv_num];

        for (int i = 0; i < 100; i++) {
            sort_key[i] = "sort_key_" + std::to_string(i);
            value[i] = "value_" + std::to_string(i);
        }

        for (int i = 0; i < 100; i++) {
            request.kvs.emplace_back();
            request.kvs.back().key.assign(sort_key[i].data(), 0, sort_key[i].size());
            request.kvs.back().value.assign(value[i].data(), 0, value[i].size());
        }

        {
            dsn::fail::cfg("db_write_batch_put", "100%1*return()");
            err = _write_svc->multi_put(ctx, request, response);
            ASSERT_EQ(err, FAIL_DB_WRITE_BATCH_PUT);
            verify_response(response, err, decree);
        }

        {
            dsn::fail::cfg("db_write", "100%1*return()");
            err = _write_svc->multi_put(ctx, request, response);
            ASSERT_EQ(err, FAIL_DB_WRITE);
            verify_response(response, err, decree);
        }

        { // success
            err = _write_svc->multi_put(ctx, request, response);
            ASSERT_EQ(err, 0);
            verify_response(response, 0, decree);
        }

        dsn::fail::teardown();
    }

    void test_multi_remove()
    {
        dsn::fail::setup();

        dsn::apps::multi_remove_request request;
        dsn::apps::multi_remove_response response;

        int64_t decree = 10;
        std::string hash_key = "hash_key";

        // alarm for empty request
        request.hash_key = dsn::blob(hash_key.data(), 0, hash_key.size());
        int err = _write_svc->multi_remove(decree, request, response);
        ASSERT_EQ(err, 0);
        verify_response(response, rocksdb::Status::kInvalidArgument, decree);

        constexpr int kv_num = 100;
        std::string sort_key[kv_num];

        for (int i = 0; i < kv_num; i++) {
            sort_key[i] = "sort_key_" + std::to_string(i);
        }

        for (int i = 0; i < kv_num; i++) {
            request.sort_keys.emplace_back();
            request.sort_keys.back().assign(sort_key[i].data(), 0, sort_key[i].size());
        }

        {
            dsn::fail::cfg("db_write_batch_delete", "100%1*return()");
            err = _write_svc->multi_remove(decree, request, response);
            ASSERT_EQ(err, FAIL_DB_WRITE_BATCH_DELETE);
            verify_response(response, err, decree);
        }

        {
            dsn::fail::cfg("db_write", "100%1*return()");
            err = _write_svc->multi_remove(decree, request, response);
            ASSERT_EQ(err, FAIL_DB_WRITE);
            verify_response(response, err, decree);
        }

        { // success
            err = _write_svc->multi_remove(decree, request, response);
            ASSERT_EQ(err, 0);
            verify_response(response, 0, decree);
        }

        dsn::fail::teardown();
    }

    void test_batched_writes()
    {
        int64_t decree = 10;
        std::string hash_key = "hash_key";

        auto ctx = db_write_context::create(decree, 1000);

        constexpr int kv_num = 100;
        dsn::blob key[kv_num];
        std::string value[kv_num];

        for (int i = 0; i < kv_num; i++) {
            std::string sort_key = "sort_key_" + std::to_string(i);
            pegasus::pegasus_generate_key(key[i], hash_key, sort_key);

            value[i] = "value_" + std::to_string(i);
        }

        // It's dangerous to use std::vector<> here, since the address
        // of response may be changed due to capacity increase.
        std::array<dsn::apps::update_response, kv_num> responses;
        {
            _write_svc->batch_prepare(decree);
            for (int i = 0; i < kv_num; i++) {
                dsn::apps::update_request req;
                req.key = key[i];
                _write_svc->batch_put(ctx, req, responses[i]);
            }
            for (int i = 0; i < kv_num; i++) {
                _write_svc->batch_remove(decree, key[i], responses[i]);
            }
            _write_svc->batch_commit(decree);
        }

        for (const dsn::apps::update_response &resp : responses) {
            verify_response(resp, 0, decree);
        }
    }

    template <typename TResponse>
    void verify_response(const TResponse &response, int err, int64_t decree)
    {
        ASSERT_EQ(response.error, err);
        ASSERT_EQ(response.app_id, _gpid.get_app_id());
        ASSERT_EQ(response.partition_index, _gpid.get_partition_index());
        ASSERT_EQ(response.decree, decree);
        ASSERT_EQ(response.server, _write_svc->_impl->_primary_host_port);
        ASSERT_EQ(_write_svc->_impl->_rocksdb_wrapper->_write_batch->Count(), 0);
        ASSERT_EQ(_write_svc->_impl->_update_responses.size(), 0);
    }

    void db_get(const dsn::blob &raw_key, db_get_context *get_ctx)
    {
        ASSERT_EQ(rocksdb::Status::kOk,
                  _write_svc->_impl->_rocksdb_wrapper->get(raw_key.to_string_view(), get_ctx));
    }

    void get_value_from_db(const dsn::blob &raw_key, std::string &user_value)
    {
        db_get_context get_ctx;
        db_get(raw_key, &get_ctx);
        ASSERT_TRUE(get_ctx.found) << "key not found in DB";
        ASSERT_FALSE(get_ctx.expired) << "key expired in DB";
        dsn::blob data;
        pegasus_extract_user_data(_write_svc->_impl->_rocksdb_wrapper->_pegasus_data_version,
                                  std::move(get_ctx.raw_value),
                                  data);
        user_value = data.to_string();
    }

    void get_value_from_db(const dsn::blob &raw_key, int64_t &user_value)
    {
        std::string data;
        get_value_from_db(raw_key, data);
        ASSERT_TRUE(dsn::buf2int64(data, user_value));
    }
};

INSTANTIATE_TEST_SUITE_P(, pegasus_write_service_test, ::testing::Values(false, true));

TEST_P(pegasus_write_service_test, multi_put) { test_multi_put(); }

TEST_P(pegasus_write_service_test, multi_remove) { test_multi_remove(); }

TEST_P(pegasus_write_service_test, batched_writes) { test_batched_writes(); }

TEST_P(pegasus_write_service_test, duplicate_not_batched)
{
    std::string hash_key = "hash_key";
    constexpr int kv_num = 100;
    std::string sort_key[kv_num];
    std::string value[kv_num];

    for (int i = 0; i < 100; i++) {
        sort_key[i] = "sort_key_" + std::to_string(i);
        value[i] = "value_" + std::to_string(i);
    }

    dsn::apps::duplicate_request duplicate;
    dsn::apps::duplicate_entry entry;
    entry.timestamp = 1000;
    entry.cluster_id = 2;
    dsn::apps::duplicate_response resp;

    {
        dsn::apps::multi_put_request mput;
        for (int i = 0; i < 100; i++) {
            mput.kvs.emplace_back();
            mput.kvs.back().key.assign(sort_key[i].data(), 0, sort_key[i].size());
            mput.kvs.back().value.assign(value[i].data(), 0, value[i].size());
        }
        dsn::message_ptr mput_msg = pegasus::create_multi_put_request(mput);

        entry.task_code = dsn::apps::RPC_RRDB_RRDB_MULTI_PUT;
        entry.raw_message = dsn::move_message_to_blob(mput_msg.get());
        duplicate.entries.emplace_back(entry);
        _write_svc->duplicate(1, duplicate, resp);
        ASSERT_EQ(resp.error, 0);
    }

    {
        dsn::apps::multi_remove_request mremove;
        for (int i = 0; i < 100; i++) {
            mremove.sort_keys.emplace_back();
            mremove.sort_keys.back().assign(sort_key[i].data(), 0, sort_key[i].size());
        }
        dsn::message_ptr mremove_msg = pegasus::create_multi_remove_request(mremove);

        entry.task_code = dsn::apps::RPC_RRDB_RRDB_MULTI_REMOVE;
        entry.raw_message = dsn::move_message_to_blob(mremove_msg.get());

        _write_svc->duplicate(1, duplicate, resp);
        ASSERT_EQ(resp.error, 0);
    }
}

TEST_P(pegasus_write_service_test, duplicate_batched)
{
    std::string hash_key = "hash_key";
    constexpr int kv_num = 100;
    std::string sort_key[kv_num];
    std::string value[kv_num];

    for (int i = 0; i < 100; i++) {
        sort_key[i] = "sort_key_" + std::to_string(i);
        value[i] = "value_" + std::to_string(i);
    }

    {
        dsn::apps::duplicate_request duplicate;
        dsn::apps::duplicate_entry entry;
        entry.timestamp = 1000;
        entry.cluster_id = 2;
        dsn::apps::duplicate_response resp;

        for (int i = 0; i < kv_num; i++) {
            dsn::apps::update_request request;
            pegasus::pegasus_generate_key(request.key, hash_key, sort_key[i]);
            request.value.assign(value[i].data(), 0, value[i].size());

            dsn::message_ptr msg_ptr = pegasus::create_put_request(request);
            entry.raw_message = dsn::move_message_to_blob(msg_ptr.get());
            entry.task_code = dsn::apps::RPC_RRDB_RRDB_PUT;
            duplicate.entries.emplace_back(entry);
            _write_svc->duplicate(1, duplicate, resp);
            ASSERT_EQ(resp.error, 0);
        }
    }
}

TEST_P(pegasus_write_service_test, illegal_duplicate_request)
{
    std::string hash_key = "hash_key";
    std::string sort_key = "sort_key";
    std::string value = "value";

    // cluster=13 is from nowhere
    dsn::apps::duplicate_request duplicate;
    dsn::apps::duplicate_entry entry;
    entry.timestamp = 1000;
    entry.cluster_id = 2;
    duplicate.entries.emplace_back(entry);
    dsn::apps::duplicate_response resp;

    dsn::apps::update_request request;
    pegasus::pegasus_generate_key(request.key, hash_key, sort_key);
    request.value.assign(value.data(), 0, value.size());

    dsn::message_ptr msg_ptr = pegasus::create_put_request(request); // auto release memory
    entry.raw_message = dsn::move_message_to_blob(msg_ptr.get());
    entry.task_code = dsn::apps::RPC_RRDB_RRDB_PUT;
    _write_svc->duplicate(1, duplicate, resp);
    ASSERT_EQ(resp.error, rocksdb::Status::kInvalidArgument);
}

TEST_P(pegasus_write_service_test, duplicate_incr)
{
    std::string hash_key = "dup_incr_hash";
    std::string sort_key = "dup_incr_sort";
    dsn::blob raw_key;
    pegasus::pegasus_generate_key(raw_key, hash_key, sort_key);

    dsn::apps::duplicate_request duplicate;
    dsn::apps::duplicate_entry entry;
    entry.timestamp = 1000;
    entry.cluster_id = 2;
    dsn::apps::duplicate_response resp;

    // First put base value "0" via duplicate
    {
        dsn::apps::update_request put_req;
        put_req.key = raw_key;
        put_req.value = dsn::blob::create_from_bytes("0");
        dsn::message_ptr put_msg = pegasus::create_put_request(put_req);
        entry.task_code = dsn::apps::RPC_RRDB_RRDB_PUT;
        entry.raw_message = dsn::move_message_to_blob(put_msg.get());
        duplicate.entries.clear();
        duplicate.entries.emplace_back(entry);
        _write_svc->duplicate(1, duplicate, resp);
        ASSERT_EQ(resp.error, rocksdb::Status::kOk);
    }

    // Then duplicate INCR with increment=1
    {
        dsn::apps::incr_request incr_req;
        incr_req.key = raw_key;
        incr_req.increment = 1;
        incr_req.expire_ts_seconds = 0;
        dsn::message_ptr incr_msg = pegasus::create_incr_request(incr_req);
        entry.task_code = dsn::apps::RPC_RRDB_RRDB_INCR;
        entry.raw_message = dsn::move_message_to_blob(incr_msg.get());
        duplicate.entries.clear();
        duplicate.entries.emplace_back(entry);
        _write_svc->duplicate(2, duplicate, resp);
        ASSERT_EQ(resp.error, rocksdb::Status::kOk);
    }

    int64_t value_after_incr = 0;
    get_value_from_db(raw_key, value_after_incr);
    ASSERT_EQ(value_after_incr, 1);
}

TEST_P(pegasus_write_service_test, duplicate_check_and_set)
{
    std::string hash_key = "dup_cas_hash";
    std::string sort_key = "dup_cas_sort";
    std::string check_sort_key = sort_key;
    std::string set_sort_key = sort_key;
    dsn::blob hash_key_blob;
    dsn::blob check_sort_key_blob;
    dsn::blob set_sort_key_blob;
    dsn::blob check_operand;
    dsn::blob set_value;
    hash_key_blob.assign(hash_key.data(), 0, hash_key.size());
    check_sort_key_blob.assign(check_sort_key.data(), 0, check_sort_key.size());
    set_sort_key_blob.assign(set_sort_key.data(), 0, set_sort_key.size());
    check_operand.assign("old", 0, 3);
    set_value.assign("new", 0, 3);

    dsn::blob raw_key;
    pegasus::pegasus_generate_key(raw_key, hash_key, sort_key);

    dsn::apps::duplicate_request duplicate;
    dsn::apps::duplicate_entry entry;
    entry.timestamp = 1000;
    entry.cluster_id = 2;
    dsn::apps::duplicate_response resp;

    // First put "old" via duplicate
    {
        dsn::apps::update_request put_req;
        put_req.key = raw_key;
        put_req.value = dsn::blob::create_from_bytes("old");
        dsn::message_ptr put_msg = pegasus::create_put_request(put_req);
        entry.task_code = dsn::apps::RPC_RRDB_RRDB_PUT;
        entry.raw_message = dsn::move_message_to_blob(put_msg.get());
        duplicate.entries.clear();
        duplicate.entries.emplace_back(entry);
        _write_svc->duplicate(1, duplicate, resp);
        ASSERT_EQ(resp.error, rocksdb::Status::kOk);
    }

    // Then duplicate CHECK_AND_SET: check value=="old", set to "new"
    {
        dsn::apps::check_and_set_request cas_req;
        cas_req.hash_key = hash_key_blob;
        cas_req.check_sort_key = check_sort_key_blob;
        cas_req.check_type = dsn::apps::cas_check_type::CT_VALUE_BYTES_EQUAL;
        cas_req.check_operand = check_operand;
        cas_req.set_diff_sort_key = false;
        cas_req.set_value = set_value;
        cas_req.set_expire_ts_seconds = 0;
        cas_req.return_check_value = false;
        dsn::message_ptr cas_msg = pegasus::create_check_and_set_request(cas_req);
        entry.task_code = dsn::apps::RPC_RRDB_RRDB_CHECK_AND_SET;
        entry.raw_message = dsn::move_message_to_blob(cas_msg.get());
        duplicate.entries.clear();
        duplicate.entries.emplace_back(entry);
        _write_svc->duplicate(2, duplicate, resp);
        ASSERT_EQ(resp.error, rocksdb::Status::kOk);
    }

    std::string value_after_cas;
    get_value_from_db(raw_key, value_after_cas);
    ASSERT_EQ(value_after_cas, "new");
}

TEST_P(pegasus_write_service_test, duplicate_check_and_mutate)
{
    std::string hash_key = "dup_cam_hash";
    std::string sort_key = "dup_cam_sort";
    dsn::blob hash_key_blob;
    dsn::blob check_sort_key_blob;
    dsn::blob check_operand;
    dsn::blob mutate_sort_key;
    dsn::blob mutate_value;
    hash_key_blob.assign(hash_key.data(), 0, hash_key.size());
    check_sort_key_blob.assign(sort_key.data(), 0, sort_key.size());
    check_operand.assign("old", 0, 3);
    mutate_sort_key.assign(sort_key.data(), 0, sort_key.size());
    mutate_value.assign("new", 0, 3);

    dsn::blob raw_key;
    pegasus::pegasus_generate_key(raw_key, hash_key, sort_key);

    dsn::apps::duplicate_request duplicate;
    dsn::apps::duplicate_entry entry;
    entry.timestamp = 1000;
    entry.cluster_id = 2;
    dsn::apps::duplicate_response resp;

    // First put "old" via duplicate
    {
        dsn::apps::update_request put_req;
        put_req.key = raw_key;
        put_req.value = dsn::blob::create_from_bytes("old");
        dsn::message_ptr put_msg = pegasus::create_put_request(put_req);
        entry.task_code = dsn::apps::RPC_RRDB_RRDB_PUT;
        entry.raw_message = dsn::move_message_to_blob(put_msg.get());
        duplicate.entries.clear();
        duplicate.entries.emplace_back(entry);
        _write_svc->duplicate(1, duplicate, resp);
        ASSERT_EQ(resp.error, rocksdb::Status::kOk);
    }

    // Then duplicate CHECK_AND_MUTATE: check value=="old", mutate with MO_PUT to "new"
    {
        dsn::apps::mutate mutate_op;
        mutate_op.operation = dsn::apps::mutate_operation::MO_PUT;
        mutate_op.sort_key = mutate_sort_key;
        mutate_op.value = mutate_value;
        mutate_op.set_expire_ts_seconds = 0;

        dsn::apps::check_and_mutate_request cam_req;
        cam_req.hash_key = hash_key_blob;
        cam_req.check_sort_key = check_sort_key_blob;
        cam_req.check_type = dsn::apps::cas_check_type::CT_VALUE_BYTES_EQUAL;
        cam_req.check_operand = check_operand;
        cam_req.mutate_list = {mutate_op};
        cam_req.return_check_value = false;
        dsn::message_ptr cam_msg = pegasus::create_check_and_mutate_request(cam_req);
        entry.task_code = dsn::apps::RPC_RRDB_RRDB_CHECK_AND_MUTATE;
        entry.raw_message = dsn::move_message_to_blob(cam_msg.get());
        duplicate.entries.clear();
        duplicate.entries.emplace_back(entry);
        _write_svc->duplicate(2, duplicate, resp);
        ASSERT_EQ(resp.error, rocksdb::Status::kOk);
    }

    std::string value_after_cam;
    get_value_from_db(raw_key, value_after_cam);
    ASSERT_EQ(value_after_cam, "new");
}

} // namespace pegasus::server
