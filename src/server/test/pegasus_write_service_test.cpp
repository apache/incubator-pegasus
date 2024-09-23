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
#include <vector>

#include "base/pegasus_key_schema.h"
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

namespace pegasus {
namespace server {

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
        _server_write = std::make_unique<pegasus_server_write>(_server.get());
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

} // namespace server
} // namespace pegasus
