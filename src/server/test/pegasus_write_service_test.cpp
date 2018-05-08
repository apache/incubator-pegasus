// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "base/pegasus_key_schema.h"
#include "pegasus_server_test_base.h"
#include "server/pegasus_server_write.h"

#include <dsn/dist/fmt_logging.h>

namespace pegasus {
namespace server {

class pegasus_write_service_test : public pegasus_server_test_base
{
protected:
    pegasus_write_service *_write_svc;
    std::unique_ptr<pegasus_server_write> _server_write;

public:
    pegasus_write_service_test() : pegasus_server_test_base()
    {
        _server_write = dsn::make_unique<pegasus_server_write>(_server.get());
        _write_svc = _server_write->_write_svc.get();
    }

    void test_multi_put()
    {
        dsn::apps::multi_put_request request;
        dsn::apps::update_response response;

        int64_t decree = 10;
        std::string hash_key = "hash_key";

        // alarm for empty request
        request.hash_key = dsn::blob(hash_key.data(), 0, hash_key.size());
        auto put_ctx = db_write_context::put(decree, 1000, 1);
        _write_svc->multi_put(put_ctx, request, response);
        ASSERT_EQ(response.error, rocksdb::Status::kInvalidArgument);

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

        _write_svc->multi_put(put_ctx, request, response);
        ASSERT_EQ(response.error, 0);
        ASSERT_EQ(response.app_id, _gpid.get_app_id());
        ASSERT_EQ(response.partition_index, _gpid.get_partition_index());
        ASSERT_EQ(response.decree, decree);
    }

    void test_multi_remove()
    {
        dsn::apps::multi_remove_request request;
        dsn::apps::multi_remove_response response;

        int64_t decree = 10;
        std::string hash_key = "hash_key";

        // alarm for empty request
        request.hash_key = dsn::blob(hash_key.data(), 0, hash_key.size());
        auto remove_ctx = db_write_context::remove(decree, 1000, 1);
        _write_svc->multi_remove(remove_ctx, request, response);
        ASSERT_EQ(response.error, rocksdb::Status::kInvalidArgument);

        constexpr int kv_num = 100;
        std::string sort_key[kv_num];

        for (int i = 0; i < kv_num; i++) {
            sort_key[i] = "sort_key_" + std::to_string(i);
        }

        for (int i = 0; i < kv_num; i++) {
            request.sort_keys.emplace_back();
            request.sort_keys.back().assign(sort_key[i].data(), 0, sort_key[i].size());
        }

        _write_svc->multi_remove(remove_ctx, request, response);
        ASSERT_EQ(response.error, 0);
        ASSERT_EQ(response.app_id, _gpid.get_app_id());
        ASSERT_EQ(response.partition_index, _gpid.get_partition_index());
        ASSERT_EQ(response.decree, decree);
    }

    void test_batched_writes()
    {
        int64_t decree = 10;
        std::string hash_key = "hash_key";

        auto put_ctx = db_write_context::put(decree, 1000, 1);
        auto remove_ctx = db_write_context::remove(decree, 1000, 1);

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
            _write_svc->batch_prepare();
            for (int i = 0; i < kv_num; i++) {
                dsn::apps::update_request req;
                req.key = key[i];
                _write_svc->batch_put(put_ctx, req, responses[i]);
            }
            for (int i = 0; i < kv_num; i++) {
                _write_svc->batch_remove(remove_ctx, key[i], responses[i]);
            }
            _write_svc->batch_commit(decree);
        }

        for (const dsn::apps::update_response &resp : responses) {
            ASSERT_EQ(resp.error, 0);
            ASSERT_EQ(resp.app_id, _gpid.get_app_id());
            ASSERT_EQ(resp.partition_index, _gpid.get_partition_index());
            ASSERT_EQ(resp.decree, decree);
        }
    }
};

TEST_F(pegasus_write_service_test, multi_put) { test_multi_put(); }

TEST_F(pegasus_write_service_test, multi_remove) { test_multi_remove(); }

TEST_F(pegasus_write_service_test, batched_writes) { test_batched_writes(); }

} // namespace server
} // namespace pegasus
