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
#include <limits>
#include <memory>
#include <string>

#include "gtest/gtest.h"
#include "pegasus_key_schema.h"
#include "pegasus_server_test_base.h"
#include "rrdb/rrdb_types.h"
#include "server/pegasus_server_write.h"
#include "server/pegasus_write_service.h"
#include "server/pegasus_write_service_impl.h"
#include "server/rocksdb_wrapper.h"
#include "utils/blob.h"
#include "utils/fail_point.h"
#include "absl/strings/string_view.h"

namespace pegasus {
namespace server {

class pegasus_write_service_impl_test : public pegasus_server_test_base
{
protected:
    std::unique_ptr<pegasus_server_write> _server_write;
    pegasus_write_service::impl *_write_impl{nullptr};
    rocksdb_wrapper *_rocksdb_wrapper{nullptr};

public:
    void SetUp() override
    {
        start();
        _server_write = std::make_unique<pegasus_server_write>(_server.get());
        _write_impl = _server_write->_write_svc->_impl.get();
        _rocksdb_wrapper = _write_impl->_rocksdb_wrapper.get();
    }

    int db_get(absl::string_view raw_key, db_get_context *get_ctx)
    {
        return _rocksdb_wrapper->get(raw_key, get_ctx);
    }

    void single_set(dsn::blob raw_key, dsn::blob user_value)
    {
        dsn::apps::update_request put;
        put.key = raw_key;
        put.value = user_value;
        db_write_context write_ctx;
        dsn::apps::update_response put_resp;
        _write_impl->batch_put(write_ctx, put, put_resp);
        ASSERT_EQ(_write_impl->batch_commit(0), 0);
    }
};

class incr_test : public pegasus_write_service_impl_test
{
public:
    void SetUp() override
    {
        pegasus_write_service_impl_test::SetUp();
        pegasus::pegasus_generate_key(
            req.key, absl::string_view("hash_key"), absl::string_view("sort_key"));
    }

    dsn::apps::incr_request req;
    dsn::apps::incr_response resp;
};

INSTANTIATE_TEST_CASE_P(, incr_test, ::testing::Values(false, true));

TEST_P(incr_test, incr_on_absent_record)
{
    // ensure key is absent
    db_get_context get_ctx;
    db_get(req.key.to_string_view(), &get_ctx);
    ASSERT_FALSE(get_ctx.found);

    req.increment = 100;
    _write_impl->incr(0, req, resp);
    ASSERT_EQ(resp.new_value, 100);

    db_get(req.key.to_string_view(), &get_ctx);
    ASSERT_TRUE(get_ctx.found);
}

TEST_P(incr_test, negative_incr_and_zero_incr)
{
    req.increment = -100;
    ASSERT_EQ(0, _write_impl->incr(0, req, resp));
    ASSERT_EQ(resp.new_value, -100);

    req.increment = -1;
    ASSERT_EQ(0, _write_impl->incr(0, req, resp));
    ASSERT_EQ(resp.new_value, -101);

    req.increment = 0;
    ASSERT_EQ(0, _write_impl->incr(0, req, resp));
    ASSERT_EQ(resp.new_value, -101);
}

TEST_P(incr_test, invalid_incr)
{
    single_set(req.key, dsn::blob::create_from_bytes("abc"));

    req.increment = 10;
    _write_impl->incr(1, req, resp);
    ASSERT_EQ(resp.error, rocksdb::Status::kInvalidArgument);
    ASSERT_EQ(resp.new_value, 0);

    single_set(req.key, dsn::blob::create_from_bytes("100"));

    req.increment = std::numeric_limits<int64_t>::max();
    _write_impl->incr(1, req, resp);
    ASSERT_EQ(resp.error, rocksdb::Status::kInvalidArgument);
    ASSERT_EQ(resp.new_value, 100);
}

TEST_P(incr_test, fail_on_get)
{
    dsn::fail::setup();
    dsn::fail::cfg("db_get", "100%1*return()");
    // when db_get failed, incr should return an error.

    req.increment = 10;
    _write_impl->incr(1, req, resp);
    ASSERT_EQ(resp.error, FAIL_DB_GET);

    dsn::fail::teardown();
}

TEST_P(incr_test, fail_on_put)
{
    dsn::fail::setup();
    dsn::fail::cfg("db_write_batch_put", "100%1*return()");
    // when rocksdb put failed, incr should return an error.

    req.increment = 10;
    _write_impl->incr(1, req, resp);
    ASSERT_EQ(resp.error, FAIL_DB_WRITE_BATCH_PUT);

    dsn::fail::teardown();
}

TEST_P(incr_test, incr_on_expire_record)
{
    // make the key expired
    req.expire_ts_seconds = 1;
    _write_impl->incr(0, req, resp);

    // check whether the key is expired
    db_get_context get_ctx;
    db_get(req.key.to_string_view(), &get_ctx);
    ASSERT_TRUE(get_ctx.expired);

    // incr the expired key
    req.increment = 100;
    req.expire_ts_seconds = 0;
    _write_impl->incr(0, req, resp);
    ASSERT_EQ(resp.new_value, 100);

    db_get(req.key.to_string_view(), &get_ctx);
    ASSERT_TRUE(get_ctx.found);
}
} // namespace server
} // namespace pegasus
