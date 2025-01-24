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
#include <string_view>
#include <utility>

#include "gtest/gtest.h"
#include "pegasus_key_schema.h"
#include "pegasus_server_test_base.h"
#include "pegasus_value_schema.h"
#include "rrdb/rrdb_types.h"
#include "server/pegasus_server_write.h"
#include "server/pegasus_write_service.h"
#include "server/pegasus_write_service_impl.h"
#include "server/rocksdb_wrapper.h"
#include "utils/blob.h"
#include "utils/defer.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"
#include "utils/string_conv.h"

// IWYU pragma: no_forward_declare <gtest/gtest.h>

namespace pegasus::server {

class PegasusWriteServiceImplTest : public pegasus_server_test_base
{
protected:
    std::unique_ptr<pegasus_server_write> _server_write;
    pegasus_write_service::impl *_write_impl{nullptr};
    rocksdb_wrapper *_rocksdb_wrapper{nullptr};

    void SetUp() override
    {
        ASSERT_EQ(dsn::ERR_OK, start());
        _server_write = std::make_unique<pegasus_server_write>(_server.get());
        _write_impl = _server_write->_write_svc->_impl.get();
        _rocksdb_wrapper = _write_impl->_rocksdb_wrapper.get();
    }

public:
    void db_get(std::string_view raw_key, db_get_context *get_ctx)
    {
        ASSERT_EQ(rocksdb::Status::kOk, _rocksdb_wrapper->get(raw_key, get_ctx));
    }

    void db_get(const dsn::blob &raw_key, db_get_context *get_ctx)
    {
        db_get(raw_key.to_string_view(), get_ctx);
    }

    void single_set(dsn::blob raw_key, dsn::blob user_value)
    {
        dsn::apps::update_request put;
        put.key = raw_key;
        put.value = user_value;

        db_write_context write_ctx;
        dsn::apps::update_response put_resp;
        ASSERT_EQ(rocksdb::Status::kOk, _write_impl->batch_put(write_ctx, put, put_resp));
        ASSERT_EQ(rocksdb::Status::kOk, _write_impl->batch_commit(0));
    }

    void extract_user_data(std::string &&raw_value, std::string &user_data)
    {
        dsn::blob data;
        pegasus_extract_user_data(_write_impl->_pegasus_data_version, std::move(raw_value), data);
        user_data = data.to_string();
    }

    void extract_user_data(std::string &&raw_value, int64_t &user_data)
    {
        std::string data;
        extract_user_data(std::move(raw_value), data);
        ASSERT_TRUE(dsn::buf2int64(data, user_data));
    }
};

// Define a base value and a checker that check if the value in db is just the base value
// at the end of the scope.
#define DEFINE_BASE_VALUE_AND_CHECKER(type, val)                                                   \
    static const type kBaseValue = val;                                                            \
    auto kBaseValueChecker = dsn::defer([this]() { check_db_record(kBaseValue); })

// Put a string value and check if the value in db is just the string value at the end of
// the scope.
#define PUT_BASE_VALUE_STRING(val)                                                                 \
    DEFINE_BASE_VALUE_AND_CHECKER(std::string, val);                                               \
    single_set(req.key, dsn::blob::create_from_bytes(std::string(kBaseValue)))

// Put a int64 value and check if the value in db is just the int64 value at the end of
// the scope.
#define PUT_BASE_VALUE_INT64(val)                                                                  \
    DEFINE_BASE_VALUE_AND_CHECKER(int64_t, val);                                                   \
    single_set(req.key, dsn::blob::create_from_numeric(kBaseValue))

class IncrTest : public PegasusWriteServiceImplTest
{
protected:
    void SetUp() override
    {
        PegasusWriteServiceImplTest::SetUp();
        generate_key("incr_hash_key", "incr_sort_key");
        req.expire_ts_seconds = 0;
    }

public:
    void generate_key(const std::string &hash_key, const std::string &sort_key)
    {
        pegasus_generate_key(req.key, hash_key, sort_key);
    }

    // Check whether the value is expected.
    template <typename TVal>
    void check_db_record(const TVal &expected_value)
    {
        db_get_context get_ctx;
        db_get(req.key, &get_ctx);
        ASSERT_TRUE(get_ctx.found);
        ASSERT_FALSE(get_ctx.expired);

        TVal actual_value;
        extract_user_data(std::move(get_ctx.raw_value), actual_value);
        ASSERT_EQ(expected_value, actual_value);
    }

    // Check whether the key is expired.
    void check_db_record_expired()
    {
        db_get_context get_ctx;
        db_get(req.key, &get_ctx);
        ASSERT_TRUE(get_ctx.found);
        ASSERT_TRUE(get_ctx.expired);
    }

    // Test if the result in response is correct while no error is returned.
    virtual void test_incr(int64_t base, int64_t increment) = 0;

    // Test if both the result in response and the value in db are correct while no error
    // is returned.
    void test_incr_and_check_db_record(int64_t base, int64_t increment)
    {
        test_incr(base, increment);
        check_db_record(base + increment);
    }

    // Test if incr could be executed correctly while the key does not exist in db previously.
    void test_incr_on_absent_record(int64_t increment)
    {
        // Ensure that the key is absent.
        db_get_context get_ctx;
        db_get(req.key, &get_ctx);
        ASSERT_FALSE(get_ctx.found);
        ASSERT_FALSE(get_ctx.expired);

        test_incr_and_check_db_record(0, increment);
    }

    // Test if incr could be executed correctly while the key has existed in db.
    void test_incr_on_existing_record(int64_t base, int64_t increment)
    {
        // Load a record beforehand as the existing one.
        single_set(req.key, dsn::blob::create_from_numeric(base));

        test_incr_and_check_db_record(base, increment);
    }

    dsn::apps::incr_request req;
    dsn::apps::incr_response resp;
};

class NonIdempotentIncrTest : public IncrTest
{
public:
    void test_non_idempotent_incr(int64_t increment, int expected_ret_err, int expected_resp_err)
    {
        req.increment = increment;
        ASSERT_EQ(expected_ret_err, _write_impl->incr(0, req, resp));
        ASSERT_EQ(expected_resp_err, resp.error);
    }

    void test_incr(int64_t base, int64_t increment) override
    {
        test_non_idempotent_incr(increment, rocksdb::Status::kOk, rocksdb::Status::kOk);
        ASSERT_EQ(base + increment, resp.new_value);
    }
};

TEST_P(NonIdempotentIncrTest, IncrOneOnAbsentRecord) { test_incr_on_absent_record(1); }

TEST_P(NonIdempotentIncrTest, IncrBigOnAbsentRecord) { test_incr_on_absent_record(1); }

TEST_P(NonIdempotentIncrTest, IncrOneOnExistingRecord) { test_incr_on_existing_record(10, 1); }

TEST_P(NonIdempotentIncrTest, IncrBigOnExistingRecord) { test_incr_on_existing_record(10, 100); }

TEST_P(NonIdempotentIncrTest, IncrNegative)
{
    test_incr_on_absent_record(-100);
    test_incr_and_check_db_record(-100, -1);
}

TEST_P(NonIdempotentIncrTest, IncrZero)
{
    test_incr_on_absent_record(0);
    test_incr_on_existing_record(10, 0);
    test_incr_on_existing_record(-10, 0);
}

TEST_P(NonIdempotentIncrTest, IncrOnNonNumericRecord)
{
    PUT_BASE_VALUE_STRING("abc");

    test_non_idempotent_incr(1, rocksdb::Status::kOk, rocksdb::Status::kInvalidArgument);
}

TEST_P(NonIdempotentIncrTest, IncrOverflowed)
{
    PUT_BASE_VALUE_INT64(100);

    test_non_idempotent_incr(std::numeric_limits<int64_t>::max(),
                             rocksdb::Status::kOk,
                             rocksdb::Status::kInvalidArgument);
    ASSERT_EQ(kBaseValue, resp.new_value);
}

TEST_P(NonIdempotentIncrTest, FailOnGet)
{
    PUT_BASE_VALUE_INT64(100);

    dsn::fail::setup();
    // When db_get failed, incr should return an error.
    dsn::fail::cfg("db_get", "100%1*return()");

    test_non_idempotent_incr(10, FAIL_DB_GET, FAIL_DB_GET);

    dsn::fail::teardown();
}

TEST_P(NonIdempotentIncrTest, FailOnPut)
{
    PUT_BASE_VALUE_INT64(100);

    dsn::fail::setup();
    // When rocksdb put failed, incr should return an error.
    dsn::fail::cfg("db_write_batch_put", "100%1*return()");

    test_non_idempotent_incr(10, FAIL_DB_WRITE_BATCH_PUT, FAIL_DB_WRITE_BATCH_PUT);

    dsn::fail::teardown();
}

TEST_P(NonIdempotentIncrTest, IncrOnExpireRecord)
{
    // Make the key expired.
    req.expire_ts_seconds = 1;
    test_non_idempotent_incr(10, rocksdb::Status::kOk, rocksdb::Status::kOk);

    check_db_record_expired();

    // Incr the expired key.
    req.expire_ts_seconds = 0;
    test_incr_and_check_db_record(0, 100);
}

INSTANTIATE_TEST_SUITE_P(PegasusWriteServiceImplTest,
                         NonIdempotentIncrTest,
                         testing::Values(false, true));

class IdempotentIncrTest : public IncrTest
{
public:
    // Test make_idempotent for incr.
    void test_make_idempotent(int64_t increment, int expected_err)
    {
        req.increment = increment;
        const int err = _write_impl->make_idempotent(req, err_resp, update);
        ASSERT_EQ(expected_err, err);
        if (expected_err == rocksdb::Status::kOk) {
            return;
        }

        ASSERT_EQ(expected_err, err_resp.error);
    }

    // Test if make_idempotent for incr is successful; then, write the idempotent put
    // request into db.
    void test_idempotent_incr(int64_t increment, int expected_err)
    {
        test_make_idempotent(increment, rocksdb::Status::kOk);

        db_write_context write_ctx;
        ASSERT_EQ(expected_err, _write_impl->put(write_ctx, update, resp));
        ASSERT_EQ(expected_err, resp.error);
    }

    void test_incr(int64_t base, int64_t increment) override
    {
        test_idempotent_incr(increment, rocksdb::Status::kOk);
        ASSERT_EQ(base + increment, resp.new_value);
    }

    dsn::apps::incr_response err_resp;
    dsn::apps::update_request update;
};

TEST_P(IdempotentIncrTest, IncrOneOnAbsentRecord) { test_incr_on_absent_record(1); }

TEST_P(IdempotentIncrTest, IncrBigOnAbsentRecord) { test_incr_on_absent_record(100); }

TEST_P(IdempotentIncrTest, IncrOneOnExistingRecord) { test_incr_on_existing_record(10, 1); }

TEST_P(IdempotentIncrTest, IncrBigOnExistingRecord) { test_incr_on_existing_record(10, 100); }

TEST_P(IdempotentIncrTest, IncrNegative)
{
    test_incr_on_absent_record(-100);
    test_incr_and_check_db_record(-100, -1);
}

TEST_P(IdempotentIncrTest, IncrZero)
{
    test_incr_on_absent_record(0);
    test_incr_on_existing_record(10, 0);
    test_incr_on_existing_record(-10, 0);
}

TEST_P(IdempotentIncrTest, IncrOnNonNumericRecord)
{
    PUT_BASE_VALUE_STRING("abc");

    test_make_idempotent(1, rocksdb::Status::kInvalidArgument);
}

TEST_P(IdempotentIncrTest, IncrOverflowed)
{
    PUT_BASE_VALUE_INT64(100);

    test_make_idempotent(std::numeric_limits<int64_t>::max(), rocksdb::Status::kInvalidArgument);
    ASSERT_EQ(kBaseValue, err_resp.new_value);
}

TEST_P(IdempotentIncrTest, FailOnGet)
{
    PUT_BASE_VALUE_INT64(100);

    dsn::fail::setup();
    // When db_get failed, make_idempotent should return an error.
    dsn::fail::cfg("db_get", "100%1*return()");

    test_make_idempotent(10, FAIL_DB_GET);

    dsn::fail::teardown();
}

TEST_P(IdempotentIncrTest, FailOnPut)
{
    PUT_BASE_VALUE_INT64(100);

    dsn::fail::setup();
    // When rocksdb put failed, it should return an error while writing put request.
    dsn::fail::cfg("db_write_batch_put", "100%1*return()");

    test_idempotent_incr(10, FAIL_DB_WRITE_BATCH_PUT);

    dsn::fail::teardown();
}

TEST_P(IdempotentIncrTest, IncrOnExpireRecord)
{
    // Make the key expired.
    req.expire_ts_seconds = 1;
    test_idempotent_incr(10, rocksdb::Status::kOk);

    check_db_record_expired();

    // Incr the expired key.
    req.expire_ts_seconds = 0;
    test_incr_and_check_db_record(0, 100);
}

INSTANTIATE_TEST_SUITE_P(PegasusWriteServiceImplTest,
                         IdempotentIncrTest,
                         testing::Values(false, true));

} // namespace pegasus::server
