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

#include "pegasus_server_test_base.h"
#include "server/pegasus_server_write.h"
#include "server/pegasus_write_service_impl.h"
#include "message_utils.h"

#include <dsn/utility/defer.h>

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
        _server_write = dsn::make_unique<pegasus_server_write>(_server.get(), true);
        _write_impl = _server_write->_write_svc->_impl.get();
        _rocksdb_wrapper = _write_impl->_rocksdb_wrapper.get();
    }

    uint64_t read_timestamp_from(dsn::string_view raw_key)
    {
        std::string raw_value;
        rocksdb::Status s = _write_impl->_db->Get(
            _write_impl->_rd_opts, utils::to_rocksdb_slice(raw_key), &raw_value);

        uint64_t local_timetag =
            pegasus_extract_timetag(_write_impl->_pegasus_data_version, raw_value);
        return extract_timestamp_from_timetag(local_timetag);
    }

    // start with duplicating.
    void set_app_duplicating()
    {
        _server->stop(false);
        dsn::replication::destroy_replica(_replica);

        dsn::app_info app_info;
        app_info.app_type = "pegasus";
        app_info.duplicating = true;
        _replica =
            dsn::replication::create_test_replica(_replica_stub, _gpid, app_info, "./", false);
        _server = dsn::make_unique<pegasus_server_impl>(_replica);

        SetUp();
    }

    int db_get(dsn::string_view raw_key, db_get_context *get_ctx)
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

TEST_F(pegasus_write_service_impl_test, put_verify_timetag)
{
    set_app_duplicating();

    dsn::blob raw_key;
    pegasus::pegasus_generate_key(
        raw_key, dsn::string_view("hash_key"), dsn::string_view("sort_key"));
    std::string value = "value";
    int64_t decree = 10;

    /// insert timestamp 10
    uint64_t timestamp = 10;
    auto ctx = db_write_context::create(decree, timestamp);
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);
    ASSERT_EQ(read_timestamp_from(raw_key), timestamp);

    /// insert timestamp 15, which overwrites the previous record
    timestamp = 15;
    ctx = db_write_context::create(decree, timestamp);
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);
    ASSERT_EQ(read_timestamp_from(raw_key), timestamp);

    /// insert timestamp 15 from remote, which will overwrite the previous record,
    /// since its cluster id is larger (current cluster_id=1)
    timestamp = 15;
    ctx.remote_timetag = pegasus::generate_timetag(timestamp, 2, false);
    ctx.verify_timetag = true;
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value + "_new", 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);
    ASSERT_EQ(read_timestamp_from(raw_key), timestamp);
    std::string raw_value;
    dsn::blob user_value;
    rocksdb::Status s =
        _write_impl->_db->Get(_write_impl->_rd_opts, utils::to_rocksdb_slice(raw_key), &raw_value);
    pegasus_extract_user_data(_write_impl->_pegasus_data_version, std::move(raw_value), user_value);
    ASSERT_EQ(user_value.to_string(), "value_new");

    // write retry
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value + "_new", 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);

    /// insert timestamp 16 from local, which will overwrite the remote record,
    /// since its timestamp is larger
    timestamp = 16;
    ctx = db_write_context::create(decree, timestamp);
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);
    ASSERT_EQ(read_timestamp_from(raw_key), timestamp);

    // write retry
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);
}

// verify timetag on data version v0
TEST_F(pegasus_write_service_impl_test, verify_timetag_compatible_with_version_0)
{
    dsn::fail::setup();
    dsn::fail::cfg("db_get", "100%1*return()");
    // if db_write_batch_put_ctx invokes db_get, this test must fail.

    const_cast<uint32_t &>(_write_impl->_pegasus_data_version) = 0; // old version

    dsn::blob raw_key;
    pegasus::pegasus_generate_key(
        raw_key, dsn::string_view("hash_key"), dsn::string_view("sort_key"));
    std::string value = "value";
    int64_t decree = 10;
    uint64_t timestamp = 10;

    auto ctx = db_write_context::create_duplicate(decree, timestamp, true);
    ASSERT_EQ(0, _write_impl->db_write_batch_put_ctx(ctx, raw_key, value, 0));
    ASSERT_EQ(0, _write_impl->db_write(ctx.decree));
    _write_impl->clear_up_batch_states(decree, 0);

    dsn::fail::teardown();
}

class incr_test : public pegasus_write_service_impl_test
{
public:
    void SetUp() override
    {
        pegasus_write_service_impl_test::SetUp();
        pegasus::pegasus_generate_key(
            req.key, dsn::string_view("hash_key"), dsn::string_view("sort_key"));
    }

    dsn::apps::incr_request req;
    dsn::apps::incr_response resp;
};

TEST_F(incr_test, incr_on_absent_record)
{
    // ensure key is absent
    db_get_context get_ctx;
    db_get(req.key, &get_ctx);
    ASSERT_FALSE(get_ctx.found);

    req.increment = 100;
    _write_impl->incr(0, req, resp);
    ASSERT_EQ(resp.new_value, 100);

    db_get(req.key, &get_ctx);
    ASSERT_TRUE(get_ctx.found);
}

TEST_F(incr_test, negative_incr_and_zero_incr)
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

TEST_F(incr_test, invalid_incr)
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

TEST_F(incr_test, fail_on_get)
{
    dsn::fail::setup();
    dsn::fail::cfg("db_get", "100%1*return()");
    // when db_get failed, incr should return an error.

    req.increment = 10;
    _write_impl->incr(1, req, resp);
    ASSERT_EQ(resp.error, FAIL_DB_GET);

    dsn::fail::teardown();
}

TEST_F(incr_test, fail_on_put)
{
    dsn::fail::setup();
    dsn::fail::cfg("db_write_batch_put", "100%1*return()");
    // when rocksdb put failed, incr should return an error.

    req.increment = 10;
    _write_impl->incr(1, req, resp);
    ASSERT_EQ(resp.error, FAIL_DB_WRITE_BATCH_PUT);

    dsn::fail::teardown();
}

TEST_F(incr_test, incr_on_expire_record)
{
    // make the key expired
    req.expire_ts_seconds = 1;
    _write_impl->incr(0, req, resp);

    // check whether the key is expired
    db_get_context get_ctx;
    db_get(req.key, &get_ctx);
    ASSERT_TRUE(get_ctx.expired);

    // incr the expired key
    req.increment = 100;
    req.expire_ts_seconds = 0;
    _write_impl->incr(0, req, resp);
    ASSERT_EQ(resp.new_value, 100);

    db_get(req.key, &get_ctx);
    ASSERT_TRUE(get_ctx.found);
}
} // namespace server
} // namespace pegasus
