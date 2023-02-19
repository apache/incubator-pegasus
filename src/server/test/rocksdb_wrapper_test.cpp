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

#include "server/pegasus_server_write.h"
#include "server/pegasus_write_service_impl.h"
#include "pegasus_server_test_base.h"

namespace pegasus {
namespace server {
class rocksdb_wrapper_test : public pegasus_server_test_base
{
protected:
    std::unique_ptr<pegasus_server_write> _server_write;
    rocksdb_wrapper *_rocksdb_wrapper{nullptr};
    dsn::blob _raw_key;

public:
    void SetUp() override
    {
        start();
        _server_write = dsn::make_unique<pegasus_server_write>(_server.get());
        _rocksdb_wrapper = _server_write->_write_svc->_impl->_rocksdb_wrapper.get();

        pegasus::pegasus_generate_key(
            _raw_key, dsn::string_view("hash_key"), dsn::string_view("sort_key"));
    }

    void single_set(db_write_context write_ctx,
                    dsn::blob raw_key,
                    dsn::string_view user_value,
                    int32_t expire_ts_seconds)
    {
        ASSERT_EQ(_rocksdb_wrapper->write_batch_put_ctx(
                      write_ctx, raw_key, user_value, expire_ts_seconds),
                  0);
        ASSERT_EQ(_rocksdb_wrapper->write(0), 0);
        _rocksdb_wrapper->clear_up_write_batch();
    }

    // start with duplicating.
    void set_app_duplicating()
    {
        _server->stop(false);
        dsn::replication::destroy_replica(_replica);

        dsn::app_info app_info;
        app_info.app_type = "pegasus";
        app_info.duplicating = true;
        _replica = dsn::replication::create_test_replica(
            _replica_stub, _gpid, app_info, "./", false, false);
        _server = dsn::make_unique<mock_pegasus_server_impl>(_replica);

        SetUp();
    }

    uint64_t read_timestamp_from(dsn::string_view raw_value)
    {
        uint64_t local_timetag =
            pegasus_extract_timetag(_rocksdb_wrapper->_pegasus_data_version, raw_value);
        return extract_timestamp_from_timetag(local_timetag);
    }
};

TEST_F(rocksdb_wrapper_test, get)
{
    // not found
    db_get_context get_ctx1;
    _rocksdb_wrapper->get(_raw_key, &get_ctx1);
    ASSERT_FALSE(get_ctx1.found);

    // expired
    int32_t expired_ts = utils::epoch_now();
    db_write_context write_ctx;
    std::string value = "abc";
    single_set(write_ctx, _raw_key, value, expired_ts);
    db_get_context get_ctx2;
    _rocksdb_wrapper->get(_raw_key, &get_ctx2);
    ASSERT_TRUE(get_ctx2.found);
    ASSERT_TRUE(get_ctx2.expired);
    ASSERT_EQ(get_ctx2.expire_ts, expired_ts);

    // found
    expired_ts = INT32_MAX;
    db_get_context get_ctx3;
    single_set(write_ctx, _raw_key, value, expired_ts);
    _rocksdb_wrapper->get(_raw_key, &get_ctx3);
    ASSERT_TRUE(get_ctx2.found);
    ASSERT_FALSE(get_ctx3.expired);
    ASSERT_EQ(get_ctx3.expire_ts, expired_ts);
    dsn::blob user_value;
    pegasus_extract_user_data(
        _rocksdb_wrapper->_pegasus_data_version, std::move(get_ctx3.raw_value), user_value);
    ASSERT_EQ(user_value, value);
}

TEST_F(rocksdb_wrapper_test, put_verify_timetag)
{
    set_app_duplicating();

    /// insert timestamp 10
    int64_t decree = 10;
    uint64_t timestamp = 10;
    std::string value = "value_10";
    auto ctx = db_write_context::create(decree, timestamp);
    single_set(ctx, _raw_key, value, 0);

    db_get_context get_ctx1;
    _rocksdb_wrapper->get(_raw_key, &get_ctx1);
    ASSERT_TRUE(get_ctx1.found);
    ASSERT_FALSE(get_ctx1.expired);
    ASSERT_EQ(read_timestamp_from(get_ctx1.raw_value), timestamp);
    dsn::blob user_value;
    pegasus_extract_user_data(
        _rocksdb_wrapper->_pegasus_data_version, std::move(get_ctx1.raw_value), user_value);
    ASSERT_EQ(user_value, value);

    /// insert timestamp 15, which overwrites the previous record
    timestamp = 15;
    value = "value_15";
    ctx = db_write_context::create(decree, timestamp);
    single_set(ctx, _raw_key, value, 0);

    db_get_context get_ctx2;
    _rocksdb_wrapper->get(_raw_key, &get_ctx2);
    ASSERT_TRUE(get_ctx2.found);
    ASSERT_FALSE(get_ctx2.expired);
    ASSERT_EQ(read_timestamp_from(get_ctx2.raw_value), timestamp);
    pegasus_extract_user_data(
        _rocksdb_wrapper->_pegasus_data_version, std::move(get_ctx2.raw_value), user_value);
    ASSERT_EQ(user_value, value);

    /// insert timestamp 15 from remote, which will overwrite the previous record,
    /// since its cluster id is larger (current cluster_id=1)
    timestamp = 15;
    value = "value_15_new";
    ctx.remote_timetag = pegasus::generate_timetag(timestamp, 2, false);
    ctx.verify_timetag = true;
    single_set(ctx, _raw_key, value, 0);

    db_get_context get_ctx3;
    _rocksdb_wrapper->get(_raw_key, &get_ctx3);
    ASSERT_TRUE(get_ctx3.found);
    ASSERT_FALSE(get_ctx3.expired);
    ASSERT_EQ(read_timestamp_from(get_ctx3.raw_value), timestamp);
    pegasus_extract_user_data(
        _rocksdb_wrapper->_pegasus_data_version, std::move(get_ctx3.raw_value), user_value);
    ASSERT_EQ(user_value, value);

    /// write retry
    single_set(ctx, _raw_key, value, 0);

    /// insert timestamp 16 from local, which will overwrite the remote record,
    /// since its timestamp is larger
    timestamp = 16;
    value = "value_16";
    ctx = db_write_context::create(decree, timestamp);
    single_set(ctx, _raw_key, value, 0);

    db_get_context get_ctx4;
    _rocksdb_wrapper->get(_raw_key, &get_ctx4);
    ASSERT_TRUE(get_ctx4.found);
    ASSERT_FALSE(get_ctx4.expired);
    ASSERT_EQ(read_timestamp_from(get_ctx4.raw_value), timestamp);
    pegasus_extract_user_data(
        _rocksdb_wrapper->_pegasus_data_version, std::move(get_ctx4.raw_value), user_value);
    ASSERT_EQ(user_value, value);

    // write retry
    single_set(ctx, _raw_key, value, 0);
}

// verify timetag on data version v0
TEST_F(rocksdb_wrapper_test, verify_timetag_compatible_with_version_0)
{
    const_cast<uint32_t &>(_rocksdb_wrapper->_pegasus_data_version) = 0; // old version

    /// write data with data version 0
    std::string value = "value";
    int64_t decree = 10;
    uint64_t timestamp = 10;
    auto ctx = db_write_context::create_duplicate(decree, timestamp, true);
    single_set(ctx, _raw_key, value, 0);

    db_get_context get_ctx;
    _rocksdb_wrapper->get(_raw_key, &get_ctx);
    ASSERT_TRUE(get_ctx.found);
    ASSERT_FALSE(get_ctx.expired);
    dsn::blob user_value;
    pegasus_extract_user_data(
        _rocksdb_wrapper->_pegasus_data_version, std::move(get_ctx.raw_value), user_value);
    ASSERT_EQ(user_value, value);
}
} // namespace server
} // namespace pegasus
