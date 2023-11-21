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
#include <stdint.h>
#include <memory>
#include <string>
#include <utility>

#include "common/fs_manager.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "pegasus_key_schema.h"
#include "pegasus_server_test_base.h"
#include "pegasus_utils.h"
#include "pegasus_value_schema.h"
#include "replica/replica.h"
#include "replica/replica_stub.h"
#include "server/pegasus_server_write.h"
#include "server/pegasus_write_service.h"
#include "server/pegasus_write_service_impl.h"
#include "server/rocksdb_wrapper.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/fmt_logging.h"
#include "absl/strings/string_view.h"

namespace pegasus {
namespace server {
class rocksdb_wrapper_test : public pegasus_server_test_base
{
protected:
    std::unique_ptr<pegasus_server_write> _server_write;
    rocksdb_wrapper *_rocksdb_wrapper{nullptr};
    dsn::blob _raw_key;

public:
    rocksdb_wrapper_test() = default;

    void SetUp() override
    {
        ASSERT_EQ(::dsn::ERR_OK, start());
        _server_write = std::make_unique<pegasus_server_write>(_server.get());
        _rocksdb_wrapper = _server_write->_write_svc->_impl->_rocksdb_wrapper.get();

        pegasus::pegasus_generate_key(
            _raw_key, absl::string_view("hash_key"), absl::string_view("sort_key"));
    }

    void single_set(db_write_context write_ctx,
                    dsn::blob raw_key,
                    absl::string_view user_value,
                    int32_t expire_ts_seconds)
    {
        ASSERT_EQ(_rocksdb_wrapper->write_batch_put_ctx(
                      write_ctx, raw_key.to_string_view(), user_value, expire_ts_seconds),
                  0);
        ASSERT_EQ(_rocksdb_wrapper->write(0), 0);
        _rocksdb_wrapper->clear_up_write_batch();
    }

    // start with duplicating.
    void set_app_duplicating()
    {
        _server->stop(false);
        delete _replica;

        dsn::app_info app_info;
        app_info.app_type = "pegasus";
        app_info.duplicating = true;

        auto *dn = _replica_stub->get_fs_manager()->find_best_dir_for_new_replica(_gpid);
        CHECK_NOTNULL(dn, "");
        _replica = new dsn::replication::replica(_replica_stub, _gpid, app_info, dn, false, false);
        _server = std::make_unique<mock_pegasus_server_impl>(_replica);

        SetUp();
    }

    uint64_t read_timestamp_from(absl::string_view raw_value)
    {
        uint64_t local_timetag =
            pegasus_extract_timetag(_rocksdb_wrapper->_pegasus_data_version, raw_value);
        return extract_timestamp_from_timetag(local_timetag);
    }
};

INSTANTIATE_TEST_CASE_P(, rocksdb_wrapper_test, ::testing::Values(false, true));

TEST_P(rocksdb_wrapper_test, get)
{
    // not found
    db_get_context get_ctx1;
    _rocksdb_wrapper->get(_raw_key.to_string_view(), &get_ctx1);
    ASSERT_FALSE(get_ctx1.found);

    // expired
    int32_t expired_ts = utils::epoch_now();
    db_write_context write_ctx;
    std::string value = "abc";
    single_set(write_ctx, _raw_key, value, expired_ts);
    db_get_context get_ctx2;
    _rocksdb_wrapper->get(_raw_key.to_string_view(), &get_ctx2);
    ASSERT_TRUE(get_ctx2.found);
    ASSERT_TRUE(get_ctx2.expired);
    ASSERT_EQ(get_ctx2.expire_ts, expired_ts);

    // found
    expired_ts = INT32_MAX;
    db_get_context get_ctx3;
    single_set(write_ctx, _raw_key, value, expired_ts);
    _rocksdb_wrapper->get(_raw_key.to_string_view(), &get_ctx3);
    ASSERT_TRUE(get_ctx2.found);
    ASSERT_FALSE(get_ctx3.expired);
    ASSERT_EQ(get_ctx3.expire_ts, expired_ts);
    dsn::blob user_value;
    pegasus_extract_user_data(
        _rocksdb_wrapper->_pegasus_data_version, std::move(get_ctx3.raw_value), user_value);
    ASSERT_EQ(user_value.to_string(), value);
}

TEST_P(rocksdb_wrapper_test, put_verify_timetag)
{
    set_app_duplicating();

    /// insert timestamp 10
    int64_t decree = 10;
    uint64_t timestamp = 10;
    std::string value = "value_10";
    auto ctx = db_write_context::create(decree, timestamp);
    single_set(ctx, _raw_key, value, 0);

    db_get_context get_ctx1;
    _rocksdb_wrapper->get(_raw_key.to_string_view(), &get_ctx1);
    ASSERT_TRUE(get_ctx1.found);
    ASSERT_FALSE(get_ctx1.expired);
    ASSERT_EQ(read_timestamp_from(get_ctx1.raw_value), timestamp);
    dsn::blob user_value;
    pegasus_extract_user_data(
        _rocksdb_wrapper->_pegasus_data_version, std::move(get_ctx1.raw_value), user_value);
    ASSERT_EQ(user_value.to_string(), value);

    /// insert timestamp 15, which overwrites the previous record
    timestamp = 15;
    value = "value_15";
    ctx = db_write_context::create(decree, timestamp);
    single_set(ctx, _raw_key, value, 0);

    db_get_context get_ctx2;
    _rocksdb_wrapper->get(_raw_key.to_string_view(), &get_ctx2);
    ASSERT_TRUE(get_ctx2.found);
    ASSERT_FALSE(get_ctx2.expired);
    ASSERT_EQ(read_timestamp_from(get_ctx2.raw_value), timestamp);
    pegasus_extract_user_data(
        _rocksdb_wrapper->_pegasus_data_version, std::move(get_ctx2.raw_value), user_value);
    ASSERT_EQ(user_value.to_string(), value);

    /// insert timestamp 15 from remote, which will overwrite the previous record,
    /// since its cluster id is larger (current cluster_id=1)
    timestamp = 15;
    value = "value_15_new";
    ctx.remote_timetag = pegasus::generate_timetag(timestamp, 2, false);
    ctx.verify_timetag = true;
    single_set(ctx, _raw_key, value, 0);

    db_get_context get_ctx3;
    _rocksdb_wrapper->get(_raw_key.to_string_view(), &get_ctx3);
    ASSERT_TRUE(get_ctx3.found);
    ASSERT_FALSE(get_ctx3.expired);
    ASSERT_EQ(read_timestamp_from(get_ctx3.raw_value), timestamp);
    pegasus_extract_user_data(
        _rocksdb_wrapper->_pegasus_data_version, std::move(get_ctx3.raw_value), user_value);
    ASSERT_EQ(user_value.to_string(), value);

    /// write retry
    single_set(ctx, _raw_key, value, 0);

    /// insert timestamp 16 from local, which will overwrite the remote record,
    /// since its timestamp is larger
    timestamp = 16;
    value = "value_16";
    ctx = db_write_context::create(decree, timestamp);
    single_set(ctx, _raw_key, value, 0);

    db_get_context get_ctx4;
    _rocksdb_wrapper->get(_raw_key.to_string_view(), &get_ctx4);
    ASSERT_TRUE(get_ctx4.found);
    ASSERT_FALSE(get_ctx4.expired);
    ASSERT_EQ(read_timestamp_from(get_ctx4.raw_value), timestamp);
    pegasus_extract_user_data(
        _rocksdb_wrapper->_pegasus_data_version, std::move(get_ctx4.raw_value), user_value);
    ASSERT_EQ(user_value.to_string(), value);

    // write retry
    single_set(ctx, _raw_key, value, 0);
}

// verify timetag on data version v0
TEST_P(rocksdb_wrapper_test, verify_timetag_compatible_with_version_0)
{
    const_cast<uint32_t &>(_rocksdb_wrapper->_pegasus_data_version) = 0; // old version

    /// write data with data version 0
    std::string value = "value";
    int64_t decree = 10;
    uint64_t timestamp = 10;
    auto ctx = db_write_context::create_duplicate(decree, timestamp, true);
    single_set(ctx, _raw_key, value, 0);

    db_get_context get_ctx;
    _rocksdb_wrapper->get(_raw_key.to_string_view(), &get_ctx);
    ASSERT_TRUE(get_ctx.found);
    ASSERT_FALSE(get_ctx.expired);
    dsn::blob user_value;
    pegasus_extract_user_data(
        _rocksdb_wrapper->_pegasus_data_version, std::move(get_ctx.raw_value), user_value);
    ASSERT_EQ(user_value.to_string(), value);
}
} // namespace server
} // namespace pegasus
