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
    pegasus_write_service::impl *_write_impl{nullptr};
    rocksdb_wrapper *_rocksdb_wrapper{nullptr};
    dsn::blob _raw_key;

public:
    void SetUp() override
    {
        start();
        _server_write = dsn::make_unique<pegasus_server_write>(_server.get(), true);
        _write_impl = _server_write->_write_svc->_impl.get();
        _rocksdb_wrapper = _write_impl->_rocksdb_wrapper.get();

        pegasus::pegasus_generate_key(
            _raw_key, dsn::string_view("hash_key"), dsn::string_view("sort_key"));
    }

    void single_set(dsn::blob raw_key, dsn::blob user_value, int32_t expire_ts_seconds)
    {
        dsn::apps::update_request put;
        put.key = raw_key;
        put.value = user_value;
        put.expire_ts_seconds = expire_ts_seconds;
        db_write_context write_ctx;
        dsn::apps::update_response put_resp;
        _write_impl->batch_put(write_ctx, put, put_resp);
        ASSERT_EQ(_write_impl->batch_commit(0), 0);
    }
};

TEST_F(rocksdb_wrapper_test, get)
{
    // not found
    db_get_context get_ctx1;
    _rocksdb_wrapper->get(_raw_key, &get_ctx1);
    ASSERT_FALSE(get_ctx1.found);

    // expired
    int32_t expired_ts = 123;
    db_get_context get_ctx2;
    single_set(_raw_key, dsn::blob::create_from_bytes("abc"), expired_ts);
    _rocksdb_wrapper->get(_raw_key, &get_ctx2);
    ASSERT_TRUE(get_ctx2.found);
    ASSERT_TRUE(get_ctx2.expired);
    ASSERT_EQ(get_ctx2.expire_ts, expired_ts);

    // found
    expired_ts = INT32_MAX;
    db_get_context get_ctx3;
    single_set(_raw_key, dsn::blob::create_from_bytes("abc"), expired_ts);
    _rocksdb_wrapper->get(_raw_key, &get_ctx3);
    ASSERT_TRUE(get_ctx2.found);
    ASSERT_FALSE(get_ctx3.expired);
    ASSERT_EQ(get_ctx3.expire_ts, expired_ts);
}
} // namespace server
} // namespace pegasus
