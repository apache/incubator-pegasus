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
#include <rocksdb/write_batch.h>
#include <stdint.h>
#include <memory>
#include <string>
#include <vector>

#include "base/pegasus_key_schema.h"
#include "common/gpid.h"
#include "gtest/gtest.h"
#include "message_utils.h"
#include "pegasus_rpc_types.h"
#include "pegasus_server_test_base.h"
#include "rrdb/rrdb_types.h"
#include "runtime/rpc/rpc_holder.h"
#include "server/pegasus_server_write.h"
#include "server/pegasus_write_service.h"
#include "server/pegasus_write_service_impl.h"
#include "server/rocksdb_wrapper.h"
#include "utils/blob.h"
#include "utils/fail_point.h"
#include "utils/rand.h"

namespace dsn {
class message_ex;
} // namespace dsn

namespace pegasus {
namespace server {

class pegasus_server_write_test : public pegasus_server_test_base
{
    std::unique_ptr<pegasus_server_write> _server_write;

public:
    pegasus_server_write_test() : pegasus_server_test_base()
    {
        start();
        _server_write = std::make_unique<pegasus_server_write>(_server.get());
    }

    void test_batch_writes()
    {
        dsn::fail::setup();

        dsn::fail::cfg("db_write_batch_put", "10%return()");
        dsn::fail::cfg("db_write_batch_remove", "10%return()");
        dsn::fail::cfg("db_write", "10%return()");

        for (int decree = 1; decree <= 1000; decree++) {
            RPC_MOCKING(put_rpc) RPC_MOCKING(remove_rpc)
            {
                dsn::blob key;
                pegasus_generate_key(key, std::string("hash"), std::string("sort"));
                dsn::apps::update_request req;
                req.key = key;
                req.value.assign("value", 0, 5);

                int put_rpc_cnt = dsn::rand::next_u32(1, 10);
                int remove_rpc_cnt = dsn::rand::next_u32(1, 10);
                int total_rpc_cnt = put_rpc_cnt + remove_rpc_cnt;
                /**
                 * writes[0] ~ writes[total_rpc_cnt-1] will be released by their corresponding
                 * rpc_holders, which created in on_batched_write_requests. So we don't need to
                 * release them here
                 **/
                dsn::message_ex *writes[total_rpc_cnt];
                for (int i = 0; i < put_rpc_cnt; i++) {
                    writes[i] = pegasus::create_put_request(req);
                }
                for (int i = put_rpc_cnt; i < total_rpc_cnt; i++) {
                    writes[i] = pegasus::create_remove_request(key);
                }

                int err =
                    _server_write->on_batched_write_requests(writes, total_rpc_cnt, decree, 0);
                switch (err) {
                case FAIL_DB_WRITE_BATCH_PUT:
                case FAIL_DB_WRITE_BATCH_DELETE:
                case FAIL_DB_WRITE:
                case 0:
                    break;
                default:
                    ASSERT_TRUE(false) << "unacceptable error: " << err;
                }

                // make sure everything is cleanup after batch write.
                ASSERT_TRUE(_server_write->_put_rpc_batch.empty());
                ASSERT_TRUE(_server_write->_remove_rpc_batch.empty());
                ASSERT_TRUE(_server_write->_write_svc->_batch_qps_perfcounters.empty());
                ASSERT_TRUE(_server_write->_write_svc->_batch_latency_perfcounters.empty());
                ASSERT_EQ(_server_write->_write_svc->_batch_start_time, 0);
                ASSERT_EQ(_server_write->_write_svc->_impl->_rocksdb_wrapper->_write_batch->Count(),
                          0);
                ASSERT_EQ(_server_write->_write_svc->_impl->_update_responses.size(), 0);

                ASSERT_EQ(put_rpc::mail_box().size(), put_rpc_cnt);
                ASSERT_EQ(remove_rpc::mail_box().size(), remove_rpc_cnt);
                for (auto &rpc : put_rpc::mail_box()) {
                    verify_response(rpc.response(), err, decree);
                }
                for (auto &rpc : remove_rpc::mail_box()) {
                    verify_response(rpc.response(), err, decree);
                }
            }
        }

        dsn::fail::teardown();
    }

    void verify_response(const dsn::apps::update_response &response, int err, int64_t decree)
    {
        ASSERT_EQ(response.error, err);
        ASSERT_EQ(response.app_id, _gpid.get_app_id());
        ASSERT_EQ(response.partition_index, _gpid.get_partition_index());
        ASSERT_EQ(response.decree, decree);
        ASSERT_EQ(response.server, _server_write->_write_svc->_impl->_primary_address);
    }
};

INSTANTIATE_TEST_CASE_P(, pegasus_server_write_test, ::testing::Values(false, true));

TEST_P(pegasus_server_write_test, batch_writes) { test_batch_writes(); }

} // namespace server
} // namespace pegasus
