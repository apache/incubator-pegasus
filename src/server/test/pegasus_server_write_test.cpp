// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_server_test_base.h"
#include "message_utils.h"
#include "server/pegasus_server_write.h"
#include "server/pegasus_write_service_impl.h"
#include "base/pegasus_key_schema.h"

#include <dsn/utility/fail_point.h>
#include <dsn/utility/defer.h>

namespace pegasus {
namespace server {

class pegasus_server_write_test : public pegasus_server_test_base
{
    std::unique_ptr<pegasus_server_write> _server_write;

public:
    pegasus_server_write_test() : pegasus_server_test_base()
    {
        start();
        _server_write = dsn::make_unique<pegasus_server_write>(_server.get(), true);
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
                auto writes = new dsn::message_ex *[total_rpc_cnt];
                for (int i = 0; i < put_rpc_cnt; i++) {
                    writes[i] = pegasus::create_put_request(req);
                }
                for (int i = put_rpc_cnt; i < total_rpc_cnt; i++) {
                    writes[i] = pegasus::create_remove_request(key);
                }
                auto cleanup = dsn::defer([=]() { delete[] writes; });

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
                ASSERT_EQ(_server_write->_write_svc->_impl->_batch.Count(), 0);
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

TEST_F(pegasus_server_write_test, batch_writes) { test_batch_writes(); }

} // namespace server
} // namespace pegasus
