// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <fmt/core.h>
#include <memory>
#include <string>
#include <vector>

#include "common/replication.codes.h"
#include "gtest/gtest.h"
#include "meta/meta_rpc_types.h"
#include "meta/meta_service.h"
#include "meta_admin_types.h"
#include "meta_test_base.h"
#include "rpc/network.sim.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_holder.h"
#include "rpc/rpc_host_port.h"
#include "rpc/rpc_message.h"
#include "rpc/serialization.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"

namespace dsn {
namespace replication {

class meta_service_test : public meta_test_base
{
public:
    void check_status_failure()
    {
        fail::setup();
        fail::cfg("meta_server_failure_detector_get_leader", "return(false#localhost:10086)");

        /** can't forward to others */
        RPC_MOCKING(app_env_rpc)
        {
            host_port leader;
            auto rpc = create_fake_rpc();
            rpc.dsn_request()->header->context.u.is_forward_supported = false;
            ASSERT_FALSE(_ms->check_status_and_authz(rpc, &leader));
            ASSERT_EQ(ERR_FORWARD_TO_OTHERS, rpc.response().err);
            ASSERT_EQ(leader.to_string(), "localhost:10086");
            ASSERT_EQ(app_env_rpc::forward_mail_box().size(), 0);
        }

        /** forward to others */
        RPC_MOCKING(app_env_rpc)
        {
            auto rpc = create_fake_rpc();
            ASSERT_FALSE(_ms->check_status_and_authz(rpc));
            ASSERT_EQ(app_env_rpc::forward_mail_box().size(), 1);
            ASSERT_STREQ("127.0.0.1:10086",
                         app_env_rpc::forward_mail_box()[0].remote_address().to_string());
        }

        fail::teardown();
    }

    void check_status_success()
    {
        fail::setup();
        fail::cfg("meta_server_failure_detector_get_leader", "return(true#localhost:10086)");

        RPC_MOCKING(app_env_rpc)
        {
            host_port leader;
            auto rpc = create_fake_rpc();
            ASSERT_TRUE(_ms->check_status_and_authz(rpc, &leader));
            ASSERT_EQ(app_env_rpc::forward_mail_box().size(), 0);
        }

        fail::teardown();
    }

    void check_op_status_lock()
    {
        SetUp();

        meta_op_status st = _ms->get_op_status();
        ASSERT_EQ(meta_op_status::FREE, st);
        bool res = _ms->try_lock_meta_op_status(meta_op_status::BULKLOAD);
        ASSERT_TRUE(res);
        res = _ms->try_lock_meta_op_status(meta_op_status::BULKLOAD);
        ASSERT_FALSE(res);
        st = _ms->get_op_status();
        ASSERT_EQ(meta_op_status::BULKLOAD, st);
        res = _ms->try_lock_meta_op_status(meta_op_status::BACKUP);
        ASSERT_FALSE(res);
        st = _ms->get_op_status();
        ASSERT_EQ(meta_op_status::BULKLOAD, st);
        _ms->unlock_meta_op_status();
        st = _ms->get_op_status();
        ASSERT_EQ(meta_op_status::FREE, st);

        TearDown();
    }

private:
    app_env_rpc create_fake_rpc()
    {
        dsn::message_ptr fake_request = dsn::message_ex::create_request(RPC_CM_UPDATE_APP_ENV);
        configuration_update_app_env_request request;
        ::dsn::marshall(fake_request, request);

        dsn::message_ex *recvd_request = fake_request->copy(true, true);
        std::unique_ptr<tools::sim_network_provider> sim_net(
            new tools::sim_network_provider(nullptr, nullptr));
        recvd_request->io_session = sim_net->create_client_session(rpc_address());
        return app_env_rpc::auto_reply(recvd_request);
    }
};

TEST_F(meta_service_test, check_status_failure) { check_status_failure(); }

TEST_F(meta_service_test, check_status_success) { check_status_success(); }

TEST_F(meta_service_test, check_op_status_lock) { check_op_status_lock(); }

} // namespace replication
} // namespace dsn
