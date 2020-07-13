// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "meta_test_base.h"

#include <dsn/utility/fail_point.h>

namespace dsn {
namespace replication {

class meta_service_test : public meta_test_base
{
public:
    void check_status_failure()
    {
        fail::setup();
        fail::cfg("meta_server_failure_detector_get_leader", "return(false#1.2.3.4:10086)");

        /** can't forward to others */
        RPC_MOCKING(app_env_rpc)
        {
            rpc_address leader;
            auto rpc = create_fake_rpc();
            rpc.dsn_request()->header->context.u.is_forward_supported = false;
            bool res = _ms->check_status(rpc, &leader);
            ASSERT_EQ(false, res);
            ASSERT_EQ(ERR_FORWARD_TO_OTHERS, rpc.response().err);
            ASSERT_EQ(leader.to_std_string(), "1.2.3.4:10086");
            ASSERT_EQ(app_env_rpc::forward_mail_box().size(), 0);
        }

        /** forward to others */
        RPC_MOCKING(app_env_rpc)
        {
            auto rpc = create_fake_rpc();
            bool res = _ms->check_status(rpc);
            ASSERT_EQ(false, res);
            ASSERT_EQ(app_env_rpc::forward_mail_box().size(), 1);
            ASSERT_EQ(app_env_rpc::forward_mail_box()[0].remote_address().to_std_string(),
                      "1.2.3.4:10086");
        }

        fail::teardown();
    }

    void check_status_success()
    {
        fail::setup();
        fail::cfg("meta_server_failure_detector_get_leader", "return(true#1.2.3.4:10086)");

        RPC_MOCKING(app_env_rpc)
        {
            rpc_address leader;
            auto rpc = create_fake_rpc();
            auto res = _ms->check_status(rpc, &leader);
            ASSERT_EQ(true, res);
            ASSERT_EQ(app_env_rpc::forward_mail_box().size(), 0);
        }

        fail::teardown();
    }

private:
    app_env_rpc create_fake_rpc()
    {
        dsn::message_ptr fake_request = dsn::message_ex::create_request(RPC_CM_UPDATE_APP_ENV);
        configuration_update_app_env_request request;
        ::dsn::marshall(fake_request, request);

        dsn::message_ex *recvd_request = fake_request->copy(true, true);
        return app_env_rpc::auto_reply(recvd_request);
    }
};

TEST_F(meta_service_test, check_status_failure) { check_status_failure(); }

TEST_F(meta_service_test, check_status_success) { check_status_success(); }

} // namespace replication
} // namespace dsn
