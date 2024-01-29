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

#include <stdint.h>
#include <unistd.h>
#include <map>
#include <memory>
#include <string>
#include <utility>

#include "backup_types.h"
#include "common/backup_common.h"
#include "common/replication.codes.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "meta/meta_backup_service.h"
#include "meta/meta_data.h"
#include "meta/meta_rpc_types.h"
#include "meta/meta_service.h"
#include "meta/server_state.h"
#include "meta_test_base.h"
#include "runtime/rpc/rpc_message.h"
#include "runtime/rpc/serialization.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/zlocks.h"

namespace dsn {
namespace replication {

class server_state_restore_test : public meta_test_base
{
public:
    server_state_restore_test()
        : _old_app_name("test_table"),
          _new_app_name("new_table"),
          _cluster_name("onebox"),
          _provider("local_service")
    {
    }

    void SetUp() override
    {
        meta_test_base::SetUp();

        // create a test app with 8 partitions.
        create_app(_old_app_name);
    }

    start_backup_app_response start_backup(int64_t app_id,
                                           const std::string user_specified_path = "")
    {
        auto request = std::make_unique<start_backup_app_request>();
        request->app_id = app_id;
        request->backup_provider_type = _provider;
        if (!user_specified_path.empty()) {
            request->__set_backup_path(user_specified_path);
        }

        start_backup_app_rpc rpc(std::move(request), RPC_CM_START_BACKUP_APP);
        _ms->_backup_handler =
            std::make_shared<backup_service>(_ms.get(), "mock_policy_root", _cluster_name, nullptr);
        _ms->_backup_handler->start_backup_app(rpc);
        wait_all();
        return rpc.response();
    }

    configuration_restore_request create_restore_request(
        int32_t old_app_id, int64_t backup_id, const std::string user_specified_restore_path = "")
    {
        configuration_restore_request req;
        req.app_id = old_app_id;
        req.app_name = _old_app_name;
        req.new_app_name = _new_app_name;
        req.time_stamp = backup_id;
        req.cluster_name = _cluster_name;
        req.backup_provider_name = _provider;
        if (!user_specified_restore_path.empty()) {
            req.__set_restore_path(user_specified_restore_path);
        }
        return req;
    }

    void test_restore_app(const std::string user_specified_path = "")
    {
        int32_t old_app_id;
        {
            zauto_read_lock l;
            _ss->lock_read(l);
            const std::shared_ptr<app_state> &app = _ss->get_app(_old_app_name);
            old_app_id = app->app_id;
        }

        // test backup app
        auto backup_resp = start_backup(old_app_id, user_specified_path);
        ASSERT_EQ(ERR_OK, backup_resp.err);
        ASSERT_TRUE(backup_resp.__isset.backup_id);
        int64_t backup_id = backup_resp.backup_id;

        // test sync_app_from_backup_media()
        auto req = create_restore_request(old_app_id, backup_id, user_specified_path);
        dsn::message_ex *msg = dsn::message_ex::create_request(RPC_CM_START_RESTORE);
        dsn::marshall(msg, req);
        error_code ret = ERR_UNKNOWN;
        dsn::blob app_info;
        _ss->sync_app_from_backup_media(
            req, [&ret, &app_info](dsn::error_code err, const dsn::blob &app_info_data) {
                ret = err;
                app_info = app_info_data;
            });
        while (ret == ERR_UNKNOWN) {
            // sleep 10 ms.
            usleep(10 * 1000);
        }
        ASSERT_EQ(ERR_OK, ret);
        ASSERT_LT(0, app_info.length());

        // test restore_app_info()
        int32_t new_app_id = _ss->next_app_id();
        auto pair = _ss->restore_app_info(msg, req, app_info);
        ASSERT_EQ(ERR_OK, pair.first);
        const std::shared_ptr<app_state> &new_app = pair.second;
        ASSERT_EQ(new_app_id, new_app->app_id);
        ASSERT_EQ(_new_app_name, new_app->app_name);
        ASSERT_EQ(app_status::AS_CREATING, new_app->status);

        // check app_envs
        auto it = new_app->envs.find(backup_restore_constant::BLOCK_SERVICE_PROVIDER);
        ASSERT_NE(new_app->envs.end(), it);
        ASSERT_EQ(_provider, it->second);
        it = new_app->envs.find(backup_restore_constant::CLUSTER_NAME);
        ASSERT_NE(new_app->envs.end(), it);
        ASSERT_EQ(_cluster_name, it->second);
        it = new_app->envs.find(backup_restore_constant::APP_NAME);
        ASSERT_NE(new_app->envs.end(), it);
        ASSERT_EQ(_old_app_name, it->second);
        it = new_app->envs.find(backup_restore_constant::APP_ID);
        ASSERT_NE(new_app->envs.end(), it);
        ASSERT_EQ(std::to_string(old_app_id), it->second);
        it = new_app->envs.find(backup_restore_constant::BACKUP_ID);
        ASSERT_NE(new_app->envs.end(), it);
        ASSERT_EQ(std::to_string(backup_id), it->second);
        if (!user_specified_path.empty()) {
            it = new_app->envs.find(backup_restore_constant::RESTORE_PATH);
            ASSERT_NE(new_app->envs.end(), it);
            ASSERT_EQ(user_specified_path, it->second);
        }
    }

protected:
    const std::string _old_app_name;
    const std::string _new_app_name;
    const std::string _cluster_name;
    const std::string _provider;
};

TEST_F(server_state_restore_test, test_restore_app) { test_restore_app(); }

TEST_F(server_state_restore_test, test_restore_app_with_specific_path)
{
    test_restore_app("test_path");
}

} // namespace replication
} // namespace dsn
