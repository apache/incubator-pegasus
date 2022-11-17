/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include <gtest/gtest.h>
#include "runtime/api_task.h"
#include "runtime/api_layer1.h"
#include "runtime/app_model.h"
#include "utils/api_utilities.h"

#include "meta/meta_service.h"
#include "meta/server_state.h"

#include "meta_service_test_app.h"

namespace dsn {
namespace replication {

static const std::vector<std::string> keys = {"manual_compact.once.trigger_time",
                                              "manual_compact.once.target_level",
                                              "manual_compact.once.bottommost_level_compaction",
                                              "manual_compact.periodic.trigger_time",
                                              "manual_compact.periodic.target_level",
                                              "manual_compact.periodic.bottommost_level_compaction",
                                              "rocksdb.usage_scenario",
                                              "rocksdb.checkpoint.reserve_min_count",
                                              "rocksdb.checkpoint.reserve_time_seconds"};
static const std::vector<std::string> values = {
    "p1v1", "p1v2", "p1v3", "p2v1", "p2v2", "p2v3", "p3v1", "p3v2", "p3v3"};

static const std::vector<std::string> del_keys = {"manual_compact.once.trigger_time",
                                                  "manual_compact.periodic.trigger_time",
                                                  "rocksdb.usage_scenario"};
static const std::set<std::string> del_keys_set = {"manual_compact.once.trigger_time",
                                                   "manual_compact.periodic.trigger_time",
                                                   "rocksdb.usage_scenario"};

static const std::string clear_prefix = "rocksdb";

// if str = "prefix.xxx" then return prefix
// else return ""
static std::string acquire_prefix(const std::string &str)
{
    auto index = str.find('.');
    if (index == std::string::npos) {
        return "";
    } else {
        return str.substr(0, index);
    }
}

void meta_service_test_app::app_envs_basic_test()
{
    // create a fake app
    dsn::app_info info;
    info.is_stateful = true;
    info.app_id = 1;
    info.app_type = "simple_kv";
    info.app_name = "test_app1";
    info.max_replica_count = 3;
    info.partition_count = 32;
    info.status = dsn::app_status::AS_CREATING;
    info.envs.clear();
    std::shared_ptr<app_state> fake_app = app_state::create(info);

    // create meta_service
    std::shared_ptr<meta_service> meta_svc = std::make_shared<meta_service>();
    meta_service *svc = meta_svc.get();

    svc->_meta_opts.cluster_root = "/meta_test";
    svc->_meta_opts.meta_state_service_type = "meta_state_service_simple";
    svc->remote_storage_initialize();

    std::string apps_root = "/meta_test/apps";
    std::shared_ptr<server_state> ss = svc->_state;
    ss->initialize(svc, apps_root);

    ss->_all_apps.emplace(std::make_pair(fake_app->app_id, fake_app));
    dsn::error_code ec = ss->sync_apps_to_remote_storage();
    ASSERT_EQ(ec, dsn::ERR_OK);

    std::cout << "test server_state::set_app_envs()..." << std::endl;
    {
        configuration_update_app_env_request request;
        request.__set_app_name(fake_app->app_name);
        request.__set_op(app_env_operation::type::APP_ENV_OP_SET);
        request.__set_keys(keys);
        request.__set_values(values);

        dsn::message_ptr binary_req = dsn::message_ex::create_request(RPC_CM_UPDATE_APP_ENV);
        dsn::marshall(binary_req, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(binary_req);
        app_env_rpc rpc(recv_msg); // don't need reply
        ss->set_app_envs(rpc);
        ss->wait_all_task();
        std::shared_ptr<app_state> app = ss->get_app(fake_app->app_name);
        ASSERT_TRUE(app != nullptr);
        for (int idx = 0; idx < keys.size(); idx++) {
            const std::string &key = keys[idx];
            ASSERT_EQ(app->envs.count(key), 1);
            ASSERT_EQ(app->envs.at(key), values[idx]);
        }
    }

    std::cout << "test server_state::del_app_envs()..." << std::endl;
    {
        configuration_update_app_env_request request;
        request.__set_app_name(fake_app->app_name);
        request.__set_op(app_env_operation::type::APP_ENV_OP_DEL);
        request.__set_keys(del_keys);

        dsn::message_ptr binary_req = dsn::message_ex::create_request(RPC_CM_UPDATE_APP_ENV);
        dsn::marshall(binary_req, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(binary_req);
        app_env_rpc rpc(recv_msg); // don't need reply
        ss->del_app_envs(rpc);
        ss->wait_all_task();

        std::shared_ptr<app_state> app = ss->get_app(fake_app->app_name);
        ASSERT_TRUE(app != nullptr);
        for (int idx = 0; idx < keys.size(); idx++) {
            const std::string &key = keys[idx];
            if (del_keys_set.count(key) >= 1) {
                ASSERT_EQ(app->envs.count(key), 0);
            } else {
                ASSERT_EQ(app->envs.count(key), 1);
                ASSERT_EQ(app->envs.at(key), values[idx]);
            }
        }
    }

    std::cout << "test server_state::clear_app_envs()..." << std::endl;
    {
        // test specify prefix
        {
            configuration_update_app_env_request request;
            request.__set_app_name(fake_app->app_name);
            request.__set_op(app_env_operation::type::APP_ENV_OP_CLEAR);
            request.__set_clear_prefix(clear_prefix);

            dsn::message_ptr binary_req = dsn::message_ex::create_request(RPC_CM_UPDATE_APP_ENV);
            dsn::marshall(binary_req, request);
            dsn::message_ex *recv_msg = create_corresponding_receive(binary_req);
            app_env_rpc rpc(recv_msg); // don't need reply
            ss->clear_app_envs(rpc);
            ss->wait_all_task();

            std::shared_ptr<app_state> app = ss->get_app(fake_app->app_name);
            ASSERT_TRUE(app != nullptr);
            for (int idx = 0; idx < keys.size(); idx++) {
                const std::string &key = keys[idx];
                if (del_keys_set.count(key) <= 0) {
                    if (acquire_prefix(key) == clear_prefix) {
                        ASSERT_EQ(app->envs.count(key), 0);
                    } else {
                        ASSERT_EQ(app->envs.count(key), 1);
                        ASSERT_EQ(app->envs.at(key), values[idx]);
                    }
                } else {
                    // key already delete
                    ASSERT_EQ(app->envs.count(key), 0);
                }
            }
        }

        // test clear all
        {
            configuration_update_app_env_request request;
            request.__set_app_name(fake_app->app_name);
            request.__set_op(app_env_operation::type::APP_ENV_OP_CLEAR);
            request.__set_clear_prefix("");

            dsn::message_ptr binary_req = dsn::message_ex::create_request(RPC_CM_UPDATE_APP_ENV);
            dsn::marshall(binary_req, request);
            dsn::message_ex *recv_msg = create_corresponding_receive(binary_req);
            app_env_rpc rpc(recv_msg); // don't need reply
            ss->clear_app_envs(rpc);
            ss->wait_all_task();

            std::shared_ptr<app_state> app = ss->get_app(fake_app->app_name);
            ASSERT_TRUE(app != nullptr);
            ASSERT_TRUE(app->envs.empty());
        }
    }
}
} // namespace replication
} // namespace dsn
