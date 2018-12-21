#include <gtest/gtest.h>
#include <dsn/service_api_c.h>

#include "dist/replication/meta_server/meta_service.h"
#include "dist/replication/meta_server/server_state.h"

#include "meta_service_test_app.h"

using namespace ::dsn::replication;

static const std::vector<std::string> keys = {
    "p1.k1", "p1.k2", "p1.k3", "p2.k1", "p2.k2", "p2.k3", "p3.k1", "p3.k2", "p3.k3"};
static const std::vector<std::string> values = {
    "p1v1", "p1v2", "p1v3", "p2v1", "p2v2", "p2v3", "p3v1", "p3v2", "p3v3"};

static const std::vector<std::string> del_keys = {"p1.k1", "p2.k1", "p3.k1"};
static const std::set<std::string> del_keys_set = {"p1.k1", "p2.k1", "p3.k1"};

static const std::string clear_prefix = "p1";

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

        dsn::message_ex *binary_req = dsn::message_ex::create_request(RPC_CM_UPDATE_APP_ENV);
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

        dsn::message_ex *binary_req = dsn::message_ex::create_request(RPC_CM_UPDATE_APP_ENV);
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

            dsn::message_ex *binary_req = dsn::message_ex::create_request(RPC_CM_UPDATE_APP_ENV);
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

            dsn::message_ex *binary_req = dsn::message_ex::create_request(RPC_CM_UPDATE_APP_ENV);
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
