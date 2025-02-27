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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "common/replica_envs.h"
#include "common/replication.codes.h"
#include "dsn.layer2_types.h"
#include "gtest/gtest.h"
#include "meta/meta_data.h"
#include "meta/meta_rpc_types.h"
#include "meta/meta_service.h"
#include "meta/server_state.h"
#include "meta_admin_types.h"
#include "meta_service_test_app.h"
#include "rpc/rpc_holder.h"
#include "rpc/rpc_message.h"
#include "rpc/serialization.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"
#include "utils/flags.h"

DSN_DECLARE_string(cluster_root);
DSN_DECLARE_string(meta_state_service_type);

namespace dsn::replication {

namespace {

// If `str` is <prefix>.xxx, return <prefix>; otherwise return empty string("").
std::string acquire_prefix(const std::string &str)
{
    const auto &dot = str.find('.');
    if (dot == std::string::npos) {
        return {};
    }

    return str.substr(0, dot);
}

} // anonymous namespace

class server_state_test
{
public:
    server_state_test() : _ms(create_meta_service()), _ss(create_server_state(_ms.get())) {}

    void load_apps(const std::vector<std::string> &app_names)
    {
        const auto &apps = fake_apps(app_names);
        for (const auto &[_, app] : apps) {
            _ss->_all_apps.emplace(std::make_pair(app->app_id, app));
        }

        ASSERT_EQ(dsn::ERR_OK, _ss->sync_apps_to_remote_storage());
    }

    [[nodiscard]] std::shared_ptr<app_state> get_app(const std::string &app_name) const
    {
        return _ss->get_app(app_name);
    }

    app_env_rpc set_app_envs(const configuration_update_app_env_request &request)
    {
        auto rpc = create_app_env_rpc(request);
        _ss->set_app_envs(rpc);
        _ss->wait_all_task();

        return rpc;
    }

    void test_set_app_envs(const std::string &app_name,
                           const std::vector<std::string> &env_keys,
                           const std::vector<std::string> &env_vals,
                           const error_code expected_err)
    {
        configuration_update_app_env_request request;
        request.__set_app_name(app_name);
        request.__set_op(app_env_operation::type::APP_ENV_OP_SET);
        request.__set_keys(env_keys);
        request.__set_values(env_vals);

        auto rpc = set_app_envs(request);
        ASSERT_EQ(expected_err, rpc.response().err);
    }

    app_env_rpc del_app_envs(const configuration_update_app_env_request &request)
    {
        auto rpc = create_app_env_rpc(request);
        _ss->del_app_envs(rpc);
        _ss->wait_all_task();

        return rpc;
    }

    void test_del_app_envs(const std::string &app_name,
                           const std::vector<std::string> &env_keys,
                           const error_code expected_err)
    {
        configuration_update_app_env_request request;
        request.__set_app_name(app_name);
        request.__set_op(app_env_operation::type::APP_ENV_OP_DEL);
        request.__set_keys(env_keys);

        auto rpc = del_app_envs(request);
        ASSERT_EQ(expected_err, rpc.response().err);
    }

    app_env_rpc clear_app_envs(const configuration_update_app_env_request &request)
    {
        auto rpc = create_app_env_rpc(request);
        _ss->clear_app_envs(rpc);
        _ss->wait_all_task();

        return rpc;
    }

    void test_clear_app_envs(const std::string &app_name,
                             const std::string &prefix,
                             const error_code expected_err)
    {
        configuration_update_app_env_request request;
        request.__set_app_name(app_name);
        request.__set_op(app_env_operation::type::APP_ENV_OP_CLEAR);
        request.__set_clear_prefix(prefix);

        auto rpc = clear_app_envs(request);
        ASSERT_EQ(expected_err, rpc.response().err);
    }

private:
    static std::shared_ptr<app_state> fake_app_state(const std::string &app_name,
                                                     const int32_t app_id)
    {
        dsn::app_info info;
        info.is_stateful = true;
        info.app_id = app_id;
        info.app_type = "simple_kv";
        info.app_name = app_name;
        info.max_replica_count = 3;
        info.partition_count = 32;
        info.status = dsn::app_status::AS_CREATING;
        info.envs.clear();
        return app_state::create(info);
    }

    static std::map<std::string, std::shared_ptr<app_state>>
    fake_apps(const std::vector<std::string> &app_names)
    {
        std::map<std::string, std::shared_ptr<app_state>> apps;

        int32_t app_id = 1;
        std::transform(app_names.begin(),
                       app_names.end(),
                       std::inserter(apps, apps.end()),
                       [&app_id](const std::string &app_name) {
                           return std::make_pair(app_name, fake_app_state(app_name, app_id++));
                       });

        return apps;
    }

    static std::unique_ptr<meta_service> create_meta_service()
    {
        auto ms = std::make_unique<meta_service>();

        FLAGS_cluster_root = "/meta_test";
        FLAGS_meta_state_service_type = "meta_state_service_simple";
        ms->remote_storage_initialize();

        return ms;
    }

    static std::shared_ptr<server_state> create_server_state(meta_service *ms)
    {
        std::string apps_root("/meta_test/apps");
        const auto &ss = ms->_state;
        ss->initialize(ms, apps_root);

        return ss;
    }

    static app_env_rpc create_app_env_rpc(const configuration_update_app_env_request &request)
    {
        dsn::message_ptr binary_req(dsn::message_ex::create_request(RPC_CM_UPDATE_APP_ENV));
        dsn::marshall(binary_req, request);
        dsn::message_ex *recv_msg = create_corresponding_receive(binary_req);
        return app_env_rpc(recv_msg); // Don't need to reply.
    }

    std::unique_ptr<meta_service> _ms;
    std::shared_ptr<server_state> _ss;
};

void meta_service_test_app::app_envs_basic_test()
{
    static const std::vector<std::string> kKeys = {
        dsn::replica_envs::MANUAL_COMPACT_ONCE_TRIGGER_TIME,
        dsn::replica_envs::MANUAL_COMPACT_ONCE_TARGET_LEVEL,
        dsn::replica_envs::MANUAL_COMPACT_ONCE_BOTTOMMOST_LEVEL_COMPACTION,
        dsn::replica_envs::MANUAL_COMPACT_PERIODIC_TRIGGER_TIME,
        dsn::replica_envs::MANUAL_COMPACT_PERIODIC_TARGET_LEVEL,
        dsn::replica_envs::MANUAL_COMPACT_PERIODIC_BOTTOMMOST_LEVEL_COMPACTION,
        dsn::replica_envs::ROCKSDB_USAGE_SCENARIO,
        dsn::replica_envs::ROCKSDB_CHECKPOINT_RESERVE_MIN_COUNT,
        dsn::replica_envs::ROCKSDB_CHECKPOINT_RESERVE_TIME_SECONDS};

    static const std::vector<std::string> kValues = {
        "1712846598",
        "6",
        dsn::replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_FORCE,
        "1712846598",
        "-1",
        dsn::replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP,
        dsn::replica_envs::ROCKSDB_ENV_USAGE_SCENARIO_NORMAL,
        "1",
        "0"};

    server_state_test test;
    test.load_apps({"test_app1",
                    "test_set_app_envs_not_found",
                    "test_set_app_envs_dropping",
                    "test_set_app_envs_dropped_after",
                    "test_del_app_envs_not_found",
                    "test_del_app_envs_dropping",
                    "test_del_app_envs_dropped_after",
                    "test_clear_app_envs_not_found",
                    "test_clear_app_envs_dropping",
                    "test_clear_app_envs_dropped_after"});

#define TEST_SET_APP_ENVS_FAILED(action, err_code)                                                 \
    std::cout << "test server_state::set_app_envs(" #action ")..." << std::endl;                   \
    do {                                                                                           \
        fail::setup();                                                                             \
        fail::cfg("set_app_envs_failed", "void(" #action ")");                                     \
                                                                                                   \
        test.test_set_app_envs("test_set_app_envs_" #action,                                       \
                               {replica_envs::ROCKSDB_WRITE_BUFFER_SIZE},                          \
                               {"67108864"},                                                       \
                               err_code);                                                          \
                                                                                                   \
        fail::teardown();                                                                          \
    } while (0)

    // Failed to setting envs while table was not found.
    TEST_SET_APP_ENVS_FAILED(not_found, ERR_APP_NOT_EXIST);

    // Failed to setting envs while table was being dropped as the intermediate state.
    TEST_SET_APP_ENVS_FAILED(dropping, ERR_BUSY_DROPPING);

    // The table was found dropped after the new envs had been persistent on the remote
    // meta storage.
    TEST_SET_APP_ENVS_FAILED(dropped_after, ERR_APP_DROPPED);

#undef TEST_SET_APP_ENVS_FAILED

    // Normal case for setting envs.
    std::cout << "test server_state::set_app_envs(success)..." << std::endl;
    {
        test.test_set_app_envs("test_app1", kKeys, kValues, ERR_OK);

        const auto &app = test.get_app("test_app1");
        ASSERT_TRUE(app);

        for (size_t idx = 0; idx < kKeys.size(); ++idx) {
            const auto &key = kKeys[idx];

            // Every env should be inserted.
            ASSERT_EQ(1, app->envs.count(key));
            ASSERT_EQ(kValues[idx], app->envs.at(key));
        }
    }

#define TEST_DEL_APP_ENVS_FAILED(action, err_code)                                                 \
    std::cout << "test server_state::del_app_envs(" #action ")..." << std::endl;                   \
    do {                                                                                           \
        test.test_set_app_envs("test_del_app_envs_" #action,                                       \
                               {replica_envs::ROCKSDB_WRITE_BUFFER_SIZE},                          \
                               {"67108864"},                                                       \
                               ERR_OK);                                                            \
                                                                                                   \
        fail::setup();                                                                             \
        fail::cfg("del_app_envs_failed", "void(" #action ")");                                     \
                                                                                                   \
        test.test_del_app_envs(                                                                    \
            "test_del_app_envs_" #action, {replica_envs::ROCKSDB_WRITE_BUFFER_SIZE}, err_code);    \
                                                                                                   \
        fail::teardown();                                                                          \
    } while (0)

    // Failed to deleting envs while table was not found.
    TEST_DEL_APP_ENVS_FAILED(not_found, ERR_APP_NOT_EXIST);

    // Failed to deleting envs while table was being dropped as the intermediate state.
    TEST_DEL_APP_ENVS_FAILED(dropping, ERR_BUSY_DROPPING);

    // The table was found dropped after the new envs had been persistent on the remote
    // meta storage.
    TEST_DEL_APP_ENVS_FAILED(dropped_after, ERR_APP_DROPPED);

#undef TEST_DEL_APP_ENVS_FAILED

    static const std::vector<std::string> kDelKeyList = {
        dsn::replica_envs::MANUAL_COMPACT_ONCE_TRIGGER_TIME,
        dsn::replica_envs::MANUAL_COMPACT_PERIODIC_TRIGGER_TIME,
        dsn::replica_envs::ROCKSDB_USAGE_SCENARIO};
    static const std::set<std::string> kDelKeySet(kDelKeyList.begin(), kDelKeyList.end());

    // Normal case for deleting envs.
    std::cout << "test server_state::del_app_envs(success)..." << std::endl;
    {
        test.test_del_app_envs("test_app1", kDelKeyList, ERR_OK);

        const auto &app = test.get_app("test_app1");
        ASSERT_TRUE(app);

        for (size_t idx = 0; idx < kKeys.size(); ++idx) {
            const std::string &key = kKeys[idx];
            if (kDelKeySet.count(key) > 0) {
                // The env in `kDelKeySet` should be deleted.
                ASSERT_EQ(0, app->envs.count(key));
                continue;
            }

            // The env should still exist if it is not in `kDelKeySet`.
            ASSERT_EQ(1, app->envs.count(key));
            ASSERT_EQ(kValues[idx], app->envs.at(key));
        }
    }

#define TEST_CLEAR_APP_ENVS_FAILED(action, err_code)                                               \
    std::cout << "test server_state::clear_app_envs(" #action ")..." << std::endl;                 \
    do {                                                                                           \
        test.test_set_app_envs("test_clear_app_envs_" #action,                                     \
                               {replica_envs::ROCKSDB_WRITE_BUFFER_SIZE},                          \
                               {"67108864"},                                                       \
                               ERR_OK);                                                            \
                                                                                                   \
        fail::setup();                                                                             \
        fail::cfg("clear_app_envs_failed", "void(" #action ")");                                   \
                                                                                                   \
        test.test_clear_app_envs("test_clear_app_envs_" #action, "", err_code);                    \
                                                                                                   \
        fail::teardown();                                                                          \
    } while (0)

    // Failed to clearing envs while table was not found.
    TEST_CLEAR_APP_ENVS_FAILED(not_found, ERR_APP_NOT_EXIST);

    // Failed to clearing envs while table was being dropped as the intermediate state.
    TEST_CLEAR_APP_ENVS_FAILED(dropping, ERR_BUSY_DROPPING);

    // The table was found dropped after the new envs had been persistent on the remote
    // meta storage.
    TEST_CLEAR_APP_ENVS_FAILED(dropped_after, ERR_APP_DROPPED);

#undef TEST_CLEAR_APP_ENVS_FAILED

    std::cout << "test server_state::clear_app_envs(success)..." << std::endl;
    {
        // Test specifying prefix.
        {
            static const std::string kClearPrefix = "rocksdb";
            test.test_clear_app_envs("test_app1", kClearPrefix, ERR_OK);

            const auto &app = test.get_app("test_app1");
            ASSERT_TRUE(app);

            for (size_t idx = 0; idx < kKeys.size(); ++idx) {
                const std::string &key = kKeys[idx];
                if (kDelKeySet.count(key) > 0) {
                    // The env should have been deleted during test for `del_app_envs`.
                    ASSERT_EQ(0, app->envs.count(key));
                    continue;
                }

                if (acquire_prefix(key) == kClearPrefix) {
                    // The env with specified prefix should be deleted.
                    ASSERT_EQ(0, app->envs.count(key));
                    continue;
                }

                // Otherwise, the env should still exist.
                ASSERT_EQ(1, app->envs.count(key));
                ASSERT_EQ(kValues[idx], app->envs.at(key));
            }
        }

        // Test clearing all.
        {
            test.test_clear_app_envs("test_app1", "", ERR_OK);

            const auto &app = test.get_app("test_app1");
            ASSERT_TRUE(app);

            // All envs should be cleared.
            ASSERT_TRUE(app->envs.empty());
        }
    }
}

} // namespace dsn::replication
