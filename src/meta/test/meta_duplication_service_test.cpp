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

// IWYU pragma: no_include <ext/alloc_traits.h>
#include <fmt/core.h>
#include <unistd.h>
#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <ostream>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/common.h"
#include "common/duplication_common.h"
#include "common/gpid.h"
#include "common/replication.codes.h"
#include "common/replication_other_types.h"
#include "dsn.layer2_types.h"
#include "duplication_types.h"
#include "gtest/gtest.h"
#include "gutil/map_util.h"
#include "http/http_server.h"
#include "http/http_status_code.h"
#include "meta/duplication/duplication_info.h"
#include "meta/duplication/meta_duplication_service.h"
#include "meta/meta_data.h"
#include "meta/meta_http_service.h"
#include "meta/meta_service.h"
#include "meta/meta_state_service_utils.h"
#include "meta/server_state.h"
#include "meta/test/misc/misc.h"
#include "meta_test_base.h"
#include "rpc/rpc_address.h"
#include "rpc/rpc_host_port.h"
#include "runtime/api_layer1.h"
#include "utils/blob.h"
#include "utils/error_code.h"
#include "utils/fail_point.h"
#include "utils/time_utils.h"
#include "utils_types.h"

namespace dsn::replication {

class meta_duplication_service_test : public meta_test_base
{
public:
    static const std::string kTestAppName;
    static const std::string kTestRemoteClusterName;
    static const std::string kTestRemoteAppName;
    static const int32_t kTestRemoteReplicaCount;

    meta_duplication_service_test() = default;

    duplication_add_response create_dup(const std::string &app_name,
                                        const std::string &remote_cluster,
                                        const bool specified,
                                        const std::string &remote_app_name,
                                        const int32_t remote_replica_count)
    {
        auto req = std::make_unique<duplication_add_request>();
        req->app_name = app_name;
        req->remote_cluster_name = remote_cluster;
        if (specified) {
            req->__set_remote_app_name(remote_app_name);
            req->__set_remote_replica_count(remote_replica_count);
        }

        duplication_add_rpc rpc(std::move(req), RPC_CM_ADD_DUPLICATION);
        dup_svc().add_duplication(rpc);
        wait_all();
        return rpc.response();
    }

    duplication_add_response create_dup(const std::string &app_name,
                                        const std::string &remote_cluster,
                                        const std::string &remote_app_name,
                                        const int32_t remote_replica_count)
    {
        return create_dup(app_name, remote_cluster, true, remote_app_name, remote_replica_count);
    }

    duplication_add_response create_dup_unspecified(const std::string &app_name,
                                                    const std::string &remote_cluster)
    {
        return create_dup(app_name, remote_cluster, false, "", 0);
    }

    duplication_add_response create_dup(const std::string &app_name,
                                        const std::string &remote_cluster,
                                        const int32_t remote_replica_count)
    {
        return create_dup(app_name, remote_cluster, app_name, remote_replica_count);
    }

    duplication_add_response create_dup(const std::string &app_name,
                                        const int32_t remote_replica_count)
    {
        return create_dup(app_name, kTestRemoteClusterName, remote_replica_count);
    }

    duplication_add_response create_dup(const std::string &app_name)
    {
        return create_dup(app_name, kTestRemoteClusterName, kTestRemoteReplicaCount);
    }

    duplication_query_response query_dup_info(const std::string &app_name)
    {
        auto req = std::make_unique<duplication_query_request>();
        req->app_name = app_name;

        duplication_query_rpc rpc(std::move(req), RPC_CM_QUERY_DUPLICATION);
        dup_svc().query_duplication_info(rpc.request(), rpc.response());

        return rpc.response();
    }

    duplication_list_response list_dup_info(const std::string &app_name_pattern,
                                            utils::pattern_match_type::type match_type)
    {
        auto req = std::make_unique<duplication_list_request>();
        req->app_name_pattern = app_name_pattern;
        req->match_type = match_type;

        duplication_list_rpc rpc(std::move(req), RPC_CM_LIST_DUPLICATION);
        dup_svc().list_duplication_info(rpc.request(), rpc.response());

        return rpc.response();
    }

    duplication_modify_response
    change_dup_status(const std::string &app_name, dupid_t dupid, duplication_status::type status)
    {
        auto req = std::make_unique<duplication_modify_request>();
        req->dupid = dupid;
        req->app_name = app_name;
        req->__set_status(status);

        duplication_modify_rpc rpc(std::move(req), RPC_CM_MODIFY_DUPLICATION);
        dup_svc().modify_duplication(rpc);
        wait_all();

        return rpc.response();
    }

    duplication_modify_response
    update_fail_mode(const std::string &app_name, dupid_t dupid, duplication_fail_mode::type fmode)
    {
        auto req = std::make_unique<duplication_modify_request>();
        req->dupid = dupid;
        req->app_name = app_name;
        req->__set_fail_mode(fmode);

        duplication_modify_rpc rpc(std::move(req), RPC_CM_MODIFY_DUPLICATION);
        dup_svc().modify_duplication(rpc);
        wait_all();

        return rpc.response();
    }

    duplication_sync_response
    duplication_sync(const host_port &hp,
                     std::map<gpid, std::vector<duplication_confirm_entry>> confirm_list)
    {
        auto req = std::make_unique<duplication_sync_request>();
        SET_IP_AND_HOST_PORT_BY_DNS(*req, node, hp);
        req->confirm_list = confirm_list;

        duplication_sync_rpc rpc(std::move(req), RPC_CM_DUPLICATION_SYNC);
        dup_svc().duplication_sync(rpc);
        wait_all();

        return rpc.response();
    }

    void recover_from_meta_state()
    {
        dup_svc().recover_from_meta_state();
        wait_all();
    }

    void create_follower_app_for_duplication(const std::shared_ptr<duplication_info> &dup,
                                             const std::shared_ptr<app_state> &app)
    {
        dup_svc().create_follower_app_for_duplication(dup, app);
    }

    void mark_follower_app_created_for_duplication(const std::shared_ptr<duplication_info> &dup,
                                                   const std::shared_ptr<app_state> &app)
    {
        dup_svc().mark_follower_app_created_for_duplication(dup, app);
    }

    [[nodiscard]] static duplication_status::type
    next_status(const std::shared_ptr<duplication_info> &dup)
    {
        return dup->_next_status;
    }

    void force_update_dup_status(const std::shared_ptr<duplication_info> &dup,
                                 duplication_status::type to_status)
    {
        dup->_status = to_status;
    }

    /// === Tests ===

    void test_new_dup_from_init()
    {
        create_app(kTestAppName);
        auto app = find_app(kTestAppName);

        int last_dup = 0;
        for (int i = 0; i < 1000; i++) {
            auto dup = dup_svc().new_dup_from_init(
                kTestRemoteClusterName, kTestRemoteAppName, kTestRemoteReplicaCount, {}, app);

            ASSERT_GT(dup->id, 0);
            ASSERT_FALSE(dup->is_altering());
            ASSERT_EQ(duplication_status::DS_INIT, dup->_status);
            ASSERT_EQ(duplication_status::DS_INIT, dup->_next_status);

            const auto &entry = dup->to_partition_level_entry_for_sync();
            ASSERT_EQ(app->partition_count, entry.progress.size());
            for (int partition_index = 0; partition_index < app->partition_count;
                 ++partition_index) {
                ASSERT_TRUE(gutil::ContainsKey(entry.progress, partition_index));
                ASSERT_EQ(invalid_decree, gutil::FindOrDie(entry.progress, partition_index));
            }

            if (last_dup != 0) {
                ASSERT_GT(dup->id, last_dup);
            }
            last_dup = dup->id;
        }
    }

    void test_recover_from_meta_state()
    {
        size_t total_apps_num = 2;
        std::vector<std::string> test_apps(total_apps_num);

        // app -> <dupid -> dup>
        std::map<std::string, std::map<dupid_t, duplication_info_s_ptr>> app_to_duplications;

        for (int i = 0; i < total_apps_num; i++) {
            test_apps[i] = "test_app_" + std::to_string(i);
            create_app(test_apps[i]);

            auto resp = create_dup(test_apps[i]);
            ASSERT_EQ(ERR_OK, resp.err);

            auto app = find_app(test_apps[i]);
            app_to_duplications[test_apps[i]] = app->duplications;

            // update progress
            auto dup = app->duplications[resp.dupid];
            duplication_sync_rpc rpc(std::make_unique<duplication_sync_request>(),
                                     RPC_CM_DUPLICATION_SYNC);
            duplication_confirm_entry entry;
            entry.confirmed_decree = 1000;
            dup_svc().do_update_partition_confirmed(dup, rpc, 1, entry);
            wait_all();

            entry.confirmed_decree = 2000;
            dup_svc().do_update_partition_confirmed(dup, rpc, 2, entry);
            wait_all();

            entry.confirmed_decree = 1000;
            dup_svc().do_update_partition_confirmed(dup, rpc, 4, entry);
            wait_all();
        }

        // reset meta server states
        SetUp();

        recover_from_meta_state();

        for (int i = 0; i < test_apps.size(); i++) {
            auto app = find_app(test_apps[i]);
            ASSERT_EQ(app->duplicating, true);

            auto &before = app_to_duplications[test_apps[i]];
            auto &after = app->duplications;
            ASSERT_EQ(before.size(), after.size());

            for (auto &kv : before) {
                dupid_t dupid = kv.first;
                auto &dup = kv.second;

                ASSERT_TRUE(after.find(dupid) != after.end());
                ASSERT_TRUE(dup->equals_to(*after[dupid])) << *dup << std::endl << *after[dupid];
            }
        }
    }

    std::shared_ptr<app_state> mock_test_case_and_recover(std::vector<std::string> nodes,
                                                          std::string value)
    {
        TearDown();
        SetUp();

        create_app(kTestAppName);
        auto app = find_app(kTestAppName);

        std::queue<std::string> q_nodes;
        for (auto n : nodes) {
            q_nodes.push(std::move(n));
        }
        _ms->get_meta_storage()->create_node_recursively(
            std::move(q_nodes), blob::create_from_bytes(std::move(value)), []() mutable {});
        wait_all();

        SetUp();
        recover_from_meta_state();

        return find_app(kTestAppName);
    }

    // Corrupted meta data may result from bad write to meta-store.
    // This test ensures meta-server is still able to recover when
    // meta data is corrupted.
    void test_recover_from_corrupted_meta_data()
    {
        create_app(kTestAppName);
        auto app = find_app(kTestAppName);

        // recover from /<app>/dup
        app = mock_test_case_and_recover({_ss->get_app_path(*app), std::string("dup")}, "");
        ASSERT_FALSE(app->duplicating);
        ASSERT_TRUE(app->duplications.empty());

        // recover from /<app>/duplication/xxx/
        app = mock_test_case_and_recover({dup_svc().get_duplication_path(*app), std::string("xxx")},
                                         "");
        ASSERT_FALSE(app->duplicating);
        ASSERT_TRUE(app->duplications.empty());

        // recover from /<app>/duplication/123/, but its value is empty
        app = mock_test_case_and_recover({dup_svc().get_duplication_path(*app), std::string("123")},
                                         "");
        ASSERT_FALSE(app->duplicating);
        ASSERT_TRUE(app->duplications.empty());

        // recover from /<app>/duplication/<dup_id>/0, but its confirmed_decree is not valid integer
        TearDown();
        SetUp();
        create_app(kTestAppName);
        app = find_app(kTestAppName);
        auto test_dup = create_dup(kTestAppName, kTestRemoteClusterName, kTestRemoteReplicaCount);
        ASSERT_EQ(test_dup.err, ERR_OK);
        duplication_info_s_ptr dup = app->duplications[test_dup.dupid];
        _ms->get_meta_storage()->create_node(meta_duplication_service::get_partition_path(dup, "0"),
                                             blob::create_from_bytes("xxx"),
                                             []() mutable {});
        wait_all();
        SetUp();
        recover_from_meta_state();
        app = find_app(kTestAppName);
        ASSERT_TRUE(app->duplicating);
        ASSERT_EQ(app->duplications.size(), 1);
        for (int i = 0; i < app->partition_count; i++) {
            ASSERT_EQ(app->duplications[test_dup.dupid]->_progress[i].is_inited, i != 0);
        }

        // recover from /<app>/duplication/<dup_id>/x, its pid is not valid integer
        TearDown();
        SetUp();
        create_app(kTestAppName);
        app = find_app(kTestAppName);
        test_dup = create_dup(kTestAppName, kTestRemoteClusterName, kTestRemoteReplicaCount);
        ASSERT_EQ(test_dup.err, ERR_OK);
        dup = app->duplications[test_dup.dupid];
        _ms->get_meta_storage()->create_node(meta_duplication_service::get_partition_path(dup, "x"),
                                             blob::create_from_bytes("xxx"),
                                             []() mutable {});
        wait_all();
        SetUp();
        recover_from_meta_state();
        ASSERT_TRUE(app->duplicating);
        ASSERT_EQ(app->duplications.size(), 1);
        for (int i = 0; i < app->partition_count; i++) {
            ASSERT_EQ(app->duplications[test_dup.dupid]->_progress[i].is_inited, true);
        }
    }

    void test_add_duplication()
    {
        static const std::string kTestSameAppName(kTestAppName + "_same");
        static const std::string kTestAnotherAppName(kTestAppName + "_another");
        static const std::string kTestUnspecifiedAppName(kTestAppName + "_unspecified");

        create_app(kTestAppName);
        create_app(kTestSameAppName);
        create_app(kTestAnotherAppName);
        create_app(kTestUnspecifiedAppName);

        struct TestData
        {
            std::string app_name;
            std::string remote_cluster_name;

            bool specified;
            std::string remote_app_name;
            int32_t remote_replica_count;

            error_code wec;
        } tests[] = {
            // The general case that duplicating to remote cluster with specified remote_app_name.
            {kTestAppName,
             kTestRemoteClusterName,
             true,
             kTestRemoteAppName,
             kTestRemoteReplicaCount,
             ERR_OK},
            // Add a duplication that has been existing for the same table with the same remote
            // cluster.
            {kTestAppName,
             kTestRemoteClusterName,
             false,
             kTestRemoteAppName,
             kTestRemoteReplicaCount,
             ERR_DUP_EXIST},
            // The general case that duplicating to remote cluster with same remote_app_name.
            {kTestSameAppName,
             kTestRemoteClusterName,
             true,
             kTestSameAppName,
             kTestRemoteReplicaCount,
             ERR_OK},
            // It is not allowed that remote_cluster_name does not exist in "duplication-group".
            {kTestAppName,
             "test-invalid-remote",
             true,
             kTestRemoteAppName,
             kTestRemoteReplicaCount,
             ERR_INVALID_PARAMETERS},
            // Duplicating to local cluster is not allowed.
            {kTestAppName,
             get_current_dup_cluster_name(),
             true,
             kTestRemoteAppName,
             kTestRemoteReplicaCount,
             ERR_INVALID_PARAMETERS},
            // It is not allowed that remote_cluster_name exists in "duplication-group" but not
            // exists in "pegasus.clusters".
            {kTestAppName,
             "cluster_without_address_for_test",
             true,
             kTestRemoteAppName,
             kTestRemoteReplicaCount,
             ERR_INVALID_PARAMETERS},
            // The attempt that duplicates another app to the same remote app would be blocked.
            {kTestAnotherAppName,
             kTestRemoteClusterName,
             true,
             kTestRemoteAppName,
             kTestRemoteReplicaCount,
             ERR_INVALID_PARAMETERS},
            // The attempt that duplicates another app to the different remote app would be
            // ok.
            {kTestAnotherAppName,
             kTestRemoteClusterName,
             true,
             kTestAppName,
             kTestRemoteReplicaCount,
             ERR_OK},
            // Add a duplication without specifying remote_app_name and remote_replica_count.
            {kTestUnspecifiedAppName,
             kTestRemoteClusterName,
             false,
             kTestUnspecifiedAppName,
             3,
             ERR_OK},
        };

        for (auto test : tests) {
            duplication_add_response resp;
            if (test.specified) {
                resp = create_dup(test.app_name,
                                  test.remote_cluster_name,
                                  test.remote_app_name,
                                  test.remote_replica_count);
            } else {
                resp = create_dup_unspecified(test.app_name, test.remote_cluster_name);
            }

            ASSERT_EQ(test.wec, resp.err);

            if (test.wec != ERR_OK) {
                continue;
            }

            auto app = find_app(test.app_name);
            auto dup = app->duplications[resp.dupid];
            ASSERT_TRUE(dup != nullptr);
            ASSERT_EQ(app->app_id, dup->app_id);
            ASSERT_EQ(duplication_status::DS_PREPARE, dup->_status);
            ASSERT_EQ(test.remote_cluster_name, dup->remote_cluster_name);
            ASSERT_EQ(test.remote_app_name, resp.remote_app_name);
            ASSERT_EQ(test.remote_app_name, dup->remote_app_name);
            ASSERT_EQ(test.remote_replica_count, resp.remote_replica_count);
            ASSERT_EQ(test.remote_replica_count, dup->remote_replica_count);
            ASSERT_EQ(resp.dupid, dup->id);
            ASSERT_TRUE(app->duplicating);
        }
    }

    void test_list_dup_app_state(const std::string &app_name, const duplication_app_state &state)
    {
        const auto &app = find_app(app_name);

        // Each app id should be matched.
        ASSERT_EQ(app->app_id, state.appid);

        // The number of returned duplications for each table should be as expected.
        ASSERT_EQ(app->duplications.size(), state.duplications.size());

        for (const auto &[dup_id, dup] : app->duplications) {
            // Each dup id should be matched.
            ASSERT_TRUE(gutil::ContainsKey(state.duplications, dup_id));
            ASSERT_EQ(dup_id, gutil::FindOrDie(state.duplications, dup_id).dupid);

            // The number of returned partitions should be as expected.
            ASSERT_EQ(app->partition_count,
                      gutil::FindOrDie(state.duplications, dup_id).partition_states.size());
        }
    }

    void test_list_dup_info(const std::vector<std::string> &app_names,
                            const std::string &app_name_pattern,
                            utils::pattern_match_type::type match_type)
    {
        const auto &resp = list_dup_info(app_name_pattern, match_type);

        // Request for listing duplications should be successful.
        ASSERT_EQ(ERR_OK, resp.err);

        // The number of returned tables should be as expected.
        ASSERT_EQ(app_names.size(), resp.app_states.size());

        for (const auto &app_name : app_names) {
            // Each table name should be in the returned list.
            ASSERT_TRUE(gutil::ContainsKey(resp.app_states, app_name));

            // Test the states of each table.
            test_list_dup_app_state(app_name, gutil::FindOrDie(resp.app_states, app_name));
        }
    }
};

const std::string meta_duplication_service_test::kTestAppName = "test_app";
const std::string meta_duplication_service_test::kTestRemoteClusterName = "slave-cluster";
const std::string meta_duplication_service_test::kTestRemoteAppName = "remote_test_app";
const int32_t meta_duplication_service_test::kTestRemoteReplicaCount = 3;

// This test ensures that duplication upon an unavailable app will
// be rejected with ERR_APP_NOT_EXIST.
TEST_F(meta_duplication_service_test, dup_op_upon_unavail_app)
{
    const std::string test_app_not_exist = "test_app_not_exists";
    const std::string test_app_unavail = "test_app_unavail";

    create_app(kTestAppName);
    auto app = find_app(kTestAppName);
    ASSERT_EQ(kTestAppName, app->app_name);

    create_app(test_app_unavail);
    find_app(test_app_unavail)->status = app_status::AS_DROPPED;

    struct TestData
    {
        std::string app;
        error_code wec;
    } tests[] = {
        {test_app_not_exist, ERR_APP_NOT_EXIST},
        {test_app_unavail, ERR_APP_NOT_EXIST},
        {kTestAppName, ERR_OK},
    };

    for (auto test : tests) {
        const auto &resp = create_dup(test.app);
        ASSERT_EQ(test.wec, resp.err);

        ASSERT_EQ(test.wec, query_dup_info(test.app).err);

        // For the response with some error, `dupid` doesn't matter.
        dupid_t test_dup = test.wec == ERR_OK ? resp.dupid : static_cast<dupid_t>(dsn_now_s());
        ASSERT_EQ(test.wec,
                  change_dup_status(test.app, test_dup, duplication_status::DS_REMOVED).err);
    }
}

TEST_F(meta_duplication_service_test, add_duplication) { test_add_duplication(); }

// Ensure meta server never creates another dup to the same remote cluster and app,
// if there's already one existed.
TEST_F(meta_duplication_service_test, dont_create_if_existed)
{
    create_app(kTestAppName);
    auto app = find_app(kTestAppName);
    ASSERT_EQ(kTestAppName, app->app_name);

    create_dup(kTestAppName);
    create_dup(kTestAppName);
    dupid_t dupid = create_dup(kTestAppName).dupid;

    {
        auto resp = query_dup_info(kTestAppName);
        ASSERT_EQ(resp.err, ERR_OK);
        ASSERT_EQ(resp.entry_list.size(), 1);

        const auto &duplication_entry = resp.entry_list.back();
        ASSERT_EQ(duplication_entry.status, duplication_status::DS_PREPARE);
        ASSERT_EQ(duplication_entry.dupid, dupid);
    }
}

TEST_F(meta_duplication_service_test, add_dup_with_params)
{
    create_app(kTestAppName);
    auto app = find_app(kTestAppName);
    ASSERT_EQ(kTestAppName, app->app_name);

    const dupid_t dupid =
        create_dup(
            kTestAppName, kTestRemoteClusterName, kTestRemoteAppName, kTestRemoteReplicaCount)
            .dupid;

    const auto &resp = query_dup_info(kTestAppName);
    ASSERT_EQ(ERR_OK, resp.err);
    ASSERT_EQ(1, resp.entry_list.size());

    const auto &duplication_entry = resp.entry_list.back();
    ASSERT_EQ(dupid, duplication_entry.dupid);
    ASSERT_EQ(duplication_status::DS_PREPARE, duplication_entry.status);
    ASSERT_EQ(kTestRemoteAppName, duplication_entry.remote_app_name);
    ASSERT_EQ(kTestRemoteReplicaCount, duplication_entry.remote_replica_count);
}

TEST_F(meta_duplication_service_test, change_duplication_status)
{
    create_app(kTestAppName);
    auto app = find_app(kTestAppName);
    dupid_t test_dup = create_dup(kTestAppName).dupid;
    change_dup_status(kTestAppName, test_dup, duplication_status::DS_APP);
    change_dup_status(kTestAppName, test_dup, duplication_status::DS_LOG);

    struct TestData
    {
        std::string app;
        dupid_t dupid;
        duplication_status::type status;

        error_code wec;
    } tests[] = {
        {kTestAppName, test_dup + 1, duplication_status::DS_REMOVED, ERR_OBJECT_NOT_FOUND},

        // ok test
        {kTestAppName, test_dup, duplication_status::DS_PAUSE, ERR_OK}, // start->pause
        {kTestAppName, test_dup, duplication_status::DS_PAUSE, ERR_OK}, // pause->pause
        {kTestAppName, test_dup, duplication_status::DS_LOG, ERR_OK},   // pause->start
        {kTestAppName, test_dup, duplication_status::DS_LOG, ERR_OK},   // start->start
    };

    for (auto test : tests) {
        auto resp = change_dup_status(test.app, test.dupid, test.status);
        ASSERT_EQ(test.wec, resp.err);
    }
}

// this test ensures that dupid is always increment and larger than zero.
TEST_F(meta_duplication_service_test, new_dup_from_init) { test_new_dup_from_init(); }

TEST_F(meta_duplication_service_test, remove_dup)
{
    create_app(kTestAppName);
    auto app = find_app(kTestAppName);

    auto resp = create_dup(kTestAppName);
    ASSERT_EQ(ERR_OK, resp.err);
    dupid_t dupid1 = resp.dupid;

    ASSERT_EQ(app->duplicating, true);
    auto dup = app->duplications.find(dupid1)->second;

    auto resp2 = change_dup_status(kTestAppName, dupid1, duplication_status::DS_REMOVED);
    ASSERT_EQ(ERR_OK, resp2.err);
    // though this duplication is unreferenced, its status still updated correctly
    ASSERT_EQ(dup->status(), duplication_status::DS_REMOVED);

    ASSERT_EQ(app->duplicating, false);
    // ensure duplication removed
    ASSERT_EQ(app->duplications.find(dupid1), app->duplications.end());
    _ms->get_meta_storage()->get_children(std::string(dup->store_path),
                                          [](bool node_exists, const std::vector<std::string> &) {
                                              // ensure node cleaned up
                                              ASSERT_FALSE(node_exists);
                                          });

    // reset meta server states
    SetUp();
    recover_from_meta_state();

    ASSERT_EQ(app->duplicating, false);
    ASSERT_EQ(app->duplications.find(dupid1), app->duplications.end());
}

TEST_F(meta_duplication_service_test, duplication_sync)
{
    const auto &server_nodes = ensure_enough_alive_nodes(3);
    const std::string test_app("test_app_0");
    create_app(test_app);
    auto app = find_app(test_app);

    // Generate all primaries on node[0].
    for (auto &pc : app->pcs) {
        pc.ballot = random32(1, 10000);
        SET_IP_AND_HOST_PORT_BY_DNS(pc, primary, server_nodes[0]);
        SET_IPS_AND_HOST_PORTS_BY_DNS(pc, secondaries, server_nodes[1], server_nodes[2]);
    }

    initialize_node_state();

    const auto &node = server_nodes[0];
    const dupid_t dupid = create_dup(test_app).dupid;
    auto dup = app->duplications[dupid];
    for (int i = 0; i < app->partition_count; i++) {
        dup->init_progress(i, invalid_decree);
    }

    {
        std::map<gpid, std::vector<duplication_confirm_entry>> confirm_list;

        duplication_confirm_entry ce;
        ce.dupid = dupid;

        ce.confirmed_decree = 5;
        confirm_list[gpid(app->app_id, 1)].push_back(ce);

        ce.confirmed_decree = 6;
        confirm_list[gpid(app->app_id, 2)].push_back(ce);

        ce.confirmed_decree = 7;
        confirm_list[gpid(app->app_id, 3)].push_back(ce);

        duplication_sync_response resp = duplication_sync(node, confirm_list);
        ASSERT_EQ(ERR_OK, resp.err);
        ASSERT_EQ(1, resp.dup_map.size());
        ASSERT_EQ(1, resp.dup_map[app->app_id].size());
        ASSERT_EQ(dupid, resp.dup_map[app->app_id][dupid].dupid);
        ASSERT_EQ(duplication_status::DS_PREPARE, resp.dup_map[app->app_id][dupid].status);
        ASSERT_EQ(dup->create_timestamp_ms, resp.dup_map[app->app_id][dupid].create_ts);
        ASSERT_EQ(dup->remote_cluster_name, resp.dup_map[app->app_id][dupid].remote);
        ASSERT_EQ(dup->fail_mode(), resp.dup_map[app->app_id][dupid].fail_mode);

        auto progress_map = resp.dup_map[app->app_id][dupid].progress;
        ASSERT_EQ(8, progress_map.size());
        ASSERT_EQ(5, progress_map[1]);
        ASSERT_EQ(6, progress_map[2]);
        ASSERT_EQ(7, progress_map[3]);

        // ensure no updated progresses will also be included in response
        for (int p = 4; p < 8; p++) {
            ASSERT_EQ(progress_map[p], invalid_decree);
        }
        ASSERT_EQ(progress_map[0], invalid_decree);
    }

    { // duplication not existed will be ignored
        std::map<gpid, std::vector<duplication_confirm_entry>> confirm_list;

        duplication_confirm_entry ce;
        ce.dupid = dupid + 1; // not created
        ce.confirmed_decree = 5;
        confirm_list[gpid(app->app_id, 1)].push_back(ce);

        duplication_sync_response resp = duplication_sync(node, confirm_list);
        ASSERT_EQ(resp.err, ERR_OK);
        ASSERT_EQ(resp.dup_map.size(), 1);
        ASSERT_TRUE(resp.dup_map[app->app_id].find(dupid + 1) == resp.dup_map[app->app_id].end());
    }

    { // app not existed will be ignored
        std::map<gpid, std::vector<duplication_confirm_entry>> confirm_list;

        duplication_confirm_entry ce;
        ce.dupid = dupid;
        ce.confirmed_decree = 5;
        confirm_list[gpid(app->app_id + 1, 1)].push_back(ce);

        const auto resp = duplication_sync(node, confirm_list);
        ASSERT_EQ(resp.err, ERR_OK);
        ASSERT_EQ(resp.dup_map.size(), 1);
        ASSERT_TRUE(resp.dup_map.find(app->app_id + 1) == resp.dup_map.end());
    }

    { // duplication removed will be ignored
        change_dup_status(test_app, dupid, duplication_status::DS_REMOVED);

        std::map<gpid, std::vector<duplication_confirm_entry>> confirm_list;

        duplication_confirm_entry ce;
        ce.dupid = dupid;
        ce.confirmed_decree = 5;
        confirm_list[gpid(app->app_id, 1)].push_back(ce);

        const auto resp = duplication_sync(node, confirm_list);
        ASSERT_EQ(resp.err, ERR_OK);
        ASSERT_EQ(resp.dup_map.size(), 0);
    }
}

// This test ensures that duplications persisted on meta storage can be
// correctly restored.
TEST_F(meta_duplication_service_test, recover_from_meta_state) { test_recover_from_meta_state(); }

TEST_F(meta_duplication_service_test, query_duplication_info)
{
    create_app(kTestAppName);
    auto app = find_app(kTestAppName);

    dupid_t test_dup = create_dup(kTestAppName).dupid;
    change_dup_status(kTestAppName, test_dup, duplication_status::DS_PAUSE);

    auto resp = query_dup_info(kTestAppName);
    ASSERT_EQ(ERR_OK, resp.err);
    ASSERT_EQ(1, resp.entry_list.size());
    ASSERT_EQ(duplication_status::DS_PREPARE, resp.entry_list.back().status);
    ASSERT_EQ(test_dup, resp.entry_list.back().dupid);
    ASSERT_EQ(app->app_id, resp.appid);

    change_dup_status(kTestAppName, test_dup, duplication_status::DS_REMOVED);
    resp = query_dup_info(kTestAppName);
    ASSERT_EQ(ERR_OK, resp.err);
    ASSERT_TRUE(resp.entry_list.empty());
}

TEST_F(meta_duplication_service_test, list_duplication_info)
{
    // Remove all tables from memory to prevent later tests from interference.
    clear_apps();

    // Create some tables with some partitions and duplications randomly.
    create_app(kTestAppName, 8);
    create_dup(kTestAppName);
    create_dup(kTestAppName);

    std::string app_name_1("test_list_dup");
    create_app(app_name_1, 4);
    create_dup(app_name_1);

    std::string app_name_2("list_apps_test");
    create_app(app_name_2, 8);

    std::string app_name_3("test_dup_list");
    create_app(app_name_3, 8);
    create_dup(app_name_3);
    create_dup(app_name_3);
    create_dup(app_name_3);

    // Test for returning all of the existing tables.
    test_list_dup_info({kTestAppName, app_name_1, app_name_2, app_name_3},
                       {},
                       utils::pattern_match_type::PMT_MATCH_ALL);

    // Test for returning the tables whose name are matched exactly.
    test_list_dup_info({app_name_2}, app_name_2, utils::pattern_match_type::PMT_MATCH_EXACT);

    // Test for returning the tables whose name are matched with pattern anywhere.
    test_list_dup_info(
        {kTestAppName, app_name_2}, "app", utils::pattern_match_type::PMT_MATCH_ANYWHERE);

    // Test for returning the tables whose name are matched with pattern as prefix.
    test_list_dup_info({kTestAppName, app_name_1, app_name_3},
                       "test",
                       utils::pattern_match_type::PMT_MATCH_PREFIX);

    // Test for returning the tables whose name are matched with pattern as postfix.
    test_list_dup_info({app_name_2}, "test", utils::pattern_match_type::PMT_MATCH_POSTFIX);
}

TEST_F(meta_duplication_service_test, re_add_duplication)
{
    create_app(kTestAppName);
    auto app = find_app(kTestAppName);

    auto test_dup = create_dup(kTestAppName);
    ASSERT_EQ(test_dup.err, ERR_OK);
    ASSERT_TRUE(app->duplications[test_dup.dupid] != nullptr);
    auto resp = change_dup_status(kTestAppName, test_dup.dupid, duplication_status::DS_REMOVED);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_TRUE(app->duplications.find(test_dup.dupid) == app->duplications.end());

    sleep(1);

    auto test_dup_2 = create_dup(kTestAppName);
    ASSERT_EQ(test_dup_2.appid, app->app_id);
    ASSERT_EQ(test_dup_2.err, ERR_OK);

    // once duplication is removed, all its state is not valid anymore.
    ASSERT_EQ(app->duplications.size(), 1);
    ASSERT_NE(test_dup.dupid, test_dup_2.dupid);

    auto dup_list = query_dup_info(kTestAppName).entry_list;
    ASSERT_EQ(dup_list.size(), 1);
    ASSERT_EQ(dup_list.begin()->status, duplication_status::DS_PREPARE);
    ASSERT_EQ(dup_list.begin()->dupid, test_dup_2.dupid);

    // reset meta server states
    SetUp();

    recover_from_meta_state();
    app = find_app(kTestAppName);
    ASSERT_TRUE(app->duplications.find(test_dup.dupid) == app->duplications.end());
    ASSERT_EQ(app->duplications.size(), 1);
}

TEST_F(meta_duplication_service_test, recover_from_corrupted_meta_data)
{
    test_recover_from_corrupted_meta_data();
}

TEST_F(meta_duplication_service_test, query_duplication_handler)
{
    create_app(kTestAppName);
    create_dup(kTestAppName, kTestRemoteClusterName, kTestRemoteAppName, kTestRemoteReplicaCount);
    meta_http_service mhs(_ms.get());

    http_request fake_req;
    http_response fake_resp;
    fake_req.query_args["name"] = kTestAppName + "not_found";
    mhs.query_duplication_handler(fake_req, fake_resp);
    ASSERT_EQ(fake_resp.status_code, http_status_code::kNotFound);

    const auto &duplications = find_app(kTestAppName)->duplications;
    ASSERT_EQ(1, duplications.size());
    auto dup = duplications.begin()->second;

    fake_req.query_args["name"] = kTestAppName;
    mhs.query_duplication_handler(fake_req, fake_resp);
    ASSERT_EQ(http_status_code::kOk, fake_resp.status_code);
    char ts_buf[32];
    utils::time_ms_to_date_time(
        static_cast<uint64_t>(dup->create_timestamp_ms), ts_buf, sizeof(ts_buf));
    ASSERT_EQ(std::string() + R"({"1":{"create_ts":")" + ts_buf + R"(","dupid":)" +
                  std::to_string(dup->id) +
                  R"(,"fail_mode":"FAIL_SLOW","partition_states":{)"
                  R"("0":{"confirmed_decree":-1,"last_committed_decree":-1},)"
                  R"("1":{"confirmed_decree":-1,"last_committed_decree":-1},)"
                  R"("2":{"confirmed_decree":-1,"last_committed_decree":-1},)"
                  R"("3":{"confirmed_decree":-1,"last_committed_decree":-1},)"
                  R"("4":{"confirmed_decree":-1,"last_committed_decree":-1},)"
                  R"("5":{"confirmed_decree":-1,"last_committed_decree":-1},)"
                  R"("6":{"confirmed_decree":-1,"last_committed_decree":-1},)"
                  R"("7":{"confirmed_decree":-1,"last_committed_decree":-1})"
                  R"(},"remote":"slave-cluster","remote_app_name":"remote_test_app",)"
                  R"("remote_replica_count":3,"status":"DS_PREPARE"},"appid":2})",
              fake_resp.body);
}

TEST_F(meta_duplication_service_test, fail_mode)
{
    create_app(kTestAppName);
    auto app = find_app(kTestAppName);

    auto dup_add_resp = create_dup(kTestAppName);
    auto dup = app->duplications[dup_add_resp.dupid];
    ASSERT_EQ(dup->fail_mode(), duplication_fail_mode::FAIL_SLOW);
    ASSERT_EQ(dup->status(), duplication_status::DS_PREPARE);

    auto resp = update_fail_mode(kTestAppName, dup->id, duplication_fail_mode::FAIL_SKIP);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(dup->fail_mode(), duplication_fail_mode::FAIL_SKIP);
    ASSERT_EQ(dup->status(), duplication_status::DS_PREPARE);

    // change nothing
    resp = update_fail_mode(kTestAppName, dup->id, duplication_fail_mode::FAIL_SKIP);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(dup->fail_mode(), duplication_fail_mode::FAIL_SKIP);
    ASSERT_EQ(dup->status(), duplication_status::DS_PREPARE);

    // change status but fail mode not changed
    change_dup_status(kTestAppName, dup->id, duplication_status::DS_APP);
    change_dup_status(kTestAppName, dup->id, duplication_status::DS_LOG);
    resp = change_dup_status(kTestAppName, dup->id, duplication_status::DS_PAUSE);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(dup->fail_mode(), duplication_fail_mode::FAIL_SKIP);
    ASSERT_EQ(dup->status(), duplication_status::DS_PAUSE);

    // ensure dup_sync will synchronize fail_mode
    const auto hp = generate_node_list(3)[0];
    for (auto &pc : app->pcs) {
        SET_IP_AND_HOST_PORT_BY_DNS(pc, primary, hp);
    }
    initialize_node_state();
    auto sync_resp = duplication_sync(hp, {});
    ASSERT_TRUE(sync_resp.dup_map[app->app_id][dup->id].__isset.fail_mode);
    ASSERT_EQ(sync_resp.dup_map[app->app_id][dup->id].fail_mode, duplication_fail_mode::FAIL_SKIP);

    // ensure recovery will not lose fail_mode.
    SetUp();
    recover_from_meta_state();
    app = find_app(kTestAppName);
    dup = app->duplications[dup->id];
    ASSERT_EQ(dup->fail_mode(), duplication_fail_mode::FAIL_SKIP);
}

TEST_F(meta_duplication_service_test, create_follower_app_for_duplication)
{
    struct test_case
    {
        std::string fail_cfg_name;
        std::string fail_cfg_action;
        bool is_altering;
        duplication_status::type cur_status;
        duplication_status::type next_status;
    } test_cases[] = {{"update_app_request_ok",
                       "void()",
                       false,
                       duplication_status::DS_APP,
                       duplication_status::DS_INIT},
                      // the case just `palace holder`, actually
                      // `trigger_follower_duplicate_checkpoint` is failed by default in unit test
                      {"update_dup_status_failed",
                       "off()",
                       false,
                       duplication_status::DS_PREPARE,
                       duplication_status::DS_INIT},
                      {"persist_dup_status_failed",
                       "return()",
                       true,
                       duplication_status::DS_PREPARE,
                       duplication_status::DS_APP}};

    for (const auto &test : test_cases) {
        const auto test_app = test.fail_cfg_name;
        create_app(test_app);
        auto app = find_app(test_app);

        auto dup_add_resp = create_dup(test_app);
        auto dup = app->duplications[dup_add_resp.dupid];

        fail::setup();
        fail::cfg(test.fail_cfg_name, test.fail_cfg_action);
        create_follower_app_for_duplication(dup, app);
        wait_all();
        fail::teardown();
        ASSERT_EQ(dup->is_altering(), test.is_altering);
        ASSERT_EQ(next_status(dup), test.next_status);
        ASSERT_EQ(dup->status(), test.cur_status);
    }
}

TEST_F(meta_duplication_service_test, mark_follower_app_created_for_duplication)
{
    struct test_case
    {
        std::vector<std::string> fail_cfg_name;
        std::vector<std::string> fail_cfg_action;
        int32_t remote_replica_count;
        duplication_status::type cur_status;
        duplication_status::type next_status;
        bool is_altering;
    } test_cases[] = {// 3 remote replicas with both primary and secondaries valid.
                      {{"on_follower_app_created", "create_app_ok"},
                       {"void(ERR_OK)", "void(true,2,0)"},
                       3,
                       duplication_status::DS_LOG,
                       duplication_status::DS_INIT,
                       false},
                      // The follower app failed to be marked as created.
                      {{"on_follower_app_created", "create_app_ok"},
                       {"void(ERR_TIMEOUT)", "void(true,2,0)"},
                       3,
                       duplication_status::DS_APP,
                       duplication_status::DS_INIT,
                       false},
                      //
                      // 3 remote replicas with primary invalid and all secondaries valid.
                      {{"on_follower_app_created", "create_app_ok"},
                       {"void(ERR_OK)", "void(false,2,0)"},
                       3,
                       duplication_status::DS_APP,
                       duplication_status::DS_INIT,
                       false},
                      // 3 remote replicas with primary valid and only one secondary present
                      // and valid.
                      {{"on_follower_app_created", "create_app_ok"},
                       {"void(ERR_OK)", "void(true,1,0)"},
                       3,
                       duplication_status::DS_LOG,
                       duplication_status::DS_INIT,
                       false},
                      // 3 remote replicas with primary valid and one secondary invalid.
                      {{"on_follower_app_created", "create_app_ok"},
                       {"void(ERR_OK)", "void(true,1,1)"},
                       3,
                       duplication_status::DS_APP,
                       duplication_status::DS_INIT,
                       false},
                      // 3 remote replicas with primary valid and only one secondary present
                      // and invalid.
                      {{"on_follower_app_created", "create_app_ok"},
                       {"void(ERR_OK)", "void(true,0,1)"},
                       3,
                       duplication_status::DS_APP,
                       duplication_status::DS_INIT,
                       false},
                      // 3 remote replicas with primary valid and both secondaries absent.
                      {
                          {"on_follower_app_created", "create_app_ok"},
                          {"void(ERR_OK)", "void(true,0,0)"},
                          3,
                          duplication_status::DS_APP,
                          duplication_status::DS_INIT,
                          false,
                      },
                      // 1 remote replicas with primary valid.
                      {
                          {"on_follower_app_created", "create_app_ok"},
                          {"void(ERR_OK)", "void(true,0,0)"},
                          1,
                          duplication_status::DS_LOG,
                          duplication_status::DS_INIT,
                          false,
                      },
                      // 1 remote replicas with primary invalid.
                      {
                          {"on_follower_app_created", "create_app_ok"},
                          {"void(ERR_OK)", "void(false,0,0)"},
                          1,
                          duplication_status::DS_APP,
                          duplication_status::DS_INIT,
                          false,
                      },
                      // The case is just a "palace holder", actually
                      // `check_follower_app_if_create_completed` would fail by default
                      // in unit test.
                      {{"on_follower_app_created", "create_app_failed"},
                       {"void(ERR_OK)", "off()"},
                       3,
                       duplication_status::DS_APP,
                       duplication_status::DS_INIT,
                       false},
                      {{"on_follower_app_created", "create_app_ok", "persist_dup_status_failed"},
                       {"void(ERR_OK)", "void(true,2,0)", "return()"},
                       3,
                       duplication_status::DS_APP,
                       duplication_status::DS_LOG,
                       true}};

    size_t i = 0;
    for (const auto &test : test_cases) {
        const auto &app_name =
            fmt::format("mark_follower_app_created_for_duplication_test_{}", i++);
        create_app(app_name);

        auto app = find_app(app_name);
        auto dup_add_resp = create_dup(app_name, test.remote_replica_count);
        auto dup = app->duplications[dup_add_resp.dupid];

        // 'mark_follower_app_created_for_duplication' must execute under
        // duplication_status::DS_APP, so force update it.
        force_update_dup_status(dup, duplication_status::DS_APP);

        fail::setup();
        for (int i = 0; i < test.fail_cfg_name.size(); i++) {
            fail::cfg(test.fail_cfg_name[i], test.fail_cfg_action[i]);
        }
        mark_follower_app_created_for_duplication(dup, app);
        wait_all();
        fail::teardown();

        ASSERT_EQ(dup->is_altering(), test.is_altering);
        ASSERT_EQ(next_status(dup), test.next_status);
        ASSERT_EQ(dup->status(), test.cur_status);
    }
}

} // namespace dsn::replication
