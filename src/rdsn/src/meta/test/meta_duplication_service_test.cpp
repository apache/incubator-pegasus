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
#include "utils/fmt_logging.h"
#include "common/common.h"
#include "utils/time_utils.h"

#include "meta/server_load_balancer.h"
#include "meta/meta_server_failure_detector.h"
#include "meta/meta_http_service.h"
#include "meta/duplication/meta_duplication_service.h"
#include "meta/test/misc/misc.h"

#include "meta_service_test_app.h"
#include "meta_test_base.h"
#include "utils/fail_point.h"

namespace dsn {
namespace replication {

class meta_duplication_service_test : public meta_test_base
{
public:
    meta_duplication_service_test() {}

    duplication_add_response create_dup(const std::string &app_name,
                                        const std::string &remote_cluster = "slave-cluster",
                                        bool freezed = false)
    {
        auto req = make_unique<duplication_add_request>();
        req->app_name = app_name;
        req->remote_cluster_name = remote_cluster;

        duplication_add_rpc rpc(std::move(req), RPC_CM_ADD_DUPLICATION);
        dup_svc().add_duplication(rpc);
        wait_all();
        return rpc.response();
    }

    duplication_query_response query_dup_info(const std::string &app_name)
    {
        auto req = make_unique<duplication_query_request>();
        req->app_name = app_name;

        duplication_query_rpc rpc(std::move(req), RPC_CM_QUERY_DUPLICATION);
        dup_svc().query_duplication_info(rpc.request(), rpc.response());

        return rpc.response();
    }

    duplication_modify_response
    change_dup_status(const std::string &app_name, dupid_t dupid, duplication_status::type status)
    {
        auto req = make_unique<duplication_modify_request>();
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
        auto req = make_unique<duplication_modify_request>();
        req->dupid = dupid;
        req->app_name = app_name;
        req->__set_fail_mode(fmode);

        duplication_modify_rpc rpc(std::move(req), RPC_CM_MODIFY_DUPLICATION);
        dup_svc().modify_duplication(rpc);
        wait_all();

        return rpc.response();
    }

    duplication_sync_response
    duplication_sync(const rpc_address &node,
                     std::map<gpid, std::vector<duplication_confirm_entry>> confirm_list)
    {
        auto req = make_unique<duplication_sync_request>();
        req->node = node;
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

    void check_follower_app_if_create_completed(const std::shared_ptr<duplication_info> &dup)
    {
        dup_svc().check_follower_app_if_create_completed(dup);
    }

    duplication_status::type next_status(const std::shared_ptr<duplication_info> &dup) const
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
        std::string test_app = "test-app";
        create_app(test_app);
        auto app = find_app(test_app);
        std::string remote_cluster_address = "dsn://slave-cluster/temp";

        int last_dup = 0;
        for (int i = 0; i < 1000; i++) {
            auto dup = dup_svc().new_dup_from_init(
                remote_cluster_address, std::vector<rpc_address>(), app);

            ASSERT_GT(dup->id, 0);
            ASSERT_FALSE(dup->is_altering());
            ASSERT_EQ(dup->_status, duplication_status::DS_INIT);
            ASSERT_EQ(dup->_next_status, duplication_status::DS_INIT);

            auto ent = dup->to_duplication_entry();
            for (int j = 0; j < app->partition_count; j++) {
                ASSERT_EQ(ent.progress[j], invalid_decree);
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
            duplication_sync_rpc rpc(make_unique<duplication_sync_request>(),
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
                ASSERT_TRUE(dup->equals_to(*after[dupid])) << dup->to_string() << std::endl
                                                           << after[dupid]->to_string();
            }
        }
    }

    std::shared_ptr<app_state> mock_test_case_and_recover(std::vector<std::string> nodes,
                                                          std::string value)
    {
        TearDown();
        SetUp();

        std::string test_app = "test-app";
        create_app(test_app);
        auto app = find_app(test_app);
        std::string remote_cluster_address = "dsn://slave-cluster/temp";

        std::queue<std::string> q_nodes;
        for (auto n : nodes) {
            q_nodes.push(std::move(n));
        }
        _ms->get_meta_storage()->create_node_recursively(
            std::move(q_nodes), blob::create_from_bytes(std::move(value)), []() mutable {});
        wait_all();

        SetUp();
        recover_from_meta_state();

        return find_app(test_app);
    }

    // Corrupted meta data may result from bad write to meta-store.
    // This test ensures meta-server is still able to recover when
    // meta data is corrupted.
    void test_recover_from_corrupted_meta_data()
    {
        std::string test_app = "test-app";
        create_app(test_app);
        auto app = find_app(test_app);

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
        create_app(test_app);
        app = find_app(test_app);
        auto test_dup = create_dup(test_app, "slave-cluster", true);
        ASSERT_EQ(test_dup.err, ERR_OK);
        duplication_info_s_ptr dup = app->duplications[test_dup.dupid];
        _ms->get_meta_storage()->create_node(meta_duplication_service::get_partition_path(dup, "0"),
                                             blob::create_from_bytes("xxx"),
                                             []() mutable {});
        wait_all();
        SetUp();
        recover_from_meta_state();
        app = find_app(test_app);
        ASSERT_TRUE(app->duplicating);
        ASSERT_EQ(app->duplications.size(), 1);
        for (int i = 0; i < app->partition_count; i++) {
            ASSERT_EQ(app->duplications[test_dup.dupid]->_progress[i].is_inited, i != 0);
        }

        // recover from /<app>/duplication/<dup_id>/x, its pid is not valid integer
        TearDown();
        SetUp();
        create_app(test_app);
        app = find_app(test_app);
        test_dup = create_dup(test_app, "slave-cluster", true);
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
        std::string test_app = "test-app";
        std::string test_app_invalid_ver = "test-app-invalid-ver";

        std::string invalid_remote = "test-invalid-remote";
        std::string ok_remote = "slave-cluster";

        std::string cluster_without_address = "cluster_without_address_for_test";

        create_app(test_app);

        create_app(test_app_invalid_ver);
        find_app(test_app_invalid_ver)->envs["value_version"] = "0";

        struct TestData
        {
            std::string app;
            std::string remote;

            error_code wec;
        } tests[] = {
            //        {test_app_invalid_ver, ok_remote, ERR_INVALID_VERSION},

            {test_app, ok_remote, ERR_OK},

            {test_app, invalid_remote, ERR_INVALID_PARAMETERS},

            {test_app, get_current_cluster_name(), ERR_INVALID_PARAMETERS},

            {test_app, cluster_without_address, ERR_INVALID_PARAMETERS},
        };

        for (auto tt : tests) {
            auto resp = create_dup(tt.app, tt.remote);
            ASSERT_EQ(tt.wec, resp.err);

            if (tt.wec == ERR_OK) {
                auto app = find_app(test_app);
                auto dup = app->duplications[resp.dupid];
                ASSERT_TRUE(dup != nullptr);
                ASSERT_EQ(dup->app_id, app->app_id);
                ASSERT_EQ(dup->_status, duplication_status::DS_PREPARE);
                ASSERT_EQ(dup->follower_cluster_name, ok_remote);
                ASSERT_EQ(resp.dupid, dup->id);
                ASSERT_EQ(app->duplicating, true);
            }
        }
    }
};

// This test ensures that duplication upon an unavailable app will
// be rejected with ERR_APP_NOT_EXIST.
TEST_F(meta_duplication_service_test, dup_op_upon_unavail_app)
{
    std::string test_app = "test-app";
    std::string test_app_not_exist = "test-app-not-exists";
    std::string test_app_unavail = "test-app-unavail";

    create_app(test_app);
    auto app = find_app(test_app);

    create_app(test_app_unavail);
    find_app(test_app_unavail)->status = app_status::AS_DROPPED;

    dupid_t test_dup = create_dup(test_app).dupid;

    struct TestData
    {
        std::string app;

        error_code wec;
    } tests[] = {
        {test_app_not_exist, ERR_APP_NOT_EXIST},
        {test_app_unavail, ERR_APP_NOT_EXIST},

        {test_app, ERR_OK},
    };

    for (auto tt : tests) {
        ASSERT_EQ(query_dup_info(tt.app).err, tt.wec);
        ASSERT_EQ(create_dup(tt.app).err, tt.wec);
        ASSERT_EQ(change_dup_status(tt.app, test_dup, duplication_status::DS_REMOVED).err, tt.wec);
    }
}

TEST_F(meta_duplication_service_test, add_duplication) { test_add_duplication(); }

// Ensure meta server never creates another dup to the same remote cluster and app,
// if there's already one existed.
TEST_F(meta_duplication_service_test, dont_create_if_existed)
{
    std::string test_app = "test-app";

    create_app(test_app);
    auto app = find_app(test_app);

    create_dup(test_app);
    create_dup(test_app);
    dupid_t dupid = create_dup(test_app).dupid;

    {
        auto resp = query_dup_info(test_app);
        ASSERT_EQ(resp.err, ERR_OK);
        ASSERT_EQ(resp.entry_list.size(), 1);

        const auto &duplication_entry = resp.entry_list.back();
        ASSERT_EQ(duplication_entry.status, duplication_status::DS_PREPARE);
        ASSERT_EQ(duplication_entry.dupid, dupid);
    }
}

TEST_F(meta_duplication_service_test, change_duplication_status)
{
    std::string test_app = "test-app";

    create_app(test_app);
    auto app = find_app(test_app);
    dupid_t test_dup = create_dup(test_app).dupid;
    change_dup_status(test_app, test_dup, duplication_status::DS_APP);
    change_dup_status(test_app, test_dup, duplication_status::DS_LOG);

    struct TestData
    {
        std::string app;
        dupid_t dupid;
        duplication_status::type status;

        error_code wec;
    } tests[] = {
        {test_app, test_dup + 1, duplication_status::DS_REMOVED, ERR_OBJECT_NOT_FOUND},

        // ok test
        {test_app, test_dup, duplication_status::DS_PAUSE, ERR_OK}, // start->pause
        {test_app, test_dup, duplication_status::DS_PAUSE, ERR_OK}, // pause->pause
        {test_app, test_dup, duplication_status::DS_LOG, ERR_OK},   // pause->start
        {test_app, test_dup, duplication_status::DS_LOG, ERR_OK},   // start->start
    };

    for (auto tt : tests) {
        auto resp = change_dup_status(tt.app, tt.dupid, tt.status);
        ASSERT_EQ(resp.err, tt.wec);
    }
}

// this test ensures that dupid is always increment and larger than zero.
TEST_F(meta_duplication_service_test, new_dup_from_init) { test_new_dup_from_init(); }

TEST_F(meta_duplication_service_test, remove_dup)
{
    std::string test_app = "test-app";
    create_app(test_app);
    auto app = find_app(test_app);

    auto resp = create_dup(test_app);
    ASSERT_EQ(ERR_OK, resp.err);
    dupid_t dupid1 = resp.dupid;

    ASSERT_EQ(app->duplicating, true);
    auto dup = app->duplications.find(dupid1)->second;

    auto resp2 = change_dup_status(test_app, dupid1, duplication_status::DS_REMOVED);
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
    std::vector<rpc_address> server_nodes = ensure_enough_alive_nodes(3);
    rpc_address node = server_nodes[0];

    std::string test_app = "test_app_0";
    create_app(test_app);
    auto app = find_app(test_app);

    // generate all primaries on node[0]
    for (partition_configuration &pc : app->partitions) {
        pc.ballot = random32(1, 10000);
        pc.primary = server_nodes[0];
        pc.secondaries.push_back(server_nodes[1]);
        pc.secondaries.push_back(server_nodes[2]);
    }

    initialize_node_state();

    dupid_t dupid = create_dup(test_app).dupid;
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
        ASSERT_EQ(resp.err, ERR_OK);
        ASSERT_EQ(resp.dup_map.size(), 1);
        ASSERT_EQ(resp.dup_map[app->app_id].size(), 1);
        ASSERT_EQ(resp.dup_map[app->app_id][dupid].dupid, dupid);
        ASSERT_EQ(resp.dup_map[app->app_id][dupid].status, duplication_status::DS_PREPARE);
        ASSERT_EQ(resp.dup_map[app->app_id][dupid].create_ts, dup->create_timestamp_ms);
        ASSERT_EQ(resp.dup_map[app->app_id][dupid].remote, dup->follower_cluster_name);
        ASSERT_EQ(resp.dup_map[app->app_id][dupid].fail_mode, dup->fail_mode());

        auto progress_map = resp.dup_map[app->app_id][dupid].progress;
        ASSERT_EQ(progress_map.size(), 8);
        ASSERT_EQ(progress_map[1], 5);
        ASSERT_EQ(progress_map[2], 6);
        ASSERT_EQ(progress_map[3], 7);

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

        duplication_sync_response resp = duplication_sync(node, confirm_list);
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

        duplication_sync_response resp = duplication_sync(node, confirm_list);
        ASSERT_EQ(resp.err, ERR_OK);
        ASSERT_EQ(resp.dup_map.size(), 0);
    }
}

// This test ensures that duplications persisted on meta storage can be
// correctly restored.
TEST_F(meta_duplication_service_test, recover_from_meta_state) { test_recover_from_meta_state(); }

TEST_F(meta_duplication_service_test, query_duplication_info)
{
    std::string test_app = "test-app";

    create_app(test_app);
    auto app = find_app(test_app);

    dupid_t test_dup = create_dup(test_app).dupid;
    change_dup_status(test_app, test_dup, duplication_status::DS_PAUSE);

    auto resp = query_dup_info(test_app);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.entry_list.size(), 1);
    ASSERT_EQ(resp.entry_list.back().status, duplication_status::DS_PREPARE);
    ASSERT_EQ(resp.entry_list.back().dupid, test_dup);
    ASSERT_EQ(resp.appid, app->app_id);

    change_dup_status(test_app, test_dup, duplication_status::DS_REMOVED);
    resp = query_dup_info(test_app);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.entry_list.size(), 0);
}

TEST_F(meta_duplication_service_test, re_add_duplication)
{
    std::string test_app = "test-app";

    create_app(test_app);
    auto app = find_app(test_app);

    auto test_dup = create_dup(test_app);
    ASSERT_EQ(test_dup.err, ERR_OK);
    ASSERT_TRUE(app->duplications[test_dup.dupid] != nullptr);
    auto resp = change_dup_status(test_app, test_dup.dupid, duplication_status::DS_REMOVED);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_TRUE(app->duplications.find(test_dup.dupid) == app->duplications.end());

    sleep(1);

    auto test_dup_2 = create_dup(test_app);
    ASSERT_EQ(test_dup_2.appid, app->app_id);
    ASSERT_EQ(test_dup_2.err, ERR_OK);

    // once duplication is removed, all its state is not valid anymore.
    ASSERT_EQ(app->duplications.size(), 1);
    ASSERT_NE(test_dup.dupid, test_dup_2.dupid);

    auto dup_list = query_dup_info(test_app).entry_list;
    ASSERT_EQ(dup_list.size(), 1);
    ASSERT_EQ(dup_list.begin()->status, duplication_status::DS_PREPARE);
    ASSERT_EQ(dup_list.begin()->dupid, test_dup_2.dupid);

    // reset meta server states
    SetUp();

    recover_from_meta_state();
    app = find_app(test_app);
    ASSERT_TRUE(app->duplications.find(test_dup.dupid) == app->duplications.end());
    ASSERT_EQ(app->duplications.size(), 1);
}

TEST_F(meta_duplication_service_test, recover_from_corrupted_meta_data)
{
    test_recover_from_corrupted_meta_data();
}

TEST_F(meta_duplication_service_test, query_duplication_handler)
{
    std::string test_app = "test-app";
    create_app(test_app);
    create_dup(test_app);
    meta_http_service mhs(_ms.get());

    http_request fake_req;
    http_response fake_resp;
    fake_req.query_args["name"] = test_app + "not-found";
    mhs.query_duplication_handler(fake_req, fake_resp);
    ASSERT_EQ(fake_resp.status_code, http_status_code::not_found);

    const auto &duplications = find_app(test_app)->duplications;
    ASSERT_EQ(duplications.size(), 1);
    auto dup = duplications.begin()->second;

    fake_req.query_args["name"] = test_app;
    mhs.query_duplication_handler(fake_req, fake_resp);
    ASSERT_EQ(fake_resp.status_code, http_status_code::ok);
    char ts_buf[32];
    utils::time_ms_to_date_time(
        static_cast<uint64_t>(dup->create_timestamp_ms), ts_buf, sizeof(ts_buf));
    ASSERT_EQ(fake_resp.body,
              std::string() + R"({"1":{"create_ts":")" + ts_buf + R"(","dupid":)" +
                  std::to_string(dup->id) +
                  R"(,"fail_mode":"FAIL_SLOW")"
                  R"(,"remote":"slave-cluster","status":"DS_PREPARE"},"appid":2})");
}

TEST_F(meta_duplication_service_test, fail_mode)
{
    std::string test_app = "test-app";
    create_app(test_app);
    auto app = find_app(test_app);

    auto dup_add_resp = create_dup(test_app);
    auto dup = app->duplications[dup_add_resp.dupid];
    ASSERT_EQ(dup->fail_mode(), duplication_fail_mode::FAIL_SLOW);
    ASSERT_EQ(dup->status(), duplication_status::DS_PREPARE);

    auto resp = update_fail_mode(test_app, dup->id, duplication_fail_mode::FAIL_SKIP);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(dup->fail_mode(), duplication_fail_mode::FAIL_SKIP);
    ASSERT_EQ(dup->status(), duplication_status::DS_PREPARE);

    // change nothing
    resp = update_fail_mode(test_app, dup->id, duplication_fail_mode::FAIL_SKIP);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(dup->fail_mode(), duplication_fail_mode::FAIL_SKIP);
    ASSERT_EQ(dup->status(), duplication_status::DS_PREPARE);

    // change status but fail mode not changed
    change_dup_status(test_app, dup->id, duplication_status::DS_APP);
    change_dup_status(test_app, dup->id, duplication_status::DS_LOG);
    resp = change_dup_status(test_app, dup->id, duplication_status::DS_PAUSE);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(dup->fail_mode(), duplication_fail_mode::FAIL_SKIP);
    ASSERT_EQ(dup->status(), duplication_status::DS_PAUSE);

    // ensure dup_sync will synchronize fail_mode
    std::vector<rpc_address> server_nodes = generate_node_list(3);
    rpc_address node = server_nodes[0];
    for (partition_configuration &pc : app->partitions) {
        pc.primary = server_nodes[0];
    }
    initialize_node_state();
    duplication_sync_response sync_resp = duplication_sync(node, {});
    ASSERT_TRUE(sync_resp.dup_map[app->app_id][dup->id].__isset.fail_mode);
    ASSERT_EQ(sync_resp.dup_map[app->app_id][dup->id].fail_mode, duplication_fail_mode::FAIL_SKIP);

    // ensure recovery will not lose fail_mode.
    SetUp();
    recover_from_meta_state();
    app = find_app(test_app);
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
        std::string test_app = test.fail_cfg_name;
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

TEST_F(meta_duplication_service_test, check_follower_app_if_create_completed)
{
    struct test_case
    {
        std::vector<std::string> fail_cfg_name;
        std::vector<std::string> fail_cfg_action;
        bool is_altering;
        duplication_status::type cur_status;
        duplication_status::type next_status;
    } test_cases[] = {{{"create_app_ok"},
                       {"void()"},
                       false,
                       duplication_status::DS_LOG,
                       duplication_status::DS_INIT},
                      // the case just `palace holder`, actually
                      // `check_follower_app_if_create_completed` is failed by default in unit test
                      {{"create_app_failed"},
                       {"off()"},
                       false,
                       duplication_status::DS_APP,
                       duplication_status::DS_INIT},
                      {{"create_app_ok", "persist_dup_status_failed"},
                       {"void()", "return()"},
                       true,
                       duplication_status::DS_APP,
                       duplication_status::DS_LOG}};

    for (const auto &test : test_cases) {
        std::string test_app =
            fmt::format("{}{}", test.fail_cfg_name[0], test.fail_cfg_name.size());
        create_app(test_app);
        auto app = find_app(test_app);

        auto dup_add_resp = create_dup(test_app);
        auto dup = app->duplications[dup_add_resp.dupid];
        // 'check_follower_app_if_create_completed' must execute under duplication_status::DS_APP,
        // so force update it
        force_update_dup_status(dup, duplication_status::DS_APP);
        fail::setup();
        for (int i = 0; i < test.fail_cfg_name.size(); i++) {
            fail::cfg(test.fail_cfg_name[i], test.fail_cfg_action[i]);
        }
        check_follower_app_if_create_completed(dup);
        wait_all();
        fail::teardown();
        ASSERT_EQ(dup->is_altering(), test.is_altering);
        ASSERT_EQ(next_status(dup), test.next_status);
        ASSERT_EQ(dup->status(), test.cur_status);
    }
}

} // namespace replication
} // namespace dsn
