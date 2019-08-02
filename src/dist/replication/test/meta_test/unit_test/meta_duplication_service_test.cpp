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
#include <dsn/dist/fmt_logging.h>

#include "dist/replication/meta_server/server_load_balancer.h"
#include "dist/replication/meta_server/meta_server_failure_detector.h"
#include "dist/replication/meta_server/duplication/meta_duplication_service.h"
#include "dist/replication/test/meta_test/misc/misc.h"

#include "meta_service_test_app.h"
#include "meta_test_base.h"

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
        req->freezed = freezed;

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

    duplication_status_change_response
    change_dup_status(const std::string &app_name, dupid_t dupid, duplication_status::type status)
    {
        auto req = make_unique<duplication_status_change_request>();
        req->dupid = dupid;
        req->app_name = app_name;
        req->status = status;

        duplication_status_change_rpc rpc(std::move(req), RPC_CM_CHANGE_DUPLICATION_STATUS);
        dup_svc().change_duplication_status(rpc);
        wait_all();

        return rpc.response();
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
            auto dup = dup_svc().new_dup_from_init(remote_cluster_address, app);

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

    void test_add_duplication()
    {
        std::string test_app = "test-app";
        std::string test_app_invalid_ver = "test-app-invalid-ver";

        std::string invalid_remote = "test-invalid-remote";
        std::string ok_remote = "slave-cluster";

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
        };

        for (auto tt : tests) {
            auto resp = create_dup(tt.app, tt.remote);
            ASSERT_EQ(tt.wec, resp.err);

            if (tt.wec == ERR_OK) {
                auto app = find_app(test_app);
                auto dup = app->duplications[resp.dupid];
                ASSERT_TRUE(dup != nullptr);
                ASSERT_EQ(dup->app_id, app->app_id);
                ASSERT_EQ(dup->_status, duplication_status::DS_START);
                ASSERT_EQ(dup->remote, ok_remote);
                ASSERT_EQ(resp.dupid, dup->id);
                ASSERT_EQ(app->duplicating, true);
            }
        }
    }

    std::shared_ptr<server_state> _ss;
    std::unique_ptr<meta_service> _ms;
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
        ASSERT_EQ(change_dup_status(tt.app, test_dup, duplication_status::DS_PAUSE).err, tt.wec);
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
        ASSERT_EQ(duplication_entry.status, duplication_status::DS_START);
        ASSERT_EQ(duplication_entry.dupid, dupid);
    }
}

TEST_F(meta_duplication_service_test, change_duplication_status)
{
    std::string test_app = "test-app";

    create_app(test_app);
    auto app = find_app(test_app);
    dupid_t test_dup = create_dup(test_app).dupid;

    struct TestData
    {
        std::string app;
        dupid_t dupid;
        duplication_status::type status;

        error_code wec;
    } tests[] = {
        {test_app, test_dup + 1, duplication_status::DS_INIT, ERR_OBJECT_NOT_FOUND},

        // ok test
        {test_app, test_dup, duplication_status::DS_PAUSE, ERR_OK}, // start->pause
        {test_app, test_dup, duplication_status::DS_PAUSE, ERR_OK}, // pause->pause
        {test_app, test_dup, duplication_status::DS_START, ERR_OK}, // pause->start
        {test_app, test_dup, duplication_status::DS_START, ERR_OK}, // start->start
    };

    for (auto tt : tests) {
        auto resp = change_dup_status(tt.app, tt.dupid, tt.status);
        ASSERT_EQ(resp.err, tt.wec);
    }
}

// this test ensures that dupid is always increment and larger than zero.
TEST_F(meta_duplication_service_test, new_dup_from_init) { test_new_dup_from_init(); }

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
    ASSERT_EQ(resp.entry_list.back().status, duplication_status::DS_PAUSE);
    ASSERT_EQ(resp.entry_list.back().dupid, test_dup);
    ASSERT_EQ(resp.appid, app->app_id);

    change_dup_status(test_app, test_dup, duplication_status::DS_REMOVED);
    resp = query_dup_info(test_app);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.entry_list.size(), 0);
}

} // namespace replication
} // namespace dsn
