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
#include <dsn/utility/fail_point.h>

#include "meta_test_base.h"

namespace dsn {
namespace replication {
class bulk_load_service_test : public meta_test_base
{
public:
    bulk_load_service_test() {}

    /// bulk load functions

    start_bulk_load_response start_bulk_load(const std::string &app_name)
    {
        auto request = dsn::make_unique<start_bulk_load_request>();
        request->app_name = app_name;
        request->cluster_name = CLUSTER;
        request->file_provider_type = PROVIDER;

        start_bulk_load_rpc rpc(std::move(request), RPC_CM_START_BULK_LOAD);
        bulk_svc().on_start_bulk_load(rpc);
        wait_all();
        return rpc.response();
    }

    error_code check_start_bulk_load_request_params(const std::string provider,
                                                    int32_t app_id,
                                                    int32_t partition_count)
    {
        std::string hint_msg;
        return bulk_svc().check_bulk_load_request_params(
            APP_NAME, CLUSTER, provider, app_id, partition_count, hint_msg);
    }

    error_code control_bulk_load(int32_t app_id,
                                 bulk_load_control_type::type type,
                                 bulk_load_status::type app_status)
    {
        bulk_svc()._app_bulk_load_info[app_id].status = app_status;

        auto request = dsn::make_unique<control_bulk_load_request>();
        request->app_name = APP_NAME;
        request->type = type;

        control_bulk_load_rpc rpc(std::move(request), RPC_CM_CONTROL_BULK_LOAD);
        bulk_svc().on_control_bulk_load(rpc);
        wait_all();
        return rpc.response().err;
    }

    void mock_meta_bulk_load_context(int32_t app_id,
                                     int32_t in_progress_partition_count,
                                     bulk_load_status::type status)
    {
        bulk_svc()._bulk_load_app_id.insert(app_id);
        bulk_svc()._apps_in_progress_count[app_id] = in_progress_partition_count;
        bulk_svc()._app_bulk_load_info[app_id].status = status;
        for (int i = 0; i < in_progress_partition_count; ++i) {
            gpid pid = gpid(app_id, i);
            bulk_svc()._partition_bulk_load_info[pid].status = status;
        }
    }

    void on_partition_bulk_load_reply(error_code err,
                                      const bulk_load_request &request,
                                      const bulk_load_response &response)
    {
        bulk_svc().on_partition_bulk_load_reply(err, request, response);
    }

    bool app_is_bulk_loading(const std::string &app_name)
    {
        return find_app(app_name)->is_bulk_loading;
    }

    bool need_update_metadata(gpid pid)
    {
        return bulk_svc().is_partition_metadata_not_updated(pid);
    }

    bulk_load_status::type get_app_bulk_load_status(int32_t app_id)
    {
        return bulk_svc().get_app_bulk_load_status_unlocked(app_id);
    }

    void test_on_partition_ingestion_reply(ingestion_response &resp,
                                           const gpid &pid,
                                           error_code rpc_err = ERR_OK)
    {
        bulk_svc().on_partition_ingestion_reply(rpc_err, std::move(resp), APP_NAME, pid);
        wait_all();
    }

    void reset_local_bulk_load_states(int32_t app_id, const std::string &app_name)
    {
        bulk_svc().reset_local_bulk_load_states(app_id, app_name);
    }

public:
    int32_t APP_ID = 1;
    std::string APP_NAME = "bulk_load_test";
    int32_t PARTITION_COUNT = 8;
    std::string CLUSTER = "cluster";
    std::string PROVIDER = "local_service";
    int64_t BALLOT = 4;
};

/// start bulk load unit tests
TEST_F(bulk_load_service_test, start_bulk_load_with_not_existed_app)
{
    auto resp = start_bulk_load("table_not_exist");
    ASSERT_EQ(resp.err, ERR_APP_NOT_EXIST);
}

TEST_F(bulk_load_service_test, start_bulk_load_with_wrong_provider)
{
    create_app(APP_NAME);
    error_code err = check_start_bulk_load_request_params("wrong_provider", 1, PARTITION_COUNT);
    ASSERT_EQ(err, ERR_INVALID_PARAMETERS);
}

TEST_F(bulk_load_service_test, start_bulk_load_succeed)
{
    create_app(APP_NAME);
    fail::setup();
    fail::cfg("meta_check_bulk_load_request_params", "return()");
    fail::cfg("meta_bulk_load_partition_bulk_load", "return()");

    auto resp = start_bulk_load(APP_NAME);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_TRUE(app_is_bulk_loading(APP_NAME));

    fail::teardown();
}

/// control bulk load unit tests
TEST_F(bulk_load_service_test, control_bulk_load_test)
{
    create_app(APP_NAME);
    std::shared_ptr<app_state> app = find_app(APP_NAME);
    app->is_bulk_loading = true;
    mock_meta_bulk_load_context(app->app_id, app->partition_count, bulk_load_status::BLS_INVALID);
    fail::setup();
    fail::cfg("meta_update_app_status_on_remote_storage_unlocked", "return()");

    // TODO(heyuchen): add restart/cancel/force_cancel test cases
    struct control_test
    {
        bulk_load_control_type::type type;
        bulk_load_status::type app_status;
        error_code expected_err;
    } tests[] = {
        {bulk_load_control_type::BLC_PAUSE, bulk_load_status::BLS_DOWNLOADING, ERR_OK},
        {bulk_load_control_type::BLC_PAUSE, bulk_load_status::BLS_DOWNLOADED, ERR_INVALID_STATE}};

    for (auto test : tests) {
        ASSERT_EQ(control_bulk_load(app->app_id, test.type, test.app_status), test.expected_err);
    }
    reset_local_bulk_load_states(app->app_id, APP_NAME);
    fail::teardown();
}

/// bulk load process unit tests
class bulk_load_process_test : public bulk_load_service_test
{
public:
    void SetUp()
    {
        bulk_load_service_test::SetUp();
        create_app(APP_NAME);

        fail::setup();
        fail::cfg("meta_check_bulk_load_request_params", "return()");
        fail::cfg("meta_bulk_load_partition_bulk_load", "return()");
        fail::cfg("meta_bulk_load_resend_request", "return()");

        auto resp = start_bulk_load(APP_NAME);
        ASSERT_EQ(resp.err, ERR_OK);
        std::shared_ptr<app_state> app = find_app(APP_NAME);
        _app_id = app->app_id;
        _partition_count = app->partition_count;
        ASSERT_EQ(app->is_bulk_loading, true);
    }

    void TearDown()
    {
        fail::teardown();
        bulk_load_service_test::TearDown();
    }

    void create_request(bulk_load_status::type status)
    {
        _req.app_name = APP_NAME;
        _req.ballot = BALLOT;
        _req.cluster_name = CLUSTER;
        _req.pid = gpid(_app_id, _pidx);
        _req.primary_addr = PRIMARY;
        _req.meta_bulk_load_status = status;
    }

    void create_basic_response(error_code err, bulk_load_status::type status)
    {
        _resp.app_name = APP_NAME;
        _resp.pid = gpid(_app_id, _pidx);
        _resp.err = err;
        _resp.primary_bulk_load_status = status;
    }

    void mock_response_progress(error_code progress_err, bool finish_download)
    {
        create_basic_response(ERR_OK, bulk_load_status::BLS_DOWNLOADING);

        partition_bulk_load_state state, state2;
        int32_t secondary2_progress = finish_download ? 100 : 0;
        int32_t total_progress = finish_download ? 100 : 66;
        state.__set_download_status(ERR_OK);
        state.__set_download_progress(100);
        state2.__set_download_status(progress_err);
        state2.__set_download_progress(secondary2_progress);

        _resp.group_bulk_load_state[PRIMARY] = state;
        _resp.group_bulk_load_state[SECONDARY1] = state;
        _resp.group_bulk_load_state[SECONDARY2] = state2;
        _resp.__set_total_download_progress(total_progress);
    }

    void mock_response_bulk_load_metadata()
    {
        mock_response_progress(ERR_OK, false);

        file_meta f_meta;
        f_meta.name = "mock_remote_file";
        f_meta.size = 100;
        f_meta.md5 = "mock_md5";

        bulk_load_metadata metadata;
        metadata.files.emplace_back(f_meta);
        metadata.file_total_size = 100;

        _resp.__set_metadata(metadata);
    }

    void mock_response_ingestion_status(ingestion_status::type secondary_istatus)
    {
        create_basic_response(ERR_OK, bulk_load_status::BLS_INGESTING);

        partition_bulk_load_state state, state2;
        state.__set_ingest_status(ingestion_status::IS_SUCCEED);
        state2.__set_ingest_status(secondary_istatus);

        _resp.group_bulk_load_state[PRIMARY] = state;
        _resp.group_bulk_load_state[SECONDARY1] = state;
        _resp.group_bulk_load_state[SECONDARY2] = state2;
        _resp.__set_is_group_ingestion_finished(secondary_istatus == ingestion_status::IS_SUCCEED);
    }

    void mock_response_cleaned_up_flag(bool all_cleaned_up, bulk_load_status::type status)
    {
        create_basic_response(ERR_OK, status);

        partition_bulk_load_state state, state2;
        state.__set_is_cleaned_up(true);
        _resp.group_bulk_load_state[PRIMARY] = state;
        _resp.group_bulk_load_state[SECONDARY1] = state;

        state2.__set_is_cleaned_up(all_cleaned_up);
        _resp.group_bulk_load_state[SECONDARY2] = state2;
        _resp.__set_is_group_bulk_load_context_cleaned_up(all_cleaned_up);
    }

    void mock_response_paused(bool is_group_paused)
    {
        create_basic_response(ERR_OK, bulk_load_status::BLS_PAUSED);

        partition_bulk_load_state state, state2;
        state.__set_is_paused(true);
        state2.__set_is_paused(is_group_paused);

        _resp.group_bulk_load_state[PRIMARY] = state;
        _resp.group_bulk_load_state[SECONDARY1] = state;
        _resp.group_bulk_load_state[SECONDARY2] = state2;
        _resp.__set_is_group_bulk_load_paused(is_group_paused);
    }

    void test_on_partition_bulk_load_reply(int32_t in_progress_count,
                                           bulk_load_status::type status,
                                           error_code resp_err = ERR_OK)
    {
        mock_meta_bulk_load_context(_app_id, in_progress_count, status);
        create_request(status);
        auto response = _resp;
        response.err = resp_err;
        on_partition_bulk_load_reply(ERR_OK, _req, response);
        wait_all();
    }

    void mock_ingestion_context(error_code err, int32_t rocksdb_err, int32_t in_progress_count)
    {
        mock_meta_bulk_load_context(_app_id, in_progress_count, bulk_load_status::BLS_INGESTING);
        _ingestion_resp.err = err;
        _ingestion_resp.rocksdb_error = rocksdb_err;
    }

public:
    const int32_t _pidx = 0;
    const rpc_address PRIMARY = rpc_address("127.0.0.1", 10086);
    const rpc_address SECONDARY1 = rpc_address("127.0.0.1", 10085);
    const rpc_address SECONDARY2 = rpc_address("127.0.0.1", 10087);

    int32_t _app_id;
    int32_t _partition_count;
    bulk_load_request _req;
    bulk_load_response _resp;
    ingestion_response _ingestion_resp;
};

/// on_partition_bulk_load_reply unit tests

// TODO(heyuchen):
// add `downloading_fs_error` unit tests after implement function `handle_bulk_load_failed`
// add `downloading_corrupt` unit tests after implement function `handle_bulk_load_failed`

TEST_F(bulk_load_process_test, downloading_busy)
{
    test_on_partition_bulk_load_reply(
        _partition_count, bulk_load_status::BLS_DOWNLOADING, ERR_BUSY);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
}

TEST_F(bulk_load_process_test, downloading_report_metadata)
{
    mock_response_bulk_load_metadata();
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_DOWNLOADING);

    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_FALSE(need_update_metadata(gpid(_app_id, _pidx)));
}

TEST_F(bulk_load_process_test, normal_downloading)
{
    mock_response_progress(ERR_OK, false);
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
}

TEST_F(bulk_load_process_test, downloaded_succeed)
{
    mock_response_progress(ERR_OK, true);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADED);
}

TEST_F(bulk_load_process_test, start_ingesting)
{
    fail::cfg("meta_bulk_load_partition_ingestion", "return()");
    mock_response_progress(ERR_OK, true);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_DOWNLOADED);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_INGESTING);
}

TEST_F(bulk_load_process_test, ingestion_running)
{
    mock_response_ingestion_status(ingestion_status::IS_RUNNING);
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_INGESTING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_INGESTING);
}

// TODO(heyuchen): add ingestion_error unit tests after implement function `handle_app_failed`

TEST_F(bulk_load_process_test, normal_succeed)
{
    mock_response_ingestion_status(ingestion_status::IS_SUCCEED);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_INGESTING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_SUCCEED);
}

TEST_F(bulk_load_process_test, succeed_not_all_finished)
{
    mock_response_cleaned_up_flag(false, bulk_load_status::BLS_SUCCEED);
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_SUCCEED);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_SUCCEED);
}

TEST_F(bulk_load_process_test, succeed_all_finished)
{
    mock_response_cleaned_up_flag(true, bulk_load_status::BLS_SUCCEED);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_SUCCEED);
    ASSERT_FALSE(app_is_bulk_loading(APP_NAME));
}

// TODO(heyuchen): add half cleanup test while failed

TEST_F(bulk_load_process_test, pausing)
{
    mock_response_paused(false);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_PAUSING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_PAUSING);
}

TEST_F(bulk_load_process_test, pause_succeed)
{
    mock_response_paused(true);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_PAUSING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_PAUSED);
}

// TODO(heyuchen): add other unit tests for `on_partition_bulk_load_reply`

/// on_partition_ingestion_reply unit tests
// TODO(heyuchen):
// add ingest_rpc_error unit tests after implement function `rollback_downloading`
// add ingest_wrong unit tests after implement function `handle_app_failed`

TEST_F(bulk_load_process_test, ingest_empty_write_error)
{
    fail::cfg("meta_bulk_load_partition_ingestion", "return()");
    mock_ingestion_context(ERR_TRY_AGAIN, 11, _partition_count);
    test_on_partition_ingestion_reply(_ingestion_resp, gpid(_app_id, _pidx));
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_INGESTING);
}

TEST_F(bulk_load_process_test, ingest_succeed)
{
    mock_ingestion_context(ERR_OK, 0, 1);
    test_on_partition_ingestion_reply(_ingestion_resp, gpid(_app_id, _pidx));
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_INGESTING);
}

} // namespace replication
} // namespace dsn
