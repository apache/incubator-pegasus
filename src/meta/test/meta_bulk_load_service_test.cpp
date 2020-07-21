// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

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

    error_code query_bulk_load(const std::string &app_name)
    {
        auto request = dsn::make_unique<query_bulk_load_request>();
        request->app_name = app_name;

        query_bulk_load_rpc rpc(std::move(request), RPC_CM_QUERY_BULK_LOAD_STATUS);
        bulk_svc().on_query_bulk_load_status(rpc);
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

    int32_t get_app_in_process_count(int32_t app_id)
    {
        return bulk_svc()._apps_in_progress_count[app_id];
    }

    /// Used for bulk_load_failover_test

    void initialize_meta_server_with_mock_bulk_load(
        const std::unordered_set<int32_t> &app_id_set,
        const std::unordered_map<app_id, app_bulk_load_info> &app_bulk_load_info_map,
        const std::unordered_map<app_id, std::unordered_map<int32_t, partition_bulk_load_info>>
            &partition_bulk_load_info_map,
        const std::vector<app_info> &app_list)
    {
        // initialize meta service
        auto meta_svc = new fake_receiver_meta_service();
        meta_svc->remote_storage_initialize();

        // initialize server_state
        auto state = meta_svc->_state;
        state->initialize(meta_svc, meta_svc->_cluster_root + "/apps");
        _app_root = state->_apps_root;
        meta_svc->_started = true;
        _ms.reset(meta_svc);

        // initialize bulk load service
        _ms->_bulk_load_svc = make_unique<bulk_load_service>(
            _ms.get(), meta_options::concat_path_unix_style(_ms->_cluster_root, "bulk_load"));
        mock_bulk_load_on_remote_storage(
            app_id_set, app_bulk_load_info_map, partition_bulk_load_info_map);

        // mock app
        for (auto &info : app_list) {
            mock_app_on_remote_stroage(info);
        }
        state->initialize_data_structure();

        _ms->set_function_level(meta_function_level::fl_steady);
        _ms->_failure_detector.reset(new meta_server_failure_detector(_ms.get()));
        _ss = _ms->_state;
    }

    void mock_bulk_load_on_remote_storage(
        const std::unordered_set<int32_t> &app_id_set,
        const std::unordered_map<app_id, app_bulk_load_info> &app_bulk_load_info_map,
        const std::unordered_map<app_id, std::unordered_map<int32_t, partition_bulk_load_info>>
            &partition_bulk_load_info_map)
    {
        std::string path = bulk_svc()._bulk_load_root;
        blob value = blob();
        std::unordered_map<int32_t, partition_bulk_load_info> pinfo_map;
        // create bulk_load_root
        _ms->get_meta_storage()->create_node(
            std::move(path),
            std::move(value),
            [this,
             &app_id_set,
             &app_bulk_load_info_map,
             &partition_bulk_load_info_map,
             &pinfo_map]() {
                for (const auto app_id : app_id_set) {
                    auto app_iter = app_bulk_load_info_map.find(app_id);
                    auto partition_iter = partition_bulk_load_info_map.find(app_id);
                    if (app_iter != app_bulk_load_info_map.end()) {
                        mock_app_bulk_load_info_on_remote_stroage(
                            app_iter->second,
                            partition_iter == partition_bulk_load_info_map.end()
                                ? pinfo_map
                                : partition_iter->second);
                    }
                }
            });
        wait_all();
    }

    void mock_app_bulk_load_info_on_remote_stroage(
        const app_bulk_load_info &ainfo,
        const std::unordered_map<int32_t, partition_bulk_load_info> &partition_bulk_load_info_map)
    {
        std::string app_path = bulk_svc().get_app_bulk_load_path(ainfo.app_id);
        blob value = json::json_forwarder<app_bulk_load_info>::encode(ainfo);
        // create app_bulk_load_info
        _ms->get_meta_storage()->create_node(
            std::move(app_path),
            std::move(value),
            [this, app_path, &ainfo, &partition_bulk_load_info_map]() {
                ddebug_f("create app({}) app_id={} bulk load dir({}), bulk_load_status={}",
                         ainfo.app_name,
                         ainfo.app_id,
                         app_path,
                         dsn::enum_to_string(ainfo.status));
                for (const auto kv : partition_bulk_load_info_map) {
                    mock_partition_bulk_load_info_on_remote_stroage(gpid(ainfo.app_id, kv.first),
                                                                    kv.second);
                }
            });
    }

    void mock_partition_bulk_load_info_on_remote_stroage(const gpid &pid,
                                                         const partition_bulk_load_info &pinfo)
    {
        std::string partition_path = bulk_svc().get_partition_bulk_load_path(pid);
        blob value = json::json_forwarder<partition_bulk_load_info>::encode(pinfo);
        _ms->get_meta_storage()->create_node(
            std::move(partition_path), std::move(value), [this, partition_path, pid, &pinfo]() {
                ddebug_f("create partition[{}] bulk load dir({}), bulk_load_status={}",
                         pid,
                         partition_path,
                         dsn::enum_to_string(pinfo.status));
            });
    }

    void mock_app_on_remote_stroage(const app_info &info)
    {
        static const char *lock_state = "lock";
        static const char *unlock_state = "unlock";
        std::string path = _app_root;

        _ms->get_meta_storage()->create_node(
            std::move(path), blob(lock_state, 0, strlen(lock_state)), [this]() {
                ddebug_f("create app root {}", _app_root);
            });
        wait_all();

        blob value = json::json_forwarder<app_info>::encode(info);
        _ms->get_meta_storage()->create_node(
            _app_root + "/" + boost::lexical_cast<std::string>(info.app_id),
            std::move(value),
            [this, &info]() {
                ddebug_f("create app({}) app_id={}, dir succeed", info.app_name, info.app_id);
                for (int i = 0; i < info.partition_count; ++i) {
                    partition_configuration config;
                    config.max_replica_count = 3;
                    config.pid = gpid(info.app_id, i);
                    config.ballot = BALLOT;
                    blob v = json::json_forwarder<partition_configuration>::encode(config);
                    _ms->get_meta_storage()->create_node(
                        _app_root + "/" + boost::lexical_cast<std::string>(info.app_id) + "/" +
                            boost::lexical_cast<std::string>(i),
                        std::move(v),
                        [info, i, this]() {
                            ddebug_f("create app({}), partition({}.{}) dir succeed",
                                     info.app_name,
                                     info.app_id,
                                     i);
                        });
                }
            });
        wait_all();

        std::string app_root = _app_root;
        _ms->get_meta_storage()->set_data(
            std::move(app_root), blob(unlock_state, 0, strlen(unlock_state)), []() {});
        wait_all();
    }

    int32_t get_app_id_set_size() { return bulk_svc()._bulk_load_app_id.size(); }

    int32_t get_partition_bulk_load_info_size(int32_t app_id)
    {
        int count = 0;
        for (const auto kv : bulk_svc()._partition_bulk_load_info) {
            if (kv.first.get_app_id() == app_id) {
                ++count;
            }
        }
        return count;
    }

    bool is_app_bulk_load_states_reset(int32_t app_id)
    {
        return bulk_svc()._bulk_load_app_id.find(app_id) == bulk_svc()._bulk_load_app_id.end();
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

    struct control_test
    {
        bulk_load_control_type::type type;
        bulk_load_status::type app_status;
        error_code expected_err;
    } tests[] = {
        {bulk_load_control_type::BLC_PAUSE, bulk_load_status::BLS_DOWNLOADING, ERR_OK},
        {bulk_load_control_type::BLC_PAUSE, bulk_load_status::BLS_DOWNLOADED, ERR_INVALID_STATE},
        {bulk_load_control_type::BLC_RESTART, bulk_load_status::BLS_PAUSED, ERR_OK},
        {bulk_load_control_type::BLC_RESTART, bulk_load_status::BLS_PAUSING, ERR_INVALID_STATE},
        {bulk_load_control_type::BLC_CANCEL, bulk_load_status::BLS_DOWNLOADING, ERR_OK},
        {bulk_load_control_type::BLC_CANCEL, bulk_load_status::BLS_PAUSED, ERR_OK},
        {bulk_load_control_type::BLC_CANCEL, bulk_load_status::BLS_INGESTING, ERR_INVALID_STATE},
        {bulk_load_control_type::BLC_FORCE_CANCEL, bulk_load_status::BLS_SUCCEED, ERR_OK}};

    for (auto test : tests) {
        ASSERT_EQ(control_bulk_load(app->app_id, test.type, test.app_status), test.expected_err);
    }
    reset_local_bulk_load_states(app->app_id, APP_NAME);
    fail::teardown();
}

/// query bulk load status unit tests
TEST_F(bulk_load_service_test, query_bulk_load_status_with_wrong_state)
{
    create_app(APP_NAME);
    ASSERT_EQ(query_bulk_load(APP_NAME), ERR_INVALID_STATE);
}

TEST_F(bulk_load_service_test, query_bulk_load_status_success)
{
    create_app(APP_NAME);
    auto app = find_app(APP_NAME);
    app->is_bulk_loading = true;
    ASSERT_EQ(query_bulk_load(APP_NAME), ERR_OK);
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

TEST_F(bulk_load_process_test, downloading_fs_error)
{
    test_on_partition_bulk_load_reply(
        _partition_count, bulk_load_status::BLS_DOWNLOADING, ERR_FS_INTERNAL);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);
}

TEST_F(bulk_load_process_test, downloading_busy)
{
    test_on_partition_bulk_load_reply(
        _partition_count, bulk_load_status::BLS_DOWNLOADING, ERR_BUSY);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
}

TEST_F(bulk_load_process_test, downloading_corrupt)
{
    mock_response_progress(ERR_CORRUPTION, false);
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);
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

TEST_F(bulk_load_process_test, ingestion_error)
{
    mock_response_ingestion_status(ingestion_status::IS_FAILED);
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_INGESTING);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);
}

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

TEST_F(bulk_load_process_test, cancel_not_all_finished)
{
    mock_response_cleaned_up_flag(false, bulk_load_status::BLS_CANCELED);
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_CANCELED);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_CANCELED);
}

TEST_F(bulk_load_process_test, cancel_all_finished)
{
    mock_response_cleaned_up_flag(true, bulk_load_status::BLS_CANCELED);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_CANCELED);
    ASSERT_FALSE(app_is_bulk_loading(APP_NAME));
}

TEST_F(bulk_load_process_test, failed_not_all_finished)
{
    mock_response_cleaned_up_flag(false, bulk_load_status::BLS_FAILED);
    test_on_partition_bulk_load_reply(_partition_count, bulk_load_status::BLS_FAILED);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);
}

TEST_F(bulk_load_process_test, failed_all_finished)
{
    mock_response_cleaned_up_flag(true, bulk_load_status::BLS_FAILED);
    test_on_partition_bulk_load_reply(1, bulk_load_status::BLS_FAILED);
    ASSERT_FALSE(app_is_bulk_loading(APP_NAME));
}

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

TEST_F(bulk_load_process_test, rpc_error)
{
    mock_meta_bulk_load_context(_app_id, _partition_count, bulk_load_status::BLS_DOWNLOADED);
    create_request(bulk_load_status::BLS_DOWNLOADED);
    on_partition_bulk_load_reply(ERR_TIMEOUT, _req, _resp);
    wait_all();
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_in_process_count(_app_id), _partition_count);
}

TEST_F(bulk_load_process_test, response_invalid_state)
{
    test_on_partition_bulk_load_reply(
        _partition_count, bulk_load_status::BLS_SUCCEED, ERR_INVALID_STATE);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_app_in_process_count(_app_id), _partition_count);
}

TEST_F(bulk_load_process_test, response_object_not_found)
{
    test_on_partition_bulk_load_reply(
        _partition_count, bulk_load_status::BLS_CANCELED, ERR_OBJECT_NOT_FOUND);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_CANCELED);
    ASSERT_EQ(get_app_in_process_count(_app_id), _partition_count);
}

/// on_partition_ingestion_reply unit tests
TEST_F(bulk_load_process_test, ingest_rpc_error)
{
    mock_ingestion_context(ERR_OK, 1, _partition_count);
    test_on_partition_ingestion_reply(_ingestion_resp, gpid(_app_id, _pidx), ERR_TIMEOUT);
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_DOWNLOADING);
}

TEST_F(bulk_load_process_test, ingest_empty_write_error)
{
    fail::cfg("meta_bulk_load_partition_ingestion", "return()");
    mock_ingestion_context(ERR_TRY_AGAIN, 11, _partition_count);
    test_on_partition_ingestion_reply(_ingestion_resp, gpid(_app_id, _pidx));
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_INGESTING);
}

TEST_F(bulk_load_process_test, ingest_wrong)
{
    mock_ingestion_context(ERR_OK, 1, _partition_count);
    test_on_partition_ingestion_reply(_ingestion_resp, gpid(_app_id, _pidx));
    wait_all();
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_FAILED);
}

TEST_F(bulk_load_process_test, ingest_succeed)
{
    mock_ingestion_context(ERR_OK, 0, 1);
    test_on_partition_ingestion_reply(_ingestion_resp, gpid(_app_id, _pidx));
    ASSERT_EQ(get_app_bulk_load_status(_app_id), bulk_load_status::BLS_INGESTING);
}

class bulk_load_failover_test : public bulk_load_service_test
{
public:
    bulk_load_failover_test() {}

    void SetUp()
    {
        fail::setup();
        fail::cfg("meta_bulk_load_partition_bulk_load", "return()");
        fail::cfg("meta_bulk_load_partition_ingestion", "return()");
    }

    void TearDown()
    {
        clean_up();
        fail::teardown();
        bulk_load_service_test::TearDown();
    }

    void try_to_continue_bulk_load(bulk_load_status::type app_status, bool is_bulk_loading = true)
    {
        prepare_bulk_load_structures(SYNC_APP_ID,
                                     SYNC_PARTITION_COUNT,
                                     SYNC_APP_NAME,
                                     app_status,
                                     _pstatus_map,
                                     is_bulk_loading);
        initialize_meta_server_with_mock_bulk_load(
            _app_id_set, _app_bulk_load_info_map, _partition_bulk_load_info_map, _app_info_list);
        bulk_svc().initialize_bulk_load_service();
        wait_all();
    }

    void
    prepare_bulk_load_structures(int32_t app_id,
                                 int32_t partition_count,
                                 std::string &app_name,
                                 bulk_load_status::type app_status,
                                 std::unordered_map<int32_t, bulk_load_status::type> &pstatus_map,
                                 bool is_bulk_loading)
    {
        _app_id_set.insert(app_id);
        mock_app_bulk_load_info(app_id, partition_count, app_name, app_status);
        mock_partition_bulk_load_info(app_id, pstatus_map);
        add_to_app_info_list(app_id, partition_count, app_name, is_bulk_loading);
    }

    void mock_app_bulk_load_info(int32_t app_id,
                                 int32_t partition_count,
                                 std::string &app_name,
                                 bulk_load_status::type status)
    {
        app_bulk_load_info ainfo;
        ainfo.app_id = app_id;
        ainfo.app_name = app_name;
        ainfo.cluster_name = CLUSTER;
        ainfo.file_provider_type = PROVIDER;
        ainfo.partition_count = partition_count;
        ainfo.status = status;
        _app_bulk_load_info_map[app_id] = ainfo;
    }

    void
    mock_partition_bulk_load_info(int32_t app_id,
                                  std::unordered_map<int32_t, bulk_load_status::type> &pstatus_map)
    {
        if (pstatus_map.size() <= 0) {
            return;
        }
        std::unordered_map<int32_t, partition_bulk_load_info> pinfo_map;
        for (auto iter = pstatus_map.begin(); iter != pstatus_map.end(); ++iter) {
            partition_bulk_load_info pinfo;
            pinfo.status = iter->second;
            pinfo_map[iter->first] = pinfo;
        }
        _partition_bulk_load_info_map[app_id] = pinfo_map;
    }

    void add_to_app_info_list(int32_t app_id,
                              int32_t partition_count,
                              std::string &app_name,
                              bool is_bulk_loading)
    {
        app_info ainfo;
        ainfo.app_id = app_id;
        ainfo.app_name = app_name;
        ainfo.app_type = "pegasus";
        ainfo.is_stateful = true;
        ainfo.is_bulk_loading = is_bulk_loading;
        ainfo.max_replica_count = 3;
        ainfo.partition_count = partition_count;
        ainfo.status = app_status::AS_AVAILABLE;
        _app_info_list.emplace_back(ainfo);
    }

    void mock_pstatus_map(bulk_load_status::type status, int32_t end_index, int32_t start_index = 0)
    {
        for (auto i = start_index; i <= end_index; ++i) {
            _pstatus_map[i] = status;
        }
    }

    void clean_up()
    {
        _app_info_list.clear();
        _app_bulk_load_info_map.clear();
        _partition_bulk_load_info_map.clear();
        _pstatus_map.clear();
        _app_id_set.clear();
    }

    std::string SYNC_APP_NAME = "bulk_load_failover_table";
    int32_t SYNC_APP_ID = 2;
    int32_t SYNC_PARTITION_COUNT = 4;

    std::vector<app_info> _app_info_list;
    std::unordered_set<int32_t> _app_id_set;
    std::unordered_map<app_id, app_bulk_load_info> _app_bulk_load_info_map;
    std::unordered_map<app_id, std::unordered_map<int32_t, partition_bulk_load_info>>
        _partition_bulk_load_info_map;
    std::unordered_map<int32_t, bulk_load_status::type> _pstatus_map;
};

TEST_F(bulk_load_failover_test, sync_bulk_load)
{
    fail::cfg("meta_try_to_continue_bulk_load", "return()");

    // mock app downloading with partition[0~1] downloading
    std::unordered_map<int32_t, bulk_load_status::type> partition_bulk_load_status_map;
    partition_bulk_load_status_map[0] = bulk_load_status::BLS_DOWNLOADING;
    partition_bulk_load_status_map[1] = bulk_load_status::BLS_DOWNLOADING;
    prepare_bulk_load_structures(SYNC_APP_ID,
                                 SYNC_PARTITION_COUNT,
                                 SYNC_APP_NAME,
                                 bulk_load_status::BLS_DOWNLOADING,
                                 partition_bulk_load_status_map,
                                 true);

    // mock app failed with no partition existed
    partition_bulk_load_status_map.clear();
    partition_bulk_load_status_map[0] = bulk_load_status::BLS_FAILED;
    prepare_bulk_load_structures(APP_ID,
                                 PARTITION_COUNT,
                                 APP_NAME,
                                 bulk_load_status::type::BLS_FAILED,
                                 partition_bulk_load_status_map,
                                 true);

    initialize_meta_server_with_mock_bulk_load(
        _app_id_set, _app_bulk_load_info_map, _partition_bulk_load_info_map, _app_info_list);
    bulk_svc().initialize_bulk_load_service();
    wait_all();

    ASSERT_EQ(get_app_id_set_size(), 2);

    ASSERT_TRUE(app_is_bulk_loading(SYNC_APP_NAME));
    ASSERT_EQ(get_app_bulk_load_status(SYNC_APP_ID), bulk_load_status::BLS_DOWNLOADING);
    ASSERT_EQ(get_partition_bulk_load_info_size(SYNC_APP_ID), 2);

    ASSERT_TRUE(app_is_bulk_loading(APP_NAME));
    ASSERT_EQ(get_app_bulk_load_status(APP_ID), bulk_load_status::BLS_FAILED);
    ASSERT_EQ(get_partition_bulk_load_info_size(APP_ID), 1);
}

/// try_to_continue_bulk_load unit test
// partition_count from bulk load is SYNC_PARTITION_COUNT, app partition_count is PARTITION_COUNT
TEST_F(bulk_load_failover_test, app_info_inconsistency)
{
    prepare_bulk_load_structures(SYNC_APP_ID,
                                 PARTITION_COUNT,
                                 SYNC_APP_NAME,
                                 bulk_load_status::BLS_DOWNLOADED,
                                 _pstatus_map,
                                 true);
    _app_bulk_load_info_map[SYNC_APP_ID].partition_count = SYNC_PARTITION_COUNT;
    initialize_meta_server_with_mock_bulk_load(
        _app_id_set, _app_bulk_load_info_map, _partition_bulk_load_info_map, _app_info_list);
    bulk_svc().initialize_bulk_load_service();
    wait_all();

    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

TEST_F(bulk_load_failover_test, app_downloading_test)
{
    // Test cases:
    // - partition[0,1]=downloading, partition[2,3] not existed
    // - partition[0,1]=downloading, partition[2]=downloaded, partition[3] not exist
    // - partition[0~3]=downloading
    // - partition[0~3]=downloaded
    // - partition[0]=downloaded, partition[1~3]=downloading
    // - partition[0-3]=succeed
    struct app_downloading_test
    {
        int32_t start_index;
        int32_t end_index;
        bulk_load_status::type pstatus;
        int32_t downloaded_pidx;
        bool expected_is_bulk_loading;
        int32_t expected_in_process_count;
    } tests[] = {{0, 1, bulk_load_status::BLS_DOWNLOADING, -1, true, SYNC_PARTITION_COUNT},
                 {0, 1, bulk_load_status::BLS_DOWNLOADING, 2, false, 0},
                 {0, 3, bulk_load_status::BLS_DOWNLOADING, -1, true, SYNC_PARTITION_COUNT},
                 {0, 3, bulk_load_status::BLS_DOWNLOADED, -1, true, SYNC_PARTITION_COUNT},
                 {1, 3, bulk_load_status::BLS_DOWNLOADING, 0, true, SYNC_PARTITION_COUNT},
                 {0, 3, bulk_load_status::BLS_SUCCEED, -1, true, SYNC_PARTITION_COUNT}};

    for (const auto &test : tests) {
        SetUp();
        mock_pstatus_map(test.pstatus, test.end_index, test.start_index);
        if (test.downloaded_pidx > 0) {
            _pstatus_map[test.downloaded_pidx] = bulk_load_status::BLS_DOWNLOADED;
        }
        try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADING);
        ASSERT_EQ(app_is_bulk_loading(SYNC_APP_NAME), test.expected_is_bulk_loading);
        if (test.expected_is_bulk_loading) {
            ASSERT_EQ(get_app_bulk_load_status(SYNC_APP_ID), bulk_load_status::BLS_DOWNLOADING);
            ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), test.expected_in_process_count);
        }
        TearDown();
    }
}

TEST_F(bulk_load_failover_test, app_downloaded_test)
{
    // Test cases:
    // - partition[0]=downloaded, partition[1~3] not existed
    // - partition[0]=ingesting, partition[1~3]=succeed
    // - partition[0~3]=downloaded
    // - partition[0~3]=ingesting
    // - partition[0~2]=downloaded, partition[3]=ingesting
    struct app_downloaded_test
    {
        int32_t start_index;
        int32_t end_index;
        bulk_load_status::type pstatus;
        int32_t ingesting_pidx;
        bool expected_is_bulk_loading;
        int32_t expected_in_process_count;
    } tests[] = {{0, 0, bulk_load_status::BLS_DOWNLOADED, -1, false, 0},
                 {1, 3, bulk_load_status::BLS_SUCCEED, 0, false, 0},
                 {0, 3, bulk_load_status::BLS_DOWNLOADED, -1, true, SYNC_PARTITION_COUNT},
                 {0, 3, bulk_load_status::BLS_INGESTING, -1, true, 0},
                 {0, 2, bulk_load_status::BLS_DOWNLOADED, 3, true, 3}};

    for (const auto &test : tests) {
        SetUp();
        mock_pstatus_map(test.pstatus, test.end_index, test.start_index);
        if (test.ingesting_pidx > 0) {
            _pstatus_map[test.ingesting_pidx] = bulk_load_status::BLS_INGESTING;
        }
        try_to_continue_bulk_load(bulk_load_status::BLS_DOWNLOADED);
        ASSERT_EQ(app_is_bulk_loading(SYNC_APP_NAME), test.expected_is_bulk_loading);
        if (test.expected_is_bulk_loading) {
            ASSERT_EQ(get_app_bulk_load_status(SYNC_APP_ID), bulk_load_status::BLS_DOWNLOADED);
            ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), test.expected_in_process_count);
        }
        TearDown();
    }
}

TEST_F(bulk_load_failover_test, app_ingesting_test)
{
    // Test cases:
    // - all partition not exist
    // - partition[0~2]=ingesting, partition[3]=downloading
    // - partition[0~3]=ingesting
    // - partition[0~3]=succeed
    // - partition[0~2]=succeed, partition[3]=ingesting
    struct app_ingesting_test
    {
        int32_t end_index;
        bulk_load_status::type pstatus;
        bulk_load_status::type p3_status;
        bool expected_is_bulk_loading;
        int32_t expected_in_process_count;
    } tests[] = {{-1, bulk_load_status::BLS_INVALID, bulk_load_status::BLS_INVALID, false, 0},
                 {2, bulk_load_status::BLS_INGESTING, bulk_load_status::BLS_DOWNLOADING, false, 0},
                 {3,
                  bulk_load_status::BLS_INGESTING,
                  bulk_load_status::BLS_INVALID,
                  true,
                  SYNC_PARTITION_COUNT},
                 {3, bulk_load_status::BLS_SUCCEED, bulk_load_status::BLS_INVALID, true, 0},
                 {2, bulk_load_status::BLS_SUCCEED, bulk_load_status::BLS_INGESTING, true, 1}};

    for (const auto &test : tests) {
        SetUp();
        mock_pstatus_map(test.pstatus, test.end_index, 0);
        if (test.p3_status != bulk_load_status::BLS_INVALID) {
            _pstatus_map[3] = test.p3_status;
        }
        try_to_continue_bulk_load(bulk_load_status::BLS_INGESTING);
        ASSERT_EQ(app_is_bulk_loading(SYNC_APP_NAME), test.expected_is_bulk_loading);
        if (test.expected_is_bulk_loading) {
            ASSERT_EQ(get_app_bulk_load_status(SYNC_APP_ID), bulk_load_status::BLS_INGESTING);
            ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), test.expected_in_process_count);
        }
        TearDown();
    }
}

TEST_F(bulk_load_failover_test, app_succeed_test)
{
    // Test cases:
    // - partition[0~2]=succeed, partition[3] not exist
    // - partition[0~2]=succeed, partition[3]=failed
    // - partition[0~3]=succeed
    struct app_succeed_test
    {
        bulk_load_status::type p3_status;
        bool expected_is_bulk_loading;
    } tests[] = {{bulk_load_status::BLS_INVALID, false},
                 {bulk_load_status::BLS_FAILED, false},
                 {bulk_load_status::BLS_SUCCEED, true}};

    for (const auto &test : tests) {
        SetUp();
        mock_pstatus_map(bulk_load_status::BLS_SUCCEED, 2, 0);
        if (test.p3_status != bulk_load_status::BLS_INVALID) {
            _pstatus_map[3] = test.p3_status;
        }
        try_to_continue_bulk_load(bulk_load_status::BLS_SUCCEED);
        ASSERT_EQ(app_is_bulk_loading(SYNC_APP_NAME), test.expected_is_bulk_loading);
        if (test.expected_is_bulk_loading) {
            ASSERT_EQ(get_app_bulk_load_status(SYNC_APP_ID), bulk_load_status::BLS_SUCCEED);
            ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
        }
        TearDown();
    }
}

TEST_F(bulk_load_failover_test, app_pausing_test)
{
    // Test cases:
    // - partition[0]=pausing, partition[1~3] not existed
    // - partition[0]=downloading, partition[1]=downloaded, partition[2]=pausing,
    // partition[3]=paused
    // - partition[0~3]=pasuing
    // - partition[0]=pausing, partition[1~3]=paused
    struct app_pausing_test
    {
        bool mixed_status;
        int32_t start_index;
        bulk_load_status::type pstatus;
        bool expected_is_bulk_loading;
    } tests[] = {{false, -1, bulk_load_status::type::BLS_PAUSING, false},
                 {true, -1, bulk_load_status::type::BLS_PAUSING, true},
                 {false, 1, bulk_load_status::type::BLS_PAUSING, true},
                 {false, 1, bulk_load_status::type::BLS_PAUSED, true}};
    for (const auto &test : tests) {
        SetUp();
        if (test.mixed_status) {
            _pstatus_map[0] = bulk_load_status::BLS_DOWNLOADING;
            _pstatus_map[1] = bulk_load_status::BLS_DOWNLOADED;
            _pstatus_map[2] = bulk_load_status::BLS_PAUSING;
            _pstatus_map[3] = bulk_load_status::BLS_PAUSED;
        } else {
            _pstatus_map[0] = bulk_load_status::BLS_PAUSING;
            if (test.start_index > 0) {
                mock_pstatus_map(test.pstatus, 3, test.start_index);
            }
        }
        try_to_continue_bulk_load(bulk_load_status::BLS_PAUSING);
        ASSERT_EQ(app_is_bulk_loading(SYNC_APP_NAME), test.expected_is_bulk_loading);
        if (test.expected_is_bulk_loading) {
            ASSERT_EQ(get_app_bulk_load_status(SYNC_APP_ID), bulk_load_status::BLS_PAUSING);
            ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
        }
        TearDown();
    }
}

TEST_F(bulk_load_failover_test, app_paused_test)
{
    // Test cases:
    // - partition[0~2]=paused, partition[3] not existed
    // - partition[0~2]=paused, partition[3]=pausing
    // - partition[0~3]=paused
    struct app_paused_test
    {
        bulk_load_status::type p3_status;
        bool expected_is_bulk_loading;
        int32_t expected_in_process_count;
    } tests[] = {{bulk_load_status::BLS_INVALID, false},
                 {bulk_load_status::BLS_PAUSING, false},
                 {bulk_load_status::BLS_PAUSED, true}};

    for (const auto &test : tests) {
        SetUp();
        mock_pstatus_map(bulk_load_status::BLS_PAUSED, 2, 0);
        if (test.p3_status != bulk_load_status::BLS_INVALID) {
            _pstatus_map[3] = test.p3_status;
        }
        try_to_continue_bulk_load(bulk_load_status::BLS_PAUSED);
        ASSERT_EQ(app_is_bulk_loading(SYNC_APP_NAME), test.expected_is_bulk_loading);
        if (test.expected_is_bulk_loading) {
            ASSERT_EQ(get_app_bulk_load_status(SYNC_APP_ID), bulk_load_status::BLS_PAUSED);
            ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
        }
        TearDown();
    }
}

TEST_F(bulk_load_failover_test, app_failed_test)
{
    // Test cases:
    // - partition[0~2]=failed, partition[3] not existed
    // - partition[0~3]=failed
    // - partition[0,1]=downloading, partition[2]=downloaded, partition[3]=failed
    struct app_failed_test
    {
        bool mixed_status;
        int32_t end_index;
        bool expected_is_bulk_loading;
    } tests[] = {{false, 2, false}, {false, 3, true}, {true, -1, true}};
    for (const auto &test : tests) {
        SetUp();
        if (test.mixed_status) {
            _pstatus_map[0] = bulk_load_status::BLS_DOWNLOADING;
            _pstatus_map[1] = bulk_load_status::BLS_DOWNLOADING;
            _pstatus_map[2] = bulk_load_status::BLS_DOWNLOADED;
            _pstatus_map[3] = bulk_load_status::BLS_FAILED;
        } else {
            mock_pstatus_map(bulk_load_status::BLS_FAILED, test.end_index, 0);
        }
        try_to_continue_bulk_load(bulk_load_status::BLS_FAILED);
        ASSERT_EQ(app_is_bulk_loading(SYNC_APP_NAME), test.expected_is_bulk_loading);
        if (test.expected_is_bulk_loading) {
            ASSERT_EQ(get_app_bulk_load_status(SYNC_APP_ID), bulk_load_status::BLS_FAILED);
            ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
        }
        TearDown();
    }
}

TEST_F(bulk_load_failover_test, app_cancel_test)
{
    // Test cases:
    // - partition[0~2]=pausing, partition[3] not existed
    // - partition[0~3]=cancel
    // - partition[0~2]=ingestion, partition[3]=downloaded
    struct app_cancel_test
    {
        bulk_load_status::type pstatus;
        bulk_load_status::type p3_status;
        bool expected_is_bulk_loading;
    } tests[] = {
        {bulk_load_status::type::BLS_PAUSING, bulk_load_status::type::BLS_INVALID, false},
        {bulk_load_status::type::BLS_CANCELED, bulk_load_status::type::BLS_CANCELED, true},
        {bulk_load_status::type::BLS_INGESTING, bulk_load_status::type::BLS_DOWNLOADED, true}};
    for (const auto &test : tests) {
        SetUp();
        mock_pstatus_map(test.pstatus, 2, 0);
        if (test.p3_status != bulk_load_status::type::BLS_INVALID) {
            _pstatus_map[3] = test.p3_status;
        }
        try_to_continue_bulk_load(bulk_load_status::BLS_CANCELED);
        ASSERT_EQ(app_is_bulk_loading(SYNC_APP_NAME), test.expected_is_bulk_loading);
        if (test.expected_is_bulk_loading) {
            ASSERT_EQ(get_app_bulk_load_status(SYNC_APP_ID), bulk_load_status::BLS_CANCELED);
            ASSERT_EQ(get_app_in_process_count(SYNC_APP_ID), SYNC_PARTITION_COUNT);
        }
        TearDown();
    }
}

/// check_app_bulk_load_states unit test
// create app(is_bulk_loading=true), but no bulk load info on remote storage
TEST_F(bulk_load_failover_test, status_inconsistency_wrong_app_flag)
{
    add_to_app_info_list(SYNC_APP_ID, SYNC_PARTITION_COUNT, SYNC_APP_NAME, true);
    initialize_meta_server_with_mock_bulk_load(
        _app_id_set, _app_bulk_load_info_map, _partition_bulk_load_info_map, _app_info_list);
    bulk_svc().initialize_bulk_load_service();
    wait_all();

    ASSERT_FALSE(app_is_bulk_loading(SYNC_APP_NAME));
}

// create app bulk load info on remote storage, but this app not existed
TEST_F(bulk_load_failover_test, status_inconsistency_wrong_bulk_load_dir)
{
    std::unordered_map<int32_t, bulk_load_status::type> partition_bulk_load_status_map;
    partition_bulk_load_status_map[0] = bulk_load_status::BLS_DOWNLOADING;
    partition_bulk_load_status_map[1] = bulk_load_status::BLS_DOWNLOADING;
    prepare_bulk_load_structures(SYNC_APP_ID,
                                 PARTITION_COUNT,
                                 SYNC_APP_NAME,
                                 bulk_load_status::BLS_DOWNLOADING,
                                 partition_bulk_load_status_map,
                                 true);
    _app_info_list.clear();
    add_to_app_info_list(APP_ID, PARTITION_COUNT, APP_NAME, false);

    initialize_meta_server_with_mock_bulk_load(
        _app_id_set, _app_bulk_load_info_map, _partition_bulk_load_info_map, _app_info_list);
    bulk_svc().initialize_bulk_load_service();
    wait_all();

    ASSERT_TRUE(is_app_bulk_load_states_reset(SYNC_APP_ID));
}

} // namespace replication
} // namespace dsn
