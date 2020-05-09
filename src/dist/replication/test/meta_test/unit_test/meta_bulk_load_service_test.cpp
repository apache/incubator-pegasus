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

    bool app_is_bulk_loading(const std::string &app_name)
    {
        return find_app(app_name)->is_bulk_loading;
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
} // namespace replication
} // namespace dsn
