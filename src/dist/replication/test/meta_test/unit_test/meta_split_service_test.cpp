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
#include <dsn/service_api_c.h>

#include "meta_service_test_app.h"
#include "meta_test_base.h"

namespace dsn {
namespace replication {
class meta_split_service_test : public meta_test_base
{
public:
    meta_split_service_test() {}

    void SetUp() override
    {
        meta_test_base::SetUp();
        create_app(NAME, PARTITION_COUNT);
    }

    app_partition_split_response start_partition_split(const std::string &app_name,
                                                       int new_partition_count)
    {
        auto request = dsn::make_unique<app_partition_split_request>();
        request->app_name = app_name;
        request->new_partition_count = new_partition_count;

        app_partition_split_rpc rpc(std::move(request), RPC_CM_APP_PARTITION_SPLIT);
        split_svc().app_partition_split(rpc);
        wait_all();
        return rpc.response();
    }

    const std::string NAME = "split_table";
    const uint32_t PARTITION_COUNT = 4;
    const uint32_t NEW_PARTITION_COUNT = 8;
};

TEST_F(meta_split_service_test, start_split_with_not_existed_app)
{
    auto resp = start_partition_split("table_not_exist", PARTITION_COUNT);
    ASSERT_EQ(resp.err, ERR_APP_NOT_EXIST);
}

TEST_F(meta_split_service_test, start_split_with_wrong_params)
{
    auto resp = start_partition_split(NAME, PARTITION_COUNT);
    ASSERT_EQ(resp.err, ERR_INVALID_PARAMETERS);
    ASSERT_EQ(resp.partition_count, PARTITION_COUNT);
}

TEST_F(meta_split_service_test, start_split_succeed)
{
    auto resp = start_partition_split(NAME, NEW_PARTITION_COUNT);
    ASSERT_EQ(resp.err, ERR_OK);
    ASSERT_EQ(resp.partition_count, NEW_PARTITION_COUNT);
}

} // namespace replication
} // namespace dsn
