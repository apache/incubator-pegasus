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

#include "meta_test_base.h"

namespace dsn {
namespace replication {
class meta_app_envs_test : public meta_test_base
{
public:
    meta_app_envs_test() {}

    void SetUp() override
    {
        meta_test_base::SetUp();
        create_app(app_name);
    }

    const std::string app_name = "test_app_env";
    const std::string env_slow_query_threshold = "replica.slow_query_threshold";
};

TEST_F(meta_app_envs_test, set_slow_query_threshold)
{
    auto app = find_app(app_name);

    struct test_case
    {
        error_code err;
        std::string env_value;
        std::string expect_env_value;
    } tests[] = {{ERR_OK, "30", "30"},
                 {ERR_OK, "21", "21"},
                 {ERR_OK, "20", "20"},
                 {ERR_INVALID_PARAMETERS, "19", "20"},
                 {ERR_INVALID_PARAMETERS, "10", "20"},
                 {ERR_INVALID_PARAMETERS, "0", "20"}};

    for (auto test : tests) {
        error_code err = update_app_envs(app_name, {env_slow_query_threshold}, {test.env_value});

        ASSERT_EQ(err, test.err);
        ASSERT_EQ(app->envs.at(env_slow_query_threshold), test.expect_env_value);
    }
}
} // namespace replication
} // namespace dsn
