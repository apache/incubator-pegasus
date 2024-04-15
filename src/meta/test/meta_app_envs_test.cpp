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

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "common/replica_envs.h"
#include "gtest/gtest.h"
#include "meta/meta_data.h"
#include "meta_admin_types.h"
#include "meta_test_base.h"
#include "utils/error_code.h"

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

    void TearDown() override { drop_app(app_name); }

    const std::string app_name = "test_app_env";
};

TEST_F(meta_app_envs_test, update_app_envs_test)
{
    struct test_case
    {
        std::string env_key;
        std::string env_value;
        error_code err;
        std::string hint;
        std::string expect_value;
    } tests[] = {
        {replica_envs::WRITE_QPS_THROTTLING, "100*delay*100", ERR_OK, "", "100*delay*100"},
        {replica_envs::WRITE_QPS_THROTTLING, "20K*delay*100", ERR_OK, "", "20K*delay*100"},
        {replica_envs::WRITE_QPS_THROTTLING, "20M*delay*100", ERR_OK, "", "20M*delay*100"},
        {replica_envs::WRITE_QPS_THROTTLING,
         "20A*delay*100",
         ERR_INVALID_PARAMETERS,
         "'20A' should be an unsigned integer",
         "20M*delay*100"},
        {replica_envs::WRITE_QPS_THROTTLING,
         "-20*delay*100",
         ERR_INVALID_PARAMETERS,
         "'-20' should be an unsigned integer",
         "20M*delay*100"},
        {replica_envs::WRITE_QPS_THROTTLING,
         "",
         ERR_INVALID_PARAMETERS,
         "The value shouldn't be empty",
         "20M*delay*100"},
        {replica_envs::WRITE_QPS_THROTTLING,
         "20A*delay",
         ERR_INVALID_PARAMETERS,
         "The field count of '20A*delay' separated by '*' must be 3",
         "20M*delay*100"},
        {replica_envs::WRITE_QPS_THROTTLING,
         "20K*pass*100",
         ERR_INVALID_PARAMETERS,
         "'pass' should be 'delay' or 'reject'",
         "20M*delay*100"},
        {replica_envs::WRITE_QPS_THROTTLING,
         "20K*delay*-100",
         ERR_INVALID_PARAMETERS,
         "'-100' should be an unsigned integer",
         "20M*delay*100"},
        {replica_envs::WRITE_QPS_THROTTLING,
         "2K**delay*100",
         ERR_INVALID_PARAMETERS,
         "The field count of '2K**delay*100' separated by '*' must be 3",
         "20M*delay*100"},
        {replica_envs::WRITE_QPS_THROTTLING,
         "2K*delay**100",
         ERR_INVALID_PARAMETERS,
         "The field count of '2K*delay**100' separated by '*' must be 3",
         "20M*delay*100"},
        {replica_envs::WRITE_QPS_THROTTLING,
         "2K*delay*100,3K*delay*100",
         ERR_INVALID_PARAMETERS,
         "duplicate 'delay' config",
         "20M*delay*100"},
        {replica_envs::WRITE_QPS_THROTTLING,
         "2K*reject*100,3K*reject*100",
         ERR_INVALID_PARAMETERS,
         "duplicate 'reject' config",
         "20M*delay*100"},
        {replica_envs::WRITE_QPS_THROTTLING, "20M*reject*100", ERR_OK, "", "20M*reject*100"},
        {replica_envs::WRITE_SIZE_THROTTLING, "300*delay*100", ERR_OK, "", "300*delay*100"},
        {replica_envs::SLOW_QUERY_THRESHOLD, "30", ERR_OK, "", "30"},
        {replica_envs::SLOW_QUERY_THRESHOLD, "20", ERR_OK, "", "20"},
        {replica_envs::SLOW_QUERY_THRESHOLD,
         "19",
         ERR_INVALID_PARAMETERS,
         "invalid value '19', should be '>= 20'",
         "20"},
        {replica_envs::SLOW_QUERY_THRESHOLD,
         "0",
         ERR_INVALID_PARAMETERS,
         "invalid value '0', should be '>= 20'",
         "20"},
        {replica_envs::TABLE_LEVEL_DEFAULT_TTL, "10", ERR_OK, "", "10"},
        {replica_envs::ROCKSDB_USAGE_SCENARIO, "bulk_load", ERR_OK, "", "bulk_load"},
        {replica_envs::ROCKSDB_CHECKPOINT_RESERVE_MIN_COUNT, "30", ERR_OK, "", "30"},
        {replica_envs::ROCKSDB_CHECKPOINT_RESERVE_TIME_SECONDS, "40", ERR_OK, "", "40"},
        {replica_envs::MANUAL_COMPACT_DISABLED, "true", ERR_OK, "", "true"},
        {replica_envs::MANUAL_COMPACT_MAX_CONCURRENT_RUNNING_COUNT, "60", ERR_OK, "", "60"},
        {replica_envs::MANUAL_COMPACT_ONCE_TRIGGER_TIME, "70", ERR_OK, "", "70"},
        {replica_envs::MANUAL_COMPACT_ONCE_TARGET_LEVEL, "80", ERR_OK, "", "80"},
        {replica_envs::MANUAL_COMPACT_PERIODIC_TRIGGER_TIME, "90", ERR_OK, "", "90"},
        {replica_envs::MANUAL_COMPACT_PERIODIC_TARGET_LEVEL, "100", ERR_OK, "", "100"},
        {replica_envs::ROCKSDB_WRITE_BUFFER_SIZE,
         "100",
         ERR_INVALID_PARAMETERS,
         "invalid value '100', should be 'In range [16777216, 536870912]'",
         "67108864"},
        {replica_envs::ROCKSDB_WRITE_BUFFER_SIZE,
         "636870912",
         ERR_INVALID_PARAMETERS,
         "invalid value '636870912', should be 'In range [16777216, 536870912]'",
         "536870912"},
        {replica_envs::ROCKSDB_WRITE_BUFFER_SIZE, "67108864", ERR_OK, "", "67108864"},
        {replica_envs::MANUAL_COMPACT_PERIODIC_BOTTOMMOST_LEVEL_COMPACTION,
         replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP,
         ERR_OK,
         "",
         replica_envs::MANUAL_COMPACT_BOTTOMMOST_LEVEL_COMPACTION_SKIP},
        {replica_envs::BUSINESS_INFO, "300", ERR_OK, "", "300"},
        {replica_envs::DENY_CLIENT_REQUEST,
         "400",
         ERR_INVALID_PARAMETERS,
         "Invalid deny client args, valid include: timeout*all, "
         "timeout*write, timeout*read; reconfig*all, reconfig*write, "
         "reconfig*read",
         "400"},
        {replica_envs::DENY_CLIENT_REQUEST,
         "invalid*all",
         ERR_INVALID_PARAMETERS,
         "Invalid deny client args, valid include: timeout*all, "
         "timeout*write, timeout*read; reconfig*all, reconfig*write, "
         "reconfig*read",
         "invalid*all"},
        {replica_envs::DENY_CLIENT_REQUEST,
         "timeout*invalid",
         ERR_INVALID_PARAMETERS,
         "Invalid deny client args, valid include: timeout*all, "
         "timeout*write, timeout*read; reconfig*all, reconfig*write, "
         "reconfig*read",
         "timeout*invalid"},
        {replica_envs::DENY_CLIENT_REQUEST, "reconfig*all", ERR_OK, "", "reconfig*all"},
        {replica_envs::DENY_CLIENT_REQUEST, "reconfig*write", ERR_OK, "", "reconfig*write"},
        {replica_envs::DENY_CLIENT_REQUEST, "reconfig*read", ERR_OK, "", "reconfig*read"},
        {replica_envs::DENY_CLIENT_REQUEST, "timeout*all", ERR_OK, "", "timeout*all"},
        {replica_envs::DENY_CLIENT_REQUEST, "timeout*write", ERR_OK, "", "timeout*write"},
        {replica_envs::DENY_CLIENT_REQUEST, "timeout*read", ERR_OK, "", "timeout*read"},
        {"not_exist_env",
         "500",
         ERR_INVALID_PARAMETERS,
         "app_env 'not_exist_env' is not supported",
         ""}};

    auto app = find_app(app_name);
    for (auto test : tests) {
        configuration_update_app_env_response response =
            update_app_envs(app_name, {test.env_key}, {test.env_value});

        ASSERT_EQ(test.err, response.err) << test.env_key << " : " << test.env_value;
        ASSERT_EQ(test.hint, response.hint_message);
        const auto it = app->envs.find(test.env_key);
        if (it != app->envs.end()) {
            ASSERT_EQ(test.expect_value, it->second);
        }
    }

    {
        // Make sure all rocksdb options of ROCKSDB_DYNAMIC_OPTIONS are tested.
        // Hint: Mainly verify the update_rocksdb_dynamic_options function.
        std::map<std::string, std::string> all_test_envs;
        for (const auto &test : tests) {
            all_test_envs[test.env_key] = test.env_value;
        }
        for (const auto &option : replica_envs::ROCKSDB_DYNAMIC_OPTIONS) {
            ASSERT_TRUE(all_test_envs.find(option) != all_test_envs.end());
        }
    }
}

} // namespace replication
} // namespace dsn
