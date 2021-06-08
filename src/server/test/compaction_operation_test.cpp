/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <gtest/gtest.h>
#include "server/compaction_operation.h"
#include "server/compaction_filter_rule.h"
#include "base/pegasus_value_schema.h"
#include "base/pegasus_utils.h"
#include <dsn/utility/smart_pointers.h>

namespace pegasus {
namespace server {

TEST(compaction_filter_operation_test, all_rules_match)
{
    struct test_case
    {
        bool all_match;
        std::string hashkey;
        std::string sortkey;
        int32_t expire_ttl;
        // hashkey_rule
        std::string hashkey_pattern;
        string_match_type hashkey_match_type;
        // sortkey_rule
        std::string sortkey_pattern;
        string_match_type sortkey_match_type;
        // ttl_range_rule
        int32_t start_ttl;
        int32_t stop_ttl;
    } tests[] = {
        {true,
         "hashkey",
         "sortkey",
         1000,
         "hashkey",
         SMT_MATCH_ANYWHERE,
         "sortkey",
         SMT_MATCH_ANYWHERE,
         100,
         2000},
        {false,
         "hash_key",
         "sortkey",
         1000,
         "hashkey",
         SMT_MATCH_ANYWHERE,
         "sortkey",
         SMT_MATCH_ANYWHERE,
         100,
         2000},
        {false,
         "hashkey",
         "sort_key",
         1000,
         "hashkey",
         SMT_MATCH_ANYWHERE,
         "sortkey",
         SMT_MATCH_ANYWHERE,
         100,
         2000},
        {false,
         "hashkey",
         "sortkey",
         10000,
         "hashkey",
         SMT_MATCH_ANYWHERE,
         "sortkey",
         SMT_MATCH_ANYWHERE,
         100,
         2000},
    };

    uint32_t data_version = 1;
    filter_rules rules;
    rules.push_back(dsn::make_unique<hashkey_pattern_rule>());
    rules.push_back(dsn::make_unique<sortkey_pattern_rule>());
    rules.push_back(dsn::make_unique<ttl_range_rule>(data_version));
    delete_key update_operation(std::move(rules), data_version);
    pegasus_value_generator gen;
    auto now_ts = utils::epoch_now();
    for (const auto &test : tests) {
        auto hash_rule = static_cast<hashkey_pattern_rule *>(update_operation.rules[0].get());
        auto sort_rule = static_cast<sortkey_pattern_rule *>(update_operation.rules[1].get());
        auto ttl_rule = static_cast<ttl_range_rule *>(update_operation.rules[2].get());

        hash_rule->pattern = test.hashkey_pattern;
        hash_rule->match_type = test.hashkey_match_type;
        sort_rule->pattern = test.sortkey_pattern;
        sort_rule->match_type = test.sortkey_match_type;
        ttl_rule->start_ttl = test.start_ttl;
        ttl_rule->stop_ttl = test.stop_ttl;

        rocksdb::SliceParts svalue =
            gen.generate_value(data_version, "", test.expire_ttl + now_ts, 0);
        ASSERT_EQ(update_operation.all_rules_match(test.hashkey, test.sortkey, svalue.parts[0]),
                  test.all_match);
    }

    // all_rules_match will return false if there is no rule in this operation
    update_ttl no_rule_operation({}, data_version);
    ASSERT_EQ(no_rule_operation.all_rules_match("hash", "sort", rocksdb::Slice()), false);
}

TEST(delete_key_test, filter)
{
    struct test_case
    {
        bool filter;
        std::string hashkey;
        // hashkey_rule
        std::string hashkey_pattern;
        string_match_type hashkey_match_type;
    } tests[] = {
        {true, "hashkey", "hashkey", SMT_MATCH_ANYWHERE},
        {false, "hashkey", "hashkey111", SMT_MATCH_ANYWHERE},
    };

    uint32_t data_version = 1;
    filter_rules rules;
    rules.push_back(dsn::make_unique<hashkey_pattern_rule>());
    delete_key delete_operation(std::move(rules), data_version);
    for (const auto &test : tests) {
        auto hash_rule = static_cast<hashkey_pattern_rule *>(delete_operation.rules.begin()->get());
        hash_rule->pattern = test.hashkey_pattern;
        hash_rule->match_type = test.hashkey_match_type;
        ASSERT_EQ(test.filter,
                  delete_operation.filter(test.hashkey, "", rocksdb::Slice(), nullptr, nullptr));
    }
}

TEST(update_ttl_test, filter)
{
    struct test_case
    {
        bool value_changed;
        uint32_t expect_ts;
        std::string hashkey;
        uint32_t expire_ts;
        // hashkey_rule
        std::string hashkey_pattern;
        string_match_type hashkey_match_type;
        // operation
        update_ttl_op_type op_type;
        uint32_t timestamp;
    } tests[] = {
        {true, 1000, "hashkey", 300, "hashkey", SMT_MATCH_ANYWHERE, UTOT_FROM_NOW, 1000},
        {false, 0, "hashkey", 0, "hashkey", SMT_MATCH_ANYWHERE, UTOT_FROM_CURRENT, 1000},
        {true, 1300, "hashkey", 300, "hashkey", SMT_MATCH_ANYWHERE, UTOT_FROM_CURRENT, 1000},
        {true,
         1000 + pegasus::utils::epoch_begin,
         "hashkey",
         300,
         "hashkey",
         SMT_MATCH_ANYWHERE,
         UTOT_TIMESTAMP,
         1000 + pegasus::utils::epoch_begin},
        {false,
         1000 + pegasus::utils::epoch_begin,
         "hashkey",
         300,
         "hashkey111",
         SMT_MATCH_ANYWHERE,
         UTOT_TIMESTAMP,
         1000 + pegasus::utils::epoch_begin},
    };

    uint32_t data_version = 1;
    filter_rules rules;
    rules.push_back(dsn::make_unique<hashkey_pattern_rule>());
    update_ttl update_operation(std::move(rules), data_version);
    pegasus_value_generator gen;
    for (const auto &test : tests) {
        auto hash_rule = static_cast<hashkey_pattern_rule *>(update_operation.rules.begin()->get());
        hash_rule->pattern = test.hashkey_pattern;
        hash_rule->match_type = test.hashkey_match_type;
        update_operation.timestamp = test.timestamp;
        update_operation.type = test.op_type;

        std::string new_value;
        bool value_changed = false;
        rocksdb::SliceParts svalue = gen.generate_value(data_version, "", test.expire_ts, 0);
        uint32_t before_ts = utils::epoch_now();
        ASSERT_EQ(
            false,
            update_operation.filter(test.hashkey, "", svalue.parts[0], &new_value, &value_changed));
        ASSERT_EQ(test.value_changed, value_changed);
        if (value_changed) {
            uint32_t new_ts = pegasus_extract_expire_ts(data_version, new_value);
            switch (test.op_type) {
            case UTOT_TIMESTAMP:
                ASSERT_EQ(new_ts + pegasus::utils::epoch_begin, test.expect_ts);
                break;
            case UTOT_FROM_CURRENT:
                ASSERT_EQ(new_ts, test.expect_ts);
                break;
            case UTOT_FROM_NOW: {
                uint32_t after_ts = utils::epoch_now();
                ASSERT_GE(new_ts, test.expect_ts + before_ts);
                ASSERT_LE(new_ts, test.expect_ts + after_ts);
                break;
            }
            default:
                break;
            }
        }
    }
}
} // namespace server
} // namespace pegasus
