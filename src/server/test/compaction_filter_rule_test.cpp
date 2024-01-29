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

#include <rocksdb/slice.h>
#include <stdint.h>
#include <string>

#include "base/pegasus_utils.h"
#include "base/pegasus_value_schema.h"
#include "gtest/gtest.h"
#include "server/compaction_filter_rule.h"

namespace pegasus {
namespace server {

TEST(hashkey_pattern_rule_test, match)
{
    struct test_case
    {
        std::string hashkey;
        std::string pattern;
        string_match_type match_type;
        bool match;
    } tests[] = {
        {"sortkey", "", SMT_MATCH_ANYWHERE, false},
        {"hashkey", "hashkey", SMT_MATCH_ANYWHERE, true},
        {"hashkey", "shke", SMT_MATCH_ANYWHERE, true},
        {"hashkey", "hash", SMT_MATCH_ANYWHERE, true},
        {"hashkey", "key", SMT_MATCH_ANYWHERE, true},
        {"hashkey", "sortkey", SMT_MATCH_ANYWHERE, false},
        {"hashkey", "hashkey", SMT_MATCH_PREFIX, true},
        {"hashkey", "hash", SMT_MATCH_PREFIX, true},
        {"hashkey", "key", SMT_MATCH_PREFIX, false},
        {"hashkey", "sortkey", SMT_MATCH_PREFIX, false},
        {"hashkey", "hashkey", SMT_MATCH_POSTFIX, true},
        {"hashkey", "hash", SMT_MATCH_POSTFIX, false},
        {"hashkey", "key", SMT_MATCH_POSTFIX, true},
        {"hashkey", "sortkey", SMT_MATCH_POSTFIX, false},
        {"hash", "hashkey", SMT_MATCH_POSTFIX, false},
        {"hashkey", "hashkey", SMT_INVALID, false},
    };

    hashkey_pattern_rule rule;
    for (const auto &test : tests) {
        rule.match_type = test.match_type;
        rule.pattern = test.pattern;
        ASSERT_EQ(rule.match(test.hashkey, "", ""), test.match);
    }
}

TEST(sortkey_pattern_rule_test, match)
{
    struct test_case
    {
        std::string sortkey;
        std::string pattern;
        string_match_type match_type;
        bool match;
    } tests[] = {
        {"sortkey", "", SMT_MATCH_ANYWHERE, false},
        {"sortkey", "sortkey", SMT_MATCH_ANYWHERE, true},
        {"sortkey", "ort", SMT_MATCH_ANYWHERE, true},
        {"sortkey", "sort", SMT_MATCH_ANYWHERE, true},
        {"sortkey", "key", SMT_MATCH_ANYWHERE, true},
        {"sortkey", "hashkey", SMT_MATCH_ANYWHERE, false},
        {"sortkey", "sortkey", SMT_MATCH_PREFIX, true},
        {"sortkey", "sort", SMT_MATCH_PREFIX, true},
        {"sortkey", "key", SMT_MATCH_PREFIX, false},
        {"sortkey", "hashkey", SMT_MATCH_PREFIX, false},
        {"sortkey", "sortkey", SMT_MATCH_POSTFIX, true},
        {"sortkey", "sort", SMT_MATCH_POSTFIX, false},
        {"sortkey", "key", SMT_MATCH_POSTFIX, true},
        {"sortkey", "hashkey", SMT_MATCH_POSTFIX, false},
        {"sort", "sortkey", SMT_MATCH_POSTFIX, false},
        {"sortkey", "sortkey", SMT_INVALID, false},
    };

    sortkey_pattern_rule rule;
    for (const auto &test : tests) {
        rule.match_type = test.match_type;
        rule.pattern = test.pattern;
        ASSERT_EQ(rule.match("", test.sortkey, ""), test.match);
    }
}

TEST(ttl_range_rule_test, match)
{
    struct test_case
    {
        int32_t start_ttl;
        int32_t stop_ttl;
        int32_t expire_ttl;
        bool match;
    } tests[] = {
        {100, 1000, 1100, false},
        {100, 1000, 500, true},
        {100, 1000, 20, false},
        {100, 1000, 0, false},
        {1000, 100, 1100, false},
        {1000, 100, 500, false},
        {1000, 100, 20, false},
        {1000, 100, 0, false},
        {0, 1000, 500, true},
        {1000, 0, 500, false},
        {0, 0, 0, true},
    };

    const uint32_t data_version = 1;
    ttl_range_rule rule(data_version);
    pegasus_value_generator gen;
    auto now_ts = utils::epoch_now();
    for (const auto &test : tests) {
        rule.start_ttl = test.start_ttl;
        rule.stop_ttl = test.stop_ttl;
        rocksdb::SliceParts svalue =
            gen.generate_value(data_version, "", test.expire_ttl + now_ts, 0);
        ASSERT_EQ(rule.match("", "", svalue.parts[0].ToString()), test.match);
    }
}

TEST(compaction_filter_rule_test, create)
{
    const uint32_t data_version = 1;

    // ttl_range_rule
    std::string params_json = R"({"start_ttl":1000,"stop_ttl":2000})";
    compaction_filter_rule *rule =
        compaction_filter_rule::create<ttl_range_rule>(params_json, data_version);
    ttl_range_rule *expire_rule = static_cast<ttl_range_rule *>(rule);
    ASSERT_EQ(expire_rule->start_ttl, 1000);
    ASSERT_EQ(expire_rule->stop_ttl, 2000);
    delete expire_rule;

    // sortkey_pattern_rule
    params_json = R"({"pattern":"sortkey","match_type":"SMT_MATCH_PREFIX"})";
    rule = compaction_filter_rule::create<sortkey_pattern_rule>(params_json, data_version);
    sortkey_pattern_rule *sortkey_rule = static_cast<sortkey_pattern_rule *>(rule);
    ASSERT_EQ(sortkey_rule->pattern, "sortkey");
    ASSERT_EQ(sortkey_rule->match_type, SMT_MATCH_PREFIX);
    delete sortkey_rule;

    // hashkey_pattern_rule
    params_json = R"({"pattern":"hashkey","match_type":"SMT_MATCH_PREFIX"})";
    rule = compaction_filter_rule::create<hashkey_pattern_rule>(params_json, data_version);
    hashkey_pattern_rule *hashkey_rule = static_cast<hashkey_pattern_rule *>(rule);
    ASSERT_EQ(hashkey_rule->pattern, "hashkey");
    ASSERT_EQ(hashkey_rule->match_type, SMT_MATCH_PREFIX);
    delete hashkey_rule;

    // invalid sortkey_pattern_rule
    params_json = R"({"_patternxxx":"sortkey","match_type":"SMT_MATCH_PREFIX"})";
    rule = compaction_filter_rule::create<sortkey_pattern_rule>(params_json, data_version);
    ASSERT_EQ(rule, nullptr);
    params_json = R"({"pattern":"sortkey","_match_typexxx":"SMT_MATCH_PREFIX"})";
    rule = compaction_filter_rule::create<sortkey_pattern_rule>(params_json, data_version);
    ASSERT_EQ(rule, nullptr);
}
} // namespace server
} // namespace pegasus
