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

    rocksdb::Slice slice;
    hashkey_pattern_rule rule;
    for (const auto &test : tests) {
        rule.match_type = test.match_type;
        rule.pattern = test.pattern;
        ASSERT_EQ(rule.match(test.hashkey, "", slice), test.match);
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

    rocksdb::Slice slice;
    sortkey_pattern_rule rule;
    for (const auto &test : tests) {
        rule.match_type = test.match_type;
        rule.pattern = test.pattern;
        ASSERT_EQ(rule.match("", test.sortkey, slice), test.match);
    }
}

} // namespace server
} // namespace pegasus
