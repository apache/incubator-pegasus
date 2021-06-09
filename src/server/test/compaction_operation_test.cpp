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
#include <dsn/utility/smart_pointers.h>

namespace pegasus {
namespace server {

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
} // namespace server
} // namespace pegasus
