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

#pragma once

#include <rocksdb/slice.h>
#include <dsn/utility/enum_helper.h>
#include <gtest/gtest.h>

namespace pegasus {
namespace server {
/** compaction_filter_rule represents the compaction rule to filter the keys which are stored in
 * rocksdb. */
class compaction_filter_rule
{
public:
    virtual ~compaction_filter_rule() = default;

    // TODO(zhaoliwei): we can use `value_filed` to replace existing_value in the later,
    // after the refactor of value schema
    virtual bool match(const std::string &hash_key,
                       const std::string &sort_key,
                       const rocksdb::Slice &existing_value) const = 0;
};

enum string_match_type
{
    SMT_MATCH_ANYWHERE,
    SMT_MATCH_PREFIX,
    SMT_MATCH_POSTFIX,
    SMT_INVALID,
};
ENUM_BEGIN(string_match_type, SMT_INVALID)
ENUM_REG(SMT_MATCH_ANYWHERE)
ENUM_REG(SMT_MATCH_PREFIX)
ENUM_REG(SMT_MATCH_POSTFIX)
ENUM_END(string_match_type)

class hashkey_pattern_rule : public compaction_filter_rule
{
public:
    hashkey_pattern_rule() = default;

    bool match(const std::string &hash_key,
               const std::string &sort_key,
               const rocksdb::Slice &existing_value) const;

private:
    std::string pattern;
    string_match_type match_type;

    FRIEND_TEST(hashkey_pattern_rule_test, match);
    FRIEND_TEST(delete_key_test, filter);
    FRIEND_TEST(update_ttl_test, filter);
    FRIEND_TEST(compaction_filter_operation_test, all_rules_match);
};

class sortkey_pattern_rule : public compaction_filter_rule
{
public:
    sortkey_pattern_rule() = default;

    bool match(const std::string &hash_key,
               const std::string &sort_key,
               const rocksdb::Slice &existing_value) const;

private:
    std::string pattern;
    string_match_type match_type;

    FRIEND_TEST(sortkey_pattern_rule_test, match);
    FRIEND_TEST(compaction_filter_operation_test, all_rules_match);
};

class ttl_range_rule : public compaction_filter_rule
{
public:
    explicit ttl_range_rule(uint32_t pegasus_data_version);

    bool match(const std::string &hash_key,
               const std::string &sort_key,
               const rocksdb::Slice &existing_value) const;

private:
    // = 0 means no limit
    uint32_t start_ttl;
    uint32_t stop_ttl;
    uint32_t pegasus_data_version;

    FRIEND_TEST(ttl_range_rule_test, match);
    FRIEND_TEST(compaction_filter_operation_test, all_rules_match);
};
} // namespace server
} // namespace pegasus
