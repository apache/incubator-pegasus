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

#include "utils/enum_helper.h"
#include "common/json_helper.h"
#include <gtest/gtest.h>
#include "base/pegasus_value_schema.h"

namespace pegasus {
namespace server {
enum filter_rule_type
{
    FRT_HASHKEY_PATTERN = 0,
    FRT_SORTKEY_PATTERN,
    FRT_TTL_RANGE,
    FRT_INVALID,
};
ENUM_BEGIN(filter_rule_type, FRT_INVALID)
ENUM_REG(FRT_HASHKEY_PATTERN)
ENUM_REG(FRT_SORTKEY_PATTERN)
ENUM_REG(FRT_TTL_RANGE)
ENUM_END(filter_rule_type)

ENUM_TYPE_SERIALIZATION(filter_rule_type, FRT_INVALID)

/** compaction_filter_rule represents the compaction rule to filter the keys which are stored in
 * rocksdb. */
class compaction_filter_rule
{
public:
    template <typename T>
    static compaction_filter_rule *create(const std::string &params, uint32_t data_version)
    {
        T *rule = new T(data_version);
        if (!dsn::json::json_forwarder<T>::decode(
                dsn::blob::create_from_bytes(params.data(), params.size()), *rule)) {
            delete rule;
            return nullptr;
        }
        return rule;
    }

    template <typename T>
    static void register_component(const char *name)
    {
        dsn::utils::factory_store<compaction_filter_rule>::register_factory(
            name, create<T>, dsn::PROVIDER_TYPE_MAIN);
    }
    virtual ~compaction_filter_rule() = default;

    // TODO(zhaoliwei): we can use `value_filed` to replace existing_value in the later,
    // after the refactor of value schema
    virtual bool match(dsn::string_view hash_key,
                       dsn::string_view sort_key,
                       dsn::string_view existing_value) const = 0;
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

ENUM_TYPE_SERIALIZATION(string_match_type, SMT_INVALID)

class hashkey_pattern_rule : public compaction_filter_rule
{
public:
    hashkey_pattern_rule(uint32_t data_version = VERSION_MAX);

    bool match(dsn::string_view hash_key,
               dsn::string_view sort_key,
               dsn::string_view existing_value) const;
    DEFINE_JSON_SERIALIZATION(pattern, match_type)

private:
    std::string pattern;
    string_match_type match_type;

    FRIEND_TEST(hashkey_pattern_rule_test, match);
    FRIEND_TEST(delete_key_test, filter);
    FRIEND_TEST(update_ttl_test, filter);
    FRIEND_TEST(compaction_filter_operation_test, all_rules_match);
    FRIEND_TEST(compaction_filter_rule_test, create);
    FRIEND_TEST(compaction_filter_operation_test, create_operations);
};

class sortkey_pattern_rule : public compaction_filter_rule
{
public:
    sortkey_pattern_rule(uint32_t data_version = VERSION_MAX);

    bool match(dsn::string_view hash_key,
               dsn::string_view sort_key,
               dsn::string_view existing_value) const;
    DEFINE_JSON_SERIALIZATION(pattern, match_type)

private:
    std::string pattern;
    string_match_type match_type;

    FRIEND_TEST(sortkey_pattern_rule_test, match);
    FRIEND_TEST(compaction_filter_operation_test, all_rules_match);
    FRIEND_TEST(compaction_filter_rule_test, create);
    FRIEND_TEST(compaction_filter_operation_test, create_operations);
};

class ttl_range_rule : public compaction_filter_rule
{
public:
    explicit ttl_range_rule(uint32_t data_version);

    bool match(dsn::string_view hash_key,
               dsn::string_view sort_key,
               dsn::string_view existing_value) const;
    DEFINE_JSON_SERIALIZATION(start_ttl, stop_ttl)

private:
    // = 0 means no limit
    uint32_t start_ttl;
    uint32_t stop_ttl;
    uint32_t data_version;

    FRIEND_TEST(ttl_range_rule_test, match);
    FRIEND_TEST(compaction_filter_operation_test, all_rules_match);
    FRIEND_TEST(compaction_filter_rule_test, create);
    FRIEND_TEST(compaction_filter_operation_test, create_operations);
};

void register_compaction_filter_rules();
} // namespace server
} // namespace pegasus
