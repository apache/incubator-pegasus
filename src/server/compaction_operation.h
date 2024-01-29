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

#include <gtest/gtest_prod.h>
#include <stdint.h>
#include <algorithm>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/string_view.h"
#include "common/json_helper.h"
#include "compaction_filter_rule.h"
#include "utils/blob.h"
#include "utils/enum_helper.h"
#include "utils/factory_store.h"

namespace pegasus {
namespace server {
enum compaction_operation_type
{
    COT_UPDATE_TTL,
    COT_DELETE,
    COT_INVALID,
};
ENUM_BEGIN(compaction_operation_type, COT_INVALID)
ENUM_REG(COT_UPDATE_TTL)
ENUM_REG(COT_DELETE)
ENUM_END(compaction_operation_type)

ENUM_TYPE_SERIALIZATION(compaction_operation_type, COT_INVALID)

typedef std::vector<std::unique_ptr<compaction_filter_rule>> filter_rules;
/** compaction_operation represents the compaction operation. A compaction operation will be
 * executed when all the corresponding compaction rules are matched. */
class compaction_operation
{
public:
    template <typename T>
    static compaction_operation *create(const std::string &params, uint32_t data_version)
    {
        return T::creator(params, data_version);
    }

    template <typename T>
    static void register_component(const char *name)
    {
        dsn::utils::factory_store<compaction_operation>::register_factory(
            name, create<T>, dsn::PROVIDER_TYPE_MAIN);
    }

    compaction_operation(filter_rules &&rules, uint32_t data_version)
        : rules(std::move(rules)), data_version(data_version)
    {
    }
    explicit compaction_operation(uint32_t data_version) : data_version(data_version) {}
    virtual ~compaction_operation() = 0;

    bool all_rules_match(absl::string_view hash_key,
                         absl::string_view sort_key,
                         absl::string_view existing_value) const;
    void set_rules(filter_rules &&rules);
    /**
     * @return false indicates that this key-value should be removed
     * If you want to modify the existing_value, you can pass it back through new_value and
     * value_changed needs to be set to true in this case.
     */
    virtual bool filter(absl::string_view hash_key,
                        absl::string_view sort_key,
                        absl::string_view existing_value,
                        std::string *new_value,
                        bool *value_changed) const = 0;

protected:
    filter_rules rules;
    uint32_t data_version;
};

class delete_key : public compaction_operation
{
public:
    static compaction_operation *creator(const std::string &params, uint32_t data_version)
    {
        return new delete_key(data_version);
    }

    delete_key(filter_rules &&rules, uint32_t data_version);
    explicit delete_key(uint32_t data_version);

    bool filter(absl::string_view hash_key,
                absl::string_view sort_key,
                absl::string_view existing_value,
                std::string *new_value,
                bool *value_changed) const;

private:
    FRIEND_TEST(delete_key_test, filter);
    FRIEND_TEST(compaction_filter_operation_test, all_rules_match);
    FRIEND_TEST(compaction_filter_operation_test, create_operations);
};

enum update_ttl_op_type
{
    // update ttl to epoch_now() + value
    UTOT_FROM_NOW,
    // update ttl to {current ttl in rocksdb value} + value
    UTOT_FROM_CURRENT,
    // update ttl to value - time(nullptr), which means this key will expire at the
    // timestamp of {value}
    UTOT_TIMESTAMP,
    UTOT_INVALID,
};
ENUM_BEGIN(update_ttl_op_type, UTOT_INVALID)
ENUM_REG(UTOT_FROM_NOW)
ENUM_REG(UTOT_FROM_CURRENT)
ENUM_REG(UTOT_TIMESTAMP)
ENUM_END(update_ttl_op_type)

ENUM_TYPE_SERIALIZATION(update_ttl_op_type, UTOT_INVALID)

class update_ttl : public compaction_operation
{
public:
    static compaction_operation *creator(const std::string &params, uint32_t data_version)
    {
        update_ttl *operation = new update_ttl(data_version);
        if (!dsn::json::json_forwarder<update_ttl>::decode(
                dsn::blob::create_from_bytes(params.data(), params.size()), *operation)) {
            delete operation;
            return nullptr;
        }
        return operation;
    }

    update_ttl(filter_rules &&rules, uint32_t data_version);
    explicit update_ttl(uint32_t data_version);

    bool filter(absl::string_view hash_key,
                absl::string_view sort_key,
                absl::string_view existing_value,
                std::string *new_value,
                bool *value_changed) const;
    DEFINE_JSON_SERIALIZATION(type, value)

private:
    update_ttl_op_type type;
    uint32_t value;

    FRIEND_TEST(update_ttl_test, filter);
    FRIEND_TEST(compaction_filter_operation_test, creator);
    FRIEND_TEST(compaction_filter_operation_test, create_operations);
};

typedef std::vector<std::shared_ptr<compaction_operation>> compaction_operations;
compaction_operations create_compaction_operations(const std::string &json, uint32_t data_version);
void register_compaction_operations();
} // namespace server
} // namespace pegasus
