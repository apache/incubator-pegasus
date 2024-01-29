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

#include <absl/strings/string_view.h>
#include <cstdint>

#include "base/pegasus_utils.h"
#include "base/pegasus_value_schema.h"
#include "compaction_operation.h"
#include "server/compaction_filter_rule.h"
#include "utils/fmt_logging.h"

namespace pegasus {
namespace server {
compaction_operation::~compaction_operation() = default;

bool compaction_operation::all_rules_match(absl::string_view hash_key,
                                           absl::string_view sort_key,
                                           absl::string_view existing_value) const
{
    if (rules.empty()) {
        return false;
    }

    for (const auto &rule : rules) {
        if (!rule->match(hash_key, sort_key, existing_value)) {
            return false;
        }
    }
    return true;
}

void compaction_operation::set_rules(filter_rules &&tmp_rules) { rules.swap(tmp_rules); }

delete_key::delete_key(filter_rules &&rules, uint32_t data_version)
    : compaction_operation(std::move(rules), data_version)
{
}

delete_key::delete_key(uint32_t data_version) : compaction_operation(data_version) {}

bool delete_key::filter(absl::string_view hash_key,
                        absl::string_view sort_key,
                        absl::string_view existing_value,
                        std::string *new_value,
                        bool *value_changed) const
{
    if (!all_rules_match(hash_key, sort_key, existing_value)) {
        return false;
    }
    return true;
}

update_ttl::update_ttl(filter_rules &&rules, uint32_t data_version)
    : compaction_operation(std::move(rules), data_version)
{
}

update_ttl::update_ttl(uint32_t data_version) : compaction_operation(data_version) {}

bool update_ttl::filter(absl::string_view hash_key,
                        absl::string_view sort_key,
                        absl::string_view existing_value,
                        std::string *new_value,
                        bool *value_changed) const
{
    if (!all_rules_match(hash_key, sort_key, existing_value)) {
        return false;
    }

    uint32_t new_ts = 0;
    switch (type) {
    case update_ttl_op_type::UTOT_FROM_NOW:
        new_ts = utils::epoch_now() + value;
        break;
    case update_ttl_op_type::UTOT_FROM_CURRENT: {
        auto ttl = pegasus_extract_expire_ts(data_version, existing_value);
        if (ttl == 0) {
            return false;
        }
        new_ts = value + ttl;
        break;
    }
    case update_ttl_op_type::UTOT_TIMESTAMP:
        // make it's seconds since 2016.01.01-00:00:00 GMT
        new_ts = value - pegasus::utils::epoch_begin;
        break;
    default:
        LOG_INFO("invalid update ttl operation type");
        return false;
    }

    *new_value = std::string(existing_value.data(), existing_value.size());
    pegasus_update_expire_ts(data_version, *new_value, new_ts);
    *value_changed = true;
    return false;
}

namespace internal {
struct json_helper
{
    struct user_specified_compaction_rule
    {
        filter_rule_type type;
        std::string params;
        DEFINE_JSON_SERIALIZATION(type, params)
    };

    struct user_specified_compaction_op
    {
        compaction_operation_type type;
        std::string params;
        std::vector<user_specified_compaction_rule> rules;
        DEFINE_JSON_SERIALIZATION(type, params, rules)
    };

    // key: filter_operation_type
    std::vector<user_specified_compaction_op> ops;
    DEFINE_JSON_SERIALIZATION(ops)
};
} // namespace internal

std::unique_ptr<compaction_filter_rule> create_compaction_filter_rule(filter_rule_type type,
                                                                      const std::string &params,
                                                                      uint32_t data_version)
{
    auto rule = dsn::utils::factory_store<compaction_filter_rule>::create(
        enum_to_string(type), dsn::PROVIDER_TYPE_MAIN, params, data_version);
    return std::unique_ptr<compaction_filter_rule>(rule);
}

filter_rules create_compaction_filter_rules(
    const std::vector<internal::json_helper::user_specified_compaction_rule> &rules,
    uint32_t data_version)
{
    filter_rules res;
    for (const auto &rule : rules) {
        auto operation_rule = create_compaction_filter_rule(rule.type, rule.params, data_version);
        if (operation_rule != nullptr) {
            res.emplace_back(std::move(operation_rule));
        }
    }
    return res;
}

compaction_operations create_compaction_operations(const std::string &json, uint32_t data_version)
{
    compaction_operations res;
    internal::json_helper compaction;
    if (!dsn::json::json_forwarder<internal::json_helper>::decode(
            dsn::blob::create_from_bytes(json.data(), json.size()), compaction)) {
        LOG_INFO("invalid user specified compaction format");
        return res;
    }

    for (const auto &op : compaction.ops) {
        filter_rules rules = create_compaction_filter_rules(op.rules, data_version);
        if (rules.size() == 0) {
            continue;
        }

        compaction_operation *operation = dsn::utils::factory_store<compaction_operation>::create(
            enum_to_string(op.type), dsn::PROVIDER_TYPE_MAIN, op.params, data_version);
        if (operation != nullptr) {
            operation->set_rules(std::move(rules));
            res.emplace_back(std::shared_ptr<compaction_operation>(operation));
        }
    }
    return res;
}

void register_compaction_operations()
{
    delete_key::register_component<delete_key>(enum_to_string(COT_DELETE));
    update_ttl::register_component<update_ttl>(enum_to_string(COT_UPDATE_TTL));
    register_compaction_filter_rules();
}
} // namespace server
} // namespace pegasus
