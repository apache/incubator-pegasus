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

#include "base/pegasus_utils.h"
#include "base/pegasus_value_schema.h"
#include "compaction_operation.h"

namespace pegasus {
namespace server {
compaction_operation::~compaction_operation() = default;

bool compaction_operation::all_rules_match(const std::string &hash_key,
                                           const std::string &sort_key,
                                           const rocksdb::Slice &existing_value) const
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

delete_key::delete_key(filter_rules &&rules, uint32_t pegasus_data_version)
    : compaction_operation(std::move(rules), pegasus_data_version)
{
}

bool delete_key::filter(const std::string &hash_key,
                        const std::string &sort_key,
                        const rocksdb::Slice &existing_value,
                        std::string *new_value,
                        bool *value_changed) const
{
    if (!all_rules_match(hash_key, sort_key, existing_value)) {
        return false;
    }
    return true;
}

update_ttl::update_ttl(filter_rules &&rules, uint32_t pegasus_data_version)
    : compaction_operation(std::move(rules), pegasus_data_version)
{
}

bool update_ttl::filter(const std::string &hash_key,
                        const std::string &sort_key,
                        const rocksdb::Slice &existing_value,
                        std::string *new_value,
                        bool *value_changed) const
{
    if (!all_rules_match(hash_key, sort_key, existing_value)) {
        return false;
    }

    uint32_t new_ts = 0;
    switch (type) {
    case update_ttl_op_type::UTOT_FROM_NOW:
        new_ts = utils::epoch_now() + timestamp;
        break;
    case update_ttl_op_type::UTOT_FROM_CURRENT: {
        auto ttl =
            pegasus_extract_expire_ts(pegasus_data_version, utils::to_string_view(existing_value));
        if (ttl == 0) {
            return false;
        }
        new_ts = timestamp + ttl;
        break;
    }
    case update_ttl_op_type::UTOT_TIMESTAMP:
        // make it's seconds since 2016.01.01-00:00:00 GMT
        new_ts = timestamp - pegasus::utils::epoch_begin;
        break;
    default:
        ddebug("invalid update ttl operation type");
        return false;
    }

    *new_value = existing_value.ToString();
    pegasus_update_expire_ts(pegasus_data_version, *new_value, new_ts);
    *value_changed = true;
    return false;
}

} // namespace server
} // namespace pegasus
