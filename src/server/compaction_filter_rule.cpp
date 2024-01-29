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

#include "compaction_filter_rule.h"

#include "base/pegasus_utils.h"
#include "base/pegasus_value_schema.h"
#include "utils/fmt_logging.h"
#include "absl/strings/string_view.h"
#include "utils/strings.h"

namespace pegasus {
namespace server {
bool string_pattern_match(absl::string_view value,
                          string_match_type type,
                          absl::string_view filter_pattern)
{
    if (filter_pattern.empty())
        return false;
    if (value.length() < filter_pattern.length())
        return false;

    switch (type) {
    case string_match_type::SMT_MATCH_ANYWHERE:
        return value.find(filter_pattern) != absl::string_view::npos;
    case string_match_type::SMT_MATCH_PREFIX:
        return dsn::utils::mequals(value.data(), filter_pattern.data(), filter_pattern.length());
    case string_match_type::SMT_MATCH_POSTFIX:
        return dsn::utils::mequals(value.data() + value.length() - filter_pattern.length(),
                                   filter_pattern.data(),
                                   filter_pattern.length());
    default:
        LOG_ERROR("invalid match type {}", type);
        return false;
    }
}

hashkey_pattern_rule::hashkey_pattern_rule(uint32_t data_version) {}

bool hashkey_pattern_rule::match(absl::string_view hash_key,
                                 absl::string_view sort_key,
                                 absl::string_view existing_value) const
{
    return string_pattern_match(hash_key, match_type, pattern);
}

sortkey_pattern_rule::sortkey_pattern_rule(uint32_t data_version) {}

bool sortkey_pattern_rule::match(absl::string_view hash_key,
                                 absl::string_view sort_key,
                                 absl::string_view existing_value) const
{
    return string_pattern_match(sort_key, match_type, pattern);
}

ttl_range_rule::ttl_range_rule(uint32_t data_version) : data_version(data_version) {}

bool ttl_range_rule::match(absl::string_view hash_key,
                           absl::string_view sort_key,
                           absl::string_view existing_value) const
{
    uint32_t expire_ts = pegasus_extract_expire_ts(data_version, existing_value);
    // if start_ttl and stop_ttl = 0, it means we want to delete keys which have no ttl
    if (0 == expire_ts && 0 == start_ttl && 0 == stop_ttl) {
        return true;
    }

    auto now_ts = utils::epoch_now();
    if (start_ttl + now_ts <= expire_ts && stop_ttl + now_ts >= expire_ts) {
        return true;
    }
    return false;
}

void register_compaction_filter_rules()
{
    ttl_range_rule::register_component<ttl_range_rule>(enum_to_string(FRT_TTL_RANGE));
    sortkey_pattern_rule::register_component<sortkey_pattern_rule>(
        enum_to_string(FRT_SORTKEY_PATTERN));
    hashkey_pattern_rule::register_component<hashkey_pattern_rule>(
        enum_to_string(FRT_HASHKEY_PATTERN));
}
} // namespace server
} // namespace pegasus
