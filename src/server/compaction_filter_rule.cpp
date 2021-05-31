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

#include <dsn/dist/fmt_logging.h>
#include <dsn/utility/string_view.h>
#include <dsn/c/api_utilities.h>

namespace pegasus {
namespace server {
bool string_pattern_match(const std::string &value,
                          string_match_type type,
                          const std::string &filter_pattern)
{
    if (filter_pattern.length() == 0)
        return false;
    if (value.length() < filter_pattern.length())
        return false;

    switch (type) {
    case string_match_type::SMT_MATCH_ANYWHERE:
        return dsn::string_view(value).find(filter_pattern) != dsn::string_view::npos;
    case string_match_type::SMT_MATCH_PREFIX:
        return memcmp(value.data(), filter_pattern.data(), filter_pattern.length()) == 0;
    case string_match_type::SMT_MATCH_POSTFIX:
        return memcmp(value.data() + value.length() - filter_pattern.length(),
                      filter_pattern.data(),
                      filter_pattern.length()) == 0;
    default:
        derror_f("invalid match type {}", type);
        return false;
    }
}

hashkey_pattern_rule::hashkey_pattern_rule(uint32_t pegasus_data_version) {}

bool hashkey_pattern_rule::match(const std::string &hash_key,
                                 const std::string &sort_key,
                                 const rocksdb::Slice &existing_value) const
{
    return string_pattern_match(hash_key, match_type, pattern);
}

} // namespace server
} // namespace pegasus
