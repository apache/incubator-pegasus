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

#include <memory>
#include <vector>
#include "compaction_filter_rule.h"

namespace pegasus {
namespace server {

typedef std::vector<std::unique_ptr<compaction_filter_rule>> filter_rules;
/** compaction_operation represents the compaction operation. A compaction operation will be
 * executed when all the corresponding compaction rules are matched. */
class compaction_operation
{
public:
    compaction_operation(filter_rules &&rules, uint32_t pegasus_data_version)
        : rules(std::move(rules)), pegasus_data_version(pegasus_data_version)
    {
    }
    virtual ~compaction_operation() = 0;

    bool all_rules_match(const std::string &hash_key,
                         const std::string &sort_key,
                         const rocksdb::Slice &existing_value) const;
    /**
     * @return false indicates that this key-value should be removed
     * If you want to modify the existing_value, you can pass it back through new_value and
     * value_changed needs to be set to true in this case.
     */
    virtual bool filter(const std::string &hash_key,
                        const std::string &sort_key,
                        const rocksdb::Slice &existing_value,
                        std::string *new_value,
                        bool *value_changed) const = 0;

protected:
    filter_rules rules;
    uint32_t pegasus_data_version;
};

class delete_key : public compaction_operation
{
public:
    delete_key(filter_rules &&rules, uint32_t pegasus_data_version);

    bool filter(const std::string &hash_key,
                const std::string &sort_key,
                const rocksdb::Slice &existing_value,
                std::string *new_value,
                bool *value_changed) const;

private:
    FRIEND_TEST(delete_key_test, filter);
};
} // namespace server
} // namespace pegasus
