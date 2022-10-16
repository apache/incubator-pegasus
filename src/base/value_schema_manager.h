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

#include "pegasus_value_schema.h"
#include "utils/singleton.h"

namespace pegasus {

class value_schema_manager : public dsn::utils::singleton<value_schema_manager>
{
public:
    void register_schema(std::unique_ptr<value_schema> schema);
    /// using the raw value in rocksdb and data version stored in meta column family to get data
    /// version
    value_schema *get_value_schema(uint32_t meta_cf_data_version, dsn::string_view value) const;
    value_schema *get_value_schema(uint32_t version) const;
    value_schema *get_latest_value_schema() const;

private:
    value_schema_manager();
    ~value_schema_manager() = default;
    friend class dsn::utils::singleton<value_schema_manager>;

    std::array<std::unique_ptr<value_schema>, data_version::VERSION_COUNT> _schemas;
};
} // namespace pegasus
