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
#include <dsn/utility/singleton.h>

namespace pegasus {

class pegasus_value_manager : public dsn::utils::singleton<pegasus_value_manager>
{
public:
    void register_schema(pegasus_value_schema *schema);
    /// using raw value in rocksdb and data version stored in meta column family to get data version
    pegasus_value_schema *get_value_schema(uint32_t meta_cf_data_version,
                                           dsn::string_view value) const;
    pegasus_value_schema *get_value_schema(uint32_t version) const;
    pegasus_value_schema *get_latest_value_schema() const;

private:
    pegasus_value_manager() = default;
    friend class dsn::utils::singleton<pegasus_value_manager>;

    std::map<pegasus_data_version, pegasus_value_schema *> _schemas;
};
} // namespace pegasus
