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

#include "value_schema_manager.h"
#include "value_schema_v0.h"

namespace pegasus {
void value_schema_manager::register_schema(std::unique_ptr<value_schema> schema)
{
    _schemas[schema->version()] = std::move(schema);
}

value_schema *value_schema_manager::get_value_schema(uint32_t meta_cf_data_version,
                                                     dsn::string_view value) const
{
    /// TBD(zlw)
    return nullptr;
}

value_schema *value_schema_manager::get_value_schema(uint32_t version) const
{
    if (version >= _schemas.size()) {
        return nullptr;
    }
    return _schemas[version].get();
}

value_schema *value_schema_manager::get_latest_value_schema() const
{
    return _schemas.rbegin()->get();
}

/**
 * If someone wants to add a new data version, he only need to implement the new value schema,
 * and register it here.
 */
void register_value_schemas()
{
    value_schema_manager::instance().register_schema(dsn::make_unique<value_schema_v0>());
    /// TBD(zlw)
}

struct value_schemas_registerer
{
    value_schemas_registerer() { register_value_schemas(); }
};
static value_schemas_registerer value_schemas_reg;
} // namespace pegasus
