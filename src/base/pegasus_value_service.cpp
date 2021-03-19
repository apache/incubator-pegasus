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

#include "pegasus_value_service.h"

namespace pegasus {

pegasus_value_schema *pegasus_value_service::get_value_schema(uint32_t meta_store_data_version,
                                                              dsn::string_view value) const
{
    // TBD(zlw)
}

pegasus_value_schema *pegasus_value_service::get_value_schema(uint32_t version) const
{
    // TBD(zlw)
}

pegasus_value_schema *pegasus_value_service::get_latest_value_schema() const
{
    // TBD(zlw)
}

void pegasus_value_service::register_schema(pegasus_value_schema *schema)
{
    // TBD(zlw)
}

void register_value_schemas()
{
    // TBD(zlw)
}

struct value_schemas_registerer
{
    value_schemas_registerer() { register_value_schemas(); }
};
static value_schemas_registerer value_schemas_reg;

} // namespace pegasus
