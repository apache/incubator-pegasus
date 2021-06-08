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

#include "pegasus_value_generator.h"
#include "value_schema_manager.h"
#include <dsn/utility/smart_pointers.h>

namespace pegasus {
/// A higher level utility for generating value with given version.
rocksdb::SliceParts pegasus_value_generator::generate_value(dsn::string_view user_data,
                                                            uint32_t expire_ts,
                                                            uint64_t timetag)
{
    value_params params(_write_buf, _write_slices);
    params.fields[value_field_type::EXPIRE_TIMESTAMP] =
        dsn::make_unique<expire_timestamp_field>(expire_ts);
    params.fields[value_field_type::TIME_TAG] = dsn::make_unique<time_tag_field>(timetag);
    params.fields[value_field_type::USER_DATA] = dsn::make_unique<user_data_field>(user_data);

    auto schema = value_schema_manager::instance().get_latest_value_schema();
    return schema->generate_value(params);
}
} // namespace pegasus
