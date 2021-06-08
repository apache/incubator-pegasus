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

#include <rocksdb/slice.h>
#include <dsn/utility/string_view.h>

namespace pegasus {
/**
 * Helper class for generating value.
 *
 * NOTES:
 * 1. The instance of pegasus_value_generator must be alive while the returned SliceParts is.
 * The data of user_data must be alive while the returned SliceParts is, because
 * we do not copy it. And the returned SliceParts is only valid before the next invoking of
 * generate_value().
 * 2. Only the latest version of data is generated in this function, because we want to make
 * data with older version disappear gradually.
 */
class pegasus_value_generator
{
public:
    pegasus_value_generator() = default;
    ~pegasus_value_generator() = default;

    /// A higher level utility for generating value with latest version.
    rocksdb::SliceParts
    generate_value(dsn::string_view user_data, uint32_t expire_ts, uint64_t timetag);

private:
    std::string _write_buf;
    std::vector<rocksdb::Slice> _write_slices;
};
} // namespace pegasus
