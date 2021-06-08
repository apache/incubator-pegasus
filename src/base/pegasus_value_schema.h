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

#include <string>
#include <vector>

#include <rocksdb/slice.h>
#include <dsn/utility/string_view.h>

#include "value_field.h"

namespace pegasus {
enum data_version
{
    VERSION_0 = 0,
    VERSION_1 = 1,
    VERSION_2 = 2,
    VERSION_COUNT,
    VERSION_MAX = VERSION_2,
};

struct value_params
{
    value_params(std::string &buf, std::vector<rocksdb::Slice> &slices)
        : write_buf(buf), write_slices(slices)
    {
    }

    std::array<std::unique_ptr<value_field>, FIELD_COUNT> fields;
    // write_buf and write_slices are transferred from `value_generator`, which are used to
    // prevent data copy
    std::string &write_buf;
    std::vector<rocksdb::Slice> &write_slices;
};

class value_schema
{
public:
    virtual ~value_schema() = default;

    virtual std::unique_ptr<value_field> extract_field(dsn::string_view value,
                                                       value_field_type type) = 0;
    /// Extracts user value from the raw rocksdb value.
    /// In order to avoid data copy, the ownership of `raw_value` will be transferred
    /// into the returned blob value.
    virtual dsn::blob extract_user_data(std::string &&value) = 0;
    virtual void update_field(std::string &value, std::unique_ptr<value_field> field) = 0;
    virtual rocksdb::SliceParts generate_value(const value_params &params) = 0;

    virtual data_version version() const = 0;
};
} // namespace pegasus
