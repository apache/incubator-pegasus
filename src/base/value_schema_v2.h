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
#include <memory>
#include <string>

#include "pegasus_value_schema.h"
#include "utils/blob.h"
#include "absl/strings/string_view.h"
#include "value_field.h"

namespace pegasus {
/**
 *  rocksdb value:
 *  |- 1bit -|- version(7bits) -|- expire_ts(4bytes) -|- timetag(8 bytes) -|- user value(bytes) -|
 */
class value_schema_v2 : public value_schema
{
public:
    value_schema_v2() = default;

    std::unique_ptr<value_field> extract_field(absl::string_view value,
                                               value_field_type type) override;
    dsn::blob extract_user_data(std::string &&value) override;
    void update_field(std::string &value, std::unique_ptr<value_field> field) override;
    rocksdb::SliceParts generate_value(const value_params &params) override;
    data_version version() const override { return data_version::VERSION_2; }

private:
    std::unique_ptr<value_field> extract_timestamp(absl::string_view value);
    std::unique_ptr<value_field> extract_time_tag(absl::string_view value);
    void update_expire_ts(std::string &value, std::unique_ptr<value_field> field);
};
} // namespace pegasus
