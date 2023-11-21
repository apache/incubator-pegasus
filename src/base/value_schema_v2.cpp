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

#include "value_schema_v2.h"

#include <absl/strings/string_view.h>
#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <array>
#include <utility>
#include <vector>

#include "utils/endians.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"

namespace pegasus {

std::unique_ptr<value_field> value_schema_v2::extract_field(absl::string_view value,
                                                            value_field_type type)
{
    std::unique_ptr<value_field> field = nullptr;
    switch (type) {
    case value_field_type::EXPIRE_TIMESTAMP:
        field = extract_timestamp(value);
        break;
    case value_field_type::TIME_TAG:
        field = extract_time_tag(value);
        break;
    default:
        CHECK(false, "Unsupported field type: {}", type);
    }
    return field;
}

dsn::blob value_schema_v2::extract_user_data(std::string &&value)
{
    auto ret = dsn::blob::create_from_bytes(std::move(value));
    static const auto USER_DATA_OFFSET = sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint64_t);
    return ret.range(USER_DATA_OFFSET);
}

void value_schema_v2::update_field(std::string &value, std::unique_ptr<value_field> field)
{
    auto type = field->type();
    switch (field->type()) {
    case value_field_type::EXPIRE_TIMESTAMP:
        update_expire_ts(value, std::move(field));
        break;
    default:
        CHECK(false, "Unsupported update field type: {}", type);
    }
}

rocksdb::SliceParts value_schema_v2::generate_value(const value_params &params)
{
    auto expire_ts_field = static_cast<expire_timestamp_field *>(
        params.fields[value_field_type::EXPIRE_TIMESTAMP].get());
    auto timetag_field =
        static_cast<time_tag_field *>(params.fields[value_field_type::TIME_TAG].get());
    auto data_field =
        static_cast<user_data_field *>(params.fields[value_field_type::USER_DATA].get());
    if (dsn_unlikely(expire_ts_field == nullptr || data_field == nullptr ||
                     timetag_field == nullptr)) {
        CHECK(false, "USER_DATA or EXPIRE_TIMESTAMP or TIME_TAG is not provided");
        return {nullptr, 0};
    }

    params.write_buf.resize(sizeof(uint8_t) + sizeof(uint32_t) + sizeof(uint64_t));
    dsn::data_output output(params.write_buf);
    output.write_u8(0x80 | data_version::VERSION_2); // version
    output.write_u32(expire_ts_field->expire_ts);    // expire_ts
    output.write_u64(timetag_field->time_tag);       // timetag
    params.write_slices.clear();
    params.write_slices.emplace_back(params.write_buf.data(), params.write_buf.size());

    absl::string_view user_data = data_field->user_data;
    if (user_data.length() > 0) {
        params.write_slices.emplace_back(user_data.data(), user_data.length());
    }
    return {&params.write_slices[0], static_cast<int>(params.write_slices.size())};
}

std::unique_ptr<value_field> value_schema_v2::extract_timestamp(absl::string_view value)
{
    dsn::data_input input(value);
    input.skip(sizeof(uint8_t));
    return std::make_unique<expire_timestamp_field>(input.read_u32());
}

std::unique_ptr<value_field> value_schema_v2::extract_time_tag(absl::string_view value)
{
    dsn::data_input input(value);
    input.skip(sizeof(uint8_t));
    input.skip(sizeof(uint32_t));
    return std::make_unique<time_tag_field>(input.read_u64());
}

void value_schema_v2::update_expire_ts(std::string &value, std::unique_ptr<value_field> field)
{
    CHECK_GE_MSG(value.length(),
                 sizeof(uint32_t) + sizeof(uint8_t),
                 "value must include 'expire_ts' header");
    auto expire_field = static_cast<expire_timestamp_field *>(field.get());

    auto new_expire_ts = dsn::endian::hton(expire_field->expire_ts);
    memcpy(const_cast<char *>(value.data()) + sizeof(uint8_t), &new_expire_ts, sizeof(uint32_t));
}
} // namespace pegasus
