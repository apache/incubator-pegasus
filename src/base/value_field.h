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

#include "utils/fmt_utils.h"

namespace pegasus {

enum value_field_type
{
    EXPIRE_TIMESTAMP = 0,
    TIME_TAG,
    USER_DATA,
    FIELD_COUNT,
};
USER_DEFINED_ENUM_FORMATTER(value_field_type)

struct value_field
{
    virtual ~value_field() = default;
    virtual value_field_type type() = 0;
};

struct expire_timestamp_field : public value_field
{
    explicit expire_timestamp_field(uint32_t timestamp) : expire_ts(timestamp) {}
    value_field_type type() { return value_field_type::EXPIRE_TIMESTAMP; }

    uint32_t expire_ts;
};

struct time_tag_field : public value_field
{
    explicit time_tag_field(uint64_t tag) : time_tag(tag) {}
    value_field_type type() { return value_field_type::TIME_TAG; }

    uint64_t time_tag;
};

struct user_data_field : public value_field
{
    explicit user_data_field(absl::string_view data) : user_data(data) {}
    value_field_type type() { return value_field_type::USER_DATA; }

    absl::string_view user_data;
};
} // namespace pegasus
