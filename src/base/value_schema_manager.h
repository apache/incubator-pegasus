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
#include <dsn/utility/smart_pointers.h>

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
    friend class dsn::utils::singleton<value_schema_manager>;

    std::array<std::unique_ptr<value_schema>, data_version::VERSION_COUNT> _schemas;
};

/// Generates timetag in host endian.
/// \see comment on pegasus_value_generator::generate_value_v1
inline uint64_t generate_timetag(uint64_t timestamp, uint8_t cluster_id, bool deleted_tag)
{
    return timestamp << 8u | cluster_id << 1u | deleted_tag;
}

inline uint64_t extract_timestamp_from_timetag(uint64_t timetag)
{
    // 56bit: 0xFFFFFFFFFFFFFFL
    return static_cast<uint64_t>((timetag >> 8u) & 0xFFFFFFFFFFFFFFLu);
}

/// Extracts expire_ts from rocksdb value with given version.
/// The value schema must be in v0 or v1.
/// \return expire_ts in host endian
inline uint32_t pegasus_extract_expire_ts(uint32_t meta_cf_data_version, dsn::string_view value)
{
    auto schema = value_schema_manager::instance().get_value_schema(meta_cf_data_version, value);
    auto field = schema->extract_field(value, value_field_type::EXPIRE_TIMESTAMP).get();
    return static_cast<expire_timestamp_field *>(field)->expire_ts;
}

/// Extracts user value from a raw rocksdb value.
/// In order to avoid data copy, the ownership of `raw_value` will be transferred
/// into `user_data`.
/// \param user_data: the result.
inline void pegasus_extract_user_data(uint32_t meta_cf_data_version,
                                      std::string &&raw_value,
                                      ::dsn::blob &user_data)
{
    auto schema =
        value_schema_manager::instance().get_value_schema(meta_cf_data_version, raw_value);
    user_data = schema->extract_user_data(std::move(raw_value));
}

/// Extracts timetag from a v1 value.
inline uint64_t pegasus_extract_timetag(int meta_cf_data_version, dsn::string_view value)
{
    auto schema = value_schema_manager::instance().get_value_schema(meta_cf_data_version, value);
    auto field = schema->extract_field(value, value_field_type::TIME_TAG).get();
    return static_cast<time_tag_field *>(field)->time_tag;
}

/// Update expire_ts in rocksdb value with given version.
/// The value schema must be in v0 or v1.
inline void
pegasus_update_expire_ts(uint32_t meta_cf_data_version, std::string &value, uint32_t new_expire_ts)
{
    auto schema = value_schema_manager::instance().get_value_schema(meta_cf_data_version, value);
    auto expire_ts_field = dsn::make_unique<expire_timestamp_field>(new_expire_ts);
    schema->update_field(value, std::move(expire_ts_field));
}

/// \return true if expired
inline bool check_if_ts_expired(uint32_t epoch_now, uint32_t expire_ts)
{
    return expire_ts > 0 && expire_ts <= epoch_now;
}

/// \return true if expired
inline bool check_if_record_expired(uint32_t value_schema_version,
                                    uint32_t epoch_now,
                                    dsn::string_view raw_value)
{
    return check_if_ts_expired(epoch_now,
                               pegasus_extract_expire_ts(value_schema_version, raw_value));
}

} // namespace pegasus
