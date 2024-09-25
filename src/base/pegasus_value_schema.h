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
#include <stdint.h>
#include <string.h>
#include <algorithm>
#include <array>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "utils/blob.h"
#include "utils/endians.h"
#include "utils/fmt_logging.h"
#include <string_view>
#include "value_field.h"

namespace pegasus {

constexpr int PEGASUS_DATA_VERSION_MAX = 1u;

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
inline uint32_t pegasus_extract_expire_ts(uint32_t version, std::string_view value)
{
    CHECK_LE(version, PEGASUS_DATA_VERSION_MAX);
    return dsn::data_input(value).read_u32();
}

/// Extracts user value from a raw rocksdb value.
/// In order to avoid data copy, the ownership of `raw_value` will be transferred
/// into `user_data`.
/// \param user_data: the result.
inline void
pegasus_extract_user_data(uint32_t version, std::string &&raw_value, ::dsn::blob &user_data)
{
    CHECK_LE(version, PEGASUS_DATA_VERSION_MAX);

    auto *s = new std::string(std::move(raw_value));
    dsn::data_input input(*s);
    input.skip(sizeof(uint32_t));
    if (version == 1) {
        input.skip(sizeof(uint64_t));
    }
    std::string_view view = input.read_str();

    // tricky code to avoid memory copy
    std::shared_ptr<char> buf(const_cast<char *>(view.data()), [s](char *) { delete s; });
    user_data.assign(std::move(buf), 0, static_cast<unsigned int>(view.length()));
}

/// Extracts timetag from a v1 value.
inline uint64_t pegasus_extract_timetag(int version, std::string_view value)
{
    CHECK_EQ(version, 1);

    dsn::data_input input(value);
    input.skip(sizeof(uint32_t));

    return input.read_u64();
}

/// Update expire_ts in rocksdb value with given version.
/// The value schema must be in v0 or v1.
inline void pegasus_update_expire_ts(uint32_t version, std::string &value, uint32_t new_expire_ts)
{
    if (version == 0 || version == 1) {
        CHECK_GE_MSG(value.length(), sizeof(uint32_t), "value must include 'expire_ts' header");

        new_expire_ts = dsn::endian::hton(new_expire_ts);
        memcpy(const_cast<char *>(value.data()), &new_expire_ts, sizeof(uint32_t));
    } else {
        LOG_FATAL("unsupported value schema version: {}", version);
        __builtin_unreachable();
    }
}

/// \return true if expired
inline bool check_if_ts_expired(uint32_t epoch_now, uint32_t expire_ts)
{
    return expire_ts > 0 && expire_ts <= epoch_now;
}

/// \return true if expired
inline bool check_if_record_expired(uint32_t value_schema_version,
                                    uint32_t epoch_now,
                                    std::string_view raw_value)
{
    return check_if_ts_expired(epoch_now,
                               pegasus_extract_expire_ts(value_schema_version, raw_value));
}

/// Helper class for generating value.
/// NOTES:
/// * the instance of pegasus_value_generator must be alive while the returned SliceParts is.
/// * the data of user_data must be alive be alive while the returned SliceParts is, because
///   we do not copy it.
/// * the returned SliceParts is only valid before the next invoking of generate_value().
class pegasus_value_generator
{
public:
    /// A higher level utility for generating value with given version.
    /// The value schema must be in v0 or v1.
    rocksdb::SliceParts generate_value(uint32_t value_schema_version,
                                       std::string_view user_data,
                                       uint32_t expire_ts,
                                       uint64_t timetag)
    {
        if (value_schema_version == 0) {
            return generate_value_v0(expire_ts, user_data);
        } else if (value_schema_version == 1) {
            return generate_value_v1(expire_ts, timetag, user_data);
        } else {
            LOG_FATAL("unsupported value schema version: {}", value_schema_version);
            __builtin_unreachable();
        }
    }

    /// The heading expire_ts is encoded to support TTL, and the record will be
    /// automatically cleared (by \see pegasus::server::KeyWithTTLCompactionFilter)
    /// after expiration reached. The expired record will be invisible even though
    /// they are not yet compacted.
    ///
    /// rocksdb value (ver 0) = [expire_ts(uint32_t)] [user_data(bytes)]
    /// \internal
    rocksdb::SliceParts generate_value_v0(uint32_t expire_ts, std::string_view user_data)
    {
        _write_buf.resize(sizeof(uint32_t));
        _write_slices.clear();

        dsn::data_output(_write_buf).write_u32(expire_ts);
        _write_slices.emplace_back(_write_buf.data(), _write_buf.size());

        if (user_data.length() > 0) {
            _write_slices.emplace_back(user_data.data(), user_data.length());
        }

        return {&_write_slices[0], static_cast<int>(_write_slices.size())};
    }

    /// The value schema here is designed to resolve write conflicts during duplication,
    /// specifically, when two clusters configured as "master-master" are concurrently
    /// writing at the same key.
    ///
    /// Though writings on the same key from two different clusters are rare in
    /// real-world cases, it still gives a bad user experience when it happens.
    /// A simple solution is to separate the writes into two halves, each cluster
    /// is responsible for one half only. How the writes are separated is left to
    /// users. This is simple, but unfriendly to use.
    ///
    /// In our design, each value is provided with a timestamp [0, 2^56-1], which
    /// represents the data version. A write duplicated from remote cluster firstly
    /// compares its timestamp with the current one if exists. The one with
    /// larger timestamp wins.
    ///
    /// An edge case occurs when the two timestamps are completely equal, the final
    /// result is undefined. To solve this we make 7 bits of space for cluster_id
    /// (the globally unique id of a cluster). In case when the timestamps are equal,
    /// the conflicts can be resolved by comparing the cluster id.
    ///
    /// Consider another edge case in which a record is deleted from pegasus, however
    /// in the remote cluster this record is written with a new value:
    ///
    ///   A: --------- update(ts:700)---- delete ---- update duplicated from B(ts:500) --
    ///   B: ---- update(ts:500) --------------------------------------------------------
    ///
    /// Since the record is removed, the stale update will successfully though
    /// incorrectly apply. To solve this problem there's 1 bit flag marking whether the
    /// record is deleted.
    ///
    /// rocksdb value (ver 1)
    ///  = [expire_ts(uint32_t)] [timetag(uint64_t)] [user_data(bytes)]
    ///  = [expire_ts(unit32_t)]
    ///    [timestamp in μs (56 bit)] [cluster_id (7 bit)] [deleted_tag (1 bit)]
    ///    [user_data(bytes)]
    ///
    /// \internal
    rocksdb::SliceParts
    generate_value_v1(uint32_t expire_ts, uint64_t timetag, std::string_view user_data)
    {
        _write_buf.resize(sizeof(uint32_t) + sizeof(uint64_t));
        _write_slices.clear();

        dsn::data_output(_write_buf).write_u32(expire_ts).write_u64(timetag);
        _write_slices.emplace_back(_write_buf.data(), _write_buf.size());

        if (user_data.length() > 0) {
            _write_slices.emplace_back(user_data.data(), user_data.length());
        }

        return {&_write_slices[0], static_cast<int>(_write_slices.size())};
    }

private:
    std::string _write_buf;
    std::vector<rocksdb::Slice> _write_slices;
};

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
    // write_buf and write_slices are transferred from `pegasus_value_generator`, which are used to
    // prevent data copy
    std::string &write_buf;
    std::vector<rocksdb::Slice> &write_slices;
};

class value_schema
{
public:
    virtual ~value_schema() = default;

    virtual std::unique_ptr<value_field> extract_field(std::string_view value,
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
