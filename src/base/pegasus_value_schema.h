// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "endians.h"

#include <stdint.h>
#include <string.h>
#include <string>
#include <vector>

#include <dsn/utility/ports.h>
#include <dsn/utility/utils.h>
#include <dsn/utility/blob.h>
#include <dsn/utility/smart_pointers.h>
#include <dsn/utility/string_view.h>
#include <dsn/service_api_c.h>

namespace pegasus {

#define PEGASUS_VALUE_SCHEMA_MAX_VERSION 1

/// Generates timetag in host endian.
/// \see comment on pegasus_value_generator::generate_value_v1
inline uint64_t generate_timetag(uint64_t timestamp, uint8_t cluster_id, bool delete_tag)
{
    return timestamp << 8 | cluster_id << 1 | delete_tag;
}

inline uint8_t extract_cluster_id_from_timetag(uint64_t timetag)
{
    return static_cast<uint8_t>((timetag >> 1) & 0xF);
}

/// Extracts expire_ts from rocksdb value with given version.
/// The value schema must be either in v0 or v1.
/// \return expire_ts in host endian
inline uint32_t pegasus_extract_expire_ts(int version, dsn::string_view value)
{
    dassert(
        version == 0 || version == 1, "value schema version(%d) must be either v0 or v1", version);
    return data_input(value).read_u32();
}

/// Extracts user value from a raw rocksdb value.
/// In order to avoid data copy, the ownership of `raw_value` will be transferred
/// into `user_data`.
/// \param user_data: the result.
inline void pegasus_extract_user_data(int version, std::string &&raw_value, ::dsn::blob &user_data)
{
    dassert(
        version == 0 || version == 1, "value schema version(%d) must be either v0 or v1", version);

    data_input input(raw_value);
    input.skip(sizeof(uint32_t));
    if (version == 1) {
        input.skip(sizeof(uint64_t));
    }

    dsn::string_view view = input.read_str();

    // tricky code to avoid memory copy
    auto ptr = const_cast<char *>(view.data());
    auto deleter = [s = new std::string(std::move(raw_value))](char *) { delete s; };
    std::shared_ptr<char> buf(ptr, deleter);
    user_data.assign(std::move(buf), 0, static_cast<unsigned int>(view.length()));
}

/// Extracts timetag from a v1 value.
inline uint64_t pegasus_extract_timetag(int version, dsn::string_view value)
{
    dassert(version == 1, "value schema version(%d) must be v1", version);

    data_input input(value);
    input.skip(sizeof(uint32_t));

    return input.read_u64();
}

/// \return true if expired
inline bool check_if_record_expired(uint32_t epoch_now, uint32_t expire_ts)
{
    return expire_ts > 0 && expire_ts <= epoch_now;
}

/// \return true if expired
inline bool check_if_record_expired(uint32_t value_schema_version,
                                    uint32_t epoch_now,
                                    dsn::string_view raw_value)
{
    uint32_t expire_ts = pegasus_extract_expire_ts(value_schema_version, raw_value);
    return check_if_record_expired(epoch_now, expire_ts);
}

/// Helper class for generating value.
/// NOTE that the instance of pegasus_value_generator must be alive
/// while the returned SliceParts is.
class pegasus_value_generator
{
public:
    /// A higher level utility for generating value with given version.
    /// The value schema must be either in v0 or v1.
    rocksdb::SliceParts generate_value(int value_schema_version,
                                       dsn::string_view user_data,
                                       uint32_t expire_ts,
                                       uint64_t timetag)
    {
        if (value_schema_version == 0) {
            return generate_value_v0(expire_ts, user_data);
        } else if (value_schema_version == 1) {
            return generate_value_v1(expire_ts, timetag, user_data);
        } else {
            dfatal("unsupported value schema version: %d", value_schema_version);
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
    rocksdb::SliceParts generate_value_v0(uint32_t expire_ts, dsn::string_view user_data)
    {
        _write_buf.resize(sizeof(uint32_t));
        _write_slices.clear();

        data_output(_write_buf).write_u32(expire_ts);
        _write_slices.emplace_back(_write_buf.data(), _write_buf.size());

        if (user_data.length() > 0) {
            _write_slices.emplace_back(user_data.data(), user_data.length());
        }

        return rocksdb::SliceParts(&_write_slices[0], static_cast<int>(_write_slices.size()));
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
    ///  = [expire_ts(uint32_t)] [tag(uint64_t)] [user_data(bytes)]
    ///  = [expire_ts(unit32_t)]
    ///    [timestamp in μs (56 bit)] [cluster_id (7 bit)] [delete_tag (1 bit)]
    ///    [user_data(bytes)]
    ///
    /// \internal
    rocksdb::SliceParts
    generate_value_v1(uint32_t expire_ts, uint64_t tag, dsn::string_view user_data)
    {
        _write_buf.resize(sizeof(uint32_t) + sizeof(uint64_t));
        _write_slices.clear();

        data_output(_write_buf).write_u32(expire_ts).write_u64(tag);
        _write_slices.emplace_back(_write_buf.data(), _write_buf.size());

        if (user_data.length() > 0) {
            _write_slices.emplace_back(user_data.data(), user_data.length());
        }

        return rocksdb::SliceParts(&_write_slices[0], static_cast<int>(_write_slices.size()));
    }

private:
    std::string _write_buf;
    std::vector<rocksdb::Slice> _write_slices;
};

} // namespace pegasus
