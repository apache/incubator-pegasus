// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <stdint.h>
#include <string.h>
#include <string>
#include <vector>
#include <dsn/utility/ports.h>
#include <dsn/utility/utils.h>
#include <dsn/utility/blob.h>
#include <dsn/service_api_c.h>
#include <rocksdb/slice.h>

namespace pegasus {

#define PEGASUS_VALUE_SCHEMA_MAX_VERSION 0

// =====================================================================================
// rocksdb value (version 1) = [expire_ts(uint32_t)] [user_data(bytes)]

// generate rocksdb value
// T may be std::string or ::dsn::blob or rocksdb::slice.
// 'user_data' will not be copied, so 'user_data' may be referenced by 'slices'.
// some data may be put in 'buf', so 'buf' may be referenced by 'slices'.
template <typename T>
void pegasus_generate_value(uint32_t version,
                            uint32_t expire_ts,
                            T &user_data,
                            std::string &buf,
                            std::vector<rocksdb::Slice> &slices)
{
    if (version == 0) {
        buf.resize(4);
        slices.resize(2);

        // expire_ts is in big endian
        *((int32_t *)(&buf[0])) = htobe32((int32_t)expire_ts);
        slices[0] = rocksdb::Slice(buf.data(), 4);

        if (user_data.length() > 0) {
            slices[1] = rocksdb::Slice(user_data.data(), user_data.length());
        } else {
            slices.resize(1);
        }
    } else {
        dassert(false, "unsupported version %" PRIu32, version);
    }
}

// extract expire timestamp from rocksdb value
// T may be std::string or ::dsn::blob or rocksdb::slice.
template <typename T>
inline uint32_t pegasus_extract_expire_ts(uint32_t version, const T &value)
{
    if (version == 0) {
        dassert(value.length() >= 4, "value length must be no less than 4");

        // expire_ts is in big endian
        uint32_t expire_ts = be32toh(*(int32_t *)(value.data()));

        return expire_ts;
    } else {
        dassert(false, "unsupported version %" PRIu32, version);
        return 0;
    }
}

// extract data from rocksdb value.
// T may be std::string or ::dsn::blob or rocksdb::slice.
template <typename T>
inline void pegasus_extract_user_data(uint32_t version, const T &value, std::string &user_data)
{
    if (version == 0) {
        dassert(value.length() >= 4, "value length must be no less than 4");

        if (value.length() > 4) {
            user_data.assign(value.data() + 4, value.length() - 4);
        } else {
            user_data.clear();
        }
    } else {
        dassert(false, "unsupported version %" PRIu32, version);
    }
}

// extract data from rocksdb value (special implementation to avoid memory copy).
// the ownership of 'value' is tranfered into this function.
inline void pegasus_extract_user_data(uint32_t version,
                                      std::unique_ptr<std::string> value,
                                      ::dsn::blob &user_data)
{
    if (version == 0) {
        dassert(value->length() >= 4, "value length must be no less than 4");

        if (value->length() > 4) {
            // tricky code to avoid memory copy
            char *ptr = &value->front();
            unsigned int len = value->length();
            std::shared_ptr<char> buf(ptr, [s = value.release()](char *) { delete s; });
            user_data.assign(std::move(buf), 4, len - 4);
        } else {
            user_data = ::dsn::blob();
        }
    } else {
        dassert(false, "unsupported version %" PRIu32, version);
    }
}

} // namespace
