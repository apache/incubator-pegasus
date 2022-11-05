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

#include <stdint.h>
#include <string>
#include "utils/ports.h"
#include "utils/utils.h"
#include "utils/blob.h"
#include "utils/endians.h"
#include "utils/utils.h"
#include "utils/crc.h"
#include "utils/api_utilities.h"

namespace pegasus {

// =====================================================================================
// rocksdb key = [hash_key_len(uint16_t)] [hash_key(bytes)] [sort_key(bytes)]

// generate rocksdb key.
// T may be std::string or ::dsn::blob.
// data is copied into 'key'.
template <typename T>
void pegasus_generate_key(::dsn::blob &key, const T &hash_key, const T &sort_key)
{
    CHECK_LT(hash_key.length(), UINT16_MAX);

    int len = 2 + hash_key.length() + sort_key.length();
    std::shared_ptr<char> buf(::dsn::utils::make_shared_array<char>(len));

    // hash_key_len is in big endian
    uint16_t hash_key_len = hash_key.length();
    *((int16_t *)buf.get()) = ::dsn::endian::hton((uint16_t)hash_key_len);

    ::memcpy(buf.get() + 2, hash_key.data(), hash_key_len);

    if (sort_key.length() > 0) {
        ::memcpy(buf.get() + 2 + hash_key_len, sort_key.data(), sort_key.length());
    }

    key.assign(std::move(buf), 0, len);
}

// generate the adjacent next rocksdb key according to hash key.
// T may be std::string or ::dsn::blob.
// data is copied into 'next'.
template <typename T>
void pegasus_generate_next_blob(::dsn::blob &next, const T &hash_key)
{
    CHECK_LT(hash_key.length(), UINT16_MAX);

    int hash_key_len = hash_key.length();
    std::shared_ptr<char> buf(::dsn::utils::make_shared_array<char>(hash_key_len + 2));

    *((int16_t *)buf.get()) = ::dsn::endian::hton((uint16_t)hash_key_len);
    ::memcpy(buf.get() + 2, hash_key.data(), hash_key_len);

    unsigned char *p = (unsigned char *)(buf.get() + hash_key_len + 1);
    while (*p == 0xFF)
        p--;
    (*p)++;

    next.assign(std::move(buf), 0, p - (unsigned char *)(buf.get()) + 1);
}

// generate the adjacent next rocksdb key according to hash key and sort key.
// T may be std::string or ::dsn::blob.
// data is copied into 'next'.
template <typename T>
void pegasus_generate_next_blob(::dsn::blob &next, const T &hash_key, const T &sort_key)
{
    ::dsn::blob buf;
    pegasus_generate_key(buf, hash_key, sort_key);

    unsigned char *p = (unsigned char *)(buf.data() + buf.length() - 1);
    while (*p == 0xFF)
        p--;
    (*p)++;

    next = buf.range(0, p - (unsigned char *)(buf.data()) + 1);
}

// restore hash_key and sort_key from rocksdb key.
// no data copied.
inline void
pegasus_restore_key(const ::dsn::blob &key, ::dsn::blob &hash_key, ::dsn::blob &sort_key)
{
    CHECK_GE(key.length(), 2);

    // hash_key_len is in big endian
    uint16_t hash_key_len = ::dsn::endian::ntoh(*(uint16_t *)(key.data()));

    if (hash_key_len > 0) {
        CHECK_GE(key.length(), 2 + hash_key_len);
        hash_key = key.range(2, hash_key_len);
    } else {
        hash_key = ::dsn::blob();
    }

    if (key.length() > 2 + hash_key_len) {
        sort_key = key.range(2 + hash_key_len);
    } else {
        sort_key = ::dsn::blob();
    }
}

// restore hash_key and sort_key from rocksdb key.
// data is copied into output 'hash_key' and 'sort_key'.
inline void
pegasus_restore_key(const ::dsn::blob &key, std::string &hash_key, std::string &sort_key)
{
    CHECK_GE(key.length(), 2);

    // hash_key_len is in big endian
    uint16_t hash_key_len = ::dsn::endian::ntoh(*(uint16_t *)(key.data()));

    if (hash_key_len > 0) {
        CHECK_GE(key.length(), 2 + hash_key_len);
        hash_key.assign(key.data() + 2, hash_key_len);
    } else {
        hash_key.clear();
    }

    if (key.length() > 2 + hash_key_len) {
        sort_key.assign(key.data() + 2 + hash_key_len, key.length() - 2 - hash_key_len);
    } else {
        sort_key.clear();
    }
}

// calculate hash from rocksdb key or rocksdb slice
template <typename T>
inline uint64_t pegasus_key_hash(const T &key)
{
    CHECK_GE(key.size(), 2);

    // hash_key_len is in big endian
    uint16_t hash_key_len = ::dsn::endian::ntoh(*(uint16_t *)(key.data()));

    if (hash_key_len > 0) {
        // hash_key_len > 0, compute hash from hash_key
        CHECK_GE(key.size(), 2 + hash_key_len);
        return dsn::utils::crc64_calc(key.data() + 2, hash_key_len, 0);
    } else {
        // hash_key_len == 0, compute hash from sort_key
        return dsn::utils::crc64_calc(key.data() + 2, key.size() - 2, 0);
    }
}

/// Calculate hash value from hash key.
inline uint64_t pegasus_hash_key_hash(const ::dsn::blob &hash_key)
{
    return dsn::utils::crc64_calc(hash_key.data(), hash_key.length(), 0);
}

// check key should be served this partition
// Notice: partition_version should be check if is greater than 0 before calling this function
template <class T>
inline bool check_pegasus_key_hash(const T &key, int32_t pidx, int32_t partition_version)
{
    auto target_pidx = pegasus_key_hash(key) & partition_version;
    if (dsn_unlikely(target_pidx != pidx)) {
        return false;
    }
    return true;
}

} // namespace pegasus
