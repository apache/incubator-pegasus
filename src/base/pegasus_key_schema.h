// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <stdint.h>
#include <string.h>
#include <dsn/utility/ports.h>
#include <dsn/utility/utils.h>
#include <dsn/utility/blob.h>
#include <dsn/utility/utils.h>
#include <dsn/utility/crc.h>
#include <dsn/c/api_utilities.h>

namespace pegasus {

// =====================================================================================
// rocksdb key = [hash_key_len(uint16_t)] [hash_key(bytes)] [sort_key(bytes)]

// generate rocksdb key.
// T may be std::string or ::dsn::blob.
// data is copied into 'key'.
template <typename T>
void pegasus_generate_key(::dsn::blob &key, const T &hash_key, const T &sort_key)
{
    dassert(hash_key.length() < UINT16_MAX, "hash key length must be less than UINT16_MAX");

    int len = 2 + hash_key.length() + sort_key.length();
    std::shared_ptr<char> buf(::dsn::utils::make_shared_array<char>(len));

    // hash_key_len is in big endian
    uint16_t hash_key_len = hash_key.length();
    *((int16_t *)buf.get()) = htobe16((int16_t)hash_key_len);

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
    dassert(hash_key.length() < UINT16_MAX, "hash key length must be less than UINT16_MAX");

    int hash_key_len = hash_key.length();
    std::shared_ptr<char> buf(::dsn::utils::make_shared_array<char>(hash_key_len + 2));

    *((int16_t *)buf.get()) = htobe16((int16_t)hash_key_len);
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

// restore hash_key and sort_key from rocksdb value.
// no data copied.
inline void
pegasus_restore_key(const ::dsn::blob &key, ::dsn::blob &hash_key, ::dsn::blob &sort_key)
{
    dassert(key.length() >= 2, "key length must be no less than 2");

    // hash_key_len is in big endian
    uint16_t hash_key_len = be16toh(*(int16_t *)(key.data()));

    if (hash_key_len > 0) {
        dassert(key.length() >= 2 + hash_key_len,
                "key length must be no less than (2 + hash_key_len)");
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

// restore hash_key and sort_key from rocksdb value.
// data is copied into output 'hash_key' and 'sort_key'.
inline void
pegasus_restore_key(const ::dsn::blob &key, std::string &hash_key, std::string &sort_key)
{
    dassert(key.length() >= 2, "key length must be no less than 2");

    // hash_key_len is in big endian
    uint16_t hash_key_len = be16toh(*(int16_t *)(key.data()));

    if (hash_key_len > 0) {
        dassert(key.length() >= 2 + hash_key_len,
                "key length must be no less than (2 + hash_key_len)");
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

// calculate hash from rocksdb key.
inline uint64_t pegasus_key_hash(const ::dsn::blob &key)
{
    dassert(key.length() >= 2, "key length must be no less than 2");

    // hash_key_len is in big endian
    uint16_t hash_key_len = be16toh(*(int16_t *)(key.data()));

    if (hash_key_len > 0) {
        // hash_key_len > 0, compute hash from hash_key
        dassert(key.length() >= 2 + hash_key_len,
                "key length must be no less than (2 + hash_key_len)");
        return dsn::utils::crc64_calc(key.buffer_ptr() + 2, hash_key_len, 0);
    } else {
        // hash_key_len == 0, compute hash from sort_key
        return dsn::utils::crc64_calc(key.buffer_ptr() + 2, key.length() - 2, 0);
    }
}

} // namespace pegasus
