// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <rocksdb/slice_transform.h>
#include <rocksdb/slice.h>

#include <dsn/c/api_utilities.h>
#include <dsn/utility/blob.h>

namespace pegasus {
namespace server {

class HashkeyTransform : public rocksdb::SliceTransform
{
public:
    HashkeyTransform() = default;

    // NOTE: You must change the name if Transform() algorithm changed.
    const char *Name() const override { return "pegasus.HashkeyTransform"; }

    rocksdb::Slice Transform(const rocksdb::Slice &src) const override
    {
        // TODO(yingchun): There is a bug in rocksdb 5.9.2, it has been fixed by
        // cca141ecf8634a42b5eb548cb0ac3a6b77d783c1, we can remove this judgement after upgrading
        // rocksdb.
        if (src.size() < 2) {
            return src;
        }

        // hash_key_len is in big endian
        uint16_t hash_key_len = be16toh(*(int16_t *)(src.data()));
        dassert(src.size() >= 2 + hash_key_len,
                "key length must be no less than (2 + hash_key_len)");
        return rocksdb::Slice(src.data(), 2 + hash_key_len);
    }

    bool InDomain(const rocksdb::Slice &src) const override
    {
        // Empty put keys are not in domain.
        return src.size() >= 2;
    }

    bool InRange(const rocksdb::Slice &dst) const override { return true; }

    bool SameResultWhenAppended(const rocksdb::Slice &prefix) const override { return false; }
};
} // namespace server
} // namespace pegasus
