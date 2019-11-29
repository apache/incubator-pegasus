// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <rocksdb/slice_transform.h>

#include <dsn/utility/blob.h>

namespace pegasus {
namespace server {

class HashkeyTransform : public rocksdb::SliceTransform
{
public:
    HashkeyTransform() = default;

    const char *Name() const override { return "pegasus.HashkeyTransform"; }

    rocksdb::Slice Transform(const rocksdb::Slice &src) const override
    {
        ::dsn::blob hash_key, sort_key;
        pegasus_restore_key(dsn::blob(src.data(), 0, src.size()), hash_key, sort_key);
        return rocksdb::Slice(hash_key.data(), hash_key.length());
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
