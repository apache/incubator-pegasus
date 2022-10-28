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

#include <rocksdb/slice_transform.h>
#include <rocksdb/slice.h>

#include "utils/api_utilities.h"
#include "utils/blob.h"
#include "utils/endians.h"

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
        uint16_t hash_key_len = dsn::endian::ntoh(*(uint16_t *)(src.data()));
        CHECK_GE_MSG(
            src.size(), 2 + hash_key_len, "key length must be no less than (2 + hash_key_len)");
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
