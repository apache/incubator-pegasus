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

#include <rocksdb/comparator.h>

namespace rocksdb {

inline bool IsLittleEndian()
{
    static int32_t number = 0x88;
    const char *array = reinterpret_cast<const char *>(&number);
    return (array[0] == 0x88);
}

// The function is copied from rocksdb/util/coding.h

// Lower-level versions of Get... that read directly from a character buffer
// without any bounds checking.
inline uint16_t DecodeFixed16(const char *ptr)
{
    // port::kLittleEndian
    if (IsLittleEndian()) {
        // Load the raw bytes
        uint16_t result;
        memcpy(&result, ptr, sizeof(result)); // gcc optimizes this to a plain load
        return result;
    } else {
        return ((static_cast<uint16_t>(static_cast<unsigned char>(ptr[0]))) |
                (static_cast<uint16_t>(static_cast<unsigned char>(ptr[1])) << 8));
    }
}

// The function is copied from rocksdb/util/coding.h
inline bool GetFixed16(Slice *input, uint16_t *value)
{
    if (input->size() < sizeof(uint16_t)) {
        return false;
    }
    *value = DecodeFixed16(input->data());
    input->remove_prefix(sizeof(uint16_t));
    return true;
}

} // namespace rocksdb

namespace pegasus {
namespace server {

class PegasusComparator : public rocksdb::Comparator
{
public:
    PegasusComparator() {}
    virtual const char *Name() const { return "PegasusComparator"; }
    virtual int Compare(const rocksdb::Slice &left, const rocksdb::Slice &right) const
    {
        rocksdb::Slice left_slice(left);
        uint16_t left_length;
        rocksdb::GetFixed16(&left_slice, &left_length);

        rocksdb::Slice right_slice(right);
        uint16_t right_length;
        rocksdb::GetFixed16(&right_slice, &right_length);

        rocksdb::Slice left_hash_key(left_slice.data(), left_length);
        rocksdb::Slice right_hash_key(right_slice.data(), right_length);
        int ret = left_hash_key.compare(right_hash_key);
        if (ret != 0) {
            return ret;
        }
        left_slice.remove_prefix(left_length);
        right_slice.remove_prefix(right_length);

        return left_slice.compare(right_slice);
    }
    virtual void FindShortestSeparator(std::string *start, const rocksdb::Slice &limit) const
    {
        (void)start;
        (void)limit;
        // TODO
    }
    virtual void FindShortSuccessor(std::string *key) const
    {
        (void)key;
        // TODO
    }
};

} // namespace server
} // namespace pegasus
