/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <cstdint>
#include <ostream>
#include <thrift/protocol/TProtocol.h>

#include "utils/fmt_utils.h"

namespace dsn {

// Group-Partition-ID.
class gpid
{
public:
    constexpr gpid(int app_id, int pidx) : _value({.u = {app_id, pidx}}) {}

    constexpr gpid() = default;

    constexpr uint64_t value() const { return _value.value; }

    bool operator<(const gpid &r) const
    {
        return _value.u.app_id < r._value.u.app_id ||
               (_value.u.app_id == r._value.u.app_id &&
                _value.u.partition_index < r._value.u.partition_index);
    }

    constexpr bool operator==(const gpid &r) const { return value() == r.value(); }

    constexpr bool operator!=(const gpid &r) const { return value() != r.value(); }

    constexpr int32_t get_app_id() const { return _value.u.app_id; }

    constexpr int32_t get_partition_index() const { return _value.u.partition_index; }

    void set_app_id(int32_t v) { _value.u.app_id = v; }

    void set_partition_index(int32_t v) { _value.u.partition_index = v; }

    void set_value(uint64_t v) { _value.value = v; }

    bool parse_from(const char *str);

    const char *to_string() const;

    // for serialization in thrift format
    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;

    int thread_hash() const { return _value.u.app_id * 7919 + _value.u.partition_index; }

    friend std::ostream &operator<<(std::ostream &os, const gpid &id)
    {
        return os << id.to_string();
    }

private:
    union
    {
        struct
        {
            int32_t app_id;          ///< 1-based app id (0 for invalid)
            int32_t partition_index; ///< zero-based partition index
        } u;
        uint64_t value;
    } _value{.value = 0};
};

} // namespace dsn

USER_DEFINED_STRUCTURE_FORMATTER(::dsn::gpid);

namespace std {
template <>
struct hash<::dsn::gpid>
{
    size_t operator()(const ::dsn::gpid &pid) const
    {
        return static_cast<std::size_t>(pid.thread_hash());
    }
};
} // namespace std
