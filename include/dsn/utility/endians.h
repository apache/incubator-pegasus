// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <cstdint>
#include <cassert>
#include <endian.h>
#include <dsn/utility/string_view.h>

namespace dsn {

namespace endian {

inline uint16_t hton(uint16_t v) { return htobe16(v); }

inline uint32_t hton(uint32_t v) { return htobe32(v); }

inline uint64_t hton(uint64_t v) { return htobe64(v); }

inline uint16_t ntoh(uint16_t v) { return be16toh(v); }

inline uint32_t ntoh(uint32_t v) { return be32toh(v); }

inline uint64_t ntoh(uint64_t v) { return be64toh(v); }

} // namespace endian

// Write data in wire serialization.
class data_output
{
public:
    data_output(char *p, size_t size) : _ptr(p), _end(p + size) {}

    explicit data_output(std::string &s) : data_output(&s[0], s.length()) {}

    data_output &write_u16(uint16_t val) { return write_unsigned(val); }

    data_output &write_u32(uint32_t val) { return write_unsigned(val); }

    data_output &write_u64(uint64_t val) { return write_unsigned(val); }

private:
    template <typename T>
    data_output &write_unsigned(T val)
    {
        static_assert(std::is_unsigned<T>::value, "T must be unsigned integer");
        ensure(sizeof(val));

        val = endian::hton(val);
        memcpy(_ptr, &val, sizeof(val));
        _ptr += sizeof(val);
        return *this;
    }

    void ensure(size_t sz)
    {
        size_t cap = _end - _ptr;
        assert(cap >= sz);
    }

private:
    char *_ptr;
    char *_end;
};

// Read data that was written in wire serialization.
class data_input
{
public:
    explicit data_input(string_view s) : _p(s.data()), _size(s.size()) {}

    uint16_t read_u16() { return read_unsigned<uint16_t>(); }

    uint32_t read_u32() { return read_unsigned<uint32_t>(); }

    uint64_t read_u64() { return read_unsigned<uint64_t>(); }

    string_view read_str() { return {_p, _size}; }

    void skip(size_t sz)
    {
        ensure(sz);
        advance(sz);
    }

private:
    template <typename T>
    T read_unsigned()
    {
        static_assert(std::is_unsigned<T>::value, "T must be unsigned integer");
        ensure(sizeof(T));

        T val = 0;
        memcpy(&val, _p, sizeof(T));
        val = endian::ntoh(val);

        advance(sizeof(T));

        return val;
    }

    void advance(size_t sz)
    {
        _p += sz;
        _size -= sz;
    }

    void ensure(size_t sz) { assert(_size >= sz); }

private:
    const char *_p{nullptr};
    size_t _size{0};
};

} // namespace dsn
