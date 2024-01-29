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

#include <memory>
#include <cstring>

#include "absl/strings/string_view.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TProtocol.h>

#include "utils.h"

namespace dsn {

/// dsn::blob is a special thrift type that's not generated by thrift compiler,
/// but defined by the rDSN framework. Unlike thrift `string`, dsn::blob is
/// implemented by ref-counted buffer.
class blob
{
public:
    constexpr blob() = default;

    blob(std::shared_ptr<char> buffer, unsigned int length)
        : _holder(std::move(buffer)), _buffer(_holder.get()), _data(_holder.get()), _length(length)
    {
    }

    blob(std::shared_ptr<char> buffer, int offset, unsigned int length)
        : _holder(std::move(buffer)),
          _buffer(_holder.get()),
          _data(_holder.get() + offset),
          _length(length)
    {
    }

    /// NOTE: Use absl::string_view whenever possible.
    /// blob is designed for shared buffer, never use it as constant view.
    /// Maybe we could deprecate this function in the future.
    blob(const char *buffer, int offset, unsigned int length)
        : _buffer(buffer), _data(buffer + offset), _length(length)
    {
    }

    /// Create shared buffer from allocated raw bytes.
    /// NOTE: this operation is not efficient since it involves a memory copy.
    static blob create_from_bytes(const char *s, size_t len)
    {
        std::shared_ptr<char> s_arr(new char[len], std::default_delete<char[]>());
        memcpy(s_arr.get(), s, len);
        return blob(std::move(s_arr), 0, static_cast<unsigned int>(len));
    }

    /// Create shared buffer without copying data.
    static blob create_from_bytes(std::string &&bytes)
    {
        auto s = new std::string(std::move(bytes));
        std::shared_ptr<char> buf(const_cast<char *>(s->data()), [s](char *) { delete s; });
        return blob(std::move(buf), 0, static_cast<unsigned int>(s->length()));
    }

    void assign(const std::shared_ptr<char> &buffer, int offset, unsigned int length)
    {
        _holder = buffer;
        _buffer = _holder.get();
        _data = _holder.get() + offset;
        _length = length;
    }

    void assign(std::shared_ptr<char> &&buffer, int offset, unsigned int length)
    {
        _holder = std::move(buffer);
        _buffer = (_holder.get());
        _data = (_holder.get() + offset);
        _length = length;
    }

    /// Deprecated. Use absl::string_view whenever possible.
    void assign(const char *buffer, int offset, unsigned int length)
    {
        _holder = nullptr;
        _buffer = buffer;
        _data = buffer + offset;
        _length = length;
    }

    const char *data() const noexcept { return _data; }

    unsigned int length() const noexcept { return _length; }
    unsigned int size() const noexcept { return _length; }

    std::shared_ptr<char> buffer() const { return _holder; }

    const char *buffer_ptr() const { return _holder.get(); }

    // offset can be negative for buffer dereference
    blob range(int offset) const
    {
        // offset cannot exceed the current length value
        assert(offset <= static_cast<int>(_length));

        blob temp = *this;
        temp._data += offset;
        temp._length -= offset;
        return temp;
    }

    blob range(int offset, unsigned int len) const
    {
        // offset cannot exceed the current length value
        assert(offset <= static_cast<int>(_length));

        blob temp = *this;
        temp._data += offset;
        temp._length -= offset;

        // buffer length must exceed the required length
        assert(temp._length >= len);
        temp._length = len;
        return temp;
    }

    bool operator==(const blob &r) const
    {
        // not implemented
        assert(false);
        return false;
    }

    std::string to_string() const
    {
        if (_length == 0)
            return {};
        return std::string(_data, _length);
    }

    absl::string_view to_string_view() const { return absl::string_view(_data, _length); }

    uint32_t read(::apache::thrift::protocol::TProtocol *iprot);
    uint32_t write(::apache::thrift::protocol::TProtocol *oprot) const;

private:
    friend class binary_writer;
    std::shared_ptr<char> _holder;
    const char *_buffer{nullptr};
    const char *_data{nullptr};
    unsigned int _length{0}; // data length
};

class blob_string
{
private:
    blob &_buffer;

public:
    blob_string(blob &bb) : _buffer(bb) {}

    void clear() { _buffer.assign(std::shared_ptr<char>(nullptr), 0, 0); }
    void resize(std::size_t new_size)
    {
        std::shared_ptr<char> b(utils::make_shared_array<char>(new_size));
        _buffer.assign(b, 0, static_cast<int>(new_size));
    }
    void assign(const char *ptr, std::size_t size)
    {
        std::shared_ptr<char> b(utils::make_shared_array<char>(size));
        memcpy(b.get(), ptr, size);
        _buffer.assign(b, 0, static_cast<int>(size));
    }
    const char *data() const { return _buffer.data(); }
    size_t size() const { return _buffer.length(); }

    char &operator[](int pos) { return const_cast<char *>(_buffer.data())[pos]; }
};

inline uint32_t blob::read(apache::thrift::protocol::TProtocol *iprot)
{
    // for optimization, it is dangerous if the oprot is not a binary proto
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        static_cast<apache::thrift::protocol::TBinaryProtocol *>(iprot);
    blob_string str(*this);
    return binary_proto->readString<blob_string>(str);
}

inline uint32_t blob::write(apache::thrift::protocol::TProtocol *oprot) const
{
    apache::thrift::protocol::TBinaryProtocol *binary_proto =
        static_cast<apache::thrift::protocol::TBinaryProtocol *>(oprot);
    return binary_proto->writeString<blob_string>(blob_string(const_cast<blob &>(*this)));
}

} // namespace dsn
