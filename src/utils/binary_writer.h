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
#include <cstring>
#include <string_view>
#include <vector>

#include "utils/blob.h"
#include "utils/ports.h"

namespace dsn {

class binary_writer
{
public:
    binary_writer();
    explicit binary_writer(int reserved_buffer_size);

    virtual ~binary_writer() = default;

    virtual void flush();

    // Write data of POD types into the buffers.
    template <typename TVal>
    void write_pod(const TVal &val);

    // Write data of built-in types into the buffers.
    void write(const int8_t &val) { write_pod(val); }
    void write(const uint8_t &val) { write_pod(val); }
    void write(const int16_t &val) { write_pod(val); }
    void write(const uint16_t &val) { write_pod(val); }
    void write(const int32_t &val) { write_pod(val); }
    void write(const uint32_t &val) { write_pod(val); }
    void write(const int64_t &val) { write_pod(val); }
    void write(const uint64_t &val) { write_pod(val); }
    void write(const bool &val) { write_pod(val); }

    // Write bytes in string_view into the buffers.
    void write(std::string_view val);

    // Write bytes in blob into the buffers.
    void write(const blob &val);

    // Write `size` bytes from `buffer` into the buffers.
    void write(const char *buffer, int size);

    // Just increase the buffers by `size` bytes without writing any data into it.
    void write_empty(int size);

    // Commit the current buffer and return a blob filled with all bytes over all buffers.
    blob get_buffer();

    // Return a blob filled with all bytes over all buffers without committing the current
    // buffer and thus future written bytes will continue to be put into the current buffer.
    [[nodiscard]] blob get_current_buffer() const;

    // Get the total size in bytes over all buffers.
    [[nodiscard]] int total_size() const { return _total_size; }

protected:
    // Commit the current buffer and create a new buffer of at least `size` bytes.
    void create_buffer(size_t size);

    // Allocate space of at least `size` bytes for a new buffer into `bb`.
    virtual void create_new_buffer(size_t size, /*out*/ blob &bb);

    // Commit the current buffer.
    void commit();

private:
    // Write data of bytes-like types into buffers.
    template <typename TBytes>
    void write_bytes(const TBytes &val);

    std::vector<blob> _buffers;

    // The current buffer is just the last buffer of `_buffers`.
    char *_current_buffer;
    int _current_offset;
    int _current_buffer_length;

    int _total_size;
    int _reserved_size_per_buffer;
    static const int kReservedSizePerBuffer;

    DISALLOW_COPY_AND_ASSIGN(binary_writer);
    DISALLOW_MOVE_AND_ASSIGN(binary_writer);
};

//--------------- inline implementation -------------------
template <typename TVal>
inline void binary_writer::write_pod(const TVal &val)
{
    write(reinterpret_cast<const char *>(&val), static_cast<int>(sizeof(val)));
}

template <typename TBytes>
inline void binary_writer::write_bytes(const TBytes &val)
{
    const auto len = static_cast<int>(val.length());
    write_pod(len);
    if (len > 0) {
        write(val.data(), len);
    }
}

inline void binary_writer::write(std::string_view val) { write_bytes(val); }

inline void binary_writer::write(const blob &val) { write_bytes(val); }

} // namespace dsn
