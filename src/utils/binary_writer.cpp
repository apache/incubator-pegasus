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

#include "binary_writer.h"

#include <memory>

#include "utils.h"
#include "utils/blob.h"

namespace dsn {

const int binary_writer::kReservedSizePerBuffer = 256;

binary_writer::binary_writer() : binary_writer(0) {}

binary_writer::binary_writer(int reserved_buffer_size)
    : _current_buffer(nullptr),
      _current_offset(0),
      _current_buffer_length(0),
      _total_size(0),
      _reserved_size_per_buffer((reserved_buffer_size == 0) ? kReservedSizePerBuffer
                                                            : reserved_buffer_size)
{
    _buffers.reserve(1);
}

binary_writer::binary_writer(blob &buffer)
    : _buffers({buffer}),
      _current_buffer(const_cast<char *>(buffer.data())),
      _current_offset(0),
      _current_buffer_length(static_cast<int>(buffer.length())),
      _total_size(0),
      _reserved_size_per_buffer(kReservedSizePerBuffer)
{
}

void binary_writer::flush() { commit(); }

void binary_writer::create_buffer(size_t size)
{
    commit();

    blob bb;
    create_new_buffer(size, bb);
    _buffers.push_back(bb);

    _current_buffer = (char *)bb.data();
    _current_buffer_length = bb.length();
}

void binary_writer::create_new_buffer(size_t size, /*out*/ blob &bb)
{
    bb.assign(::dsn::utils::make_shared_array<char>(size), 0, (int)size);
}

void binary_writer::commit()
{
    if (_current_offset > 0) {
        *_buffers.rbegin() = _buffers.rbegin()->range(0, _current_offset);

        _current_offset = 0;
        _current_buffer_length = 0;
    }
}

blob binary_writer::get_buffer()
{
    commit();

    if (_buffers.size() == 1) {
        return _buffers[0];
    } else if (_total_size == 0) {
        return blob();
    } else {
        std::shared_ptr<char> bptr(::dsn::utils::make_shared_array<char>(_total_size));
        blob bb(bptr, _total_size);
        const char *ptr = bb.data();

        for (int i = 0; i < static_cast<int>(_buffers.size()); i++) {
            memcpy((void *)ptr, (const void *)_buffers[i].data(), (size_t)_buffers[i].length());
            ptr += _buffers[i].length();
        }
        return bb;
    }
}

blob binary_writer::get_current_buffer()
{
    if (_buffers.size() == 1) {
        return _current_offset > 0 ? _buffers[0].range(0, _current_offset) : _buffers[0];
    } else {
        std::shared_ptr<char> bptr(::dsn::utils::make_shared_array<char>(_total_size));
        blob bb(bptr, _total_size);
        const char *ptr = bb.data();

        for (int i = 0; i < static_cast<int>(_buffers.size()); i++) {
            size_t len = (size_t)_buffers[i].length();
            if (_current_offset > 0 && i + 1 == (int)_buffers.size()) {
                len = _current_offset;
            }

            memcpy((void *)ptr, (const void *)_buffers[i].data(), len);
            ptr += _buffers[i].length();
        }
        return bb;
    }
}

void binary_writer::write_empty(int sz)
{
    int sz0 = sz;
    int rem_size = _current_buffer_length - _current_offset;
    if (rem_size >= sz) {
        _current_offset += sz;
    } else {
        _current_offset += rem_size;
        sz -= rem_size;

        int allocSize = _reserved_size_per_buffer;
        if (sz > allocSize)
            allocSize = sz;

        create_buffer(allocSize);
        _current_offset += sz;
    }

    _total_size += sz0;
}

void binary_writer::write(const char *buffer, int sz)
{
    int rem_size = _current_buffer_length - _current_offset;
    if (rem_size >= sz) {
        memcpy((void *)(_current_buffer + _current_offset), buffer, (size_t)sz);
        _current_offset += sz;
        _total_size += sz;
    } else {
        if (rem_size > 0) {
            memcpy((void *)(_current_buffer + _current_offset), buffer, (size_t)rem_size);
            _current_offset += rem_size;
            _total_size += rem_size;
            sz -= rem_size;
        }

        int allocSize = _reserved_size_per_buffer;
        if (sz > allocSize)
            allocSize = sz;

        create_buffer(allocSize);
        memcpy((void *)(_current_buffer + _current_offset), buffer + rem_size, (size_t)sz);
        _current_offset += sz;
        _total_size += sz;
    }
}

bool binary_writer::next(void **data, int *size)
{
    int rem_size = _current_buffer_length - _current_offset;
    if (rem_size == 0) {
        create_buffer(_reserved_size_per_buffer);
        rem_size = _current_buffer_length;
    }

    *size = rem_size;
    *data = (void *)(_current_buffer + _current_offset);
    _current_offset = _current_buffer_length;
    _total_size += rem_size;
    return true;
}

bool binary_writer::backup(int count)
{
    assert(count <= _current_offset);
    _current_offset -= count;
    _total_size -= count;
    return true;
}

} // namespace dsn
