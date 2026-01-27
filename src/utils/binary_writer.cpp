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

#include <algorithm>
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

void binary_writer::flush() { commit(); }

void binary_writer::create_buffer(size_t size)
{
    commit();

    blob bb;
    create_new_buffer(size, bb);
    _buffers.push_back(bb);

    _current_buffer = const_cast<char *>(bb.data());
    _current_buffer_length = static_cast<int>(bb.length());
}

void binary_writer::create_new_buffer(size_t size, /*out*/ blob &bb)
{
    bb.assign(utils::make_shared_array<char>(size), 0, size);
}

void binary_writer::commit()
{
    if (_current_offset <= 0) {
        return;
    }

    // Commit the last buffer.
    *_buffers.rbegin() = _buffers.rbegin()->range(0, _current_offset);

    _current_offset = 0;
    _current_buffer_length = 0;
}

blob binary_writer::get_buffer()
{
    commit();

    if (_buffers.size() == 1) {
        return _buffers[0];
    }

    if (_total_size == 0) {
        return {};
    }

    blob bb(utils::make_shared_array<char>(_total_size), _total_size);
    auto *ptr = const_cast<char *>(bb.data());

    for (const auto &buf : _buffers) {
        std::memcpy(ptr, buf.data(), buf.length());
        ptr += buf.length();
    }

    return bb;
}

blob binary_writer::get_current_buffer() const
{
    if (_buffers.size() == 1) {
        return _current_offset > 0 ? _buffers[0].range(0, _current_offset) : _buffers[0];
    }

    blob bb(utils::make_shared_array<char>(_total_size), _total_size);
    if (_buffers.empty()) {
        // TODO(wangdan): just return a default-initialized blob object?
        return bb;
    }

    auto *ptr = const_cast<char *>(bb.data());
    int i = 0;

    // Now the size of _buffers is at least 2.
    for (; i < static_cast<int>(_buffers.size()) - 1; ++i) {
        std::memcpy(ptr, _buffers[i].data(), _buffers[i].length());
        ptr += _buffers[i].length();
    }

    // Get bytes from the last buffer(namely current buffer).
    if (_current_offset > 0) {
        std::memcpy(ptr, _buffers[i].data(), _current_offset);
    } else {
        std::memcpy(ptr, _buffers[i].data(), _buffers[i].length());
    }

    return bb;
}

void binary_writer::write_empty(int size)
{
    const int remaining_size = _current_buffer_length - _current_offset;
    if (remaining_size >= size) {
        _current_offset += size;
        _total_size += size;
        return;
    }

    _current_offset += remaining_size;
    _total_size += remaining_size;
    size -= remaining_size;

    // Because the create_buffer() function will commit the last buffer - that is, it reads
    // `_current_offset` first and then resets it - we need to ensure that `_current_offset`
    // has already been updated to the latest value before create_buffer() is called.
    create_buffer(std::max(size, _reserved_size_per_buffer));

    _current_offset += size;
    _total_size += size;
}

void binary_writer::write(const char *buffer, int size)
{
    const int remaining_size = _current_buffer_length - _current_offset;
    if (remaining_size >= size) {
        std::memcpy(_current_buffer + _current_offset, buffer, size);
        _current_offset += size;
        _total_size += size;
        return;
    }

    if (remaining_size > 0) {
        std::memcpy(_current_buffer + _current_offset, buffer, remaining_size);
        _current_offset += remaining_size;
        _total_size += remaining_size;
        size -= remaining_size;
    }

    // Because the create_buffer() function will commit the last buffer - that is, it reads
    // `_current_offset` first and then resets it - we need to ensure that `_current_offset`
    // has already been updated to the latest value before create_buffer() is called.
    create_buffer(std::max(size, _reserved_size_per_buffer));

    std::memcpy(_current_buffer + _current_offset, buffer + remaining_size, size);
    _current_offset += size;
    _total_size += size;
}

} // namespace dsn
