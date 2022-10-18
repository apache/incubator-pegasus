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

#include <cstring>

#include <gtest/gtest_prod.h>

#include "utils/blob.h"

namespace dsn {
class binary_reader
{
public:
    // given bb on ctor
    binary_reader(const blob &blob);
    binary_reader(blob &&blob);

    // or delayed init
    binary_reader() {}

    virtual ~binary_reader() {}

    void init(const blob &bb);
    void init(blob &&bb);

    template <typename T>
    int read_pod(/*out*/ T &val);
    template <typename T>
    int read(/*out*/ T &val)
    {
        // read of this type is not implemented
        assert(false);
        return 0;
    }
    int read(/*out*/ int8_t &val) { return read_pod(val); }
    int read(/*out*/ uint8_t &val) { return read_pod(val); }
    int read(/*out*/ int16_t &val) { return read_pod(val); }
    int read(/*out*/ uint16_t &val) { return read_pod(val); }
    int read(/*out*/ int32_t &val) { return read_pod(val); }
    int read(/*out*/ uint32_t &val) { return read_pod(val); }
    int read(/*out*/ int64_t &val) { return read_pod(val); }
    int read(/*out*/ uint64_t &val) { return read_pod(val); }
    int read(/*out*/ bool &val) { return read_pod(val); }

    int read(/*out*/ std::string &s);
    virtual int read(char *buffer, int sz);
    int read(blob &blob);
    virtual int read(blob &blob, int len);

    blob get_buffer() const { return _blob; }
    blob get_remaining_buffer() const { return _blob.range(static_cast<int>(_ptr - _blob.data())); }
    bool is_eof() const { return _ptr >= _blob.data() + _size; }
    int total_size() const { return _size; }
    int get_remaining_size() const { return _remaining_size; }

protected:
    int inner_read(blob &blob, int len);
    int inner_read(char *buffer, int sz);

private:
    blob _blob;
    int _size;
    const char *_ptr;
    int _remaining_size;

    FRIEND_TEST(binary_reader_test, inner_read);
};

template <typename T>
inline int binary_reader::read_pod(/*out*/ T &val)
{
    if (sizeof(T) <= get_remaining_size()) {
        memcpy((void *)&val, _ptr, sizeof(T));
        _ptr += sizeof(T);
        _remaining_size -= sizeof(T);
        return static_cast<int>(sizeof(T));
    } else {
        // read beyond the end of buffer
        assert(false);
        return 0;
    }
}
} // namespace dsn
