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

#include "binary_reader.h"

#include <memory>
#include <utility>

#include "utils.h"
#include "utils/ports.h"

namespace dsn {

binary_reader::binary_reader(const blob &bb) { init(bb); }
binary_reader::binary_reader(blob &&bb) { init(std::move(bb)); }

void binary_reader::init(const blob &bb)
{
    _blob = bb;
    _size = bb.length();
    _ptr = bb.data();
    _remaining_size = _size;
}

void binary_reader::init(blob &&bb)
{
    _blob = std::move(bb);
    _size = _blob.length();
    _ptr = _blob.data();
    _remaining_size = _size;
}

int binary_reader::read(/*out*/ std::string &s)
{
    int len;
    if (0 == read(len))
        return 0;

    s.resize(len, 0);

    if (len > 0) {
        int x = read((char *)&s[0], len);
        return x == 0 ? x : (x + sizeof(len));
    } else {
        return static_cast<int>(sizeof(len));
    }
}

int binary_reader::read(blob &blob)
{
    int len;
    if (0 == read(len))
        return 0;

    return read(blob, len);
}

int binary_reader::read(blob &blob, int len)
{
    auto res = inner_read(blob, len);
    if (dsn_unlikely(res < 0)) {
        assert(false);
    }
    return res;
}

int binary_reader::read(char *buffer, int sz)
{
    auto res = inner_read(buffer, sz);
    if (dsn_unlikely(res < 0)) {
        assert(false);
    }
    return res;
}

int binary_reader::inner_read(blob &blob, int len)
{
    if (len <= get_remaining_size()) {
        blob = _blob.range(static_cast<int>(_ptr - _blob.data()), len);

        // optimization: zero-copy
        if (!blob.buffer()) {
            std::shared_ptr<char> buffer(::dsn::utils::make_shared_array<char>(len));
            memcpy(buffer.get(), blob.data(), blob.length());
            blob = ::dsn::blob(buffer, blob.length());
        }

        _ptr += len;
        _remaining_size -= len;
        return len + sizeof(len);
    } else {
        return -1;
    }
}

int binary_reader::inner_read(char *buffer, int sz)
{
    if (sz <= get_remaining_size()) {
        memcpy((void *)buffer, _ptr, sz);
        _ptr += sz;
        _remaining_size -= sz;
        return sz;
    } else {
        return -1;
    }
}
} // namespace dsn
