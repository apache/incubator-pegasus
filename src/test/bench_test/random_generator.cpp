// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <assert.h>
#include "random_generator.h"

namespace pegasus {
namespace test {

random_generator::random_generator(double compression_ratio, uint32_t value_size)
{
    // We use a limited amount of data over and over again and ensure
    // that it is larger than the compression window (32KB), and also
    // large enough to serve all typical value sizes we want to write.
    std::string piece;
    while (_data.size() < (unsigned)std::max(1048576U, value_size)) {
        // Add a short fragment that is as compressible as specified
        // by compression_ratio.
        /*test::*/
        random_string(100, &piece);
        _data.append(piece);
    }
    _pos = 0;
}

std::string random_generator::random_string(int len, std::string *dst)
{
    dst->resize(len);
    for (int i = 0; i < len; i++) {
        (*dst)[i] = static_cast<char>(' ' + uniform(95)); // ' ' .. '~'
    }
    return *dst;
}

std::string random_generator::generate(unsigned int len)
{
    assert(len <= _data.size());
    if (_pos + len > _data.size()) {
        _pos = 0;
    }
    _pos += len;
    return _data.substr(_pos - len, len);
}

uint32_t random_generator::uniform(int n) { return dsn::rand::next_u32() % n; }
} // namespace test
} // namespace pegasus
