// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <algorithm>
#include "random_generator.h"

namespace pegasus {
namespace test {
random_generator *random_generator::get_instance()
{
    static random_generator random_generator_;
    return &random_generator_;
}

std::string random_generator::random_string(int len)
{
    std::string dst(len, 0);
    std::generate(std::begin(dst), std::end(dst), random_char);
    return dst;
}

uint32_t random_generator::uniform(int n) { return dsn::rand::next_u32() % n; }

char random_generator::random_char() { return ' ' + uniform(95); }

random_generator::random_generator() {}
} // namespace test
} // namespace pegasus
