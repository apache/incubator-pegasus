// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>
#include <dsn/utility/rand.h>

namespace pegasus {
namespace test {
// Helper for quickly generating random data.
class random_generator
{
public:
    random_generator(double compression_ratio, uint32_t value_size);
    std::string random_string(int len, std::string *dst);
    std::string generate(unsigned int len);
    uint32_t uniform(int n);

private:
    std::string _data;
    unsigned int _pos;
};
} // namespace test
} // namespace pegasus
