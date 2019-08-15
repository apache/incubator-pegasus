// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <string>
#include <dsn/utility/rand.h>

namespace pegasus {
namespace test {
class random_generator
{
public:
    static random_generator *get_instance();
    static std::string random_string(int len);
    static uint32_t uniform(int n);
    static char random_char();

private:
    random_generator();
};
} // namespace test
} // namespace pegasus
