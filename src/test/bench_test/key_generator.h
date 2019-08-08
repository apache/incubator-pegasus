// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include <vector>
#include "utils.h"

namespace pegasus {
namespace test {

class key_generator {
public:
    key_generator(write_mode mode, uint64_t num, uint64_t num_per_set = 64 * 1024);
    uint64_t next();

private:
    write_mode _mode;
    const uint64_t _num;
    uint64_t _next;
    std::vector<uint64_t> _values;
};

}
}

