// Copyright (c) 2018, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

namespace pegasus {
namespace test {

enum operation_type
{
    kRead = 0,
    kWrite,
    kDelete,
    kScan,
    kOthers
};

enum write_mode
{
    RANDOM,
    SEQUENTIAL,
    UNIQUE_RANDOM
};
} // namespace test
} // namespace pegasus
