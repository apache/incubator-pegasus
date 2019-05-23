// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include "pegasus_server_test_base.h"

#include <dsn/utility/fail_point.h>
#include <dsn/utility/defer.h>

namespace pegasus {
namespace server {

class capacity_unit_calculator_test : public pegasus_server_test_base
{
    std::unique_ptr<capacity_unit_calculator> _cal;

public:
    capacity_unit_calculator_test() : pegasus_server_test_base()
    {
        _cal = dsn::make_unique<capacity_unit_calculator>(_server.get());
    }

    void test_init() {}
};

TEST_F(capacity_unit_calculator_test, init) {
    test_init();
}

TEST_F(capacity_unit_calculator_test, get) {}

TEST_F(capacity_unit_calculator_test, multi_get) {}

TEST_F(capacity_unit_calculator_test, scan) {}

TEST_F(capacity_unit_calculator_test, sortkey_count) {}

TEST_F(capacity_unit_calculator_test, ttl) {}

TEST_F(capacity_unit_calculator_test, put) {}

TEST_F(capacity_unit_calculator_test, remove) {}

TEST_F(capacity_unit_calculator_test, multi_put) {}

TEST_F(capacity_unit_calculator_test, multi_remove) {}

TEST_F(capacity_unit_calculator_test, incr) {}

TEST_F(capacity_unit_calculator_test, check_and_set) {}

TEST_F(capacity_unit_calculator_test, check_and_mutate) {}

} // namespace server
} // namespace pegasus
