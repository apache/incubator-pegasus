// Copyright (c) 2017, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <cstdlib>
#include <string>
#include <vector>
#include <map>

#include <dsn/service_api_c.h>
#include <unistd.h>
#include <pegasus/client.h>
#include <gtest/gtest.h>

using namespace ::pegasus;

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    int ans = RUN_ALL_TESTS();
    dsn_exit(ans);
}
