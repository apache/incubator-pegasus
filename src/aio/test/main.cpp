// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>
#include <dsn/service_api_cpp.h>

GTEST_API_ int main(int argc, char **argv)
{
    testing::InitGoogleTest(&argc, argv);
    dsn_run_config("config.ini", false);
    int g_test_ret = RUN_ALL_TESTS();
#ifndef ENABLE_GCOV
    dsn_exit(g_test_ret);
#endif
    return g_test_ret;
}
