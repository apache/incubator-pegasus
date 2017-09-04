#include <gtest/gtest.h>
#include "dist/replication/meta_server/meta_data.h"

using namespace dsn::replication;

TEST(meta_data, dropped_cmp)
{
    dsn::rpc_address n;

    dropped_replica d1, d2;
    // time not equal
    {
        d1 = {n, 10, 5, 5, 5};
        d2 = {n, 9, 20, 5, 5};
        ASSERT_TRUE(dropped_cmp(d1, d2) > 0);
        ASSERT_TRUE(dropped_cmp(d2, d1) < 0);
    }
    // ballot not equal
    {
        d1 = {n, 0, 4, 4, 4};
        d2 = {n, 0, 5, 3, 3};

        ASSERT_TRUE(dropped_cmp(d1, d2) < 0);
        ASSERT_TRUE(dropped_cmp(d2, d1) > 0);
    }
    // last_committed_decree not equal
    {
        d1 = {n, 0, 4, 4, 4};
        d2 = {n, 0, 4, 6, 3};

        ASSERT_TRUE(dropped_cmp(d1, d2) < 0);
        ASSERT_TRUE(dropped_cmp(d2, d1) > 0);
    }
    // last_prepared_deree not equal
    {
        d1 = {n, 0, 7, 8, 9};
        d2 = {n, 0, 7, 8, 10};

        ASSERT_TRUE(dropped_cmp(d1, d2) < 0);
        ASSERT_TRUE(dropped_cmp(d2, d1) > 0);
    }
    // the same
    {
        d1 = {n, 0, 6, 6, 7};
        d2 = {n, 0, 6, 6, 7};

        ASSERT_TRUE(dropped_cmp(d1, d2) == 0);
        ASSERT_TRUE(dropped_cmp(d2, d1) == 0);
    }
}
