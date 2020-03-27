// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>

#include "replica_test_base.h"

namespace dsn {
namespace replication {

class log_block_test : public replica_test_base
{
};

TEST_F(log_block_test, constructor)
{
    log_block block(1);
    ASSERT_EQ(block.data().size(), 1);
    ASSERT_EQ(block.size(), 16);
    ASSERT_EQ(block.mutations().size(), 0);
    ASSERT_EQ(block.callbacks().size(), 0);
    ASSERT_EQ(block.start_offset(), 1);
}

TEST_F(log_block_test, append_mutation)
{
    log_block block(10);
    for (int i = 0; i < 5; i++) {
        block.append_mutation(create_test_mutation(1 + i, "test"), nullptr);
    }
    ASSERT_EQ(block.start_offset(), 10);
    ASSERT_EQ(block.mutations().size(), 5);
    ASSERT_EQ(block.callbacks().size(), 0);

    // each mutation occupies 2 blobs, one for mutation header, one for mutation data.
    ASSERT_EQ(block.data().size(), 1 + 5 * 2);
}
} // namespace replication
} // namespace dsn
