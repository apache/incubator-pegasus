/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#include "common//duplication_common.h"
#include <gtest/gtest.h>

namespace dsn {
namespace replication {

TEST(duplication_common, get_duplication_cluster_id)
{
    ASSERT_EQ(get_duplication_cluster_id("master-cluster").get_value(), 1);
    ASSERT_EQ(get_duplication_cluster_id("slave-cluster").get_value(), 2);

    ASSERT_EQ(get_duplication_cluster_id("").get_error().code(), ERR_INVALID_PARAMETERS);
    ASSERT_EQ(get_duplication_cluster_id("unknown").get_error().code(), ERR_OBJECT_NOT_FOUND);
}

TEST(duplication_common, get_distinct_cluster_id_set)
{
    ASSERT_EQ(get_distinct_cluster_id_set(), std::set<uint8_t>({1, 2}));
}

} // namespace replication
} // namespace dsn
