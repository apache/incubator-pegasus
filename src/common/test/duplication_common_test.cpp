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

#include <cstdint>

#include "gtest/gtest.h"
#include "test_util/test_util.h"
#include "utils/error_code.h"
#include "utils/flags.h"

DSN_DECLARE_string(dup_cluster_name);
DSN_DECLARE_bool(dup_ignore_other_cluster_ids);

namespace dsn {
namespace replication {

TEST(duplication_common, get_duplication_cluster_id)
{
    ASSERT_EQ(1, get_duplication_cluster_id("master-cluster").get_value());
    ASSERT_EQ(2, get_duplication_cluster_id("slave-cluster").get_value());

    ASSERT_EQ(ERR_INVALID_PARAMETERS, get_duplication_cluster_id("").get_error().code());
    ASSERT_EQ(ERR_OBJECT_NOT_FOUND, get_duplication_cluster_id("unknown").get_error().code());
}

TEST(duplication_common, get_current_dup_cluster_id)
{
    ASSERT_EQ(1, get_current_dup_cluster_id());

    // Current cluster id is static, thus updating dup cluster name should never change
    // current cluster id.
    PRESERVE_FLAG(dup_cluster_name);
    FLAGS_dup_cluster_name = "slave-cluster";
    ASSERT_EQ(1, get_current_dup_cluster_id());
}

TEST(duplication_common, get_distinct_cluster_id_set)
{
    ASSERT_EQ(std::set<uint8_t>({1, 2}), get_distinct_cluster_id_set());
}

TEST(duplication_common, is_dup_cluster_id_configured)
{
    ASSERT_FALSE(is_dup_cluster_id_configured(0));
    ASSERT_TRUE(is_dup_cluster_id_configured(1));
    ASSERT_TRUE(is_dup_cluster_id_configured(2));
    ASSERT_FALSE(is_dup_cluster_id_configured(3));
}

TEST(duplication_common, dup_ignore_other_cluster_ids)
{
    PRESERVE_FLAG(dup_ignore_other_cluster_ids);
    FLAGS_dup_ignore_other_cluster_ids = true;

    for (uint8_t id = 0; id < 4; ++id) {
        ASSERT_TRUE(is_dup_cluster_id_configured(id));
    }
}

} // namespace replication
} // namespace dsn
