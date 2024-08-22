/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include "common/common.h"

#include "gtest/gtest.h"
#include "test_util/test_util.h"
#include "utils/flags.h"

DSN_DECLARE_string(dup_cluster_name);

namespace dsn {

TEST(duplication_common, get_current_cluster_name)
{
    ASSERT_STREQ("master-cluster", get_current_cluster_name());
}

TEST(duplication_common, get_current_dup_cluster_name)
{
    ASSERT_STREQ("master-cluster", get_current_dup_cluster_name());

    PRESERVE_FLAG(dup_cluster_name);
    FLAGS_dup_cluster_name = "slave-cluster";
    ASSERT_STREQ("slave-cluster", get_current_dup_cluster_name());
}

} // namespace dsn
