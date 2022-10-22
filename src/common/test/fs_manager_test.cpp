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

#include <gtest/gtest.h>
#include "utils/fail_point.h"

#include "common/fs_manager.h"

namespace dsn {
namespace replication {

TEST(fs_manager, dir_update_disk_status)
{
    std::shared_ptr<dir_node> node = std::make_shared<dir_node>("tag", "path");
    struct update_disk_status
    {
        bool update_status;
        bool mock_insufficient;
        disk_status::type old_disk_status;
        disk_status::type new_disk_status;
        bool expected_ret;
    } tests[] = {
        {false, false, disk_status::NORMAL, disk_status::NORMAL, false},
        {false, true, disk_status::NORMAL, disk_status::NORMAL, false},
        {true, false, disk_status::NORMAL, disk_status::NORMAL, false},
        {true, false, disk_status::SPACE_INSUFFICIENT, disk_status::NORMAL, true},
        {true, true, disk_status::NORMAL, disk_status::SPACE_INSUFFICIENT, true},
        {true, true, disk_status::SPACE_INSUFFICIENT, disk_status::SPACE_INSUFFICIENT, false}};
    for (const auto &test : tests) {
        node->status = test.old_disk_status;
        fail::setup();
        if (test.mock_insufficient) {
            fail::cfg("filesystem_get_disk_space_info", "return(insufficient)");
        } else {
            fail::cfg("filesystem_get_disk_space_info", "return(normal)");
        }
        ASSERT_EQ(test.expected_ret, node->update_disk_stat(test.update_status));
        ASSERT_EQ(test.new_disk_status, node->status);
        fail::teardown();
    }
}

} // namespace replication
} // namespace dsn
