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

#include <fstream>
#include <gtest/gtest.h>

#include "utils/filesystem.h"

#include "common/replication_common.h"

namespace dsn {
namespace replication {

TEST(replication_common, get_data_dir_test)
{
    std::vector<std::string> data_dirs;
    std::vector<std::string> data_dir_tags;
    std::string err_msg = "";

    // Test cases:
    // - default dir: ""
    // - invalid dir:
    //   - "wrong_dir"
    //   - "tag:dir:wrong"
    //   - "tag:"
    //   - "tag1:disk,tag2,disk"
    //   - "tag:disk1,tag:disk2"
    // - valid: "tag1:disk1,tag2:disk2"
    struct get_data_dir_test
    {
        std::string data_dir_str;
        bool expected_val;
        int32_t expected_length;
    } tests[] = {{"", true, 1},
                 {"wrong_dir", false, 0},
                 {"tag:dir:wrong", false, 0},
                 {"tag:", false, 0},
                 {"tag1:disk,tag2,disk", false, 0},
                 {"tag:disk1,tag:disk2", false, 0},
                 {"tag1:disk1", true, 1},
                 {"tag1:disk1, ", true, 1},
                 {"tag1:disk1,tag2:disk2", true, 2}};
    for (const auto &test : tests) {
        data_dirs.clear();
        data_dir_tags.clear();
        bool flag = replication_options::get_data_dir_and_tag(
            test.data_dir_str, "test_dir", "replica", data_dirs, data_dir_tags, err_msg);
        ASSERT_EQ(flag, test.expected_val);
        ASSERT_EQ(data_dirs.size(), data_dir_tags.size());
        ASSERT_EQ(data_dirs.size(), test.expected_length);
    }
}

TEST(replication_common, get_black_list_test)
{
    std::string fname = "black_list_file";
    ASSERT_TRUE(utils::filesystem::create_file(fname));
    std::ofstream test_file;
    test_file.open(fname);
    test_file << "disk1\ndisk2\n";
    test_file.close();

    std::vector<std::string> black_list;
    // Test cases:
    // - file name not set
    // - file not exist
    // - file exist
    struct get_black_list_test
    {
        std::string fname;
        bool has_black_list;
    } tests[] = {{"", false}, {"file_not_exist", false}, {"black_list_file", true}};
    for (const auto &test : tests) {
        black_list.clear();
        replication_options::get_data_dirs_in_black_list(test.fname, black_list);
        ASSERT_EQ(!black_list.empty(), test.has_black_list);
    }
    utils::filesystem::remove_file_name(fname);
}

TEST(replication_common, check_in_black_list_test)
{
    std::vector<std::string> black_list;
    black_list.emplace_back("dir1/");
    black_list.emplace_back("dir2/");

    // Test cases:
    // - empty black list
    // - not in list
    // - in list
    struct check_in_list_test
    {
        bool list_empty;
        std::string dir_str;
        bool expected_result;
    } tests[]{{true, "dir1", false}, {false, "testdir", false}, {false, "dir2", true}};
    for (const auto &test : tests) {
        std::vector<std::string> test_list;
        if (!test.list_empty) {
            test_list = black_list;
        }
        ASSERT_EQ(replication_options::check_if_in_black_list(test_list, test.dir_str),
                  test.expected_result);
    }
}

} // namespace replication
} // namespace dsn
