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
#include <boost/filesystem.hpp>
#include <nlohmann/json.hpp>

#include "block_service/local/local_service.h"

namespace dsn {
namespace dist {
namespace block_service {

// Simple tests for nlohmann::json serialization, via NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE.

TEST(local_service, store_metadata)
{
    local_file_object file("a.txt");
    error_code ec = file.store_metadata();
    ASSERT_EQ(ec, ERR_OK);

    auto meta_file_path = local_service::get_metafile(file.file_name());
    ASSERT_TRUE(boost::filesystem::exists(meta_file_path));

    std::ifstream ifs(meta_file_path);
    nlohmann::json j;
    ifs >> j;
    ASSERT_EQ(j["md5"], "");
    ASSERT_EQ(j["size"], 0);
}

TEST(local_service, load_metadata)
{
    local_file_object file("a.txt");
    auto meta_file_path = local_service::get_metafile(file.file_name());

    {
        std::ofstream ofs(meta_file_path);
        nlohmann::json j({{"md5", "abcde"}, {"size", 5}});
        ofs << j;
        ofs.close();

        ASSERT_EQ(file.load_metadata(), ERR_OK);
        ASSERT_EQ(file.get_md5sum(), "abcde");
        ASSERT_EQ(file.get_size(), 5);
    }

    {
        std::ofstream ofs(meta_file_path);
        ofs << "invalid json string";
        ofs.close();

        local_file_object file2("a.txt");
        ASSERT_EQ(file2.load_metadata(), ERR_FS_INTERNAL);
    }

    {
        std::ofstream ofs(meta_file_path);
        nlohmann::json j({{"md5", "abcde"}, {"no such key", "illegal"}});
        ofs << j;
        ofs.close();

        local_file_object file2("a.txt");
        ASSERT_EQ(file2.load_metadata(), ERR_FS_INTERNAL);
    }
}

} // namespace block_service
} // namespace dist
} // namespace dsn
