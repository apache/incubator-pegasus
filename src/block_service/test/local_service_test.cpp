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

#include <boost/filesystem/operations.hpp>
#include <nlohmann/detail/json_ref.hpp>
#include <nlohmann/json.hpp>
#include <nlohmann/json_fwd.hpp>
#include <rocksdb/env.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <initializer_list>
#include <map>
#include <stdexcept>
#include <string>
#include <vector>

#include "block_service/local/local_service.h"
#include "gtest/gtest.h"
#include "test_util/test_util.h"
#include "utils/env.h"
#include "utils/error_code.h"

namespace dsn {
namespace dist {
namespace block_service {

// Simple tests for nlohmann::json serialization, via NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE.
class local_service_test : public pegasus::encrypt_data_test_base
{
};

INSTANTIATE_TEST_CASE_P(, local_service_test, ::testing::Values(false, true));

TEST_P(local_service_test, store_metadata)
{
    local_file_object file("a.txt");
    error_code ec = file.store_metadata();
    ASSERT_EQ(ERR_OK, ec);

    auto meta_file_path = local_service::get_metafile(file.file_name());
    ASSERT_TRUE(boost::filesystem::exists(meta_file_path));

    std::string data;
    auto s = rocksdb::ReadFileToString(
        dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive), meta_file_path, &data);
    ASSERT_TRUE(s.ok()) << s.ToString();

    nlohmann::json j = nlohmann::json::parse(data);
    ASSERT_EQ("", j["md5"]);
    ASSERT_EQ(0, j["size"]);
}

TEST_P(local_service_test, load_metadata)
{
    local_file_object file("a.txt");
    auto meta_file_path = local_service::get_metafile(file.file_name());

    {
        nlohmann::json j({{"md5", "abcde"}, {"size", 5}});
        std::string data = j.dump();
        auto s =
            rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive),
                                       rocksdb::Slice(data),
                                       meta_file_path,
                                       /* should_sync */ true);
        ASSERT_TRUE(s.ok()) << s.ToString();

        ASSERT_EQ(ERR_OK, file.load_metadata());
        ASSERT_EQ("abcde", file.get_md5sum());
        ASSERT_EQ(5, file.get_size());
    }

    {
        auto s =
            rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive),
                                       rocksdb::Slice("invalid json string"),
                                       meta_file_path,
                                       /* should_sync */ true);
        ASSERT_TRUE(s.ok()) << s.ToString();

        local_file_object file2("a.txt");
        ASSERT_EQ(file2.load_metadata(), ERR_FS_INTERNAL);
    }

    {
        nlohmann::json j({{"md5", "abcde"}, {"no such key", "illegal"}});
        std::string data = j.dump();
        auto s =
            rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive),
                                       rocksdb::Slice(data),
                                       meta_file_path,
                                       /* should_sync */ true);
        ASSERT_TRUE(s.ok()) << s.ToString();

        local_file_object file2("a.txt");
        ASSERT_EQ(file2.load_metadata(), ERR_FS_INTERNAL);
    }
}

} // namespace block_service
} // namespace dist
} // namespace dsn
