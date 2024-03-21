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
#include <stdint.h>
#include <string>

#include "block_service/local/local_service.h"
#include "gtest/gtest.h"
#include "test_util/test_util.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/load_dump_object.h"

namespace dsn {
namespace dist {
namespace block_service {

// Simple tests for nlohmann::json serialization, via NLOHMANN_DEFINE_TYPE_NON_INTRUSIVE.
class local_service_test : public pegasus::encrypt_data_test_base
{
};

INSTANTIATE_TEST_SUITE_P(, local_service_test, ::testing::Values(false, true));

TEST_P(local_service_test, file_metadata)
{
    const int64_t kSize = 12345;
    const std::string kMD5 = "0123456789abcdef0123456789abcdef";
    auto meta_file_path = local_service::get_metafile("a.txt");
    ASSERT_EQ(ERR_OK, dsn::utils::dump_njobj_to_file(file_metadata(kSize, kMD5), meta_file_path));
    ASSERT_TRUE(boost::filesystem::exists(meta_file_path));

    file_metadata fm;
    ASSERT_EQ(ERR_OK, dsn::utils::load_njobj_from_file(meta_file_path, &fm));
    ASSERT_EQ(kSize, fm.size);
    ASSERT_EQ(kMD5, fm.md5);
}

TEST_P(local_service_test, load_metadata)
{
    local_file_object file("a.txt");
    auto meta_file_path = local_service::get_metafile(file.file_name());

    {
        ASSERT_EQ(ERR_OK,
                  dsn::utils::dump_njobj_to_file(file_metadata(5, "abcde"), meta_file_path));
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
        ASSERT_EQ(ERR_FS_INTERNAL, file2.load_metadata());
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
