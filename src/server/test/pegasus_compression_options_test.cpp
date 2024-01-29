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

#include <rocksdb/compression_type.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>
#include <map>
#include <memory>
#include <string>
#include <vector>

#include "gtest/gtest.h"
#include "pegasus_server_test_base.h"

namespace pegasus {
namespace server {

auto none = rocksdb::kNoCompression;
auto snappy = rocksdb::kSnappyCompression;
auto lz4 = rocksdb::kLZ4Compression;
auto zstd = rocksdb::kZSTD;

class pegasus_compression_options_test : public pegasus_server_test_base
{
public:
    std::string compression_header;

    pegasus_compression_options_test()
    {
        _server->_data_cf_opts.num_levels = 7;
        compression_header = _server->COMPRESSION_HEADER;
    }

    void compression_type_convert(const std::string &str, rocksdb::CompressionType type)
    {
        rocksdb::CompressionType tmp_type;
        ASSERT_TRUE(_server->compression_str_to_type(str, tmp_type)) << str;
        ASSERT_EQ(tmp_type, type) << str << " vs. " << type;

        ASSERT_EQ(str, _server->compression_type_to_str(type)) << str << " vs. " << type;
    }

    void
    compression_types_convert_ok(const std::string &config,
                                 const std::vector<rocksdb::CompressionType> &compression_per_level)
    {
        std::vector<rocksdb::CompressionType> tmp_compression_per_level;
        ASSERT_TRUE(_server->parse_compression_types(config, tmp_compression_per_level)) << config;
        ASSERT_EQ(tmp_compression_per_level, compression_per_level) << config;
    }

    void compression_types_convert_fail(const std::string &config)
    {
        static const std::vector<rocksdb::CompressionType> &old_compression_per_level = {
            none, lz4, snappy, zstd, lz4, snappy, zstd};
        std::vector<rocksdb::CompressionType> tmp_compression_per_level = old_compression_per_level;
        ASSERT_FALSE(_server->parse_compression_types(config, tmp_compression_per_level)) << config;
        ASSERT_EQ(tmp_compression_per_level, old_compression_per_level) << config;
    }

    bool compression_str_to_type(const std::string &compression_str, rocksdb::CompressionType &type)
    {
        return _server->compression_str_to_type(compression_str, type);
    }

    std::string compression_type_to_str(rocksdb::CompressionType type)
    {
        return _server->compression_type_to_str(type);
    }

    void update_app_envs(const std::map<std::string, std::string> &envs)
    {
        _server->update_app_envs(envs);
    }

    void
    check_db_compression_types(const std::vector<rocksdb::CompressionType> &compression_per_level,
                               const std::string &msg = "")
    {
        rocksdb::Options opts = _server->_db->GetOptions();
        ASSERT_EQ(opts.compression_per_level, compression_per_level) << msg;
    }
};

INSTANTIATE_TEST_SUITE_P(, pegasus_compression_options_test, ::testing::Values(false, true));

TEST_P(pegasus_compression_options_test, compression_type_convert_ok)
{
    compression_type_convert("none", none);
    compression_type_convert("snappy", snappy);
    compression_type_convert("lz4", lz4);
    compression_type_convert("zstd", zstd);
}

TEST_P(pegasus_compression_options_test, compression_type_convert_not_support)
{
    rocksdb::CompressionType tmp_type;
    ASSERT_FALSE(compression_str_to_type("not_support_zip", tmp_type));

    ASSERT_EQ("<unsupported>", compression_type_to_str(rocksdb::kZlibCompression));
    ASSERT_EQ("<unsupported>", compression_type_to_str(rocksdb::kBZip2Compression));
    ASSERT_EQ("<unsupported>", compression_type_to_str(rocksdb::kLZ4HCCompression));
    ASSERT_EQ("<unsupported>", compression_type_to_str(rocksdb::kXpressCompression));
    ASSERT_EQ("<unsupported>", compression_type_to_str(rocksdb::kZSTDNotFinalCompression));
    ASSERT_EQ("<unsupported>", compression_type_to_str(rocksdb::kDisableCompressionOption));
}

TEST_P(pegasus_compression_options_test, compression_types_convert_ok)
{
    // Old style.
    compression_types_convert_ok("none", {none, none, none, none, none, none, none});
    compression_types_convert_ok("snappy", {none, none, snappy, snappy, snappy, snappy, snappy});
    compression_types_convert_ok("lz4", {none, none, lz4, lz4, lz4, lz4, lz4});
    compression_types_convert_ok("zstd", {none, none, zstd, zstd, zstd, zstd, zstd});

    // New style.
    compression_types_convert_ok(compression_header + "none",
                                 {none, none, none, none, none, none, none});
    compression_types_convert_ok(compression_header + "none,snappy",
                                 {none, snappy, snappy, snappy, snappy, snappy, snappy});
    compression_types_convert_ok(compression_header + "none,lz4,snappy,zstd",
                                 {none, lz4, snappy, zstd, zstd, zstd, zstd});
    compression_types_convert_ok(compression_header + "none,lz4,snappy,zstd,lz4,snappy,zstd",
                                 {none, lz4, snappy, zstd, lz4, snappy, zstd});
    compression_types_convert_ok(compression_header + "none,lz4,snappy,zstd,lz4,snappy,zstd,zstd",
                                 {none, lz4, snappy, zstd, lz4, snappy, zstd});
}

TEST_P(pegasus_compression_options_test, compression_types_convert_fail)
{
    // Old style.
    compression_types_convert_fail("none1");
    compression_types_convert_fail("Snappy");
    compression_types_convert_fail(",zstd");

    // New style.
    compression_types_convert_fail(compression_header + ":snappy");
    compression_types_convert_fail(compression_header + "snappy,snappy1");
    compression_types_convert_fail("per_leve:snappy");
    compression_types_convert_fail("per_levelsnappy");
}

TEST_P(pegasus_compression_options_test, check_rocksdb_compression_types_default)
{
    start();
    check_db_compression_types({none, none, lz4, lz4, lz4, lz4}, "start with default");
}

} // namespace server
} // namespace pegasus
