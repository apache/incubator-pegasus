// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "utils/checksum.h"

#include <cstdint>
#include <tuple>
#include <vector>

#include "gtest/gtest.h"
#include "test_util/test_util.h"
#include "utils/defer.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/errors.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils_types.h"

DSN_DECLARE_bool(encrypt_data_at_rest);

namespace dsn {

struct calc_checksum_case
{
    int64_t file_size;
    utils::checksum_type::type type;
    error_code expected_err;
    std::string expected_checksum;
};

class CalcChecksumTest : public testing::TestWithParam<std::tuple<bool, calc_checksum_case>>
{
protected:
    static void test_calc_checksum()
    {
        static const std::string kFilePath("test_file_for_calc_checksum");

        const auto &[file_encrypted, test_case] = GetParam();

        // Set flag to make file encrypted or unencrypted.
        PRESERVE_FLAG(encrypt_data_at_rest);
        FLAGS_encrypt_data_at_rest = file_encrypted;

        // Generate the test file.
        std::shared_ptr<pegasus::local_test_file> local_file;
        NO_FATALS(pegasus::local_test_file::create(
            kFilePath, std::string(test_case.file_size, 'a'), local_file));

        // Check the file size.
        int64_t file_size = 0;
        ASSERT_TRUE(
            utils::filesystem::file_size(kFilePath, utils::FileDataType::kSensitive, file_size));
        ASSERT_EQ(test_case.file_size, file_size);

        // Calculate the file checksum and check the return code.
        std::string actual_checksum;
        const auto actual_status = calc_checksum(kFilePath, test_case.type, actual_checksum);
        ASSERT_EQ(test_case.expected_err, actual_status.code());
        if (!actual_status) {
            return;
        }

        // Check the file checksum only when the return code is ok.
        ASSERT_EQ(test_case.expected_checksum, actual_checksum);
    }
};

const std::vector<calc_checksum_case> calc_checksum_tests = {
    {0, utils::checksum_type::CST_MD5, ERR_OK, "d41d8cd98f00b204e9800998ecf8427e"},
    {1, utils::checksum_type::CST_MD5, ERR_OK, "0cc175b9c0f1b6a831c399e269772661"},
    {2, utils::checksum_type::CST_MD5, ERR_OK, "4124bc0a9335c27f086f24ba207a4912"},
    {4095, utils::checksum_type::CST_MD5, ERR_OK, "559110baa849c7608ee70abe1d76273e"},
    {4096, utils::checksum_type::CST_MD5, ERR_OK, "21a199c53f422a380e20b162fb6ebe9c"},
    {4097, utils::checksum_type::CST_MD5, ERR_OK, "8cfc1a0bd8cd76599e76e5e721c6e62e"},
    {4095, utils::checksum_type::CST_NONE, ERR_OK, ""},
    {4096, utils::checksum_type::CST_NONE, ERR_OK, ""},
    {4097, utils::checksum_type::CST_NONE, ERR_OK, ""},
    {4095, utils::checksum_type::CST_INVALID, ERR_NOT_IMPLEMENTED, ""},
    {4096, utils::checksum_type::CST_INVALID, ERR_NOT_IMPLEMENTED, ""},
    {4097, utils::checksum_type::CST_INVALID, ERR_NOT_IMPLEMENTED, ""},
};

TEST_P(CalcChecksumTest, CalcChecksum) { test_calc_checksum(); }

INSTANTIATE_TEST_SUITE_P(ChecksumTest,
                         CalcChecksumTest,
                         testing::Combine(testing::Bool(), testing::ValuesIn(calc_checksum_tests)));

} // namespace dsn
