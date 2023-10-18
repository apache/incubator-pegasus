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

/*
 * Description:
 *     Unit-test for rpc_address.
 *
 * Revision history:
 *     Nov., 2015, @qinzuoyan (Zuoyan Qin), first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <fmt/core.h>
#include <gtest/gtest-param-test.h>
// IWYU pragma: no_include <gtest/gtest-message.h>
// IWYU pragma: no_include <gtest/gtest-test-part.h>
#include <gtest/gtest.h>
#include <rocksdb/env.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>
#include <stdint.h>
#include <algorithm>
#include <limits>
#include <string>

#include "test_util/test_util.h"
#include "utils/env.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"
#include "utils/flags.h"
#include "utils/rand.h"

DSN_DECLARE_bool(encrypt_data_at_rest);

using namespace ::dsn;

TEST(env_test, rand)
{
    uint64_t xs[] = {0, std::numeric_limits<uint64_t>::max() - 1, 0xdeadbeef};

    for (auto &x : xs) {
        auto r = rand::next_u64(x, x);
        EXPECT_EQ(r, x);

        r = rand::next_u64(x, x + 1);
        EXPECT_TRUE(r == x || r == (x + 1));
    }
}

TEST(env_test, get_env)
{
    FLAGS_encrypt_data_at_rest = false;
    auto *env_no_enc1 = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kNonSensitive);
    auto *env_no_enc2 = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive);
    ASSERT_EQ(env_no_enc1, env_no_enc2);

    FLAGS_encrypt_data_at_rest = true;
    auto *env_no_enc3 = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kNonSensitive);
    ASSERT_EQ(env_no_enc1, env_no_enc3);

    auto *env_enc1 = dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive);
    ASSERT_NE(env_no_enc1, env_enc1);
}

class env_file_test : public pegasus::encrypt_data_test_base
{
public:
    env_file_test() : pegasus::encrypt_data_test_base()
    {
        // The size of an actual encrypted file should plus kEncryptionHeaderkSize bytes if consider
        // it as kNonSensitive.
        if (FLAGS_encrypt_data_at_rest) {
            _extra_size = dsn::utils::kEncryptionHeaderkSize;
        }
    }
    uint64_t _extra_size = 0;
};

INSTANTIATE_TEST_CASE_P(, env_file_test, ::testing::Values(false, true));

TEST_P(env_file_test, encrypt_file_2_files)
{
    const std::string kFileName = "encrypt_file_2_files";
    const std::string kEncryptedFileName = kFileName + ".encrypted";
    const uint64_t kFileContentSize = 100;
    const std::string kFileContent(kFileContentSize, 'a');

    // Prepare a non-encrypted test file.
    auto s =
        rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(dsn::utils::FileDataType::kNonSensitive),
                                   rocksdb::Slice(kFileContent),
                                   kFileName,
                                   /* should_sync */ true);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // Check file size.
    int64_t wfile_size;
    ASSERT_TRUE(dsn::utils::filesystem::file_size(
        kFileName, dsn::utils::FileDataType::kNonSensitive, wfile_size));
    ASSERT_EQ(kFileContentSize, wfile_size);

    // Check encrypt_file(src_fname, dst_fname, total_size).
    // Loop twice to check overwrite.
    for (int i = 0; i < 2; ++i) {
        uint64_t encrypt_file_size;
        s = dsn::utils::encrypt_file(kFileName, kEncryptedFileName, &encrypt_file_size);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(kFileContentSize, encrypt_file_size);
        ASSERT_TRUE(dsn::utils::filesystem::file_size(
            kEncryptedFileName, dsn::utils::FileDataType::kSensitive, wfile_size));
        ASSERT_EQ(kFileContentSize, wfile_size);
        ASSERT_TRUE(dsn::utils::filesystem::file_size(
            kEncryptedFileName, dsn::utils::FileDataType::kNonSensitive, wfile_size));
        ASSERT_EQ(kFileContentSize + _extra_size, wfile_size);
        // Check file content.
        std::string data;
        s = rocksdb::ReadFileToString(dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive),
                                      kEncryptedFileName,
                                      &data);
        ASSERT_EQ(kFileContent, data);
    }
}

TEST_P(env_file_test, encrypt_file_1_file)
{
    const std::string kFileName = "encrypt_file_1_file";
    const uint64_t kFileContentSize = 100;
    const std::string kFileContent(kFileContentSize, 'a');

    // Prepare a non-encrypted test file.
    auto s =
        rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(dsn::utils::FileDataType::kNonSensitive),
                                   rocksdb::Slice(kFileContent),
                                   kFileName,
                                   /* should_sync */ true);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // Check file size.
    int64_t wfile_size;
    ASSERT_TRUE(dsn::utils::filesystem::file_size(
        kFileName, dsn::utils::FileDataType::kNonSensitive, wfile_size));
    ASSERT_EQ(kFileContentSize, wfile_size);

    // Check encrypt_file(fname, total_size).
    uint64_t encrypt_file_size;
    s = dsn::utils::encrypt_file(kFileName, &encrypt_file_size);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_EQ(kFileContentSize, encrypt_file_size);
    ASSERT_TRUE(dsn::utils::filesystem::file_size(
        kFileName, dsn::utils::FileDataType::kSensitive, wfile_size));
    ASSERT_EQ(kFileContentSize, wfile_size);
    ASSERT_TRUE(dsn::utils::filesystem::file_size(
        kFileName, dsn::utils::FileDataType::kNonSensitive, wfile_size));
    ASSERT_EQ(kFileContentSize + _extra_size, wfile_size);
    // Check file content.
    std::string data;
    s = rocksdb::ReadFileToString(
        dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive), kFileName, &data);
    ASSERT_EQ(kFileContent, data);
}

TEST_P(env_file_test, copy_file)
{
    const std::string kFileName = "copy_file";
    const std::string kCopyFileName = kFileName + ".copy";
    const uint64_t kFileContentSize = 100;
    const std::string kFileContent(kFileContentSize, 'a');

    // Prepare an encrypted test file.
    auto s =
        rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive),
                                   rocksdb::Slice(kFileContent),
                                   kFileName,
                                   /* should_sync */ true);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // Check file size.
    int64_t wfile_size;
    ASSERT_TRUE(dsn::utils::filesystem::file_size(
        kFileName, dsn::utils::FileDataType::kSensitive, wfile_size));
    ASSERT_EQ(kFileContentSize, wfile_size);
    ASSERT_TRUE(dsn::utils::filesystem::file_size(
        kFileName, dsn::utils::FileDataType::kNonSensitive, wfile_size));
    ASSERT_EQ(kFileContentSize + _extra_size, wfile_size);

    // Check copy_file(src_fname, dst_fname, total_size).
    // Loop twice to check overwrite.
    for (int i = 0; i < 2; ++i) {
        uint64_t copy_file_size;
        s = dsn::utils::copy_file(kFileName, kCopyFileName, &copy_file_size);
        ASSERT_TRUE(s.ok()) << s.ToString();
        ASSERT_EQ(kFileContentSize, copy_file_size);
        ASSERT_TRUE(dsn::utils::filesystem::file_size(
            kCopyFileName, dsn::utils::FileDataType::kSensitive, wfile_size));
        ASSERT_EQ(kFileContentSize, wfile_size);
        ASSERT_TRUE(dsn::utils::filesystem::file_size(
            kCopyFileName, dsn::utils::FileDataType::kNonSensitive, wfile_size));
        ASSERT_EQ(kFileContentSize + _extra_size, wfile_size);
        // Check file content.
        std::string data;
        s = rocksdb::ReadFileToString(
            dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive), kCopyFileName, &data);
        ASSERT_EQ(kFileContent, data);
    }
}

TEST_P(env_file_test, copy_file_by_size)
{
    const std::string kFileName = "copy_file_by_size";
    std::string kCopyFileName = kFileName + ".copy";
    const uint64_t kFileContentSize = 100;
    const std::string kFileContent(kFileContentSize, 'a');

    // Prepare an encrypted test file.
    auto s =
        rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive),
                                   rocksdb::Slice(kFileContent),
                                   kFileName,
                                   /* should_sync */ true);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // Check file size.
    int64_t wfile_size;
    ASSERT_TRUE(dsn::utils::filesystem::file_size(
        kFileName, dsn::utils::FileDataType::kSensitive, wfile_size));
    ASSERT_EQ(kFileContentSize, wfile_size);
    ASSERT_TRUE(dsn::utils::filesystem::file_size(
        kFileName, dsn::utils::FileDataType::kNonSensitive, wfile_size));
    ASSERT_EQ(kFileContentSize + _extra_size, wfile_size);

    // Check copy_file_by_size(src_fname, dst_fname, limit_size).
    struct test_case
    {
        int64_t limit_size;
        int64_t expect_size;
    } tests[] = {{-1, kFileContentSize},
                 {0, 0},
                 {10, 10},
                 {kFileContentSize, kFileContentSize},
                 {kFileContentSize + 10, kFileContentSize}};
    for (const auto &test : tests) {
        s = dsn::utils::copy_file_by_size(kFileName, kCopyFileName, test.limit_size);
        ASSERT_TRUE(s.ok()) << s.ToString();

        int64_t actual_size;
        ASSERT_TRUE(dsn::utils::filesystem::file_size(
            kCopyFileName, dsn::utils::FileDataType::kSensitive, actual_size));
        ASSERT_EQ(test.expect_size, actual_size);
        ASSERT_TRUE(dsn::utils::filesystem::file_size(
            kCopyFileName, dsn::utils::FileDataType::kNonSensitive, wfile_size));
        ASSERT_EQ(test.expect_size + _extra_size, wfile_size);
        // Check file content.
        std::string data;
        s = rocksdb::ReadFileToString(
            dsn::utils::PegasusEnv(dsn::utils::FileDataType::kSensitive), kCopyFileName, &data);
        ASSERT_EQ(std::string(test.expect_size, 'a'), data);
    }
}

TEST_P(env_file_test, copy_non_encrypt_file)
{
    const std::string kFileName = "copy_non_encrypt_file";
    std::string kCopyFileName = kFileName + ".copy";
    const uint64_t kFileContentSize = 100;
    const std::string kFileContent(kFileContentSize, 'a');

    // Prepare a non-encrypted test file.
    auto s =
        rocksdb::WriteStringToFile(dsn::utils::PegasusEnv(dsn::utils::FileDataType::kNonSensitive),
                                   rocksdb::Slice(kFileContent),
                                   kFileName,
                                   /* should_sync */ true);
    ASSERT_TRUE(s.ok()) << s.ToString();

    // Check file size.
    int64_t wfile_size;
    ASSERT_TRUE(dsn::utils::filesystem::file_size(
        kFileName, dsn::utils::FileDataType::kNonSensitive, wfile_size));
    ASSERT_EQ(kFileContentSize, wfile_size);

    // Check copy_file() on non-sensitive file.
    s = dsn::utils::copy_file(kFileName, kCopyFileName);
    if (FLAGS_encrypt_data_at_rest) {
        // copy_file() consider the source file as encrypted, so it will fail.
        ASSERT_TRUE(s.IsCorruption()) << s.ToString();
        ASSERT_TRUE(s.ToString().find(
                        fmt::format("Corruption: Invalid encryption header in {}", kFileName)) == 0)
            << s.ToString();
    } else {
        // Although copy_file() consider the source file as non-encrypted, but it will succeed if
        // FLAGS_encrypt_data_at_rest is disabled.
        ASSERT_TRUE(s.ok()) << s.ToString();
        int64_t copy_file_size;
        ASSERT_TRUE(dsn::utils::filesystem::file_size(
            kCopyFileName, dsn::utils::FileDataType::kNonSensitive, copy_file_size));
        ASSERT_EQ(kFileContentSize, copy_file_size);
    }

    // Check copy_file_by_size() on non-sensitive file.
    s = dsn::utils::copy_file_by_size(kFileName, kCopyFileName);
    if (FLAGS_encrypt_data_at_rest) {
        // copy_file_by_size() consider the source file as encrypted, so it will fail.
        ASSERT_TRUE(s.IsCorruption()) << s.ToString();
        ASSERT_TRUE(s.ToString().find(
                        fmt::format("Corruption: Invalid encryption header in {}", kFileName)) == 0)
            << s.ToString();
    } else {
        // Although copy_file_by_size() consider the source file as non-encrypted, but it will
        // succeed if FLAGS_encrypt_data_at_rest is disabled.
        ASSERT_TRUE(s.ok()) << s.ToString();
        int64_t copy_file_size;
        ASSERT_TRUE(dsn::utils::filesystem::file_size(
            kCopyFileName, dsn::utils::FileDataType::kNonSensitive, copy_file_size));
        ASSERT_EQ(kFileContentSize, copy_file_size);
    }
}
