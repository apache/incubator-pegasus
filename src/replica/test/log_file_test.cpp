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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "aio/aio_task.h"
#include "common/replication.codes.h"
#include "gtest/gtest.h"
#include "replica/log_block.h"
#include "replica/log_file.h"
#include "replica_test_base.h"
#include "utils/autoref_ptr.h"
#include "utils/error_code.h"
#include "utils/filesystem.h"

namespace dsn::replication {

struct parse_log_file_name_case
{
    const char *path;
    error_code expected_err;
    int expected_index;
    int64_t expected_start_offset;
};

class ParseLogFileNameTest : public testing::TestWithParam<parse_log_file_name_case>
{
public:
    static void test_parse_log_file_name()
    {
        const auto &test_case = GetParam();

        int actual_index{0};
        int64_t actual_start_offset{0};
        ASSERT_EQ(test_case.expected_err,
                  log_file::parse_log_file_name(test_case.path, actual_index, actual_start_offset));

        if (test_case.expected_err != ERR_OK) {
            return;
        }

        EXPECT_EQ(test_case.expected_index, actual_index);
        EXPECT_EQ(test_case.expected_start_offset, actual_start_offset);
    }
};

TEST_P(ParseLogFileNameTest, ParseLogFileName) { test_parse_log_file_name(); }

const std::vector<parse_log_file_name_case> parse_log_file_name_tests{
    // Empty file name.
    {"", ERR_INVALID_PARAMETERS, 0, 0},

    // Invalid prefix.
    {".", ERR_INVALID_PARAMETERS, 0, 0},
    {"g", ERR_INVALID_PARAMETERS, 0, 0},
    {".g", ERR_INVALID_PARAMETERS, 0, 0},
    {".gol", ERR_INVALID_PARAMETERS, 0, 0},
    {"lo", ERR_INVALID_PARAMETERS, 0, 0},
    {"log", ERR_INVALID_PARAMETERS, 0, 0},
    {"log_", ERR_INVALID_PARAMETERS, 0, 0},
    {"logs.", ERR_INVALID_PARAMETERS, 0, 0},

    // No field.
    {"log.", ERR_INVALID_PARAMETERS, 0, 0},

    // Only one field.
    {"log.0", ERR_INVALID_PARAMETERS, 0, 0},
    {"log_0", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.012", ERR_INVALID_PARAMETERS, 0, 0},
    {"log_012", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.1", ERR_INVALID_PARAMETERS, 0, 0},
    {"log_1", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.123", ERR_INVALID_PARAMETERS, 0, 0},
    {"log_123", ERR_INVALID_PARAMETERS, 0, 0},
    {"log..0", ERR_INVALID_PARAMETERS, 0, 0},
    {"log..1", ERR_INVALID_PARAMETERS, 0, 0},
    {"log..123", ERR_INVALID_PARAMETERS, 0, 0},

    // Empty fields.
    {"log._", ERR_INVALID_PARAMETERS, 0, 0},
    {"log..", ERR_INVALID_PARAMETERS, 0, 0},
    {"log...", ERR_INVALID_PARAMETERS, 0, 0},

    // Invalid splitters.
    {"log.0_0", ERR_INVALID_PARAMETERS, 0, 0},
    {"log_0.0", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.0_1", ERR_INVALID_PARAMETERS, 0, 0},
    {"log_0.1", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.1_0", ERR_INVALID_PARAMETERS, 0, 0},
    {"log_1.0", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.1_1", ERR_INVALID_PARAMETERS, 0, 0},
    {"log_1.1", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.123_456", ERR_INVALID_PARAMETERS, 0, 0},
    {"log_123.456", ERR_INVALID_PARAMETERS, 0, 0},

    // Invalid characters.
    {"log.123.abc", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.123.a12", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.123.1a2", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.123.12a", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.abc.123", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.a12.123", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.1a2.123", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.12a.123", ERR_INVALID_PARAMETERS, 0, 0},

    // Too many fields.
    {"log.123.456.789", ERR_INVALID_PARAMETERS, 0, 0},

    // Numbers that overflow.
    {"log.2147483648.123", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.123.9223372036854775808", ERR_INVALID_PARAMETERS, 0, 0},
    {"log.2147483648.9223372036854775808", ERR_INVALID_PARAMETERS, 0, 0},

    // Valid fields.
    {"log.123.456", ERR_OK, 123, 456},
    {"log.2147483647.123", ERR_OK, 2147483647, 123},
    {"log.123.9223372036854775807", ERR_OK, 123, 9223372036854775807},
};

INSTANTIATE_TEST_SUITE_P(LogFileTest,
                         ParseLogFileNameTest,
                         testing::ValuesIn(parse_log_file_name_tests));

class log_file_test : public replica_test_base
{
public:
    void SetUp() override
    {
        utils::filesystem::remove_path(_log_dir);
        utils::filesystem::create_directory(_log_dir);
        _logf = log_file::create_write(_log_dir.c_str(), 1, _start_offset);
    }

    void TearDown() override
    {
        _logf->close();
        utils::filesystem::remove_path(_log_dir);
    }

protected:
    log_file_ptr _logf;
    size_t _start_offset{10};
};

INSTANTIATE_TEST_SUITE_P(, log_file_test, ::testing::Values(false, true));

TEST_P(log_file_test, commit_log_blocks)
{
    // write one block
    auto appender = std::make_shared<log_appender>(_start_offset);
    for (int i = 0; i < 5; i++) {
        appender->append_mutation(create_test_mutation(1 + i, "test"), nullptr);
    }
    auto tsk = _logf->commit_log_blocks(
        *appender,
        LPC_WRITE_REPLICATION_LOG_PRIVATE,
        nullptr,
        [&](error_code err, size_t sz) {
            ASSERT_EQ(err, ERR_OK);
            ASSERT_EQ(sz, appender->size());
        },
        0);
    tsk->wait();
    ASSERT_EQ(tsk->get_aio_context()->buffer_size, appender->size());
    ASSERT_EQ(tsk->get_aio_context()->file_offset,
              appender->start_offset() - _start_offset); // local offset

    // write multiple blocks
    size_t written_sz = appender->size();
    appender = std::make_shared<log_appender>(_start_offset + written_sz);
    for (int i = 0; i < 1024; i++) { // more than DEFAULT_MAX_BLOCK_BYTES
        appender->append_mutation(create_test_mutation(1 + i, std::string(1024, 'a').c_str()),
                                  nullptr);
    }
    ASSERT_GT(appender->all_blocks().size(), 1);
    tsk = _logf->commit_log_blocks(
        *appender,
        LPC_WRITE_REPLICATION_LOG_PRIVATE,
        nullptr,
        [&](error_code err, size_t sz) {
            ASSERT_EQ(err, ERR_OK);
            ASSERT_EQ(sz, appender->size());
        },
        0);
    tsk->wait();
    ASSERT_EQ(tsk->get_aio_context()->buffer_size, appender->size());
    ASSERT_EQ(tsk->get_aio_context()->file_offset, appender->start_offset() - _start_offset);
}

} // namespace dsn::replication
