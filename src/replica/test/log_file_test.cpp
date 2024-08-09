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

#include <stddef.h>
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

namespace dsn {
namespace replication {

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

} // namespace replication
} // namespace dsn
