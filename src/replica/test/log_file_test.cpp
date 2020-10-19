// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#include <gtest/gtest.h>

#include "replica_test_base.h"

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

TEST_F(log_file_test, commit_log_blocks)
{
    // write one block
    auto appender = std::make_shared<log_appender>(_start_offset);
    for (int i = 0; i < 5; i++) {
        appender->append_mutation(create_test_mutation(1 + i, "test"), nullptr);
    }
    auto tsk = _logf->commit_log_blocks(*appender,
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
        appender->append_mutation(create_test_mutation(1 + i, std::string(1024, 'a')), nullptr);
    }
    ASSERT_GT(appender->all_blocks().size(), 1);
    tsk = _logf->commit_log_blocks(*appender,
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
