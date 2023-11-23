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
#include <string>
#include <utility>
#include <vector>

#include "consensus_types.h"
#include "gtest/gtest.h"
#include "replica/log_block.h"
#include "replica/mutation.h"
#include "replica_test_base.h"
#include "utils/autoref_ptr.h"
#include "utils/binary_reader.h"
#include "utils/binary_writer.h"
#include "utils/blob.h"

namespace dsn {
namespace replication {

class log_block_test : public replica_test_base
{
};

INSTANTIATE_TEST_CASE_P(, log_block_test, ::testing::Values(false, true));

TEST_P(log_block_test, constructor)
{
    log_block block(1);
    ASSERT_EQ(block.data().size(), 1);
    ASSERT_EQ(block.size(), 16);
    ASSERT_EQ(block.start_offset(), 1);
}

TEST_P(log_block_test, log_block_header)
{
    log_block block(10);
    auto hdr = (log_block_header *)block.front().data();
    ASSERT_EQ(hdr->magic, 0xdeadbeef);
    ASSERT_EQ(hdr->length, 0);
    ASSERT_EQ(hdr->body_crc, 0);
}

class log_appender_test : public replica_test_base
{
};

INSTANTIATE_TEST_CASE_P(, log_appender_test, ::testing::Values(false, true));

TEST_P(log_appender_test, constructor)
{
    log_block block;
    binary_writer temp_writer;
    temp_writer.write(8);
    block.add(temp_writer.get_buffer());

    log_appender appender(10, block);
    ASSERT_EQ(appender.start_offset(), 10);
    ASSERT_EQ(appender.blob_count(), 2);
    ASSERT_EQ(appender.all_blocks().size(), 1);
    ASSERT_EQ(appender.mutations().size(), 0);
    ASSERT_EQ(appender.callbacks().size(), 0);
}

TEST_P(log_appender_test, append_mutation)
{
    log_appender appender(10);
    for (int i = 0; i < 5; i++) {
        appender.append_mutation(create_test_mutation(1 + i, "test"), nullptr);
    }
    ASSERT_EQ(appender.start_offset(), 10);
    ASSERT_EQ(appender.mutations().size(), 5);

    // each mutation occupies 2 blobs, one for mutation header, one for mutation data.
    ASSERT_EQ(appender.blob_count(), 1 + 5 * 2);
}

TEST_P(log_appender_test, log_block_not_full)
{
    log_appender appender(10);
    for (int i = 0; i < 5; i++) {
        appender.append_mutation(create_test_mutation(1 + i, "test"), nullptr);
    }
    ASSERT_EQ(appender.mutations().size(), 5);
    ASSERT_EQ(appender.blob_count(), 1 + 5 * 2);
    ASSERT_EQ(appender.start_offset(), 10);
    ASSERT_EQ(appender.all_blocks().size(), 1);
    ASSERT_EQ(appender.callbacks().size(), 0);
    ASSERT_EQ(appender.mutations().size(), 5);

    auto block = appender.all_blocks()[0];
    ASSERT_EQ(block.start_offset(), 10);
    ASSERT_EQ(block.data().size(), 1 + 5 * 2);
}

TEST_P(log_appender_test, log_block_full)
{
    log_appender appender(10);
    for (int i = 0; i < 1024; i++) { // more than DEFAULT_MAX_BLOCK_BYTES
        appender.append_mutation(create_test_mutation(1 + i, std::string(1024, 'a')), nullptr);
    }
    ASSERT_EQ(appender.mutations().size(), 1024);
    // two log_block_header blobs
    ASSERT_EQ(appender.blob_count(), 2 + 1024 * 2);
    // the first block's start offset
    ASSERT_EQ(appender.start_offset(), 10);
    // two log_blocks
    ASSERT_EQ(appender.all_blocks().size(), 2);

    size_t sz = 0;
    size_t start_offset = 10;
    for (const log_block &blk : appender.all_blocks()) {
        ASSERT_EQ(start_offset, blk.start_offset());
        sz += blk.size();
        start_offset += blk.size();
    }
    ASSERT_EQ(sz, appender.size());
}

TEST_P(log_appender_test, read_log_block)
{
    log_appender appender(10);
    for (int i = 0; i < 1024; i++) { // more than DEFAULT_MAX_BLOCK_BYTES
        appender.append_mutation(create_test_mutation(1 + i, std::string(1024, 'a')), nullptr);
    }
    ASSERT_EQ(appender.all_blocks().size(), 2);

    // merge into an continuous buffer, which may contains multiple blocks
    std::string buffer;
    for (const auto &block : appender.all_blocks()) {
        for (const blob &bb : block.data()) {
            buffer += bb.to_string();
        }
    }
    ASSERT_EQ(buffer.size(), appender.size());

    // read from buffer
    auto bb = blob::create_from_bytes(std::move(buffer));
    binary_reader reader(bb);
    int block_idx = 0;
    int mutation_idx = 0;
    while (!reader.is_eof()) {
        blob tmp_bb;

        ASSERT_GT(appender.all_blocks().size(), block_idx);
        ASSERT_GE(reader.get_remaining_size(), sizeof(log_block_header));
        reader.read(tmp_bb, sizeof(log_block_header));

        const auto &expected_block = appender.all_blocks()[block_idx];
        size_t blk_len = expected_block.size() - sizeof(log_block_header);
        ASSERT_GE(reader.get_remaining_size(), blk_len);
        blob blk_bb;
        reader.read(blk_bb, blk_len);
        binary_reader blk_reader(blk_bb); // reads the log block
        while (!blk_reader.is_eof()) {
            size_t read_len = blk_len - blk_reader.get_remaining_size();
            mutation_ptr mu = mutation::read_from(blk_reader, nullptr);
            ASSERT_EQ(mu->data.header.log_offset,
                      read_len + expected_block.start_offset() + sizeof(log_block_header));
            mutation_idx++;
        }

        block_idx++;
    }
    ASSERT_EQ(block_idx, appender.all_blocks().size());
    ASSERT_EQ(mutation_idx, 1024);
}

} // namespace replication
} // namespace dsn
