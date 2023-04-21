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

#pragma once

#include <stddef.h>
#include <stdint.h>
#include <algorithm>
#include <utility>
#include <vector>

#include "aio/aio_task.h"
#include "mutation.h"
#include "utils/blob.h"
#include "utils/fmt_logging.h"

namespace dsn {
namespace replication {

// each block in log file has a log_block_header
struct log_block_header
{
    int32_t magic{static_cast<int32_t>(0xdeadbeef)}; // 0xdeadbeef
    int32_t length{0};   // block data length (not including log_block_header)
    int32_t body_crc{0}; // block data crc (not including log_block_header)

    // start offset of the block (including log_block_header) in this log file
    // TODO(wutao1): this field is unusable. the value is always set, but not read.
    uint32_t local_offset{0};
};

// a memory structure holding data which belongs to one block.
class log_block
{
    std::vector<blob> _data; // the first blob is log_block_header
    size_t _size{0};         // total data size of all blobs
    int64_t _start_offset{0};

public:
    log_block();

    explicit log_block(int64_t start_offset);

    // get all blobs in the block
    const std::vector<blob> &data() const { return _data; }

    // get the first blob (which contains the log_block_header) from the block
    //
    // TODO(wutao1): refactor `front()` to `get_log_block_header()`
    // ```
    //   log_block_header *get_log_block_header()
    //   {
    //       return reinterpret_cast<log_block_header *>(const_cast<char *>(_data.front().data()));
    //   }
    // ```
    blob &front()
    {
        CHECK(!_data.empty(), "trying to get first blob out of an empty log block");
        return _data.front();
    }

    // add a blob into the block
    void add(const blob &bb)
    {
        _size += bb.length();
        _data.push_back(bb);
    }

    // return total data size in the block
    size_t size() const { return _size; }

    // global offset to start writting this block
    int64_t start_offset() const { return _start_offset; }

private:
    friend class log_appender;
    void init();
};

// Append writes into a buffer which consists of one or more fixed-size log blocks,
// which will be continuously flushed into one log file.
// Not thread-safe. Requires lock protection.
class log_appender
{
public:
    explicit log_appender(int64_t start_offset) { _blocks.emplace_back(start_offset); }

    log_appender(int64_t start_offset, log_block &block)
    {
        block._start_offset = start_offset;
        _blocks.emplace_back(std::move(block));
    }

    void append_mutation(const mutation_ptr &mu, const aio_task_ptr &cb);

    size_t size() const { return _full_blocks_size + _blocks.crbegin()->size(); }
    size_t blob_count() const { return _full_blocks_blob_cnt + _blocks.crbegin()->data().size(); }

    std::vector<mutation_ptr> mutations() const { return _mutations; }

    // The callback registered for each write.
    const std::vector<aio_task_ptr> &callbacks() const { return _callbacks; }

    // Returns the heading block's start_offset.
    int64_t start_offset() const { return _blocks.cbegin()->start_offset(); }

    std::vector<log_block> &all_blocks() { return _blocks; }

protected:
    static constexpr size_t DEFAULT_MAX_BLOCK_BYTES = 1 * 1024 * 1024; // 1MB

    // |---------------------- _blocks ----------------------|
    // | full block 0 | full block 1 | .... | unfilled block |

    // New block is appended to tail.
    // The tailing block is the only block that may be unfilled.
    std::vector<log_block> _blocks;
    size_t _full_blocks_size{0};
    size_t _full_blocks_blob_cnt{0};
    std::vector<aio_task_ptr> _callbacks;
    std::vector<mutation_ptr> _mutations;
};

} // namespace replication
} // namespace dsn
