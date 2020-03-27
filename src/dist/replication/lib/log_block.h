// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

#pragma once

#include "mutation.h"

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
    std::vector<mutation_ptr> _mutations;
    std::vector<aio_task_ptr> _callbacks;
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
        dassert(!_data.empty(), "trying to get first blob out of an empty log block");
        return _data.front();
    }

    // add a blob into the block
    void add(const blob &bb)
    {
        _size += bb.length();
        _data.push_back(bb);
    }

    void append_mutation(const mutation_ptr &mu, const aio_task_ptr &cb);

    const std::vector<mutation_ptr> &mutations() const { return _mutations; }

    const std::vector<aio_task_ptr> &callbacks() const { return _callbacks; }

    // return total data size in the block
    size_t size() const { return _size; }

    // global offset to start writting this block
    int64_t start_offset() const { return _start_offset; }

private:
    void init();
};

} // namespace replication
} // namespace dsn
