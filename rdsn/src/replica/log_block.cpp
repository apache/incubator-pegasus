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

#include "log_block.h"

namespace dsn {
namespace replication {

log_block::log_block(int64_t start_offset) : _start_offset(start_offset) { init(); }

log_block::log_block() { init(); }

void log_block::init()
{
    log_block_header hdr;

    binary_writer temp_writer;
    temp_writer.write_pod(hdr);
    add(temp_writer.get_buffer());
}

void log_appender::append_mutation(const mutation_ptr &mu, const aio_task_ptr &cb)
{
    _mutations.push_back(mu);
    if (cb) {
        _callbacks.push_back(cb);
    }
    log_block *blk = &_blocks.back();
    if (blk->size() > DEFAULT_MAX_BLOCK_BYTES) {
        _full_blocks_size += blk->size();
        _full_blocks_blob_cnt += blk->data().size();
        int64_t new_block_start_offset = blk->start_offset() + blk->size();
        _blocks.emplace_back(new_block_start_offset);
        blk = &_blocks.back();
    }
    mu->data.header.log_offset = blk->start_offset() + blk->size();
    mu->write_to([blk](const blob &bb) { blk->add(bb); });
}

} // namespace replication
} // namespace dsn
