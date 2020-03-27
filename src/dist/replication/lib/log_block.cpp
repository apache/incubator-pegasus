// Copyright (c) 2017-present, Xiaomi, Inc.  All rights reserved.
// This source code is licensed under the Apache License Version 2.0, which
// can be found in the LICENSE file in the root directory of this source tree.

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

void log_block::append_mutation(const mutation_ptr &mu, const aio_task_ptr &cb)
{
    _mutations.push_back(mu);
    if (cb) {
        _callbacks.push_back(cb);
    }
    mu->data.header.log_offset = _start_offset + size();
    mu->write_to([this](const blob &bb) { add(bb); });
}

} // namespace replication
} // namespace dsn
