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

#include <cassert>
#include <memory>
#include "utils/transient_memory.h"
#include "utils/utils.h"

namespace dsn {

thread_local tls_transient_memory_t tls_trans_memory;

static size_t tls_trans_mem_default_block_bytes = 1024 * 1024; // 1 MB

// Allocate a block from the system, the block size should be at lease "min_size".
void tls_trans_mem_alloc(size_t min_size)
{
    // Release last buffer if necessary.
    if (tls_trans_memory.magic == 0xdeadbeef) {
        tls_trans_memory.block->reset();
    } else {
        tls_trans_memory.magic = 0xdeadbeef;
        tls_trans_memory.block = new (tls_trans_memory.block_ptr_buffer) std::shared_ptr<char>();
        tls_trans_memory.committed = true;
    }

    tls_trans_memory.remain_bytes =
        (min_size > tls_trans_mem_default_block_bytes ? min_size
                                                      : tls_trans_mem_default_block_bytes);
    *tls_trans_memory.block = ::dsn::utils::make_shared_array<char>(tls_trans_memory.remain_bytes);
    tls_trans_memory.next = tls_trans_memory.block->get();
}

///
/// Api of this moudle start from here.
///
void tls_trans_mem_init(size_t default_per_block_bytes)
{
    tls_trans_mem_default_block_bytes = default_per_block_bytes;
}

void tls_trans_mem_next(void **ptr, size_t *sz, size_t min_size)
{
    if (tls_trans_memory.magic != 0xdeadbeef)
        tls_trans_mem_alloc(min_size);
    else {
        // tls_trans_mem_next and tls_trans_mem_commit must be called in pair.
        assert(tls_trans_memory.committed == true);

        if (min_size > tls_trans_memory.remain_bytes)
            tls_trans_mem_alloc(min_size);
    }

    *ptr = static_cast<void *>(tls_trans_memory.next);
    *sz = tls_trans_memory.remain_bytes;
    tls_trans_memory.committed = false;
}

void tls_trans_mem_commit(size_t use_size)
{
    // Invalid use or parameter of tls_trans_mem_commit.
    assert(tls_trans_memory.magic == 0xdeadbeef && !tls_trans_memory.committed &&
           use_size <= tls_trans_memory.remain_bytes);

    tls_trans_memory.next += use_size;
    tls_trans_memory.remain_bytes -= use_size;
    tls_trans_memory.committed = true;
}

} // namespace dsn
