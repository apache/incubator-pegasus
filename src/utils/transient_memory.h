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

#include "utils/blob.h"
#include "utils/ports.h"

namespace dsn {

/// transient memory is mainly used for allocating & freeing memory in block,
/// so as to avoid the memory fragmentation problems when server is running for a long time
///
/// the key design is allocating a block with "malloc" each time, and keep it in
/// the structure "tls_trans_memory". memory allocation of other modules should try to
/// get memory from the pre-allocated block in "tls_trans_memory":
///
/// |--------------- pre-allocated block --------------------| <- tls_trans_memory.block
/// |--memory piece 1--|--memory piece 2--|--memory piece 3--|
///
/// the tls_trans_memory->block is a shared_ptr pointing to the pre-allocated block,
/// for each memory piece allocated from the block, there is also a shared-ptr pointing the whole
/// block. please refer to @tls_trans_mem_next for details
///
/// so the pre-allocated block will be free until all the shared-ptrs are destructed.
/// tls_trans_memory.block will release an old memory_block if the remaining size is
/// too small, and other shared-ptr will release the block when "tls_trans_free" calls

typedef struct tls_transient_memory_t
{
    ~tls_transient_memory_t()
    {
        if (nullptr != block) {
            *block = nullptr;
        }
    }

    unsigned int magic;
    size_t remain_bytes;
    char block_ptr_buffer[sizeof(std::shared_ptr<char>)];
    std::shared_ptr<char> *block;
    char *next;
    bool committed;
} tls_transient_memory_t;

extern thread_local tls_transient_memory_t tls_trans_memory;

// Initialize the default block size, should call this at the beginning of the process.
void tls_trans_mem_init(size_t default_per_block_bytes);

// Acquire a memory piece from current block.
// "min_size" is the minimal size of the memory piece user needs.
// "*sz" stores the actual size the allocator gives, which is no less than min_size.
// *ptr stores the starting position of the memory piece.
// both ptr & sz shouldn't be null.
//
// "tls_trans_mem_next" should be used together with "tls_trans_mem_commit".
void tls_trans_mem_next(void **ptr, size_t *sz, size_t min_size);
void tls_trans_mem_commit(size_t use_size);

} // namespace dsn
