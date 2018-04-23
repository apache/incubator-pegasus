/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Microsoft Corporation
 *
 * -=- Robust Distributed System Nucleus (rDSN) -=-
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

#pragma once

#include <dsn/utility/ports.h>
#include <dsn/utility/blob.h>

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
    unsigned int magic;
    size_t remain_bytes;
    char block_ptr_buffer[sizeof(std::shared_ptr<char>)];
    std::shared_ptr<char> *block;
    char *next;
    bool committed;
} tls_transient_memory_t;

extern __thread tls_transient_memory_t tls_trans_memory;

// initialize the default block size, should call this at the beginning of the process
void tls_trans_mem_init(size_t default_per_block_bytes);

// acquire a memory piece from current block
// "min_size" is the minimal size of the memory piece user needs,
// "*sz" stores the actual size the allocator gives, which is no less than min_size
// *ptr stores the starting position of the memory piece
// both ptr & sz shouldn't be null
//
// "tls_trans_mem_next" should be used together with "tls_trans_mem_commit"
void tls_trans_mem_next(void **ptr, size_t *sz, size_t min_size);
void tls_trans_mem_commit(size_t use_size);

// allocate a blob, the size is "sz"
blob tls_trans_mem_alloc_blob(size_t sz);

// allocate memory
void *tls_trans_malloc(size_t sz);

// free memory, ptr shouldn't be null
void tls_trans_free(void *ptr);
}
