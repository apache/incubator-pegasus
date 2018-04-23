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

/*
 * Description:
 *     What is this file about?
 *
 * Revision history:
 *     xxxx-xx-xx, author, first version
 *     xxxx-xx-xx, author, fix bug about xxx
 */

#include <cassert>
#include <memory>
#include <dsn/utility/utils.h>
#include <dsn/utility/transient_memory.h>

namespace dsn {

__thread tls_transient_memory_t tls_trans_memory;

static size_t tls_trans_mem_default_block_bytes = 1024 * 1024; // 1 MB

// allocate a block from the system, the block size should be at lease "min_size"
void tls_trans_mem_alloc(size_t min_size)
{
    // release last buffer if necessary
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
/// api of this moudle start from here
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
        // tls_trans_mem_next and tls_trans_mem_commit must be called in pair
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
    // invalid use or parameter of tls_trans_mem_commit
    assert(tls_trans_memory.magic == 0xdeadbeef && !tls_trans_memory.committed &&
           use_size <= tls_trans_memory.remain_bytes);

    tls_trans_memory.next += use_size;
    tls_trans_memory.remain_bytes -= use_size;
    tls_trans_memory.committed = true;
}

blob tls_trans_mem_alloc_blob(size_t sz)
{
    void *ptr;
    size_t sz2;
    tls_trans_mem_next(&ptr, &sz2, sz);

    ::dsn::blob buffer((*::dsn::tls_trans_memory.block),
                       (int)((char *)(ptr) - ::dsn::tls_trans_memory.block->get()),
                       (int)sz);

    tls_trans_mem_commit(sz);
    return buffer;
}

void *tls_trans_malloc(size_t sz)
{
    sz += sizeof(std::shared_ptr<char>) + sizeof(uint32_t);
    void *ptr;
    size_t sz2;
    tls_trans_mem_next(&ptr, &sz2, sz);

    // add ref
    new (ptr) std::shared_ptr<char>(*::dsn::tls_trans_memory.block);

    // add magic
    *(uint32_t *)((char *)(ptr) + sizeof(std::shared_ptr<char>)) = 0xdeadbeef;

    tls_trans_mem_commit(sz);

    return (void *)((char *)(ptr) + sizeof(std::shared_ptr<char>) + sizeof(uint32_t));
}

void tls_trans_free(void *ptr)
{
    ptr = (void *)((char *)ptr - sizeof(uint32_t));
    // invalid transient memory block
    assert(*(uint32_t *)(ptr) == 0xdeadbeef);

    ptr = (void *)((char *)ptr - sizeof(std::shared_ptr<char>));
    ((std::shared_ptr<char> *)(ptr))->~shared_ptr<char>();
}
}
