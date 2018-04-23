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

#include <dsn/utility/transient_memory.h>
#include <gtest/gtest.h>

using namespace ::dsn;

namespace dsn {
extern void tls_trans_mem_alloc(size_t min_size);
}

TEST(core, transient_memory)
{
    tls_trans_mem_init(1024);

    void *ptr;
    size_t sz;
    tls_trans_mem_next(&ptr, &sz, 100);
    tls_trans_mem_commit(100);
    tls_trans_mem_next(&ptr, &sz, 10240);
    tls_trans_mem_commit(100);

    // malloc 10240
    tls_trans_mem_alloc(10240);
    ASSERT_EQ(0xdeadbeef, tls_trans_memory.magic);
    ASSERT_EQ(10240u, tls_trans_memory.remain_bytes);
    ASSERT_EQ((void *)tls_trans_memory.block_ptr_buffer, (void *)tls_trans_memory.block);
    ASSERT_EQ((void *)tls_trans_memory.next, (void *)tls_trans_memory.block->get());
    ASSERT_TRUE(tls_trans_memory.committed);

    // malloc 100
    tls_trans_mem_alloc(100);
    ASSERT_EQ(1024u, tls_trans_memory.remain_bytes);
    ASSERT_TRUE(tls_trans_memory.committed);

    // acquire 100
    tls_trans_mem_next(&ptr, &sz, 100);
    ASSERT_EQ((void *)tls_trans_memory.next, (void *)ptr);
    ASSERT_EQ(1024u, sz);
    ASSERT_EQ((void *)tls_trans_memory.next, (void *)tls_trans_memory.block->get());
    ASSERT_EQ(1024u, tls_trans_memory.remain_bytes);
    ASSERT_FALSE(tls_trans_memory.committed);

    // commit 100
    tls_trans_mem_commit(100);
    ASSERT_EQ((void *)tls_trans_memory.next, (void *)(tls_trans_memory.block->get() + 100));
    ASSERT_EQ(924u, tls_trans_memory.remain_bytes);
    ASSERT_TRUE(tls_trans_memory.committed);

    // acquire 200
    tls_trans_mem_next(&ptr, &sz, 200);
    ASSERT_EQ((void *)tls_trans_memory.next, (void *)ptr);
    ASSERT_EQ(924u, sz);
    ASSERT_EQ((void *)tls_trans_memory.next, (void *)(tls_trans_memory.block->get() + 100));
    ASSERT_EQ(924u, tls_trans_memory.remain_bytes);
    ASSERT_FALSE(tls_trans_memory.committed);

    // commit 300
    tls_trans_mem_commit(300);
    ASSERT_EQ((void *)tls_trans_memory.next, (void *)(tls_trans_memory.block->get() + 400));
    ASSERT_EQ(624u, tls_trans_memory.remain_bytes);
    ASSERT_TRUE(tls_trans_memory.committed);

    // acquire 10240
    tls_trans_mem_next(&ptr, &sz, 10240);
    ASSERT_EQ((void *)tls_trans_memory.next, (void *)ptr);
    ASSERT_EQ(10240u, sz);
    ASSERT_EQ((void *)tls_trans_memory.next, (void *)tls_trans_memory.block->get());
    ASSERT_EQ(10240u, tls_trans_memory.remain_bytes);
    ASSERT_FALSE(tls_trans_memory.committed);

    // commit 0
    tls_trans_mem_commit(0);
    ASSERT_EQ((void *)tls_trans_memory.next, (void *)(tls_trans_memory.block->get()));
    ASSERT_EQ(10240u, tls_trans_memory.remain_bytes);
    ASSERT_TRUE(tls_trans_memory.committed);

    tls_trans_mem_init(1024 * 1024); // restore
}
