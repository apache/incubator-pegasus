/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <gtest/gtest.h>

#include "utils/transient_memory.h"

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
