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
# include "transient_memory.h"

namespace dsn 
{
    __thread tls_transient_memory_t tls_trans_memory;

    static size_t tls_trans_mem_default_block_bytes = 1024 * 1024; // 1 MB

    void tls_trans_mem_init(size_t default_per_block_bytes)
    {
        tls_trans_mem_default_block_bytes = default_per_block_bytes;
    }

    void tls_trans_mem_alloc(size_t min_size)
    {
        // release last buffer if necessary
        if (tls_trans_memory.magic == 0xdeadbeef)
        {
            tls_trans_memory.block->reset((char*)0);
        }
        else
        {
            tls_trans_memory.magic = 0xdeadbeef;
            tls_trans_memory.block = new(tls_trans_memory.block_ptr_buffer) std::shared_ptr<char>();
            tls_trans_memory.committed = true;
        }

        tls_trans_memory.remain_bytes = (min_size > tls_trans_mem_default_block_bytes ? 
                min_size : tls_trans_mem_default_block_bytes);
        tls_trans_memory.block->reset((char*)::malloc(tls_trans_memory.remain_bytes));
        tls_trans_memory.next = tls_trans_memory.block->get();
    }
}