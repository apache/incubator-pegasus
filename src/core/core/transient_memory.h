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

# include <dsn/ports.h>
# include <dsn/cpp/utils.h>
# include <dsn/service_api_c.h>

namespace dsn 
{
    typedef struct tls_transient_memory_t
    {
        int                   magic;
        size_t                remain_bytes;
        char                  block_ptr_buffer[sizeof(std::shared_ptr<char>)];
        std::shared_ptr<char> *block;
        char*                 next;
        bool                  committed;
    } tls_transient_memory_t;

    extern __thread tls_transient_memory_t tls_trans_memory;
    extern void tls_trans_mem_init(size_t default_per_block_bytes);
    extern void tls_trans_mem_alloc(size_t min_size);

    inline void tls_trans_mem_next(void** ptr, size_t* sz, size_t min_size)
    {
        if (tls_trans_memory.magic != 0xdeadbeef)
            tls_trans_mem_alloc(min_size);
        else
        {
            dassert(tls_trans_memory.committed == true, 
                "tls_trans_mem_next and tls_trans_mem_commit must be called in pair");

            if (min_size > tls_trans_memory.remain_bytes)
                tls_trans_mem_alloc(min_size);
        }

        *ptr = (void*)tls_trans_memory.next;
        *sz = tls_trans_memory.remain_bytes;
        tls_trans_memory.committed = false;
    }

    inline void tls_trans_mem_commit(size_t use_size)
    {
        dbg_dassert(tls_trans_memory.magic == 0xdeadbeef
            && !tls_trans_memory.committed
            && use_size <= tls_trans_memory.remain_bytes,
            "invalid use or parameter of tls_trans_mem_commit");

        tls_trans_memory.next += use_size;
        tls_trans_memory.remain_bytes -= use_size;
        tls_trans_memory.committed = true;
    }
}