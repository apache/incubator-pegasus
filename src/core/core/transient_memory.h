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

#pragma once

#include <dsn/utility/ports.h>
#include <dsn/utility/utils.h>
#include <dsn/utility/blob.h>
#include <dsn/service_api_c.h>

namespace dsn {
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
extern void tls_trans_mem_init(size_t default_per_block_bytes);
extern void tls_trans_mem_alloc(size_t min_size);

extern void tls_trans_mem_next(void **ptr, size_t *sz, size_t min_size);
extern void tls_trans_mem_commit(size_t use_size);

extern blob tls_trans_mem_alloc_blob(size_t sz);

extern void *tls_trans_malloc(size_t sz);
extern void tls_trans_free(void *ptr);
}
