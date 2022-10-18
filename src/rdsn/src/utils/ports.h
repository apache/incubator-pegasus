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

#if defined(__linux__) || defined(__APPLE__)

#include <unistd.h>

#define __selectany __attribute__((weak)) extern

#ifndef O_BINARY
#define O_BINARY 0
#endif

#else

#error "unsupported platform"
#endif

// common macros and data structures
#ifndef FIELD_OFFSET
#define FIELD_OFFSET(s, field) (((size_t) & ((s *)(10))->field) - 10)
#endif

#ifndef CONTAINING_RECORD
#define CONTAINING_RECORD(address, type, field)                                                    \
    ((type *)((char *)(address)-FIELD_OFFSET(type, field)))
#endif

#ifndef MAX_COMPUTERNAME_LENGTH
#define MAX_COMPUTERNAME_LENGTH 32
#endif

#ifndef ARRAYSIZE
#define ARRAYSIZE(a) ((sizeof(a) / sizeof(*(a))) / static_cast<size_t>(!(sizeof(a) % sizeof(*(a)))))
#endif

#define snprintf_p std::snprintf
#define dsn_likely(pred) (__builtin_expect((pred), 1))
#define dsn_unlikely(pred) (__builtin_expect((pred), 0))

#define DISALLOW_COPY_AND_ASSIGN(TypeName)                                                         \
    TypeName(const TypeName &) = delete;                                                           \
    void operator=(const TypeName &) = delete

#if defined OS_LINUX || defined OS_CYGWIN

// _BIG_ENDIAN
#include <endian.h>

#elif defined __APPLE__

// BIG_ENDIAN
#include <machine/endian.h> // NOLINT(build/include)

#endif

// Cache line alignment
#if defined(__i386__) || defined(__x86_64__)
#define CACHELINE_SIZE 64
#elif defined(__powerpc64__)
// TODO(user) This is the L1 D-cache line size of our Power7 machines.
// Need to check if this is appropriate for other PowerPC64 systems.
#define CACHELINE_SIZE 128
#elif defined(__aarch64__)
#define CACHELINE_SIZE 64
#elif defined(__arm__)
// Cache line sizes for ARM: These values are not strictly correct since
// cache line sizes depend on implementations, not architectures.  There
// are even implementations with cache line sizes configurable at boot
// time.
#if defined(__ARM_ARCH_5T__)
#define CACHELINE_SIZE 32
#elif defined(__ARM_ARCH_7A__)
#define CACHELINE_SIZE 64
#endif
#endif

// This is a NOP if CACHELINE_SIZE is not defined.
#ifdef CACHELINE_SIZE
static_assert((CACHELINE_SIZE & (CACHELINE_SIZE - 1)) == 0 &&
                  (CACHELINE_SIZE & (sizeof(void *) - 1)) == 0,
              "CACHELINE_SIZE must be a power of 2 and a multiple of sizeof(void *)");
#define CACHELINE_ALIGNED __attribute__((aligned(CACHELINE_SIZE)))
#else
#define CACHELINE_ALIGNED
#endif
