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

#include <algorithm>
#include <functional>
#include <memory>
#include <new>

#include "utils/api_utilities.h"
#include "utils/fmt_logging.h"
#include "utils/ports.h"

namespace dsn {

#ifdef CACHELINE_SIZE

extern void *cacheline_aligned_alloc(size_t size);

extern void cacheline_aligned_free(void *mem_block);

template <typename T>
using cacheline_aligned_ptr = typename std::unique_ptr<T, std::function<void(void *)>>;

template <typename T>
cacheline_aligned_ptr<T> cacheline_aligned_alloc_array(size_t len)
{
    void *buffer = cacheline_aligned_alloc(sizeof(T) * len);
    if (dsn_unlikely(buffer == nullptr)) {
        return cacheline_aligned_ptr<T>(nullptr, [](void *) {});
    }

    T *array = new (buffer) T[len];

#ifndef NDEBUG
    if (sizeof(T) <= CACHELINE_SIZE && (sizeof(T) & (sizeof(T) - 1)) == 0) {
        for (size_t i = 0; i < len; ++i) {
            T *elem = &(array[i]);
            CHECK_EQ_MSG((reinterpret_cast<const uintptr_t>(elem) & (sizeof(T) - 1)),
                         0,
                         "unaligned array element for cache line: array={}, length={}, index={}, "
                         "elem={}, elem_size={}, mask={}, cacheline_size={}",
                         fmt::ptr(array),
                         len,
                         i,
                         fmt::ptr(elem),
                         sizeof(T),
                         sizeof(T) - 1,
                         CACHELINE_SIZE);
        }
    }
#endif

    return cacheline_aligned_ptr<T>(array, cacheline_aligned_free);
}

template <typename T>
cacheline_aligned_ptr<T> cacheline_aligned_alloc_array(size_t len, const T &val)
{
    auto array = cacheline_aligned_alloc_array<T>(len);
    if (array) {
        std::fill(array.get(), array.get() + len, val);
    }

    return array;
}

#endif

} // namespace dsn
