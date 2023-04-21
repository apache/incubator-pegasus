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

#include "utils/long_adder.h"

#ifdef __aarch64__
#define _mm_free(p) free(p)
#define _mm_malloc(a, b) malloc(a)
#else
#include <mm_malloc.h>
#endif //__aarch64__

#include <fmt/format.h>
#include <unistd.h>
#include <cstdlib>
#include <new>

#include "utils/fmt_logging.h"
#include "utils/process_utils.h"
#include "utils/rand.h"
#include "utils/safe_strerror_posix.h"

namespace dsn {

namespace {

const uint32_t kNumCpus = sysconf(_SC_NPROCESSORS_ONLN);
uint32_t compute_num_cells()
{
    uint32_t n = 1;
    // Calculate the size. Nearest power of two >= NCPU.
    // Also handle a negative NCPU, can happen if sysconf name is unknown
    while (kNumCpus > n) {
        n <<= 1;
    }
    return n;
}
const uint32_t kNumCells = compute_num_cells();
const uint32_t kCellMask = kNumCells - 1;

cacheline_aligned_int64 *const kCellsLocked = reinterpret_cast<cacheline_aligned_int64 *>(-1L);

} // anonymous namespace

//
// cacheline_aligned_int64
//

/* extern */ cacheline_aligned_int64_ptr new_cacheline_aligned_int64_array(uint32_t size)
{
    void *buffer = nullptr;
    int err = posix_memalign(&buffer, CACHELINE_SIZE, sizeof(cacheline_aligned_int64) * size);

    // Generally there are 2 possible errors for posix_memalign as below:
    // [EINVAL]
    //     The value of the alignment parameter is not a power of two multiple of sizeof(void *).
    // [ENOMEM]
    //     There is insufficient memory available with the requested alignment.
    // Thus making an assertion here is enough.
    CHECK_EQ_MSG(err, 0, "error calling posix_memalign: {}", utils::safe_strerror(err));

    cacheline_aligned_int64 *array = new (buffer) cacheline_aligned_int64[size];
    for (uint32_t i = 0; i < size; ++i) {
        cacheline_aligned_int64 *elem = &(array[i]);
        CHECK_EQ_MSG(
            (reinterpret_cast<const uintptr_t>(elem) & (sizeof(cacheline_aligned_int64) - 1)),
            0,
            "unaligned cacheline_aligned_int64: array={}, index={}, elem={}, mask={}",
            fmt::ptr(array),
            i,
            fmt::ptr(elem),
            sizeof(cacheline_aligned_int64) - 1);
        array[i]._value.store(0);
    }

    return cacheline_aligned_int64_ptr(array, [](cacheline_aligned_int64 *array) { free(array); });
}

/* extern */ cacheline_aligned_int64_ptr new_cacheline_aligned_int64()
{
    return new_cacheline_aligned_int64_array(1);
}

//
// striped64
//
__thread uint64_t striped64::_tls_hashcode = 0;

uint64_t striped64::get_tls_hashcode()
{
    if (dsn_unlikely(_tls_hashcode == 0)) {
        const uint64_t tid = static_cast<uint64_t>(utils::get_current_tid());
        // Avoid zero to allow xorShift rehash, and because 0 indicates an unset
        // hashcode above.
        const uint64_t hash = (tid == 0) ? rand::next_u64() : tid;
        _tls_hashcode = (hash == 0) ? 1 : hash;
    }
    return _tls_hashcode;
}

template <class Updater>
void striped64::retry_update(rehash to_rehash, Updater updater)
{
    uint64_t h = get_tls_hashcode();
    // There are three operations in this loop.
    //
    // 1. Try to add to the cacheline_aligned_int64 hash table entry for the thread if the table
    // exists. When there's contention, rehash to try a different cacheline_aligned_int64.
    // 2. Try to initialize the hash table.
    // 3. Try to update the base counter.
    //
    // These are predicated on successful CAS operations, which is why it's all wrapped in an
    // infinite retry loop.
    while (true) {
        cacheline_aligned_int64 *cells = _cells.load(std::memory_order_acquire);
        if (cells != nullptr && cells != kCellsLocked) {
            if (to_rehash == kRehash) {
                // CAS failed already, rehash before trying to increment.
                to_rehash = kNoRehash;
            } else {
                cacheline_aligned_int64 *cell = &(_cells[h & kCellMask]);
                int64_t v = cell->_value.load(std::memory_order_relaxed);
                if (cell->compare_and_set(v, updater(v))) {
                    // Successfully CAS'd the corresponding cell, done.
                    break;
                }
            }
            // Rehash since we failed to CAS, either previously or just now.
            h ^= h << 13;
            h ^= h >> 17;
            h ^= h << 5;
        } else if (cells == nullptr && _cells.compare_exchange_weak(cells, kCellsLocked)) {
            _cells_holder = new_cacheline_aligned_int64_array(kNumCells);
            cells = _cells_holder.get();
            _cells.store(cells, std::memory_order_release);
        } else {
            // Fallback to adding to the base value.
            // Means the table wasn't initialized or we failed to init it.
            int64_t v = _base.load(std::memory_order_relaxed);
            if (cas_base(v, updater(v))) {
                break;
            }
        }
    }

    // Record index for next time
    _tls_hashcode = h;
}

void striped64::internal_reset(int64_t initial_value)
{
    _base.store(initial_value);

    cacheline_aligned_int64 *c;
    do {
        c = _cells.load(std::memory_order_acquire);
    } while (c == kCellsLocked);

    if (c != nullptr) {
        for (uint32_t i = 0; i < kNumCells; ++i) {
            c[i]._value.store(0);
        }
    }
}

void striped_long_adder::increment_by(int64_t x)
{
    // Use hash table if present. If that fails, call retry_update to rehash and retry.
    // If no hash table, try to CAS the base counter. If that fails, retry_update to init the table.
    cacheline_aligned_int64 *cells = _cells.load(std::memory_order_acquire);
    if (cells != nullptr && cells != kCellsLocked) {
        cacheline_aligned_int64 *cell = &(cells[get_tls_hashcode() & kCellMask]);
        CHECK_EQ_MSG(
            (reinterpret_cast<const uintptr_t>(cell) & (sizeof(cacheline_aligned_int64) - 1)),
            0,
            "unaligned cacheline_aligned_int64 not allowed for striped64: cell={}, mask={}",
            fmt::ptr(cell),
            sizeof(cacheline_aligned_int64) - 1);

        const int64_t old = cell->_value.load(std::memory_order_relaxed);
        if (!cell->compare_and_set(old, old + x)) {
            // When we hit a hash table contention, signal retry_update to rehash.
            retry_update(kRehash, [x](int64_t old) { return old + x; });
        }
    } else {
        int64_t b = _base.load(std::memory_order_relaxed);
        if (!cas_base(b, b + x)) {
            // Attempt to initialize the table. No need to rehash since the contention was for the
            // base counter, not the hash table.
            retry_update(kNoRehash, [x](int64_t old) { return old + x; });
        }
    }
}

//
// striped_long_adder
//

int64_t striped_long_adder::value() const
{
    int64_t sum = _base.load(std::memory_order_relaxed);

    cacheline_aligned_int64 *c = _cells.load(std::memory_order_acquire);
    if (c != nullptr && c != kCellsLocked) {
        for (uint32_t i = 0; i < kNumCells; ++i) {
            sum += c[i]._value.load(std::memory_order_relaxed);
        }
    }
    return sum;
}

int64_t striped_long_adder::fetch_and_reset()
{
    int64_t sum = _base.exchange(0, std::memory_order_relaxed);

    cacheline_aligned_int64 *c = _cells.load(std::memory_order_acquire);
    if (c != nullptr && c != kCellsLocked) {
        for (uint32_t i = 0; i < kNumCells; ++i) {
            sum += c[i]._value.exchange(0, std::memory_order_relaxed);
        }
    }
    return sum;
}

//
// concurrent_long_adder
//

concurrent_long_adder::concurrent_long_adder()
    : _cells_holder(new_cacheline_aligned_int64_array(kNumCells)), _cells(_cells_holder.get())
{
}

void concurrent_long_adder::increment_by(int64_t x)
{
    auto task_id = static_cast<uint32_t>(utils::get_current_tid());
    _cells[task_id & kCellMask]._value.fetch_add(x, std::memory_order_relaxed);
}

int64_t concurrent_long_adder::value() const
{
    int64_t sum = 0;
    for (uint32_t i = 0; i < kNumCells; ++i) {
        sum += _cells[i]._value.load(std::memory_order_relaxed);
    }
    return sum;
}

void concurrent_long_adder::set(int64_t val)
{
    for (uint32_t i = 0; i < kNumCells; ++i) {
        _cells[i]._value.store(0, std::memory_order_relaxed);
    }
    _cells[0]._value.store(val, std::memory_order_relaxed);
}

int64_t concurrent_long_adder::fetch_and_reset()
{
    int64_t sum = 0;
    for (uint32_t i = 0; i < kNumCells; ++i) {
        sum += _cells[i]._value.exchange(0, std::memory_order_relaxed);
    }
    return sum;
}

} // namespace dsn
